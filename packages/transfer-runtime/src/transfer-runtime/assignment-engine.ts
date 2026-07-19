import { randomInt, randomUUID } from "node:crypto";
import { performance } from "node:perf_hooks";
import { connectedHotkeys, orchestratorIsEligible, orchestratorUnavailableReason } from "./orchestrator-push-hooks.js";
import { logger as baseLogger } from "../logger.js";
import {
	isSignedUrlTransferVersion,
	parseTransferMetadata,
	type TransferMetadata,
} from "../db/transfer-metadata.js";
import { env } from "../config.js";
import type { WorkerGatewayClient } from "./worker-gateway-client.js";
import type { TransferRuntimeRegistry } from "./transfer-runtime-registry.js";
import type { OrchestratorRoutingRegistry } from "./orchestrator-routing-registry.js";
import type {
	AssignmentSelectionRule,
	OrchestratorCandidate,
	SelectionResult,
} from "./assignment-engine-types.js";
export type { OrchestratorCandidate, SelectionResult } from "./assignment-engine-types.js";
export { setOrchestratorPushHooks, resetOrchestratorPushHooks, orchestratorPush } from "./orchestrator-push-hooks.js";
export {
	numericValue,
	allocateChunkSlices,
} from "./assignment-slice-math.js";
export type { QualifiedAssignmentPlan, QualifiedPoolSlicePlan } from "./qualified-pool-slices.js";
import {
	numericValue,
	allocateChunkSlices,
} from "./assignment-slice-math.js";
import {
	buildQualifiedPoolSlicePlan,
	countDistinctOwnerGroupsInPool,
	type QualifiedAssignmentPlan,
} from "./qualified-pool-slices.js";
import { deliverTaskOfferBatch } from "./transfer-task-offers.js";
import { sanitizeStorageDiagnostic } from "./storage-diagnostic-sanitizer.js";
import {
	TransferAssignmentLedger,
	type AssignmentLedgerDiagnostics,
} from "./transfer-assignment-ledger.js";

import {
	beginDistributePhase,
	recordDispatchQueueAge,
	recordResendPush,
	recordControlDeliveryResult,
} from "./distribute-phase-metrics.js";

const logger = baseLogger.child({ component: "assignment-engine" });


export interface AssignmentEngineDeps {
	gatewayClient: WorkerGatewayClient;
	offerRegistry: TransferRuntimeRegistry;
	routingRegistry: OrchestratorRoutingRegistry;
}


interface AssignmentLogicSummary {
	transferClass: "test" | "production";
	poolDecision: string;
	rankingDecision: string;
	candidateScreening: string;
	distributionDecision: string;
	qualifiedSelectionNarrative?: string;
}

interface AssignmentCoverageResult {
	batchIds: string[];
	assignedChunkCount: number;
	batchSlices: Array<ReturnType<typeof summarizeTaskOfferBatchSlice>>;
	pendingPushes: PendingTaskOfferBatchPush[];
	coveredAllChunks: boolean;
}

interface PendingTaskOfferBatchPush {
	batchId: string;
	orchestrator: OrchestratorCandidate;
	chunkIndices: number[];
	chunkStart: number;
	chunkEnd: number;
	totalChunks: number;
	chunkSize: number;
}

export interface AssignmentDispatchDiagnostics {
	laneCount: number;
	queued: number;
	queuedWaves: number;
	inFlight: number;
	launched: number;
	settled: number;
	delivered: number;
	failed: number;
	dropped: number;
	maxQueueAgeMs: number;
	ledgers: Record<string, AssignmentLedgerDiagnostics>;
}

interface TransferAssignmentLedgerState {
	ledger: TransferAssignmentLedger;
	testMode: boolean;
	selection: SelectionResult;
	orderedOrchestratorIds: string[];
	orderedHotkeys: string[];
	qualifiedAssignmentPlan?: QualifiedAssignmentPlan;
}

export interface ReassignChunkBundleResult {
	ok: boolean;
	batchIds: string[];
	batchSlices: Array<ReturnType<typeof summarizeTaskOfferBatchSlice>>;
	pendingPushes: PendingTaskOfferBatchPush[];
	selectionRule?: AssignmentSelectionRule;
	kind?: "no_orchs" | "partial_coverage";
	partial?: boolean;
	assignedChunkCount?: number;
	remainingChunkCount?: number;
}

import type { StaleBatchBundle } from "./transfer-overseer-recovery.js";
export type { StaleBatchBundle } from "./transfer-overseer-recovery.js";

interface VirtualTaskOfferBatchRow {
	orchestrator: OrchestratorCandidate;
	chunkIndices: number[];
	chunkStart: number;
	chunkEnd: number;
	totalChunks: number;
}

interface InsertedTaskOfferBatchRow {
	id: string;
	orchestrator_id: string;
	chunk_indices: number[];
}

type AssignmentPool = "qualifying" | "qualified";

function requiredPoolForTransfer(testMode: boolean): AssignmentPool {
	return testMode ? "qualifying" : "qualified";
}

function normalizeChunkIndices(indices: number[]): number[] {
	return [...new Set(indices)]
		.filter((index) => Number.isInteger(index) && index >= 0)
		.sort((a, b) => a - b);
}

function uniqueValidChunkIndices(indices: number[]): number[] {
	const seen = new Set<number>();
	const output: number[] = [];
	for (const index of indices) {
		if (!Number.isInteger(index) || index < 0 || seen.has(index)) {
			continue;
		}
		seen.add(index);
		output.push(index);
	}
	return output;
}

function shuffledChunkIndices(indices: number[]): number[] {
	const output = uniqueValidChunkIndices(indices);
	const original = [...output];
	for (let index = output.length - 1; index > 0; index -= 1) {
		const swapIndex = randomInt(index + 1);
		[output[index], output[swapIndex]] = [output[swapIndex]!, output[index]!];
	}
	if (output.length > 1 && output.every((value, index) => value === original[index])) {
		const swapIndex = randomInt(1, output.length);
		[output[0], output[swapIndex]] = [output[swapIndex]!, output[0]!];
	}
	return output;
}

function summarizeChunkIndices(indices: number[]): { chunkStart: number; chunkEnd: number; totalChunks: number } {
	const normalized = normalizeChunkIndices(indices);
	if (!normalized.length) {
		throw new Error("task offer batch must include at least one chunk index");
	}
	return {
		chunkStart: normalized[0]!,
		chunkEnd: normalized[normalized.length - 1]!,
		totalChunks: normalized.length,
	};
}

function mapChunkBundleToRows(
	chunkIndices: number[],
	orchestrators: OrchestratorCandidate[],
	plannedSliceSizes: number[],
): VirtualTaskOfferBatchRow[] {
	const rows: VirtualTaskOfferBatchRow[] = [];
	const normalizedChunks = uniqueValidChunkIndices(chunkIndices);
	let virtualPos = 0;
	for (let index = 0; index < orchestrators.length; index++) {
		const sliceSize = plannedSliceSizes[index] ?? 0;
		if (sliceSize <= 0) continue;
		const mapped = normalizedChunks.slice(virtualPos, virtualPos + sliceSize);
		virtualPos += sliceSize;
		for (let offset = 0; offset < mapped.length; offset += env.TRANSFER_TASK_OFFER_BATCH_MAX_TASKS) {
			const chunkSlice = mapped.slice(offset, offset + env.TRANSFER_TASK_OFFER_BATCH_MAX_TASKS);
			if (!chunkSlice.length) continue;
			const summary = summarizeChunkIndices(chunkSlice);
			rows.push({
				orchestrator: orchestrators[index]!,
				chunkIndices: chunkSlice,
				chunkStart: summary.chunkStart,
				chunkEnd: summary.chunkEnd,
				totalChunks: summary.totalChunks,
			});
		}
	}
	return rows;
}

export function mapRecoveryChunkBundleToRows(
	chunkIndices: number[],
	orchestrators: OrchestratorCandidate[],
	plannedSliceSizes: number[],
	excludeOrchestratorIdsByChunk: Map<number, string[]>,
	ownerGroupByOrchestratorId: Map<string, string | null>,
): VirtualTaskOfferBatchRow[] {
	const slots: number[] = [];
	for (let index = 0; index < orchestrators.length; index += 1) {
		for (let count = 0; count < (plannedSliceSizes[index] ?? 0); count += 1) slots.push(index);
	}
	const assignedByOrchestrator = new Map<number, number[]>();
	for (const chunkIndex of uniqueValidChunkIndices(chunkIndices)) {
		const excludedIds = new Set(excludeOrchestratorIdsByChunk.get(chunkIndex) ?? []);
		const excludedOwnerGroups = new Set(
			[...excludedIds]
				.map((orchestratorId) => ownerGroupByOrchestratorId.get(orchestratorId))
				.filter((ownerGroupId): ownerGroupId is string => Boolean(ownerGroupId)),
		);
		const eligible = (orchestratorIndex: number): boolean => {
			const candidate = orchestrators[orchestratorIndex]!;
			return !excludedIds.has(candidate.id)
				&& (!candidate.owner_group_id || !excludedOwnerGroups.has(candidate.owner_group_id));
		};
		const slotPosition = slots.findIndex(eligible);
		let orchestratorIndex: number | undefined;
		if (slotPosition >= 0) {
			[orchestratorIndex] = slots.splice(slotPosition, 1);
		} else {
			const alternatives = orchestrators
				.map((_, index) => index)
				.filter(eligible)
				.sort((left, right) => {
					const leftAssigned = assignedByOrchestrator.get(left)?.length ?? 0;
					const rightAssigned = assignedByOrchestrator.get(right)?.length ?? 0;
					const leftWeight = Math.max(1, plannedSliceSizes[left] ?? 0);
					const rightWeight = Math.max(1, plannedSliceSizes[right] ?? 0);
					const loadComparison = (leftAssigned + 1) / leftWeight - (rightAssigned + 1) / rightWeight;
					return loadComparison || left - right;
				});
			orchestratorIndex = alternatives[0];
			if (orchestratorIndex === undefined) continue;
			// Consume the excluded quota slot and redistribute it through the
			// normal ordered candidate pool instead of declaring false exhaustion.
			slots.shift();
		}
		const bucket = assignedByOrchestrator.get(orchestratorIndex!) ?? [];
		bucket.push(chunkIndex);
		assignedByOrchestrator.set(orchestratorIndex!, bucket);
	}
	const rows: VirtualTaskOfferBatchRow[] = [];
	for (const [orchestratorIndex, assigned] of assignedByOrchestrator) {
		for (let offset = 0; offset < assigned.length; offset += env.TRANSFER_TASK_OFFER_BATCH_MAX_TASKS) {
			const chunkSlice = assigned.slice(offset, offset + env.TRANSFER_TASK_OFFER_BATCH_MAX_TASKS);
			const summary = summarizeChunkIndices(chunkSlice);
			rows.push({
				orchestrator: orchestrators[orchestratorIndex]!,
				chunkIndices: chunkSlice,
				chunkStart: summary.chunkStart,
				chunkEnd: summary.chunkEnd,
				totalChunks: summary.totalChunks,
			});
		}
	}
	return rows;
}

const COVERAGE_FAILURE_MESSAGE =
	"transfer assignment failed because not all chunks could be assigned to live orchestrators";

class TransientAssignmentReadinessError extends Error {
	readonly candidateCounts: Record<string, number>;

	constructor(message: string, candidateCounts: Record<string, number>) {
		super(message);
		this.name = "TransientAssignmentReadinessError";
		this.candidateCounts = candidateCounts;
	}
}

function isTerminalAssignmentFailure(error: unknown): error is Error {
	if (!(error instanceof Error)) return false;
	if (error instanceof TransientAssignmentReadinessError) return false;
	return (
		error.message === COVERAGE_FAILURE_MESSAGE ||
		/^no ready (qualifying|qualified) orchestrators available for (test|production) transfers$/.test(error.message)
	);
}

function summarizeCandidate(candidate: OrchestratorCandidate) {
	return {
		id: candidate.id,
		hotkey: candidate.hotkey,
		prismPool: candidate.prism_pool,
	};
}

function summarizeTaskOfferBatchSlice(
	candidate: OrchestratorCandidate,
	chunkIndices: number[],
) {
	const summary = summarizeChunkIndices(chunkIndices);
	return {
		orchestratorId: candidate.id,
		hotkey: candidate.hotkey,
		prismPool: candidate.prism_pool,
		chunkIndices: [...chunkIndices],
		chunkRange: `${summary.chunkStart}-${summary.chunkEnd}`,
		sliceSize: summary.totalChunks,
	};
}

function topRankedSummary(candidates: OrchestratorCandidate[], limit = 3): string {
	const topCandidates = candidates.slice(0, limit);
	if (!topCandidates.length) return "none";
	return topCandidates
		.map((candidate, index) => {
			const score = numericValue(candidate.prism_final_score);
			return `${index + 1}:${candidate.hotkey} (${score.toFixed(4)})`;
		})
		.join(", ");
}

function orderedSummary(candidates: OrchestratorCandidate[], limit = 3): string {
	const topCandidates = candidates.slice(0, limit);
	if (!topCandidates.length) return "none";
	return topCandidates.map((candidate, index) => `${index + 1}:${candidate.hotkey}`).join(", ");
}


export function hashSeed(seed: string): number {
	let hash = 0;
	for (let index = 0; index < seed.length; index += 1) {
		hash = (hash * 31 + seed.charCodeAt(index)) >>> 0;
	}
	return hash;
}

export function rotateCandidates<T>(candidates: T[], offset: number): T[] {
	if (!candidates.length) return [];
	const normalizedOffset = ((offset % candidates.length) + candidates.length) % candidates.length;
	if (normalizedOffset === 0) return [...candidates];
	return [...candidates.slice(normalizedOffset), ...candidates.slice(0, normalizedOffset)];
}

function compareUidAscNullsLast(a: number | null, b: number | null): number {
	if (a === null && b === null) return 0;
	if (a === null) return 1;
	if (b === null) return -1;
	return a - b;
}

function buildAssignmentLogicSummary(
	testMode: boolean,
	selection: SelectionResult,
	totalChunks: number,
	sliceSizes: number[],
): AssignmentLogicSummary {
	const transferClass = testMode ? "test" : "production";
	const poolDecision = `BeamCore restricts ${transferClass} transfers to the ${selection.preferredPool} tier and found ${selection.counts.preferred} eligible orchestrators there`;
	const candidateScreening = `queried ${selection.counts.queried} ready orchestrators; ${selection.counts.controlReady} had a live NATS control session; worker-session count is not an orchestrator selection gate`;
	const nonZeroSlices = sliceSizes.filter((size) => size > 0);
	const rankingDecision =
		selection.selectionRule === "prism_final_score_desc"
			? `qualified pool: full assignable base pool ordered by PRISM score with stable UID tie-breaking, then owner-group fairness and PRISM/Hamilton distribution; top assigned: ${topRankedSummary(selection.orchestrators)}`
			: `eligible qualifying-pool orchestrators were ordered by deterministic rotation; front of rotation: ${orderedSummary(selection.orchestrators)}`;
	const distributionDecision =
		selection.selectionRule === "prism_final_score_desc"
			? `split ${totalChunks} chunks proportionally by PRISM final score across ${nonZeroSlices.length} orchestrators; slice sizes: ${nonZeroSlices.join(",")}`
			: `split ${totalChunks} chunks as evenly as possible across ${nonZeroSlices.length} orchestrators with rotated remainder priority; slice sizes: ${nonZeroSlices.join(",")}`;
	const qualifiedSelectionNarrative =
		selection.selectionRule === "prism_final_score_desc"
			? `Qualified selection used the full assignable qualified base pool (${selection.counts.preferred} hotkeys). Stage A: inter-group Hamilton on average PRISM per owner group in the base pool. Stage B: intra-group Hamilton on each member's actual PRISM.`
			: undefined;

	const summary: AssignmentLogicSummary = {
		transferClass,
		poolDecision,
		rankingDecision,
		candidateScreening,
		distributionDecision,
	};
	if (qualifiedSelectionNarrative !== undefined) {
		summary.qualifiedSelectionNarrative = qualifiedSelectionNarrative;
	}
	return summary;
}

function assertRequiredPoolSelection(
	selection: SelectionResult,
	testMode: boolean,
	transferId?: string,
	requiredPool: AssignmentPool = requiredPoolForTransfer(testMode),
) {
	const wrongPool = selection.orchestrators.filter((candidate) => candidate.prism_pool !== requiredPool);
	if (!selection.orchestrators.length || selection.preferredPool !== requiredPool || wrongPool.length > 0) {
		const transferClass = testMode ? "test" : "production";
		logger.error(
			{
				transferId,
				testMode,
				preferredPool: requiredPool,
				selectedPreferredPool: selection.preferredPool,
				selectedCount: selection.orchestrators.length,
				selectedPools: selection.orchestrators.map((candidate) => candidate.prism_pool),
				wrongPoolCandidates: wrongPool.map(summarizeCandidate),
				candidateCounts: selection.counts,
			},
			"assignment selection violated required pool restriction",
		);
		throw new Error(`no ready ${requiredPool} orchestrators available for ${transferClass} transfers`);
	}
}

export class AssignmentEngine {
	private readonly gatewayClient: WorkerGatewayClient;
	private readonly offerRegistry: TransferRuntimeRegistry;
	private readonly routingRegistry: OrchestratorRoutingRegistry;
	private readonly dispatchInFlightByTransfer = new Map<string, Set<Promise<boolean>>>();
	private readonly assignmentLedgers = new Map<string, TransferAssignmentLedgerState>();
	private readonly terminalLedgerDiagnostics = new Map<string, AssignmentLedgerDiagnostics>();
	private readonly recoveryRoutesDeferredOnce = new Set<string>();
	private dispatchStopping = false;
	private dispatchLaunched = 0;
	private dispatchSettled = 0;
	private dispatchDelivered = 0;
	private dispatchFailed = 0;
	private dispatchDropped = 0;
	private dispatchMaxQueueAgeMs = 0;

	constructor(deps: AssignmentEngineDeps) {
		this.gatewayClient = deps.gatewayClient;
		this.offerRegistry = deps.offerRegistry;
		this.routingRegistry = deps.routingRegistry;
	}

	private failTransferAssignment(transferId: string, reason: string): void {
		this.offerRegistry.failTransfer(transferId, reason);
	}

	private registerRuntimeTaskOfferBatches(input: {
		transferId: string;
		totalBytes: number;
		totalChunks: number;
		chunkSize: number;
		metadata: unknown;
		pendingPushes: PendingTaskOfferBatchPush[];
		recovery?: boolean;
	}): boolean {
		const startedAt = performance.now();
		const result = this.offerRegistry.createTaskOfferBatches(
			input.pendingPushes.map((push) => ({
				batchId: push.batchId,
				transferId: input.transferId,
				orchestratorId: push.orchestrator.id,
				orchestratorHotkey: push.orchestrator.hotkey,
				workerGatewayType: push.orchestrator.gateway_type,
				workerGatewayUrl: push.orchestrator.gateway_url,
				chunkIndices: [...push.chunkIndices],
				chunkStart: push.chunkStart,
				chunkEnd: push.chunkEnd,
				totalChunks: push.totalChunks,
				totalBytes: input.totalBytes,
				chunkSize: input.chunkSize,
				metadata: input.metadata,
				recovery: input.recovery ?? false,
			})),
		);
		if (!result.ok) {
			logger.warn(
				{ transferId: input.transferId, reason: result.reason, batchCount: input.pendingPushes.length },
				"runtime task offer batch registration failed",
			);
		}
		logger.info(
			{
				transferId: input.transferId,
				batchCount: input.pendingPushes.length,
				registrationMs: Math.round((performance.now() - startedAt) * 10) / 10,
				registered: result.ok,
			},
			"runtime assignment batches registered",
		);
		return result.ok;
	}

	private registerTransferStarted(input: {
		transferId: string;
		totalBytes: number;
		totalChunks: number;
		chunkSize: number;
		metadata: unknown;
	}): void {
		this.offerRegistry.registerTransferStarted(input);
	}

	private loadCumulativeRecoveryExclusions(input: {
		transferId: string;
		chunkIndices: number[];
		excludeOrchestratorIds: string[];
	}): { orchestratorIds: string[]; ownerGroupIds: string[] } {
		return this.offerRegistry.getRecoveryExclusions(
			input.transferId,
			input.chunkIndices,
			input.excludeOrchestratorIds,
			this.routingRegistry,
		);
	}

	private selectOrchestrators(
		testMode: boolean,
		excludeIds: string[] = [],
		orderSeed?: string,
		options: {
			excludeOwnerGroupIds?: string[];
			requiredPool?: AssignmentPool;
		} = {},
	): SelectionResult {
		const transferPool = requiredPoolForTransfer(testMode);
		const preferredPool = options.requiredPool ?? transferPool;
		if (preferredPool !== transferPool) {
			throw new Error(`transfer pool mismatch: expected ${transferPool}, received ${preferredPool}`);
		}
		const excludeOwnerGroupIds = options.excludeOwnerGroupIds ?? [];

		const allCandidates = this.routingRegistry.listReadyCandidates(excludeIds, excludeOwnerGroupIds);
		const poolReady = allCandidates.filter((candidate) => candidate.prism_pool === preferredPool);

		const controlReady = allCandidates.filter((candidate) => orchestratorIsEligible(candidate.hotkey));
		const eligible = controlReady;
		const connectedRegistry = connectedHotkeys().sort((left, right) => left.localeCompare(right));
		const queriedHotkeys = allCandidates.map((candidate) => candidate.hotkey);
		const controlReadyHotkeys = controlReady.map((candidate) => candidate.hotkey);
		const missingControlHotkeys = queriedHotkeys.filter((hotkey) => !controlReadyHotkeys.includes(hotkey));

		const preferred = eligible.filter((candidate) => candidate.prism_pool === preferredPool);

		let sorted: OrchestratorCandidate[];
		let selectionRule: AssignmentSelectionRule;

		selectionRule =
			preferredPool === "qualifying"
				? ("qualifying_equal_share_rotation" as const)
				: ("prism_final_score_desc" as const);

		if (selectionRule === "qualifying_equal_share_rotation") {
			sorted = rotateCandidates(
				[...preferred].sort((a, b) => a.hotkey.localeCompare(b.hotkey) || a.id.localeCompare(b.id)),
				preferred.length > 0 ? hashSeed(orderSeed ?? "") % preferred.length : 0,
			);
		} else {
			sorted = [...preferred].sort(
				(a, b) =>
					numericValue(b.prism_final_score) - numericValue(a.prism_final_score) ||
					compareUidAscNullsLast(a.uid, b.uid) ||
					a.hotkey.localeCompare(b.hotkey) ||
					a.id.localeCompare(b.id),
			);
		}

		const counts = {
			queried: allCandidates.length,
			controlReady: controlReady.length,
			poolReady: poolReady.length,
			eligible: eligible.length,
			preferred: preferred.length,
			selected: sorted.length,
		};

		if (!preferred.length) {
			const transferClass = testMode ? "test" : "production";
			const diagnostics = {
				queriedHotkeys,
				poolReadyHotkeys: poolReady.map((candidate) => candidate.hotkey),
				controlReadyHotkeys,
				missingControlHotkeys,
				connectedRegistryHotkeys: connectedRegistry,
			};
			if (poolReady.length > 0) {
				logger.warn(
					{
						testMode,
						preferredPool,
						excludeIds,
						candidateCounts: counts,
						diagnostics,
					},
					"required orchestrator pool has DB-qualified candidates but no live NATS control sessions",
				);
				throw new TransientAssignmentReadinessError(
					`${preferredPool} orchestrators are qualified but none currently have live NATS control sessions for ${transferClass} transfers`,
					counts,
				);
			}
			logger.warn(
				{
					testMode,
					preferredPool,
					excludeIds,
					candidateCounts: counts,
					diagnostics,
				},
				"no DB-qualified orchestrators available in required pool for assignment",
			);
			throw new Error(`no ready ${preferredPool} orchestrators available for ${transferClass} transfers`);
		}

		const result: SelectionResult = {
			orchestrators: sorted,
			preferredPool,
			selectionRule,
			counts,
			diagnostics: {
				queriedHotkeys,
				controlReadyHotkeys,
				missingControlHotkeys,
				connectedRegistryHotkeys: connectedRegistry,
			},
		};
		return result;
	}


	private async assignChunkCoverage(input: {
		transferId: string;
		chunkStart: number;
		totalChunks: number;
		chunkSize: number;
		metadata: TransferMetadata | null;
		orchestrators: OrchestratorCandidate[];
		selectionRule: SelectionResult["selectionRule"];
		plannedSliceSizes?: number[];
	}): Promise<AssignmentCoverageResult> {
		return this.assignChunkIndexCoverage({
			...input,
			chunkIndices: Array.from({ length: input.totalChunks }, (_, offset) => input.chunkStart + offset),
		});
	}

	private async assignChunkIndexCoverage(input: {
		transferId: string;
		chunkIndices: number[];
		chunkSize: number;
		metadata: TransferMetadata | null;
		orchestrators: OrchestratorCandidate[];
		selectionRule: SelectionResult["selectionRule"];
		plannedSliceSizes?: number[];
	}): Promise<AssignmentCoverageResult> {
		const batchIds: string[] = [];
		const batchSlices: Array<ReturnType<typeof summarizeTaskOfferBatchSlice>> = [];
		const pendingPushes: PendingTaskOfferBatchPush[] = [];
		const available = [...input.orchestrators];
		const chunkIndices = shuffledChunkIndices(input.chunkIndices);
		const plannedSliceSizes =
			input.plannedSliceSizes ??
			allocateChunkSlices(available, chunkIndices.length, input.selectionRule);
		const rows = mapChunkBundleToRows(chunkIndices, available, plannedSliceSizes);
		let assignedChunkCount = 0;

		for (const row of rows) {
			const batchId = randomUUID();
			batchIds.push(batchId);
			pendingPushes.push({
				batchId,
				orchestrator: row.orchestrator,
				chunkIndices: row.chunkIndices,
				chunkStart: row.chunkStart,
				chunkEnd: row.chunkEnd,
				totalChunks: row.totalChunks,
				chunkSize: input.chunkSize,
			});
			assignedChunkCount += row.totalChunks;
			batchSlices.push(summarizeTaskOfferBatchSlice(row.orchestrator, row.chunkIndices));
		}

		return {
			batchIds,
			assignedChunkCount,
			batchSlices,
			pendingPushes,
			coveredAllChunks: assignedChunkCount === chunkIndices.length,
		};
	}
	private async deliverTaskOfferBatchOnce(
		transferId: string,
		push: PendingTaskOfferBatchPush,
		onFirstSent?: () => void,
	): Promise<boolean> {
		const result = await deliverTaskOfferBatch({
			gatewayClient: this.gatewayClient,
			batchId: push.batchId,
			expectedOrchestratorId: push.orchestrator.id,
			offerRegistry: this.offerRegistry,
			...(onFirstSent ? { onFirstSent } : {}),
		});
		if (result.ok) {
			recordControlDeliveryResult(transferId, true);
			return true;
		}
		logger.warn(
			{
				transferId,
				batchId: push.batchId,
				hotkey: push.orchestrator.hotkey,
				code: result.code,
				message: result.message,
			},
			"task offer batch delivery failed",
		);
		recordControlDeliveryResult(transferId, false);
		this.offerRegistry.settleTaskOfferBatchDeliveryFailure(
			push.batchId,
			"orchestrator_nats_not_ready_or_delivery_rejected",
		);
		this.offerRegistry.recordOverseerIntervention({
			interventionType: "batch_push_failed",
			batchId: push.batchId,
			transferId,
			orchestratorId: push.orchestrator.id,
			chunkStart: push.chunkStart,
			chunkEnd: push.chunkEnd,
			totalChunks: push.totalChunks,
			outcome: "orchestrator_nats_delivery_failed",
			reason: "orchestrator_nats_not_ready_or_delivery_rejected",
			metadata: { delivery_transport: "nats" },
		});
		return false;
	}

	private transferDispatchIsTerminal(transferId: string): boolean {
		const lifecycle = this.offerRegistry.getTransferLifecycle(transferId);
		return !lifecycle || ["completed", "failed", "cancelled"].includes(lifecycle.status);
	}

	private launchTaskOfferBatch(transferId: string, push: PendingTaskOfferBatchPush): Promise<boolean> {
		const batch = this.offerRegistry.getBatch(push.batchId);
		if (!batch || !["assigned", "in_progress"].includes(batch.status)) {
			this.offerRegistry.settleUndispatchedTaskOfferBatch(push.batchId, "dispatch_batch_not_active");
			this.dispatchDropped += 1;
			return Promise.resolve(false);
		}
		const launchedAtMs = performance.now();
		const queueAgeMs = 0;
		this.dispatchMaxQueueAgeMs = Math.max(this.dispatchMaxQueueAgeMs, queueAgeMs);
		recordDispatchQueueAge(transferId, queueAgeMs);
		this.dispatchLaunched += 1;
		const inFlight = this.dispatchInFlightByTransfer.get(transferId) ?? new Set<Promise<boolean>>();
		let tracked!: Promise<boolean>;
		tracked = this.deliverTaskOfferBatchOnce(transferId, push)
			.catch((err: unknown) => {
				logger.error(sanitizeStorageDiagnostic({ operation: "task_offer_dispatch", error: err, transferId, groupId: push.batchId }), "task offer dispatch failed");
				return false;
			})
			.then((delivered) => {
				this.dispatchSettled += 1;
				if (delivered) this.dispatchDelivered += 1;
				else this.dispatchFailed += 1;
				return delivered;
			})
			.finally(() => {
				inFlight.delete(tracked);
				if (!inFlight.size) this.dispatchInFlightByTransfer.delete(transferId);
				logger.debug(
					{ transferId, batchId: push.batchId, dispatchMs: Math.round((performance.now() - launchedAtMs) * 10) / 10 },
					"runtime assignment batch dispatch settled",
				);
			});
		inFlight.add(tracked);
		this.dispatchInFlightByTransfer.set(transferId, inFlight);
		return tracked;
	}

	private async dispatchTaskOfferBatches(
		transferId: string,
		pushes: PendingTaskOfferBatchPush[],
	): Promise<boolean[]> {
		const startedAt = performance.now();
		if (this.dispatchStopping || this.transferDispatchIsTerminal(transferId)) {
			for (const push of pushes) this.offerRegistry.settleUndispatchedTaskOfferBatch(push.batchId, "dispatch_cancelled_before_send");
			this.dispatchDropped += pushes.length;
			return pushes.map(() => false);
		}
		const results = pushes.map((push): Promise<boolean> => {
			if (!this.offerRegistry.markBatchDispatchPending(push.batchId)) {
				this.dispatchDropped += 1;
				return Promise.resolve(false);
			}
			return this.launchTaskOfferBatch(transferId, push);
		});
		logger.info(
			{
				transferId,
				batchCount: pushes.length,
				launchDurationMs: Math.round((performance.now() - startedAt) * 10) / 10,
			},
			"runtime assignment dispatch wave launched",
		);
		const settled = await Promise.all(results);
		logger.info(
			{
				transferId,
				batchCount: pushes.length,
				deliveredCount: settled.filter(Boolean).length,
				dispatchMs: Math.round((performance.now() - startedAt) * 10) / 10,
			},
			"runtime assignment batch dispatch completed",
		);
		return settled;
	}

	dispatchDiagnostics(): AssignmentDispatchDiagnostics {
		const inFlight = [...this.dispatchInFlightByTransfer.values()].reduce((count, entries) => count + entries.size, 0);
		return {
			laneCount: this.dispatchInFlightByTransfer.size,
			queued: 0,
			queuedWaves: 0,
			inFlight,
			launched: this.dispatchLaunched,
			settled: this.dispatchSettled,
			delivered: this.dispatchDelivered,
			failed: this.dispatchFailed,
			dropped: this.dispatchDropped,
			maxQueueAgeMs: Math.round(this.dispatchMaxQueueAgeMs * 10) / 10,
			ledgers: Object.fromEntries([
				...this.terminalLedgerDiagnostics,
				...[...this.assignmentLedgers.entries()].map(([transferId, state]) => [transferId, state.ledger.diagnostics()] as const),
			]),
		};
	}

	releaseTransferState(transferId: string): void {
		for (const key of this.recoveryRoutesDeferredOnce) {
			if (key.startsWith(`${transferId}:`)) this.recoveryRoutesDeferredOnce.delete(key);
		}
		const ledger = this.assignmentLedgers.get(transferId);
		if (ledger) {
			this.terminalLedgerDiagnostics.set(transferId, ledger.ledger.diagnostics());
			while (this.terminalLedgerDiagnostics.size > 100) {
				const oldest = this.terminalLedgerDiagnostics.keys().next().value;
				if (!oldest) break;
				this.terminalLedgerDiagnostics.delete(oldest);
			}
			this.assignmentLedgers.delete(transferId);
		}
	}

	async shutdown(): Promise<void> {
		this.dispatchStopping = true;
		await Promise.allSettled(
			[...this.dispatchInFlightByTransfer.values()].flatMap((entries) => [...entries]),
		);
		this.dispatchInFlightByTransfer.clear();
		this.assignmentLedgers.clear();
		this.terminalLedgerDiagnostics.clear();
	}

	async resendTaskOfferBatchFromRegistry(batchId: string): Promise<boolean> {
		const batch = this.offerRegistry.getBatch(batchId);
		if (!batch || batch.status !== "assigned") {
			return false;
		}
		const [pushed = false] = await this.dispatchTaskOfferBatches(batch.transferId, [{
			batchId: batch.batchId,
			orchestrator: {
				id: batch.orchestratorId,
				hotkey: batch.orchestratorHotkey,
				uid: null,
				owner_group_id: null,
				gateway_type: batch.workerGatewayType,
				gateway_url: batch.workerGatewayUrl,
				prism_final_score: "0",
				prism_confidence_score: "0",
				prism_pool: "qualified",
			},
			chunkIndices: [...batch.chunkIndices],
			chunkStart: batch.chunkStart,
			chunkEnd: batch.chunkEnd,
			totalChunks: batch.totalChunks,
			chunkSize: batch.chunkSize,
		}]);
		if (pushed) {
			recordResendPush(batch.transferId);
		}
		return pushed;
	}

	async resendTaskOfferBatchPush(batchId: string): Promise<boolean> {
		return this.resendTaskOfferBatchFromRegistry(batchId);
	}

	private selectRecoveryAssignment(input: {
		transferId: string;
		testMode: boolean;
		chunkCount: number;
		excludeOrchestratorIds: string[];
		excludeOwnerGroupIds: string[];
		requiredPool: AssignmentPool;
	}): { selection: SelectionResult; plannedSliceSizes: number[] } {
		if (input.requiredPool === "qualifying") {
			const selection = this.selectOrchestrators(
				input.testMode,
				input.excludeOrchestratorIds,
				input.transferId,
				{
					excludeOwnerGroupIds: input.excludeOwnerGroupIds,
					requiredPool: input.requiredPool,
				},
			);
			assertRequiredPoolSelection(selection, input.testMode, input.transferId, input.requiredPool);
			return {
				selection,
				plannedSliceSizes: allocateChunkSlices(selection.orchestrators, input.chunkCount, selection.selectionRule),
			};
		}

		const context = this.offerRegistry.getInitialAssignmentContext(input.transferId);
		const originalPlan = context?.qualifiedAssignmentPlan;
		if (!context || context.pool !== "qualified" || !originalPlan) {
			throw new Error(`qualified recovery for ${input.transferId} missing in-memory initial assignment context`);
		}

		const allCandidates = this.routingRegistry.listReadyCandidates(
			input.excludeOrchestratorIds,
			input.excludeOwnerGroupIds,
		);
		const poolReady = allCandidates.filter((candidate) => candidate.prism_pool === "qualified");
		const controlReady = allCandidates.filter((candidate) => orchestratorIsEligible(candidate.hotkey));
		const eligible = controlReady;
		const preferred = eligible.filter((candidate) => candidate.prism_pool === "qualified");
		const connectedRegistry = connectedHotkeys().sort((left, right) => left.localeCompare(right));
		const queriedHotkeys = allCandidates.map((candidate) => candidate.hotkey);
		const controlReadyHotkeys = controlReady.map((candidate) => candidate.hotkey);
		const missingControlHotkeys = queriedHotkeys.filter((hotkey) => !controlReadyHotkeys.includes(hotkey));
		const byHotkey = new Map(preferred.map((candidate) => [candidate.hotkey, candidate]));
		const baseOrchs = originalPlan.baseHotkeysOrdered
			.map((hotkey) => byHotkey.get(hotkey))
			.filter((candidate): candidate is OrchestratorCandidate => Boolean(candidate));
		if (!baseOrchs.length) {
			throw new Error(`no ready qualified orchestrators remain in original recovery base pool for ${input.transferId}`);
		}

		const slicePlan = buildQualifiedPoolSlicePlan({
			transferId: input.transferId,
			baseOrchs,
			totalChunks: input.chunkCount,
			preferredHotkeys: originalPlan.counts.preferredHotkeys,
			ownerGroupsInPool: originalPlan.counts.ownerGroupsInPool,
		});
		const counts = {
			queried: allCandidates.length,
			controlReady: controlReady.length,
			poolReady: poolReady.length,
			eligible: eligible.length,
			preferred: preferred.length,
			selected: baseOrchs.length,
		};
		const selection: SelectionResult = {
			orchestrators: slicePlan.orchestrators,
			preferredPool: "qualified",
			selectionRule: "prism_final_score_desc",
			qualifiedAssignmentPlan: slicePlan.plan,
			counts,
			diagnostics: {
				queriedHotkeys,
				controlReadyHotkeys,
				missingControlHotkeys,
				connectedRegistryHotkeys: connectedRegistry,
			},
		};
		assertRequiredPoolSelection(selection, false, input.transferId, input.requiredPool);
		return { selection, plannedSliceSizes: slicePlan.sliceSizes };
	}

	private createTransferAssignmentLedger(input: {
		transferId: string;
		testMode: boolean;
		totalRoutes: number;
	}): TransferAssignmentLedgerState {
		const selection = this.selectOrchestrators(input.testMode, [], input.transferId);
		assertRequiredPoolSelection(selection, input.testMode, input.transferId);
		const baseOrchestrators = [...selection.orchestrators];
		let ledgerOrchestrators = baseOrchestrators;
		let quotas: number[];
		let qualifiedAssignmentPlan: QualifiedAssignmentPlan | undefined;
		if (selection.preferredPool === "qualified") {
			const plan = buildQualifiedPoolSlicePlan({
				transferId: input.transferId,
				baseOrchs: baseOrchestrators,
				totalChunks: input.totalRoutes,
				preferredHotkeys: selection.counts.preferred,
				ownerGroupsInPool: countDistinctOwnerGroupsInPool(baseOrchestrators),
			});
			ledgerOrchestrators = plan.orchestrators;
			quotas = plan.sliceSizes;
			qualifiedAssignmentPlan = plan.plan;
			selection.qualifiedAssignmentPlan = plan.plan;
		} else {
			quotas = allocateChunkSlices(baseOrchestrators, input.totalRoutes, selection.selectionRule);
		}
		const byHotkey = new Map(baseOrchestrators.map((candidate) => [candidate.hotkey, candidate.id]));
		const orderedHotkeys = qualifiedAssignmentPlan?.baseHotkeysOrdered ?? baseOrchestrators.map((candidate) => candidate.hotkey);
		const state: TransferAssignmentLedgerState = {
			ledger: new TransferAssignmentLedger(input.transferId, input.totalRoutes, ledgerOrchestrators, quotas),
			testMode: input.testMode,
			selection: { ...selection, orchestrators: ledgerOrchestrators },
			orderedOrchestratorIds: orderedHotkeys.map((hotkey) => byHotkey.get(hotkey)).filter((id): id is string => Boolean(id)),
			orderedHotkeys,
			...(qualifiedAssignmentPlan ? { qualifiedAssignmentPlan } : {}),
		};
		this.assignmentLedgers.set(input.transferId, state);
		logger.info(
			{
				transferId: input.transferId,
				totalRoutes: input.totalRoutes,
				candidateCount: ledgerOrchestrators.length,
				pool: selection.preferredPool,
			},
			"transfer-wide assignment ledger initialized",
		);
		return state;
	}

	private allocateLedgerRedistribution(state: TransferAssignmentLedgerState, survivors: OrchestratorCandidate[], routeCount: number): number[] {
		if (state.selection.preferredPool !== "qualified") {
			return allocateChunkSlices(survivors, routeCount, state.selection.selectionRule);
		}
		const plan = buildQualifiedPoolSlicePlan({
			transferId: state.ledger.transferId,
			baseOrchs: survivors,
			totalChunks: routeCount,
			preferredHotkeys: survivors.length,
			ownerGroupsInPool: countDistinctOwnerGroupsInPool(survivors),
		});
		const byId = new Map(plan.orchestrators.map((candidate, index) => [candidate.id, plan.sliceSizes[index] ?? 0]));
		return survivors.map((candidate) => byId.get(candidate.id) ?? 0);
	}

	private refreshAssignmentLedgerEligibility(state: TransferAssignmentLedgerState): void {
		const unavailable = state.ledger.candidateList()
			.map((candidate) => ({
				candidate,
				reason: orchestratorUnavailableReason(candidate.hotkey)
					?? (this.routingRegistry.getByHotkey(candidate.hotkey)?.uid == null ? "uid_deregistered" : null),
			}))
			.filter((entry): entry is { candidate: OrchestratorCandidate; reason: string } => entry.reason != null);
		if (!unavailable.length) return;
		const redistributed = state.ledger.removeCandidates(
			unavailable.map(({ candidate, reason }) => ({ orchestratorId: candidate.id, reason })),
			(survivors, routeCount) => this.allocateLedgerRedistribution(state, survivors, routeCount),
		);
		if (redistributed > 0) {
			logger.warn(
				{
					transferId: state.ledger.transferId,
					removedHotkeys: unavailable.map(({ candidate, reason }) => ({ hotkey: candidate.hotkey, reason })),
					redistributedRoutes: redistributed,
					ledger: state.ledger.diagnostics(),
				},
				"transfer-wide assignment ledger redistributed unavailable quota",
			);
		}
	}

	async reassignStalledChunkBundle(input: {
		transferId: string;
		chunkIndices: number[];
		excludeOrchestratorIds: string[];
		excludeOrchestratorIdsByChunk?: Map<number, string[]>;
		reason: string;
		reasonByChunk?: Map<number, string>;
		staleBatchIds?: string[];
	}): Promise<ReassignChunkBundleResult> {
		const requestedChunks = normalizeChunkIndices(input.chunkIndices);
		const chunkIndices = this.offerRegistry.filterRecoverableChunkIndices(
			input.transferId,
			requestedChunks,
			Date.now(),
		);
		const reasonForChunk = (chunkIndex: number): string => input.reasonByChunk?.get(chunkIndex) ?? input.reason;
		const empty: ReassignChunkBundleResult = {
			ok: true,
			batchIds: [],
			batchSlices: [],
			pendingPushes: [],
		};
		if (!chunkIndices.length) return empty;

		const transferRecord = this.offerRegistry.getTransferRecord(input.transferId);
		if (!transferRecord) {
			throw new Error(`transfer ${input.transferId} not found in runtime registry`);
		}

		if (input.staleBatchIds?.length) {
			for (const batchId of input.staleBatchIds) {
				const batch = this.offerRegistry.getBatch(batchId);
				if (!batch) {
					continue;
				}
				const chunkSet = new Set(chunkIndices);
				const overlaps = batch.chunkIndices.some((index) => chunkSet.has(index));
				if (!overlaps) {
					continue;
				}
				const reason = "batch_produced_no_tasks_before_timeout";
				this.offerRegistry.recordOverseerIntervention({
					interventionType: "batch_stale_timeout",
					transferId: input.transferId,
					batchId: batch.batchId,
					orchestratorId: batch.orchestratorId,
					chunkStart: batch.chunkStart,
					chunkEnd: batch.chunkEnd,
					totalChunks: batch.totalChunks,
					outcome: "stale_batch_timeout",
					reason,
				});
				this.offerRegistry.recordRecoveryOutcomeForChunks({
					transferId: input.transferId,
					chunkIndices: batch.chunkIndices.filter((index) => chunkIndices.includes(index)),
					batchId: batch.batchId,
					orchestratorId: batch.orchestratorId,
					reason,
					outcome: "orch_zero_tasks",
				});
			}
		}

		const metadata = parseTransferMetadata(transferRecord.metadata);
		const testMode = Boolean(metadata?.test_mode);
		const recoveryPool = requiredPoolForTransfer(testMode);
		const transferTotalChunks = transferRecord.totalChunks;
		const batchChunkCount = chunkIndices.length;

		type BundleOk = {
			ok: true;
			selection: SelectionResult;
			batchIds: string[];
			batchSlices: AssignmentCoverageResult["batchSlices"];
			pendingPushes: PendingTaskOfferBatchPush[];
			partial: boolean;
			assignedChunkCount: number;
			remainingChunkCount: number;
			chunksToAssign: number[];
		};
		type BundleBusy = {
			ok: false;
			kind: "no_orchs" | "partial_coverage";
		};

		const tryBundle = async (options?: {
			relaxExclusions?: boolean;
		}): Promise<BundleOk | BundleBusy> => {
			const routeSpecificExclusions = Boolean(input.excludeOrchestratorIdsByChunk?.size);
			const directRecoveryExclusionIds = routeSpecificExclusions ? [] : input.excludeOrchestratorIds;
			const cumulativeExclusions = options?.relaxExclusions
				? { orchestratorIds: [], ownerGroupIds: [] }
				: routeSpecificExclusions
					? { orchestratorIds: [], ownerGroupIds: [] }
					: this.loadCumulativeRecoveryExclusions({
						transferId: input.transferId,
						chunkIndices,
						excludeOrchestratorIds: directRecoveryExclusionIds,
					});

			let selection: SelectionResult;
			let plannedSliceSizes: number[];
			try {
				const recoveryAssignment = this.selectRecoveryAssignment({
					transferId: input.transferId,
					testMode,
					chunkCount: batchChunkCount,
					excludeOrchestratorIds: cumulativeExclusions.orchestratorIds,
					excludeOwnerGroupIds: cumulativeExclusions.ownerGroupIds,
					requiredPool: recoveryPool,
				});
				selection = recoveryAssignment.selection;
				plannedSliceSizes = recoveryAssignment.plannedSliceSizes;
			} catch {
				return { ok: false, kind: "no_orchs" };
			}
			const assignableCount = plannedSliceSizes.reduce((sum, size) => sum + Math.max(0, size), 0);
			if (assignableCount === 0) {
				return { ok: false, kind: "no_orchs" };
			}

			const candidateChunks = shuffledChunkIndices(chunkIndices).slice(0, assignableCount);
			const ownerGroupByOrchestratorId = new Map(
				// full snapshot: exclusions must also cover no-longer-eligible orchestrators
				this.routingRegistry.listSnapshot().map((record) => [record.id, record.owner_group_id]),
			);
			const batchRows = options?.relaxExclusions || !input.excludeOrchestratorIdsByChunk?.size
				? mapChunkBundleToRows(candidateChunks, selection.orchestrators, plannedSliceSizes)
				: mapRecoveryChunkBundleToRows(
					candidateChunks,
					selection.orchestrators,
					plannedSliceSizes,
					input.excludeOrchestratorIdsByChunk,
					ownerGroupByOrchestratorId,
				);
			if (!batchRows.length) {
				return { ok: false, kind: "no_orchs" };
			}
			const chunksToAssign = batchRows.flatMap((row) => row.chunkIndices);
			const partial = chunksToAssign.length < batchChunkCount;

			const batchInputs = batchRows.map((row) => ({
				id: randomUUID(),
				transfer_id: input.transferId,
				orchestrator_id: row.orchestrator.id,
				chunk_indices: row.chunkIndices,
			}));
			const insertedBatches: InsertedTaskOfferBatchRow[] = batchInputs.map((row) => ({
				id: row.id,
				orchestrator_id: row.orchestrator_id,
				chunk_indices: row.chunk_indices,
			}));
			const batchByKey = new Map(
				insertedBatches.map((row) => [
					`${row.orchestrator_id}:${row.chunk_indices.join(",")}`,
					row,
				]),
			);
			const batchIds: string[] = insertedBatches.map((row) => row.id);
			const batchSlices: AssignmentCoverageResult["batchSlices"] = [];
			const pendingPushes: PendingTaskOfferBatchPush[] = [];
			for (const row of batchRows) {
				const inserted = batchByKey.get(`${row.orchestrator.id}:${row.chunkIndices.join(",")}`);
				if (!inserted) continue;
				batchSlices.push(summarizeTaskOfferBatchSlice(row.orchestrator, row.chunkIndices));
				pendingPushes.push({
					batchId: inserted.id,
					orchestrator: row.orchestrator,
					chunkIndices: row.chunkIndices,
					chunkStart: row.chunkStart,
					chunkEnd: row.chunkEnd,
					totalChunks: row.totalChunks,
					chunkSize: transferRecord.chunkSize,
				});
			}

			return {
				ok: true,
				selection,
				batchIds,
				batchSlices,
				pendingPushes,
				partial,
				assignedChunkCount: chunksToAssign.length,
				remainingChunkCount: batchChunkCount - chunksToAssign.length,
				chunksToAssign,
			};
		};

		const groupChunksByReason = (indices: number[]): Map<string, number[]> => {
			const grouped = new Map<string, number[]>();
			for (const index of indices) {
				const reason = reasonForChunk(index);
				const bucket = grouped.get(reason) ?? [];
				bucket.push(index);
				grouped.set(reason, bucket);
			}
			return grouped;
		};

		const finishOk = (result: BundleOk): ReassignChunkBundleResult => {
			const recoveryChunks = result.chunksToAssign.filter(
				(index) => !this.offerRegistry.isChunkIndexCompleted(input.transferId, index),
			);
			this.offerRegistry.releaseRecoveredBatchMembership(input.transferId, recoveryChunks, input.reason);
			const registered = this.registerRuntimeTaskOfferBatches({
				transferId: input.transferId,
				totalBytes: transferRecord.totalBytes,
				totalChunks: transferTotalChunks,
				chunkSize: transferRecord.chunkSize,
				metadata: transferRecord.metadata,
				pendingPushes: result.pendingPushes,
				recovery: true,
			});
			if (!registered) {
				for (const [reason, reasonChunks] of groupChunksByReason(recoveryChunks)) {
					this.offerRegistry.recordRecoveryOutcomeForChunks({
						transferId: input.transferId,
						chunkIndices: reasonChunks,
						reason,
						outcome: "runtime_batch_registration_failed",
						countRecoveryAttempt: true,
					});
				}
				return {
					ok: false,
					batchIds: [],
					batchSlices: [],
					pendingPushes: [],
					kind: "partial_coverage",
				};
			}
			for (const push of result.pendingPushes) {
				for (const [reason, reasonChunks] of groupChunksByReason(push.chunkIndices)) {
					this.offerRegistry.recordRecoveryBatch({
						transferId: input.transferId,
						batchId: push.batchId,
						orchestratorId: push.orchestrator.id,
						chunkIndices: reasonChunks,
						reason,
					});
				}
			}
			beginDistributePhase(input.transferId, result.batchIds.length);
			void this.dispatchTaskOfferBatches(input.transferId, result.pendingPushes).catch((err: unknown) => {
				logger.error(sanitizeStorageDiagnostic({ operation: "bundle_reassignment_dispatch", error: err, transferId: input.transferId }), "bundle reassignment dispatch failed");
			});
			return {
				ok: true,
				batchIds: result.batchIds,
				batchSlices: result.batchSlices,
				pendingPushes: result.pendingPushes,
				selectionRule: result.selection.selectionRule,
				partial: result.partial,
				assignedChunkCount: result.assignedChunkCount,
				remainingChunkCount: result.remainingChunkCount,
			};
		};

		const deferralKeys = chunkIndices.map((chunkIndex) => `${input.transferId}:${chunkIndex}`);
		const exclusionsMayRelax = deferralKeys.every((key) => this.recoveryRoutesDeferredOnce.has(key));
		if (exclusionsMayRelax) {
			logger.warn(
				{
					transferId: input.transferId,
					chunkIndices,
					reason: input.reason,
				},
				"recovery exclusions relaxed after one deferred wave",
			);
		}
		let result = await tryBundle({ relaxExclusions: exclusionsMayRelax });
		if (!result.ok && result.kind === "no_orchs" && !exclusionsMayRelax) {
			for (const key of deferralKeys) this.recoveryRoutesDeferredOnce.add(key);
			logger.warn(
				{
					transferId: input.transferId,
					chunkIndices,
					excludeOrchestratorIds: input.excludeOrchestratorIds,
					reason: input.reason,
					strictRecoveryPool: recoveryPool === "qualified",
				},
				"recovery wave deferred because no alternative participant is eligible",
			);
		}
		if (result.ok) {
			const assigned = new Set(result.chunksToAssign);
			const deferredChunkIndices: number[] = [];
			for (const chunkIndex of chunkIndices) {
				const key = `${input.transferId}:${chunkIndex}`;
				if (assigned.has(chunkIndex)) this.recoveryRoutesDeferredOnce.delete(key);
				else {
					this.recoveryRoutesDeferredOnce.add(key);
					deferredChunkIndices.push(chunkIndex);
				}
			}
			if (deferredChunkIndices.length) {
				logger.warn(
					{
						transferId: input.transferId,
						deferredChunkIndices,
						assignedChunkCount: result.assignedChunkCount,
						reason: input.reason,
					},
					"recovery routes deferred because no strict alternative participant is eligible",
				);
			}
			return finishOk(result);
		}
		for (const [reason, reasonChunks] of groupChunksByReason(chunkIndices)) {
			this.offerRegistry.recordRecoveryOutcomeForChunks({
				transferId: input.transferId,
				chunkIndices: reasonChunks,
				reason,
				outcome: result.kind,
				countRecoveryAttempt: true,
			});
		}
		return {
			ok: false,
			batchIds: [],
			batchSlices: [],
			pendingPushes: [],
			kind: result.kind,
		};
	}

	async assignReadySignedUrlRoutes(input: { transferId: string; deliveryIndices: number[] }): Promise<string[]> {
		const transferRecord = this.offerRegistry.getTransferRecord(input.transferId);
		if (!transferRecord) {
			throw new Error(`transfer ${input.transferId} not found or not assignable in runtime registry`);
		}
		const metadata = parseTransferMetadata(transferRecord.metadata);
		if (!metadata || !isSignedUrlTransferVersion(metadata.transfer_version)) {
			throw new Error(`transfer ${input.transferId} is not a signed URL transfer`);
		}
		const chunkIndices = normalizeChunkIndices(input.deliveryIndices)
			.filter((index) => !this.offerRegistry.isChunkIndexCompleted(input.transferId, index));
		if (!chunkIndices.length) {
			return [];
		}
		const testMode = Boolean(metadata.test_mode);
		const alreadyStarted = Boolean(this.offerRegistry.getTransferSnapshot(input.transferId));
		const streamState = this.offerRegistry.getRouteStreamState(input.transferId);
		if (!streamState) throw new Error(`route stream missing for ${input.transferId}`);
		const ledgerState = this.assignmentLedgers.get(input.transferId) ?? this.createTransferAssignmentLedger({
			transferId: input.transferId,
			testMode,
			totalRoutes: streamState.totalRoutes,
		});
		this.refreshAssignmentLedgerEligibility(ledgerState);
		const reservation = ledgerState.ledger.reserve(chunkIndices.length);
		let coverage: AssignmentCoverageResult;
		try {
			coverage = await this.assignChunkIndexCoverage({
				transferId: input.transferId,
				chunkIndices,
				chunkSize: transferRecord.chunkSize,
				metadata,
				orchestrators: reservation.candidates,
				selectionRule: ledgerState.selection.selectionRule,
				plannedSliceSizes: reservation.sliceSizes,
			});
			if (!coverage.coveredAllChunks) throw new Error(COVERAGE_FAILURE_MESSAGE);
		} catch (error) {
			ledgerState.ledger.release(reservation.id);
			throw error;
		}
		if (!alreadyStarted) {
			this.registerTransferStarted({
				transferId: input.transferId,
				totalBytes: transferRecord.totalBytes,
				totalChunks: transferRecord.totalChunks,
				chunkSize: transferRecord.chunkSize,
				metadata: transferRecord.metadata,
			});
		}
		if (!this.offerRegistry.getInitialAssignmentContext(input.transferId)) {
			this.offerRegistry.setInitialAssignmentContext(input.transferId, {
				transferId: input.transferId,
				pool: ledgerState.selection.preferredPool,
				selectionRule: ledgerState.selection.selectionRule,
				orderedOrchestratorIds: ledgerState.orderedOrchestratorIds,
				orderedHotkeys: ledgerState.orderedHotkeys,
				...(ledgerState.qualifiedAssignmentPlan ? { qualifiedAssignmentPlan: ledgerState.qualifiedAssignmentPlan } : {}),
			});
		}
		const registered = this.registerRuntimeTaskOfferBatches({
			transferId: input.transferId,
			totalBytes: transferRecord.totalBytes,
			totalChunks: transferRecord.totalChunks,
			chunkSize: transferRecord.chunkSize,
			metadata: transferRecord.metadata,
			pendingPushes: coverage.pendingPushes,
		});
		if (!registered) {
			ledgerState.ledger.release(reservation.id);
			throw new Error("runtime_batch_registration_failed");
		}
		ledgerState.ledger.commit(reservation.id);
		beginDistributePhase(input.transferId, coverage.batchIds.length);
		void this.dispatchTaskOfferBatches(input.transferId, coverage.pendingPushes).catch((err: unknown) => {
			logger.error(sanitizeStorageDiagnostic({ operation: "route_assignment_dispatch", error: err, transferId: input.transferId }), "signed route incremental assignment dispatch failed");
		});
		return coverage.batchIds;
	}
	async assignTransfer(transferId: string): Promise<string[]> {
		const existingSnapshot = this.offerRegistry.getTransferSnapshot(transferId);
		if (existingSnapshot) {
			return existingSnapshot.batches.map((batch) => batch.batchId);
		}

		let transferRecord = this.offerRegistry.getTransferRecord(transferId);
		if (!transferRecord) {
			throw new Error(`transfer ${transferId} not found or not assignable in runtime registry`);
		}

		const transfer = {
			id: transferId,
			total_chunks: transferRecord.totalChunks,
			total_bytes: transferRecord.totalBytes,
			chunk_size: transferRecord.chunkSize,
			metadata: transferRecord.metadata,
		};

		const metadata = parseTransferMetadata(transfer.metadata);
		const testMode = Boolean(metadata?.test_mode);
		const logicalTotalChunks =
			isSignedUrlTransferVersion(metadata?.transfer_version) && typeof metadata?.logical_chunk_count === "number"
				? metadata.logical_chunk_count
				: transfer.total_chunks;

		try {
			if (testMode) {
				const selection = this.selectOrchestrators(testMode, [], transferId);
				assertRequiredPoolSelection(selection, testMode, transferId);
				const sliceSizes = allocateChunkSlices(
					selection.orchestrators,
					logicalTotalChunks,
					selection.selectionRule,
				);
				const logic = buildAssignmentLogicSummary(testMode, selection, logicalTotalChunks, sliceSizes);

				const coverage = await this.assignChunkCoverage({
					transferId,
					chunkStart: 0,
					totalChunks: logicalTotalChunks,
					chunkSize: transfer.chunk_size,
					metadata,
					orchestrators: selection.orchestrators,
					selectionRule: selection.selectionRule,
				});

				if (!coverage.coveredAllChunks) {
					logger.error(
						{
							transferId,
							totalChunks: logicalTotalChunks,
							selectedOrchestrators: selection.orchestrators.length,
							assignedChunkCount: coverage.assignedChunkCount,
							testMode,
						},
						"transfer assignment failed because the transfer runtime could not cover every chunk with live orchestrator acknowledgements",
					);
					throw new Error(COVERAGE_FAILURE_MESSAGE);
				}

				const captured = { selection, sliceSizes, logic, coverage };

				logger.info(
					{
						transferId,
						testMode,
						preferredPool: captured.selection.preferredPool,
						selectionRule: captured.selection.selectionRule,
						candidateCounts: captured.selection.counts,
						diagnostics: captured.selection.diagnostics,
						logic: captured.logic,
						qualifiedSelectionNarrative: captured.logic.qualifiedSelectionNarrative,
						distribution: {
							batchCount: captured.coverage.batchIds.length,
							assignedChunkCount: captured.coverage.assignedChunkCount,
							totalChunks: logicalTotalChunks,
							sliceSizes: captured.sliceSizes.filter((size) => size > 0),
						},
						batchSlices: captured.coverage.batchSlices,
					},
					"transfer assignment summary",
				);

				this.registerTransferStarted({
					transferId,
					totalBytes: Number(transfer.total_bytes),
					totalChunks: logicalTotalChunks,
					chunkSize: transfer.chunk_size,
					metadata: transfer.metadata,
				});
				this.offerRegistry.setInitialAssignmentContext(transferId, {
					transferId,
					pool: captured.selection.preferredPool,
					selectionRule: captured.selection.selectionRule,
					orderedOrchestratorIds: captured.selection.orchestrators.map((orchestrator) => orchestrator.id),
					orderedHotkeys: captured.selection.orchestrators.map((orchestrator) => orchestrator.hotkey),
				});
				this.registerRuntimeTaskOfferBatches({
					transferId,
					totalBytes: Number(transfer.total_bytes),
					totalChunks: logicalTotalChunks,
					chunkSize: transfer.chunk_size,
					metadata: transfer.metadata,
					pendingPushes: captured.coverage.pendingPushes,
				});

				beginDistributePhase(transferId, captured.coverage.batchIds.length);
				void this.dispatchTaskOfferBatches(transferId, captured.coverage.pendingPushes).catch((err: unknown) => {
					logger.error(sanitizeStorageDiagnostic({ operation: "transfer_assignment_dispatch", error: err, transferId }), "transfer assignment dispatch failed");
				});

				return captured.coverage.batchIds;
			}

			type CapturedAssign = {
				batchIds: string[];
				selection: SelectionResult;
				coverage: AssignmentCoverageResult;
				sliceSizes: number[];
			};

			let captured: CapturedAssign | undefined;

			try {
				const selection = this.selectOrchestrators(false, [], transferId);
				assertRequiredPoolSelection(selection, false, transferId);

				const slicePlan = buildQualifiedPoolSlicePlan({
					transferId,
					baseOrchs: selection.orchestrators,
					totalChunks: logicalTotalChunks,
					preferredHotkeys: selection.counts.preferred,
					ownerGroupsInPool: countDistinctOwnerGroupsInPool(selection.orchestrators),
				});
				selection.orchestrators = slicePlan.orchestrators;
				selection.qualifiedAssignmentPlan = slicePlan.plan;

				const coverage = await this.assignChunkCoverage({
					transferId,
					chunkStart: 0,
					totalChunks: logicalTotalChunks,
					chunkSize: transfer.chunk_size,
					metadata,
					orchestrators: slicePlan.orchestrators,
					selectionRule: selection.selectionRule,
					plannedSliceSizes: slicePlan.sliceSizes,
				});

				if (!coverage.coveredAllChunks) {
					throw new Error("__ASSIGN_COVERAGE_FAIL__");
				}

				captured = {
					batchIds: coverage.batchIds,
					selection,
					coverage,
					sliceSizes: slicePlan.sliceSizes,
				};
			} catch (e) {
				if ((e as Error)?.message === "__ASSIGN_COVERAGE_FAIL__") {
					logger.error(
						{
							transferId,
							totalChunks: logicalTotalChunks,
							testMode: false,
						},
						"transfer assignment failed because the transfer runtime could not cover every chunk with live orchestrator acknowledgements",
					);
					throw new Error(COVERAGE_FAILURE_MESSAGE);
				}
				throw e;
			}

			if (!captured) {
				throw new Error("assignTransfer qualified path produced no captured result");
			}

			const logic = buildAssignmentLogicSummary(
				false,
				captured.selection,
				logicalTotalChunks,
				captured.sliceSizes,
			);

			logger.info(
				{
					transferId,
					testMode: false,
					preferredPool: captured.selection.preferredPool,
					selectionRule: captured.selection.selectionRule,
					candidateCounts: captured.selection.counts,
					diagnostics: captured.selection.diagnostics,
					logic,
					qualifiedSelectionNarrative: logic.qualifiedSelectionNarrative,
					distribution: {
						batchCount: captured.coverage.batchIds.length,
						assignedChunkCount: captured.coverage.assignedChunkCount,
						totalChunks: logicalTotalChunks,
						sliceSizes: captured.sliceSizes.filter((size) => size > 0),
					},
					batchSlices: captured.coverage.batchSlices,
				},
				"transfer assignment summary",
			);

			this.registerTransferStarted({
				transferId,
				totalBytes: Number(transfer.total_bytes),
				totalChunks: logicalTotalChunks,
				chunkSize: transfer.chunk_size,
				metadata: transfer.metadata,
			});
			const qualifiedAssignmentPlan = captured.selection.qualifiedAssignmentPlan as QualifiedAssignmentPlan;
			const orchestratorIdByHotkey = new Map(captured.selection.orchestrators.map((orchestrator) => [orchestrator.hotkey, orchestrator.id]));
			this.offerRegistry.setInitialAssignmentContext(transferId, {
				transferId,
				pool: captured.selection.preferredPool,
				selectionRule: captured.selection.selectionRule,
				orderedOrchestratorIds: qualifiedAssignmentPlan.baseHotkeysOrdered
					.map((hotkey) => orchestratorIdByHotkey.get(hotkey))
					.filter((id): id is string => Boolean(id)),
				orderedHotkeys: qualifiedAssignmentPlan.baseHotkeysOrdered,
				qualifiedAssignmentPlan,
			});
			this.registerRuntimeTaskOfferBatches({
				transferId,
				totalBytes: Number(transfer.total_bytes),
				totalChunks: logicalTotalChunks,
				chunkSize: transfer.chunk_size,
				metadata: transfer.metadata,
				pendingPushes: captured.coverage.pendingPushes,
			});

			beginDistributePhase(transferId, captured.coverage.batchIds.length);
			void this.dispatchTaskOfferBatches(transferId, captured.coverage.pendingPushes).catch((err: unknown) => {
				logger.error(sanitizeStorageDiagnostic({ operation: "transfer_assignment_dispatch", error: err, transferId }), "transfer assignment dispatch failed");
			});

			return captured.batchIds;
		} catch (error) {
			if (isTerminalAssignmentFailure(error)) {
				this.failTransferAssignment(transferId, error.message);
			}
			throw error;
		}
	}

	/** @internal Unit-test hook for assignChunkCoverage. */
	async testAssignChunkCoverage(input: {
		transferId: string;
		chunkStart: number;
		totalChunks: number;
		chunkSize: number;
		metadata: TransferMetadata | null;
		orchestrators: OrchestratorCandidate[];
		selectionRule: SelectionResult["selectionRule"];
		plannedSliceSizes?: number[];
	}): Promise<AssignmentCoverageResult> {
		return this.assignChunkCoverage(input);
	}

	async expireTransferBatch(input: { batchId: string; reason: string }): Promise<void> {
		this.offerRegistry.updateBatchStatus(input.batchId, "expired", input.reason);
	}
}
