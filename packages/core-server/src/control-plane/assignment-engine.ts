/**
 * Transfer assignment: selects orchestrators by PRISM tier and score, allocates chunk slices,
 * persists assignments, creates task-offer batches, and notifies orchestrators through the live gateway.
 *
 * This transparency copy inlines supporting helpers that live in separate modules in the full
 * BeamCore tree (assignment math, transfer metadata, qualified base-pool slicing, task-offer delivery,
 * distribute-phase metrics, worker gateway interfaces, and runtime registry interfaces).
 */

import { randomUUID } from "node:crypto";

const logger = {
	info(_data: unknown, _message?: string): void {},
	warn(_data: unknown, _message?: string): void {},
	error(_data: unknown, _message?: string): void {},
	debug(_data: unknown, _message?: string): void {},
};

const ASSIGNMENT_DEFAULTS = {
	WS_ASSIGNMENT_PUSH_RETRY_ATTEMPTS: 3,
	WS_ASSIGNMENT_PUSH_RETRY_CONCURRENCY: 32,
	TRANSFER_TASK_OFFER_BATCH_SIZE: 32,
	TRANSFER_RECOVERY_SPEED_WINDOW_SECONDS: 15,
	TRANSFER_RECOVERY_TASK_PRIORITY: 10,
	SIGNED_URL_MIN_TTL_SECONDS: 600,
};

const env = ASSIGNMENT_DEFAULTS;

type SqlTag = <T = unknown>(strings: TemplateStringsArray, ...values: unknown[]) => Promise<T>;

export type Db = SqlTag & {
	begin?: <T>(callback: (sql: SqlTag) => Promise<T>) => Promise<T>;
	json?: (value: unknown) => unknown;
	unsafe?: (fragment: string) => unknown;
};

type DbConn = Db;

/** Minimal WebSocket shape used by the orchestrator registry (production uses `ws`). */
interface OrchestratorWebSocket {
	readonly readyState: number;
	send(data: string): void;
}

type OrchestratorSession =
	| { kind: "direct"; ws: OrchestratorWebSocket }
	| { kind: "relay"; ws: OrchestratorWebSocket };

const orchestratorSockets = new Map<string, OrchestratorSession>();

function isOrchestratorConnected(hotkey: string): boolean {
	const session = orchestratorSockets.get(hotkey);
	return session?.ws.readyState === 1;
}

function orchestratorIsEligible(hotkey: string): boolean {
	return isOrchestratorConnected(hotkey);
}

function connectedHotkeys(): string[] {
	return [...orchestratorSockets.entries()]
		.filter(([, session]) => session.ws.readyState === 1)
		.map(([hotkey]) => hotkey);
}

function pushToOrchestrator(hotkey: string, msg: unknown): boolean {
	const session = orchestratorSockets.get(hotkey);
	if (!session || session.ws.readyState !== 1) return false;
	if (session.kind === "relay") {
		session.ws.send(JSON.stringify({ type: "relay_to_orch", hotkey, payload: msg }));
		return true;
	}
	session.ws.send(JSON.stringify(msg));
	return true;
}

export interface OrchestratorCandidate {
	id: string;
	hotkey: string;
	gateway_type: string | null;
	gateway_url: string | null;
	uid: number | null;
	prism_final_score: string;
	prism_confidence_score: string;
	prism_pool: "qualifying" | "qualified";
	owner_group_id: string | null;
}

export type AssignmentSelectionRule =
	| "prism_final_score_desc"
	| "qualifying_equal_share_rotation"
	| "recovery_transfer_speed";

export interface SelectionResult {
	orchestrators: OrchestratorCandidate[];
	preferredPool: "qualifying" | "qualified";
	selectionRule: AssignmentSelectionRule;
	qualifiedAssignmentPlan?: unknown;
	counts: {
		queried: number;
		websocketReady: number;
		eligible: number;
		preferred: number;
		selected: number;
	};
	diagnostics: {
		queriedHotkeys: string[];
		websocketReadyHotkeys: string[];
		missingWebsocketHotkeys: string[];
		connectedRegistryHotkeys: string[];
	};
}

export interface StaleAssignmentBundle {
	transferId: string;
	assignmentId: string;
	orchestratorId: string;
	chunkStart: number;
	chunkEnd: number;
	totalChunks: number;
	reason: string;
}

export interface TransferOrchestratorSpeed {
	orchestrator_id: string;
	median_relay_s: number | null;
	completed_count: number;
	last_completed_at: Date;
}

export interface RuntimeRegisterOfferInput {
	taskId: string;
	transferId: string;
	totalChunks: number;
	workerId: string | null;
	attemptId: string;
	chunkIndex: number;
	offeredAt: Date;
	assignmentId: string;
	orchestratorId: string;
	orchestratorHotkey: string;
	chunkSize: number;
	executionContext: Record<string, unknown>;
	idempotencyKey?: string;
	sourceId: string | null;
	destinationId: string | null;
	priority: number;
}

export interface RuntimeAssignmentSnapshot {
	assignmentId: string;
	transferId: string;
	orchestratorId: string;
	orchestratorHotkey: string;
	workerGatewayType: string | null;
	workerGatewayUrl: string | null;
	chunkStart: number;
	chunkEnd: number;
	totalChunks: number;
	totalBytes: number;
	chunkSize: number;
	metadata: unknown;
	status: "assigned" | "in_progress" | "completed" | "failed" | "expired";
}

export interface TransferRecordView {
	transferId: string;
	totalChunks: number;
	totalBytes: number;
	chunkSize: number;
	metadata: unknown;
}

export interface TransferRuntimeRegistry {
	failTransfer(transferId: string, reason: string): void;
	createAssignments(input: Omit<RuntimeAssignmentSnapshot, "status">[]): { ok: boolean; reason?: string };
	registerTransferStarted(input: TransferRecordView): void;
	getRecoveryExclusions(transferId: string, chunkIndices: number[], excludeOrchestratorIds: string[], routingRegistry: OrchestratorRoutingRegistry): { orchestratorIds: string[]; ownerGroupIds: string[] };
	getRecoveryOrchestratorSpeeds(transferId: string, windowSeconds: number): TransferOrchestratorSpeed[];
	recordOverseerIntervention(input: Record<string, unknown>): void;
	getAssignment(assignmentId: string): RuntimeAssignmentSnapshot | undefined;
	filterRecoverableChunkIndices(transferId: string, requestedChunks: number[]): number[];
	getTransferRecord(transferId: string): TransferRecordView | undefined;
	isRestartRecoveredAssignment(assignmentId: string): boolean;
	recordRecoveryOutcomeForRange(input: Record<string, unknown>): void;
	isChunkIndexCompleted(transferId: string, chunkIndex: number): boolean;
	expireAssignments(transferId: string, chunkIndices: number[], reason: string): void;
	recordRecoveryOutcomeForChunks(input: Record<string, unknown>): void;
	recordRecoveryAssignment(input: Record<string, unknown>): void;
	getTransferSnapshot(transferId: string): TerminalTransferSnapshot | undefined;
	getActiveTasksForChunks(transferId: string, chunkIndices: number[]): RuntimeTaskSnapshot[];
	updateAssignmentStatus(assignmentId: string, status: string, reason?: string): void;
	isRecoveryAssignment(input: Record<string, unknown>): boolean;
	createTaskOffers(input: RuntimeRegisterOfferInput[]): { ok: boolean; reason?: string; tasks: Array<{ id: string; attemptId: string; chunkIndex: number }> };
	expireRecoveryZeroOfferAssignment(input: Record<string, unknown>): boolean;
	markAssignmentInProgress(assignmentId: string, offeredAt: Date): void;
	recordOfferDelivery(taskId: string, accepted: boolean, reason: string | null): void;
	recordFailure(input: Record<string, unknown>): void;
	invalidateTask(taskId: string): void;
}

export interface RuntimeTaskSnapshot {
	taskId: string;
	transferId: string;
	totalChunks: number;
	assignmentId: string | null;
	orchestratorId: string | null;
	orchestratorHotkey: string | null;
	chunkIndex: number;
	chunkSize: number | null;
	workerId: string | null;
	attemptId: string;
	state: string;
}

export interface TerminalTransferSnapshot {
	transferId: string;
	status: "cancelled" | "failed" | "completed";
	reason: string;
	totalChunks: number;
	completedChunks: number;
	assignments: RuntimeAssignmentSnapshot[];
	tasks: RuntimeTaskSnapshot[];
	capturedAt: Date;
}

export interface OrchestratorRoutingRegistry {
	listReadyCandidates(excludeIds: string[], excludeOwnerGroupIds: string[]): OrchestratorCandidate[];
	getOwnerGroupId(orchestratorId: string): string | null;
}

export interface DeliveryResult {
	accepted: boolean;
	reason?: string;
}

export interface TaskOfferBatchOffer {
	task_id: string;
	offer_id: string;
	chunk_size: number;
	source_url: string;
	dest_url: string;
	urls_expires_at: string;
	etag_required?: boolean;
	source_headers?: Record<string, string>;
	dest_headers?: Record<string, string>;
}

export interface WorkerGatewayClient {
	deliverTaskOfferBatch(orchestratorHotkey: string, batch: { batch_id: string; offers: TaskOfferBatchOffer[] }, channel: "orchestrator_ws"): Promise<DeliveryResult>;
}

export interface SignedChunkRoute {
	source_id: string;
	destination_id: string;
	chunk_index: number;
	delivery_index?: number;
	source_offset?: number;
	chunk_size: number;
	source_url: string;
	dest_url: string;
	expires_at?: string;
	headers?: Record<string, string>;
	metadata?: Record<string, unknown>;
}

function routeKey(sourceId: string, chunkIndex: number, destinationId: string): string {
	return `${sourceId}:${chunkIndex}:${destinationId}`;
}

function assertSignedUrlsFresh(input: { expiresAt?: string; minTtlSeconds: number; label: string }): void {
	if (!input.expiresAt) return;
	const expiresAt = Date.parse(input.expiresAt);
	if (!Number.isFinite(expiresAt)) throw new Error(`${input.label} has invalid expiration`);
	if (expiresAt - Date.now() < input.minTtlSeconds * 1000) {
		throw new Error(`${input.label} signed URLs expire too soon`);
	}
}

function isS3Config(value: unknown): boolean {
	return Boolean(value && typeof value === "object" && "bucket" in value && "key" in value);
}

async function presignGet(value: unknown): Promise<string> {
	return fallbackUrl(value);
}

async function presignPut(value: unknown, _chunkIndex: number): Promise<string> {
	return fallbackUrl(value);
}

function logWorkerTaskOfferDelivery(_input: Record<string, unknown>): void {}

function computePrismSlices(scores: number[], totalChunks: number): number[] {
	if (scores.length === 0 || totalChunks <= 0) return [];

	const totalScore = scores.reduce((sum, s) => sum + s, 0);
	if (totalScore <= 0) {
		const base = Math.floor(totalChunks / scores.length);
		const rem = totalChunks % scores.length;
		return scores.map((_, i) => base + (i < rem ? 1 : 0));
	}

	const quotas = scores.map((s) => (s / totalScore) * totalChunks);
	const slices = quotas.map((q) => Math.floor(q));
	let leftover = totalChunks - slices.reduce((a, b) => a + b, 0);

	const order = quotas
		.map((q, i) => ({ i, frac: q - Math.floor(q), score: scores[i]! }))
		.sort((a, b) => b.frac - a.frac || b.score - a.score || a.i - b.i);

	for (const { i } of order) {
		if (leftover <= 0) break;
		slices[i]!++;
		leftover--;
	}

	return slices;
}
export interface TransferMetadata {
	transfer_version?: "legacy" | "signed_url_v1" | "signed_url_v2";
	sources?: unknown[];
	destinations?: unknown[];
	chunk_plan?: unknown[];
	chunk_routes?: unknown[];
	logical_chunk_count?: number;
	delivery_task_count?: number;
	urls_expires_at?: string;
	name?: string;
	test_mode?: boolean;
}

export function parseTransferMetadata(value: unknown): TransferMetadata | null {
	if (!value) return null;
	if (typeof value === "string") {
		try {
			const parsed = JSON.parse(value) as TransferMetadata;
			return parsed && typeof parsed === "object" ? parsed : null;
		} catch {
			return null;
		}
	}
	return typeof value === "object" ? (value as TransferMetadata) : null;
}

export function isSignedUrlTransferVersion(version: unknown): boolean {
	return version === "signed_url_v1" || version === "signed_url_v2";
}

export function isSignedUrlTransferMetadata(metadata: unknown): boolean {
	return isSignedUrlTransferVersion(parseTransferMetadata(metadata)?.transfer_version);
}

export function isSignedUrlV2TransferMetadata(metadata: unknown): boolean {
	return parseTransferMetadata(metadata)?.transfer_version === "signed_url_v2";
}

/** Chunk indices `0…N−1` for overseer coverage, finalize, and missing-index scans. */
export function resolveOverseerTotalChunks(totalChunks: number, metadata: unknown): number {
	const meta = parseTransferMetadata(metadata);
	if (isSignedUrlTransferVersion(meta?.transfer_version) && typeof meta?.logical_chunk_count === "number") {
		return meta.logical_chunk_count;
	}
	return totalChunks;
}

export function numericValue(value: string | number | null | undefined, fallback = 0): number {
	if (typeof value === "number") return Number.isFinite(value) ? value : fallback;
	if (typeof value === "string") {
		const parsed = Number(value);
		return Number.isFinite(parsed) ? parsed : fallback;
	}
	return fallback;
}

export function allocateChunkSlices(
	candidates: OrchestratorCandidate[],
	totalChunks: number,
	selectionRule: AssignmentSelectionRule,
): number[] {
	if (totalChunks <= 0 || candidates.length === 0) return [];

	const activeCount = Math.min(candidates.length, totalChunks);
	const sliceSizes = new Array(candidates.length).fill(0);
	const activeCandidates = candidates.slice(0, activeCount);
	if (selectionRule === "qualifying_equal_share_rotation") {
		const base = Math.floor(totalChunks / activeCount);
		const remainder = totalChunks % activeCount;
		for (let i = 0; i < activeCount; i++) {
			sliceSizes[i] = base + (i < remainder ? 1 : 0);
		}
		return sliceSizes;
	}

	const scores = activeCandidates.map((c) => Math.max(0, numericValue(c.prism_final_score)));
	const activeSlices = computePrismSlices(scores, totalChunks);
	for (let i = 0; i < activeCount; i++) {
		sliceSizes[i] = activeSlices[i] ?? 0;
	}
	return sliceSizes;
}

export function buildRecoveryRelayWeight(medianRelayS: number): number {
	return 1 / Math.max(medianRelayS, 0.001);
}

/** Fewest speed-ranked orchs that can hold all chunks under per-orch fair share. */
export function resolveRecoveryActiveOrchCount(orchCount: number, chunkCount: number): number {
	if (chunkCount <= 0 || orchCount <= 0) return 0;
	let activeCount = Math.min(orchCount, chunkCount);
	for (let i = 0; i < 16; i++) {
		const maxSlice = Math.ceil(chunkCount / activeCount);
		const next = Math.min(orchCount, Math.max(1, Math.ceil(chunkCount / maxSlice)));
		if (next === activeCount) break;
		activeCount = next;
	}
	return activeCount;
}

export function allocateRecoverySpeedSlices(input: {
	orchCount: number;
	chunkCount: number;
	speedWeights: number[];
}): number[] {
	const { orchCount, chunkCount } = input;
	if (chunkCount <= 0 || orchCount <= 0) return [];

	const activeCount = resolveRecoveryActiveOrchCount(orchCount, chunkCount);
	const sliceSizes = new Array(orchCount).fill(0);

	const weights = input.speedWeights.slice(0, orchCount);
	while (weights.length < orchCount) {
		weights.push(1);
	}
	const activeWeights = weights.slice(0, activeCount).map((weight) => Math.max(weight, 0.0001));
	const activeSlices = computePrismSlices(activeWeights, chunkCount);
	for (let i = 0; i < activeCount; i++) {
		sliceSizes[i] = activeSlices[i] ?? 0;
	}

	const maxSlice = Math.ceil(chunkCount / activeCount);
	let overflow = 0;
	for (let i = 0; i < activeCount; i++) {
		const size = sliceSizes[i] ?? 0;
		if (size > maxSlice) {
			overflow += size - maxSlice;
			sliceSizes[i] = maxSlice;
		}
	}

	let guard = 0;
	while (overflow > 0 && guard < activeCount * chunkCount) {
		guard += 1;
		let moved = false;
		for (let i = 0; i < activeCount && overflow > 0; i++) {
			if ((sliceSizes[i] ?? 0) < maxSlice) {
				sliceSizes[i] = (sliceSizes[i] ?? 0) + 1;
				overflow -= 1;
				moved = true;
			}
		}
		if (!moved) break;
	}

	return sliceSizes;
}

export interface OwnerGroupAllocationSlot {
	ownerGroupId: string | null;
	baseMemberOrchestratorIds: string[];
	baseMemberHotkeys: string[];
	memberActualPrismScores: number[];
	averagePrismScore: number;
	interGroupSliceSize: number;
	memberSliceSizes: number[];
}

export interface QualifiedAssignmentPlan {
	version: 2;
	transferId: string;
	pool: "qualified";
	selectionRule: "prism_final_score_desc";
	counts: {
		preferredHotkeys: number;
		ownerGroupsInPool: number;
		baseHotkeyCount: number;
		virtualCompetitorCount: number;
	};
	baseHotkeysOrdered: string[];
	ownerGroupAllocationSlots: OwnerGroupAllocationSlot[];
	deliveries: Array<{
		orchestratorId: string;
		hotkey: string;
		ownerGroupId: string | null;
		chunkSliceSize: number;
		prismFinalScoreActual: number;
		interGroupSliceSize: number;
		averagePrismScoreForGroup: number;
	}>;
}

export interface QualifiedPoolSlicePlan {
	orchestrators: OrchestratorCandidate[];
	sliceSizes: number[];
	plan: QualifiedAssignmentPlan;
}

interface BasePoolGroup {
	ownerGroupId: string | null;
	members: OrchestratorCandidate[];
}

function compareBasePoolUidAscNullsLast(a: number | null, b: number | null): number {
	if (a === null && b === null) return 0;
	if (a === null) return 1;
	if (b === null) return -1;
	return a - b;
}

function sortMembers(members: OrchestratorCandidate[]): OrchestratorCandidate[] {
	return [...members].sort(
		(a, b) =>
			compareBasePoolUidAscNullsLast(a.uid, b.uid) ||
			a.hotkey.localeCompare(b.hotkey) ||
			a.id.localeCompare(b.id),
	);
}

function partitionBasePoolGroups(baseOrchs: OrchestratorCandidate[]): BasePoolGroup[] {
	const byKey = new Map<string, OrchestratorCandidate[]>();
	for (const orch of baseOrchs) {
		const key = orch.owner_group_id ?? `singleton:${orch.id}`;
		const list = byKey.get(key) ?? [];
		list.push(orch);
		byKey.set(key, list);
	}
	return [...byKey.entries()].map(([key, members]) => ({
		ownerGroupId: key.startsWith("singleton:") ? null : key,
		members: sortMembers(members),
	}));
}

function averagePrismScore(members: OrchestratorCandidate[]): number {
	const scores = members.map((m) => Math.max(0, numericValue(m.prism_final_score)));
	if (!scores.length) return 0;
	return scores.reduce((sum, s) => sum + s, 0) / scores.length;
}

function virtualCandidateForGroup(group: BasePoolGroup): OrchestratorCandidate {
	const rep = group.members[0]!;
	const avg = averagePrismScore(group.members);
	return {
		...rep,
		prism_final_score: String(avg),
	};
}

export function countDistinctOwnerGroupsInPool(candidates: OrchestratorCandidate[]): number {
	const keys = new Set<string>();
	for (const c of candidates) {
		keys.add(c.owner_group_id ?? `singleton:${c.id}`);
	}
	return keys.size;
}

export function buildQualifiedPoolSlicePlan(input: {
	transferId: string;
	baseOrchs: OrchestratorCandidate[];
	totalChunks: number;
	preferredHotkeys: number;
	ownerGroupsInPool: number;
}): QualifiedPoolSlicePlan {
	const basePoolGroups = partitionBasePoolGroups(input.baseOrchs);
	const virtualCompetitors = basePoolGroups.map((g) => virtualCandidateForGroup(g));
	const interGroupSlices = allocateChunkSlices(virtualCompetitors, input.totalChunks, "prism_final_score_desc");

	const deliveries: QualifiedAssignmentPlan["deliveries"] = [];
	const flatOrchs: OrchestratorCandidate[] = [];
	const flatSlices: number[] = [];
	const ownerGroupAllocationSlots: OwnerGroupAllocationSlot[] = [];

	for (let gi = 0; gi < basePoolGroups.length; gi++) {
		const group = basePoolGroups[gi]!;
		const interTotal = interGroupSlices[gi] ?? 0;
		const memberScores = group.members.map((m) => Math.max(0, numericValue(m.prism_final_score)));
		const memberSlices =
			interTotal > 0 && group.members.length > 0
				? computePrismSlices(memberScores, interTotal)
				: memberScores.map(() => 0);
		const avg = averagePrismScore(group.members);

		ownerGroupAllocationSlots.push({
			ownerGroupId: group.ownerGroupId,
			baseMemberOrchestratorIds: group.members.map((m) => m.id),
			baseMemberHotkeys: group.members.map((m) => m.hotkey),
			memberActualPrismScores: memberScores,
			averagePrismScore: avg,
			interGroupSliceSize: interTotal,
			memberSliceSizes: memberSlices,
		});

		for (let mi = 0; mi < group.members.length; mi++) {
			const member = group.members[mi]!;
			const slice = memberSlices[mi] ?? 0;
			if (slice <= 0) continue;
			flatOrchs.push(member);
			flatSlices.push(slice);
			deliveries.push({
				orchestratorId: member.id,
				hotkey: member.hotkey,
				ownerGroupId: group.ownerGroupId,
				chunkSliceSize: slice,
				prismFinalScoreActual: memberScores[mi] ?? 0,
				interGroupSliceSize: interTotal,
				averagePrismScoreForGroup: avg,
			});
		}
	}

	return {
		orchestrators: flatOrchs,
		sliceSizes: flatSlices,
		plan: {
			version: 2,
			transferId: input.transferId,
			pool: "qualified",
			selectionRule: "prism_final_score_desc",
			counts: {
				preferredHotkeys: input.preferredHotkeys,
				ownerGroupsInPool: input.ownerGroupsInPool,
				baseHotkeyCount: input.baseOrchs.length,
				virtualCompetitorCount: basePoolGroups.length,
			},
			baseHotkeysOrdered: input.baseOrchs.map((o) => o.hotkey),
			ownerGroupAllocationSlots,
			deliveries,
		},
	};
}

export interface DistributePhaseMetrics {
	transferId: string;
	assignmentCount: number;
	startedAtMs: number;
	wsPushSuccess: number;
	wsPushFailed: number;
	chunkAssignmentHandlerMs: number[];
	initialChunkAssignmentHandlerMs: number[];
	recoveryChunkAssignmentHandlerMs: number[];
	staleRecoveryCount: number;
	resendPushCount: number;
}

const trackers = new Map<string, DistributePhaseMetrics>();

function percentile(sorted: number[], p: number): number | null {
	if (!sorted.length) return null;
	const index = Math.min(sorted.length - 1, Math.max(0, Math.ceil((p / 100) * sorted.length) - 1));
	return sorted[index] ?? null;
}

export function beginDistributePhase(transferId: string, assignmentCount: number): void {
	trackers.set(transferId, {
		transferId,
		assignmentCount,
		startedAtMs: Date.now(),
		wsPushSuccess: 0,
		wsPushFailed: 0,
		chunkAssignmentHandlerMs: [],
		initialChunkAssignmentHandlerMs: [],
		recoveryChunkAssignmentHandlerMs: [],
		staleRecoveryCount: 0,
		resendPushCount: 0,
	});
}

export function recordWsPushResult(transferId: string, success: boolean): void {
	const tracker = trackers.get(transferId);
	if (!tracker) return;
	if (success) tracker.wsPushSuccess += 1;
	else tracker.wsPushFailed += 1;
}

export function recordChunkAssignmentHandlerMs(
	transferId: string,
	ms: number,
	phase: "initial" | "recovery" = "initial",
): void {
	const tracker = trackers.get(transferId);
	if (!tracker) return;
	tracker.chunkAssignmentHandlerMs.push(ms);
	if (phase === "recovery") {
		tracker.recoveryChunkAssignmentHandlerMs.push(ms);
	} else {
		tracker.initialChunkAssignmentHandlerMs.push(ms);
	}
}

export function recordStaleRecovery(transferId: string): void {
	const tracker = trackers.get(transferId);
	if (!tracker) return;
	tracker.staleRecoveryCount += 1;
}

export function recordResendPush(transferId: string, count = 1): void {
	const tracker = trackers.get(transferId);
	if (!tracker) return;
	tracker.resendPushCount += count;
}

export function getDistributePhaseMetrics(transferId: string): DistributePhaseMetrics | undefined {
	return trackers.get(transferId);
}

export function summarizeHandlerMs(samples: number[]): { p50: number | null; p95: number | null; count: number } {
	const sorted = [...samples].sort((a, b) => a - b);
	return {
		count: sorted.length,
		p50: percentile(sorted, 50),
		p95: percentile(sorted, 95),
	};
}

export function logDistributePhaseSummary(transferId: string, extra?: Record<string, unknown>): void {
	const tracker = trackers.get(transferId);
	if (!tracker) return;
	const handler = summarizeHandlerMs(tracker.chunkAssignmentHandlerMs);
	logger.info(
		{
			transferId,
			assignmentCount: tracker.assignmentCount,
			elapsedMs: Date.now() - tracker.startedAtMs,
			wsPushSuccess: tracker.wsPushSuccess,
			wsPushFailed: tracker.wsPushFailed,
			resendPushCount: tracker.resendPushCount,
			staleRecoveryCount: tracker.staleRecoveryCount,
			chunkAssignmentsHandlerCount: handler.count,
			chunkAssignmentsHandlerP50Ms: handler.p50 != null ? Math.round(handler.p50) : null,
			chunkAssignmentsHandlerP95Ms: handler.p95 != null ? Math.round(handler.p95) : null,
			...extra,
		},
		"distribute phase metrics",
	);
}

type PreparedTaskOffer = {
	sourceId: string | null;
	destinationId: string | null;
	chunkIndex: number;
	chunkSize: number;
	executionContext: Record<string, unknown>;
	idempotencyKey: string;
	workerOffer: Omit<TaskOfferBatchOffer, "task_id" | "offer_id">;
};

export interface DeliveredTaskOffer {
	id: string;
	offerId: string;
	chunkIndex: number;
	chunkSize: number;
	deliveryAccepted: boolean;
	deliveryReason?: string;
}

export type TaskOfferBatchDeliveryResult = {
	ok: boolean;
	tasks?: DeliveredTaskOffer[];
	code?: string;
	message?: string;
};

export interface DeliverTaskOfferBatchInput {
	gatewayClient: WorkerGatewayClient;
	batchId: string;
	expectedOrchestratorId?: string;
	recoveryBatch?: boolean;
	offerRegistry: TransferRuntimeRegistry;
}

function routeMetadata(route: SignedChunkRoute): Record<string, unknown> {
	return route.metadata && typeof route.metadata === "object" ? route.metadata : {};
}

function metadataString(meta: Record<string, unknown>, key: string): string | undefined {
	const value = meta[key];
	if (typeof value === "string") return value;
	if (typeof value === "number") return String(value);
	return undefined;
}

function metadataNumber(meta: Record<string, unknown>, key: string): number | undefined {
	const value = meta[key];
	const parsed = typeof value === "number" ? value : typeof value === "string" ? Number(value) : Number.NaN;
	return Number.isInteger(parsed) && parsed >= 0 ? parsed : undefined;
}

function transferTaskIdempotencyKey(
	transferId: string,
	assignment: { chunkIndex: number; sourceId: string | null; destinationId: string | null },
): string {
	if (assignment.sourceId && assignment.destinationId) {
		return `${transferId}:${assignment.sourceId}:${assignment.chunkIndex}:${assignment.destinationId}`;
	}
	return `${transferId}:${assignment.chunkIndex}`;
}

function fallbackUrl(value: unknown): string {
	if (!value || typeof value !== "object") {
		return "";
	}

	const maybeUrl = (value as { url?: unknown }).url;
	return typeof maybeUrl === "string" ? maybeUrl : "";
}

function fallbackHeaders(value: unknown): Record<string, string> | undefined {
	if (!value || typeof value !== "object") {
		return undefined;
	}
	const headers = (value as { headers?: unknown }).headers;
	if (!headers || typeof headers !== "object" || Array.isArray(headers)) {
		return undefined;
	}
	const clean: Record<string, string> = {};
	for (const [key, raw] of Object.entries(headers as Record<string, unknown>)) {
		if (typeof raw === "string") {
			clean[key] = raw;
		}
	}
	return Object.keys(clean).length ? clean : undefined;
}

function offerExpiry(metaExpiry?: string): string {
	return metaExpiry ?? new Date(Date.now() + ASSIGNMENT_DEFAULTS.SIGNED_URL_MIN_TTL_SECONDS * 1_000).toISOString();
}

function resolveSignedRoutesForBatchChunk(
	signedRoutesByKey: Map<string, SignedChunkRoute>,
	routesByChunkIndex: Map<number, SignedChunkRoute[]>,
	routesByDeliveryIndex: Map<number, SignedChunkRoute>,
	chunkIndex: number,
): SignedChunkRoute[] {
	const deliveryRoute = routesByDeliveryIndex.get(chunkIndex);
	if (deliveryRoute) {
		return [deliveryRoute];
	}
	const exactRoutes = routesByChunkIndex.get(chunkIndex) ?? [];
	if (exactRoutes.length) {
		return exactRoutes;
	}
	const exact = [...signedRoutesByKey.values()].find((route) => route.chunk_index === chunkIndex);
	return exact ? [exact] : [];
}

async function prepareTaskOffers(input: {
	batchId: string;
	offerRegistry: TransferRuntimeRegistry;
}): Promise<PreparedTaskOffer[]> {
	const runtimeBatch = input.offerRegistry.getAssignment(input.batchId);
	if (!runtimeBatch) {
		throw new Error(`task offer batch ${input.batchId} not found`);
	}
	const {
		transferId,
		workerGatewayUrl,
		totalBytes,
		chunkSize,
		chunkStart,
		chunkEnd,
		totalChunks,
		metadata,
	} = runtimeBatch;

	const meta = parseTransferMetadata(metadata);
	const rawSources: unknown[] = (meta?.sources as unknown[]) ?? [];
	const rawDests: unknown[] = (meta?.destinations as unknown[]) ?? [];
	const signedRoutesByKey = new Map<string, SignedChunkRoute>();
	const signedRoutesByChunkIndex = new Map<number, SignedChunkRoute[]>();
	const signedRoutesByDeliveryIndex = new Map<number, SignedChunkRoute>();
	if (meta && isSignedUrlTransferVersion(meta.transfer_version)) {
		for (const route of (meta.chunk_routes as SignedChunkRoute[] | undefined) ?? []) {
			signedRoutesByKey.set(routeKey(route.source_id, route.chunk_index, route.destination_id), route);
		}
		for (const route of signedRoutesByKey.values()) {
			const bucket = signedRoutesByChunkIndex.get(route.chunk_index) ?? [];
			bucket.push(route);
			signedRoutesByChunkIndex.set(route.chunk_index, bucket);
			const routeMeta = routeMetadata(route);
			const deliveryIndex = route.delivery_index ?? metadataNumber(routeMeta, "delivery_index");
			if (deliveryIndex !== undefined) {
				signedRoutesByDeliveryIndex.set(deliveryIndex, route);
			}
		}
	}

	const totalBytesNumber = Number(totalBytes);
	const src = rawSources[0];
	const sourceUrl = isS3Config(src) ? await presignGet(src).catch(() => fallbackUrl(src)) : fallbackUrl(src);
	const sourceHeaders = fallbackHeaders(src);
	const prepared: PreparedTaskOffer[] = [];

	for (let chunkIndex = chunkStart; chunkIndex <= chunkEnd; chunkIndex += 1) {
		const chunkKey = String(chunkIndex);
		const chunkOffset = chunkIndex * chunkSize;
		const actualChunkSize = Math.max(0, Math.min(chunkSize, totalBytesNumber - chunkOffset));

		if (meta && isSignedUrlTransferVersion(meta.transfer_version)) {
			const routes = resolveSignedRoutesForBatchChunk(
				signedRoutesByKey,
				signedRoutesByChunkIndex,
				signedRoutesByDeliveryIndex,
				chunkIndex,
			);
			if (!routes.length) {
				throw new Error(`missing signed routes for chunk ${chunkIndex}`);
			}
			for (const route of routes) {
				const routeExpiresAt = route.expires_at ?? meta.urls_expires_at;
				assertSignedUrlsFresh({
					expiresAt: routeExpiresAt,
					minTtlSeconds: ASSIGNMENT_DEFAULTS.SIGNED_URL_MIN_TTL_SECONDS,
					label: `route ${route.source_id}:${route.chunk_index}:${route.destination_id}`,
				});
				const routeMeta = routeMetadata(route);
				const uploadId = metadataString(routeMeta, "upload_id");
				const deliveryIndex = route.delivery_index ?? metadataNumber(routeMeta, "delivery_index") ?? chunkIndex;
				const multipartMetadata = meta.transfer_version === "signed_url_v2"
					? {
							transfer_id: metadataString(routeMeta, "transfer_id") ?? transferId,
							source_id: route.source_id,
							destination_id: route.destination_id,
							chunk_index: route.chunk_index,
							delivery_index: deliveryIndex,
							part_number: metadataString(routeMeta, "part_number"),
							staging_object_key:
								metadataString(routeMeta, "staging_object_key") ?? metadataString(routeMeta, "object_key"),
							commit_method: metadataString(routeMeta, "commit_method"),
							multipart_group_id: metadataString(routeMeta, "multipart_group_id"),
							multipart_created_at: metadataString(routeMeta, "multipart_created_at"),
							urls_expires_at: routeExpiresAt,
						}
					: {
							transfer_id: metadataString(routeMeta, "transfer_id") ?? transferId,
							source_id: route.source_id,
							destination_id: route.destination_id,
							chunk_index: route.chunk_index,
							delivery_index: deliveryIndex,
							upload_id: uploadId,
							part_number: metadataString(routeMeta, "part_number"),
							list_url: metadataString(routeMeta, "list_url"),
							final_object_key:
								metadataString(routeMeta, "final_object_key") ?? metadataString(routeMeta, "object_key"),
							multipart_group_id: metadataString(routeMeta, "multipart_group_id"),
							multipart_created_at: metadataString(routeMeta, "multipart_created_at"),
							urls_expires_at: routeExpiresAt,
						};
				const executionContext: Record<string, unknown> = {
					gateway_url: workerGatewayUrl,
					transfer_id: transferId,
					chunk_indices: [chunkIndex],
					chunk_offset: route.source_offset ?? chunkOffset,
					chunk_size: route.chunk_size,
					total_size: totalBytesNumber,
					source_urls: { [chunkKey]: route.source_url },
					dest_urls: { [chunkKey]: route.dest_url },
					source_id: route.source_id,
					destination_id: route.destination_id,
					delivery_index: deliveryIndex,
					route_chunk_index: route.chunk_index,
					urls_expires_at: routeExpiresAt,
					multipart_metadata: multipartMetadata,
				};
				prepared.push({
					sourceId: route.source_id,
					destinationId: route.destination_id,
					chunkIndex,
					chunkSize: route.chunk_size,
					executionContext,
					idempotencyKey: transferTaskIdempotencyKey(transferId, {
						chunkIndex,
						sourceId: route.source_id,
						destinationId: route.destination_id,
					}),
					workerOffer: {
						chunk_size: route.chunk_size,
						source_url: route.source_url,
						dest_url: route.dest_url,
						urls_expires_at: offerExpiry(routeExpiresAt),
						etag_required: true,
						...(route.headers ? { source_headers: route.headers } : {}),
					},
				});
			}
			continue;
		}

		const dst = rawDests.length > 1 ? (rawDests[chunkIndex] ?? rawDests[0]) : rawDests[0];
		const destUrl = isS3Config(dst) ? await presignPut(dst, chunkIndex).catch(() => fallbackUrl(dst)) : fallbackUrl(dst);
		const destHeaders = fallbackHeaders(dst);
		const executionContext: Record<string, unknown> = {
			gateway_url: workerGatewayUrl,
			transfer_id: transferId,
			chunk_indices: [chunkIndex],
			chunk_offset: chunkOffset,
			chunk_size: actualChunkSize,
			total_size: totalBytesNumber,
			source_urls: { [chunkKey]: sourceUrl },
			dest_urls: { [chunkKey]: destUrl },
		};

		prepared.push({
			sourceId: null,
			destinationId: null,
			chunkIndex,
			chunkSize: actualChunkSize,
			executionContext,
			idempotencyKey: transferTaskIdempotencyKey(transferId, {
				chunkIndex,
				sourceId: null,
				destinationId: null,
			}),
			workerOffer: {
				chunk_size: actualChunkSize,
				source_url: sourceUrl,
				dest_url: destUrl,
				urls_expires_at: offerExpiry(meta?.urls_expires_at),
				...(sourceHeaders ? { source_headers: sourceHeaders } : {}),
				...(destHeaders ? { dest_headers: destHeaders } : {}),
			},
		});
	}

	if (!prepared.length && totalChunks > 0) {
		throw new Error(`task offer batch ${input.batchId} produced no offers`);
	}
	return prepared;
}

function minimalBatchOffer(created: { id: string; attemptId: string; chunkIndex: number }, prepared: PreparedTaskOffer): TaskOfferBatchOffer {
	return {
		task_id: created.id,
		offer_id: created.attemptId,
		...prepared.workerOffer,
	};
}

async function markDeliveryFailed(
	registry: TransferRuntimeRegistry,
	taskId: string,
	attemptId: string,
	reason: string,
): Promise<void> {
	registry.recordOfferDelivery(taskId, false, reason);
	registry.recordFailure({ taskId, attemptId, reason, kind: "failure" });
	registry.invalidateTask(taskId);
}

export async function deliverTaskOfferBatchForAssignment(
	input: DeliverTaskOfferBatchInput,
): Promise<TaskOfferBatchDeliveryResult> {
	const startedAt = performance.now();
	const runtimeBatch = input.offerRegistry.getAssignment(input.batchId);
	if (!runtimeBatch) {
		return { ok: false, code: "batch_not_found", message: "task offer batch not found" };
	}
	if (input.expectedOrchestratorId && runtimeBatch.orchestratorId !== input.expectedOrchestratorId) {
		return { ok: false, code: "wrong_orchestrator", message: "task offer batch belongs to another orchestrator" };
	}
	if (!["assigned", "in_progress"].includes(runtimeBatch.status)) {
		return { ok: false, code: "batch_not_active", message: "task offer batch is not active" };
	}

	let prepared: PreparedTaskOffer[];
	try {
		prepared = await prepareTaskOffers({ batchId: input.batchId, offerRegistry: input.offerRegistry });
	} catch (err) {
		const message = err instanceof Error ? err.message : String(err);
		return { ok: false, code: "batch_prepare_failed", message };
	}

	const recoveryBatch =
		input.recoveryBatch ??
		input.offerRegistry.isRecoveryAssignment({
			assignmentId: input.batchId,
			transferId: runtimeBatch.transferId,
			chunkStart: runtimeBatch.chunkStart,
			chunkEnd: runtimeBatch.chunkEnd,
		});
	const taskPriority = recoveryBatch ? ASSIGNMENT_DEFAULTS.TRANSFER_RECOVERY_TASK_PRIORITY : 0;
	const offeredAt = new Date();
	const offersToCreate: RuntimeRegisterOfferInput[] = prepared.map((offer) => ({
		taskId: randomUUID(),
		transferId: runtimeBatch.transferId,
		totalChunks: runtimeBatch.totalChunks,
		workerId: null,
		attemptId: randomUUID(),
		chunkIndex: offer.chunkIndex,
		offeredAt,
		assignmentId: input.batchId,
		orchestratorId: runtimeBatch.orchestratorId,
		orchestratorHotkey: runtimeBatch.orchestratorHotkey,
		chunkSize: offer.chunkSize,
		executionContext: offer.executionContext,
		idempotencyKey: offer.idempotencyKey,
		sourceId: offer.sourceId,
		destinationId: offer.destinationId,
		priority: taskPriority,
	}));

	const created = input.offerRegistry.createTaskOffers(offersToCreate);
	if (!created.ok) {
		return {
			ok: false,
			code: "task_offer_create_failed",
			message: created.reason ?? "task offer creation failed",
			tasks: [],
		};
	}
	if (!created.tasks.length) {
		const expired = input.offerRegistry.expireRecoveryZeroOfferAssignment({
			assignmentId: input.batchId,
			transferId: runtimeBatch.transferId,
			orchestratorId: runtimeBatch.orchestratorId,
			chunkStart: runtimeBatch.chunkStart,
			chunkEnd: runtimeBatch.chunkEnd,
			totalChunks: runtimeBatch.totalChunks,
			reason: "task_offer_batch_produced_no_tasks",
		});
		return {
			ok: false,
			code: expired ? "batch_produced_no_tasks" : "no_new_task_offers",
			message: "task offer batch produced no new task offers",
			tasks: [],
		};
	}

	const preparedByKey = new Map(prepared.map((offer) => [offer.idempotencyKey, offer]));
	const createByTaskId = new Map(offersToCreate.map((offer) => [offer.taskId, offer]));
	const taskOffers = created.tasks.map((task) => {
		const create = createByTaskId.get(task.id);
		const preparedOffer = create ? preparedByKey.get(create.idempotencyKey ?? create.taskId) : undefined;
		if (!preparedOffer) {
			throw new Error(`prepared worker offer missing for task ${task.id}`);
		}
		return {
			created: task,
			prepared: preparedOffer,
			offer: minimalBatchOffer(task, preparedOffer),
		};
	});

	input.offerRegistry.markAssignmentInProgress(input.batchId, offeredAt);

	const tasks: DeliveredTaskOffer[] = [];
	const batchSize = Math.max(1, ASSIGNMENT_DEFAULTS.TRANSFER_TASK_OFFER_BATCH_SIZE);
	for (let offset = 0; offset < taskOffers.length; offset += batchSize) {
		const slice = taskOffers.slice(offset, offset + batchSize);
		const batchId = offset === 0 ? input.batchId : `${input.batchId}:${Math.floor(offset / batchSize) + 1}`;
		const queueToPushMs = performance.now() - startedAt;
		const pushStart = performance.now();
		const delivery = await input.gatewayClient
			.deliverTaskOfferBatch(runtimeBatch.orchestratorHotkey, {
				batch_id: batchId,
				offers: slice.map((task) => task.offer),
			}, "orchestrator_ws")
			.catch((err: unknown) => ({
				accepted: false,
				reason: err instanceof Error ? err.message : String(err),
			}));
		const pushMs = performance.now() - pushStart;

		for (const task of slice) {
			logWorkerTaskOfferDelivery({
				orchestratorHotkey: runtimeBatch.orchestratorHotkey,
				transferId: runtimeBatch.transferId,
				taskId: task.created.id,
				offerId: task.created.attemptId,
				chunkIndex: task.created.chunkIndex,
				deliveryChannel: "orchestrator_ws",
				queueToPushMs,
				pushMs,
				deliveryAccepted: delivery.accepted,
				...(delivery.reason ? { reason: delivery.reason } : {}),
			});

			if (!delivery.accepted) {
				await markDeliveryFailed(
					input.offerRegistry,
					task.created.id,
					task.created.attemptId,
					delivery.reason ?? "batch_delivery_failed",
				);
			} else {
				input.offerRegistry.recordOfferDelivery(task.created.id, true, null);
			}
			tasks.push({
				id: task.created.id,
				offerId: task.created.attemptId,
				chunkIndex: task.created.chunkIndex,
				chunkSize: task.prepared.chunkSize,
				deliveryAccepted: delivery.accepted,
				...(delivery.reason ? { deliveryReason: delivery.reason } : {}),
			});
		}
	}

	const acceptedCount = tasks.filter((task) => task.deliveryAccepted).length;
	logger.info(
		{
			batchId: input.batchId,
			transferId: runtimeBatch.transferId,
			orchestratorId: runtimeBatch.orchestratorId,
			offerCount: tasks.length,
			acceptedCount,
			queueMs: Math.round(performance.now() - startedAt),
			batchPhase: recoveryBatch ? "recovery" : "initial",
		},
		"task offer batch delivered",
	);

	if (acceptedCount === 0) {
		return {
			ok: false,
			code: "batch_delivery_failed",
			message: "task offer batch delivery failed",
			tasks,
		};
	}
	return { ok: true, tasks };
}

function sleepMs(ms: number): Promise<void> {
	return new Promise((resolve) => setTimeout(resolve, ms));
}

export interface AssignmentEngineDeps {
	db: Db;
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
	assignmentIds: string[];
	assignedChunkCount: number;
	assignmentSlices: Array<ReturnType<typeof summarizeAssignmentSlice>>;
	pendingPushes: PendingAssignmentPush[];
	coveredAllChunks: boolean;
}

interface PendingAssignmentPush {
	assignmentId: string;
	orchestrator: OrchestratorCandidate;
	chunkStart: number;
	chunkEnd: number;
	totalChunks: number;
	chunkSize: number;
}

export interface ReassignChunkBundleResult {
	ok: boolean;
	assignmentIds: string[];
	assignmentSlices: Array<ReturnType<typeof summarizeAssignmentSlice>>;
	pendingPushes: PendingAssignmentPush[];
	selectionRule?: AssignmentSelectionRule;
	kind?: "no_orchs" | "partial_coverage";
	partial?: boolean;
	assignedChunkCount?: number;
	remainingChunkCount?: number;
}

interface VirtualAssignmentRow {
	orchestrator: OrchestratorCandidate;
	chunkStart: number;
	chunkEnd: number;
	totalChunks: number;
}

interface InsertedAssignmentRow {
	id: string;
	orchestrator_id: string;
	chunk_start: number;
	chunk_end: number;
}

interface BundleTaskBefore {
	id: string;
	chunk_index: number;
	orchestrator_id: string;
	assigned_worker_id: string | null;
}

type AssignmentPool = "qualifying" | "qualified";

function requiredPoolForTransfer(testMode: boolean): AssignmentPool {
	return testMode ? "qualifying" : "qualified";
}

export function groupContiguousChunkIndices(indices: number[]): Array<{ chunkStart: number; chunkEnd: number }> {
	const sorted = [...new Set(indices)].sort((a, b) => a - b);
	const ranges: Array<{ chunkStart: number; chunkEnd: number }> = [];
	for (const chunkIndex of sorted) {
		const current = ranges.at(-1);
		if (current && chunkIndex === current.chunkEnd + 1) {
			current.chunkEnd = chunkIndex;
		} else {
			ranges.push({ chunkStart: chunkIndex, chunkEnd: chunkIndex });
		}
	}
	return ranges;
}

export function mapVirtualBundleToRows(
	chunkIndices: number[],
	orchestrators: OrchestratorCandidate[],
	plannedSliceSizes: number[],
): VirtualAssignmentRow[] {
	const rows: VirtualAssignmentRow[] = [];
	let virtualPos = 0;
	for (let index = 0; index < orchestrators.length; index++) {
		const sliceSize = plannedSliceSizes[index] ?? 0;
		if (sliceSize <= 0) continue;
		const mapped = chunkIndices.slice(virtualPos, virtualPos + sliceSize);
		virtualPos += sliceSize;
		if (!mapped.length) continue;
		for (const run of groupContiguousChunkIndices(mapped)) {
			rows.push({
				orchestrator: orchestrators[index]!,
				chunkStart: run.chunkStart,
				chunkEnd: run.chunkEnd,
				totalChunks: run.chunkEnd - run.chunkStart + 1,
			});
		}
	}
	return rows;
}

function interventionOutcomeForReason(reason: string): string {
	return reason === "missing_chunk_coverage" ? "missing_coverage_assigned" : "stall_redistributed";
}

const COVERAGE_FAILURE_MESSAGE =
	"transfer assignment failed because not all chunks could be assigned to live orchestrators";

function isTerminalAssignmentFailure(error: unknown): error is Error {
	if (!(error instanceof Error)) return false;
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

function summarizeAssignmentSlice(
	candidate: OrchestratorCandidate,
	chunkStart: number,
	chunkEnd: number,
	sliceSize: number,
) {
	return {
		orchestratorId: candidate.id,
		hotkey: candidate.hotkey,
		prismPool: candidate.prism_pool,
		chunkRange: `${chunkStart}-${chunkEnd}`,
		sliceSize,
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
    const candidateScreening = `queried ${selection.counts.queried} ready orchestrators; ${selection.counts.websocketReady} had a live WebSocket; worker-session count is not an orchestrator selection gate`;
    const nonZeroSlices = sliceSizes.filter((size) => size > 0);
    const rankingDecision =
        selection.selectionRule === "prism_final_score_desc"
            ? `qualified pool: full assignable base pool ordered by UID, then owner-group fairness and PRISM/Hamilton distribution; top assigned: ${topRankedSummary(selection.orchestrators)}`
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
	private readonly db: Db;
	private readonly gatewayClient: WorkerGatewayClient;
	private readonly offerRegistry: TransferRuntimeRegistry;
	private readonly routingRegistry: OrchestratorRoutingRegistry;

	constructor(deps: AssignmentEngineDeps) {
		this.db = deps.db;
		this.gatewayClient = deps.gatewayClient;
		this.offerRegistry = deps.offerRegistry;
		this.routingRegistry = deps.routingRegistry;
	}

	private failTransferAssignment(transferId: string, reason: string): void {
		this.offerRegistry.failTransfer(transferId, reason);
	}

	private registerRuntimeAssignments(input: {
		transferId: string;
		totalBytes: number;
		totalChunks: number;
		chunkSize: number;
		metadata: unknown;
		pendingPushes: PendingAssignmentPush[];
	}): boolean {
		const result = this.offerRegistry.createAssignments(
			input.pendingPushes.map((push) => ({
				assignmentId: push.assignmentId,
				transferId: input.transferId,
				orchestratorId: push.orchestrator.id,
				orchestratorHotkey: push.orchestrator.hotkey,
				workerGatewayType: push.orchestrator.gateway_type,
				workerGatewayUrl: push.orchestrator.gateway_url,
				chunkStart: push.chunkStart,
				chunkEnd: push.chunkEnd,
				totalChunks: push.totalChunks,
				totalBytes: input.totalBytes,
				chunkSize: input.chunkSize,
				metadata: input.metadata,
			})),
		);
		if (!result.ok) {
			logger.warn(
				{ transferId: input.transferId, reason: result.reason, assignmentCount: input.pendingPushes.length },
				"runtime assignment registration failed",
			);
		}
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

	private loadTransferOrchestratorSpeeds(transferId: string): TransferOrchestratorSpeed[] {
		return this.offerRegistry.getRecoveryOrchestratorSpeeds(transferId, env.TRANSFER_RECOVERY_SPEED_WINDOW_SECONDS);
	}

	private toCarveOutSpeeds(speeds: TransferOrchestratorSpeed[]) {
		return speeds
			.filter(
				(row): row is TransferOrchestratorSpeed & { median_relay_s: number } =>
					row.median_relay_s != null && Number.isFinite(row.median_relay_s),
			)
			.map((row) => ({
				orchestrator_id: row.orchestrator_id,
				median_relay_s: row.median_relay_s,
				completed_count: row.completed_count,
			}));
	}

	private selectOrchestrators(
		testMode: boolean,
		excludeIds: string[] = [],
		orderSeed?: string,
		options: {
			recoverySelection?: boolean;
			transferId?: string;
			excludeOwnerGroupIds?: string[];
			recoveryChunkCount?: number;
			recoverySpeeds?: TransferOrchestratorSpeed[];
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

		const websocketReady = allCandidates.filter((candidate) => orchestratorIsEligible(candidate.hotkey));
		const eligible = websocketReady;
		const connectedRegistry = connectedHotkeys().sort((left, right) => left.localeCompare(right));
		const queriedHotkeys = allCandidates.map((candidate) => candidate.hotkey);
		const websocketReadyHotkeys = websocketReady.map((candidate) => candidate.hotkey);
		const missingWebsocketHotkeys = queriedHotkeys.filter((hotkey) => !websocketReadyHotkeys.includes(hotkey));

		const preferred = eligible.filter(
			(c) =>
				c.prism_pool === preferredPool &&
				(preferredPool === "qualifying" || numericValue(c.prism_final_score) > 0),
		);

		let sorted: OrchestratorCandidate[];
		let selectionRule: AssignmentSelectionRule;

		if (options.recoverySelection && options.transferId) {
			const speeds =
				options.recoverySpeeds ?? this.loadTransferOrchestratorSpeeds(options.transferId);
			const speedByOrch = new Map(speeds.map((row) => [row.orchestrator_id, row]));
			const hasRelaySpeed = (candidate: OrchestratorCandidate): boolean => {
				const stat = speedByOrch.get(candidate.id);
				return stat?.median_relay_s != null && Number.isFinite(stat.median_relay_s);
			};
			const withSpeed = preferred.filter(hasRelaySpeed);
			const withoutSpeed = preferred.filter((candidate) => !hasRelaySpeed(candidate));
			withSpeed.sort((left, right) => {
				const leftSpeed = speedByOrch.get(left.id)!;
				const rightSpeed = speedByOrch.get(right.id)!;
				return (
					leftSpeed.median_relay_s! - rightSpeed.median_relay_s! ||
					rightSpeed.last_completed_at.getTime() - leftSpeed.last_completed_at.getTime() ||
					compareUidAscNullsLast(left.uid, right.uid) ||
					left.hotkey.localeCompare(right.hotkey) ||
					left.id.localeCompare(right.id)
				);
			});
			withoutSpeed.sort(
				(a, b) =>
					numericValue(b.prism_final_score) - numericValue(a.prism_final_score) ||
					compareUidAscNullsLast(a.uid, b.uid) ||
					a.hotkey.localeCompare(b.hotkey) ||
					a.id.localeCompare(b.id),
			);
			sorted = [...withSpeed, ...withoutSpeed];
			if (options.recoveryChunkCount && options.recoveryChunkCount > 0) {
				const poolSize = resolveRecoveryActiveOrchCount(sorted.length, options.recoveryChunkCount);
				sorted = sorted.slice(0, poolSize);
			}
			selectionRule = "recovery_transfer_speed";
		} else {
			selectionRule =
				preferredPool === "qualifying"
					? ("qualifying_equal_share_rotation" as const)
					: ("prism_final_score_desc" as const);

        if (selectionRule === "qualifying_equal_share_rotation") {
            sorted = rotateCandidates(
                [...preferred].sort((a, b) => a.hotkey.localeCompare(b.hotkey) || a.id.localeCompare(b.id)),
                preferred.length > 0 ? hashSeed(orderSeed ?? "") % preferred.length : 0,
            );
        } else if (preferredPool === "qualifying") {
            sorted = [...preferred].sort(
                (a, b) =>
                    numericValue(b.prism_final_score) - numericValue(a.prism_final_score) ||
                    compareUidAscNullsLast(a.uid, b.uid) ||
                    a.hotkey.localeCompare(b.hotkey) ||
                    a.id.localeCompare(b.id),
            );
        } else {
            sorted = [...preferred].sort(
                (a, b) =>
                    compareUidAscNullsLast(a.uid, b.uid) ||
                    a.hotkey.localeCompare(b.hotkey) ||
                    a.id.localeCompare(b.id),
            );
        }
        }

        const counts = {
			queried: allCandidates.length,
			websocketReady: websocketReady.length,
			eligible: eligible.length,
			preferred: preferred.length,
			selected: sorted.length,
		};

		if (!preferred.length) {
			const transferClass = testMode ? "test" : "production";
			logger.warn(
				{
					testMode,
					preferredPool,
					excludeIds,
					candidateCounts: counts,
					diagnostics: {
						queriedHotkeys,
						websocketReadyHotkeys,
						missingWebsocketHotkeys,
						connectedRegistryHotkeys: connectedRegistry,
					},
				},
				"no eligible orchestrators available in required pool for assignment",
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
				websocketReadyHotkeys,
				missingWebsocketHotkeys,
				connectedRegistryHotkeys: connectedRegistry,
			},
		};
		return result;
	}

	private async persistAssignmentPlan(
		sql: DbConn,
		transferId: string,
		pool: "qualified" | "qualifying",
		plan: QualifiedAssignmentPlan,
	): Promise<void> {
		await sql`
			INSERT INTO core.transfer_assignment_plans (transfer_id, pool, plan)
			VALUES (${transferId}, ${pool}, ${sql.json(plan as never)})
		`;
	}

	private async assignChunkCoverage(input: {
		sql: DbConn;
		transferId: string;
		chunkStart: number;
		totalChunks: number;
		chunkSize: number;
		metadata: TransferMetadata | null;
		orchestrators: OrchestratorCandidate[];
		selectionRule: SelectionResult["selectionRule"];
		plannedSliceSizes?: number[];
	}): Promise<AssignmentCoverageResult> {
		const assignmentIds: string[] = [];
		const assignmentSlices: Array<ReturnType<typeof summarizeAssignmentSlice>> = [];
		const pendingPushes: PendingAssignmentPush[] = [];
		let assignedChunkCount = 0;
		let remainingChunkStart = input.chunkStart;
		let remainingChunks = input.totalChunks;
		const available = [...input.orchestrators];
		let plannedSliceSizes =
			input.plannedSliceSizes ??
			allocateChunkSlices(available, remainingChunks, input.selectionRule);

		const coveragePairs = available
			.map((orchestrator, index) => ({
				orchestrator,
				sliceSize: plannedSliceSizes[index] ?? 0,
			}))
			.filter((entry) => entry.sliceSize > 0);
		let pairCursor = 0;

		while (remainingChunks > 0 && pairCursor < coveragePairs.length) {
			const { orchestrator, sliceSize } = coveragePairs[pairCursor]!;
			pairCursor += 1;

			let sliceRemaining = sliceSize;
			while (sliceRemaining > 0) {
				const batchSize = Math.min(sliceRemaining, env.TRANSFER_TASK_OFFER_BATCH_SIZE);
				const chunkStart = remainingChunkStart;
				const chunkEnd = remainingChunkStart + batchSize - 1;
				const assignmentId = randomUUID();

				assignmentIds.push(assignmentId);
				pendingPushes.push({
					assignmentId,
					orchestrator,
					chunkStart,
					chunkEnd,
					totalChunks: batchSize,
					chunkSize: input.chunkSize,
				});
				assignedChunkCount += batchSize;
				assignmentSlices.push(summarizeAssignmentSlice(orchestrator, chunkStart, chunkEnd, batchSize));
				remainingChunkStart += batchSize;
				remainingChunks -= batchSize;
				sliceRemaining -= batchSize;
			}
		}

		return {
			assignmentIds,
			assignedChunkCount,
			assignmentSlices,
			pendingPushes,
			coveredAllChunks: remainingChunks === 0,
		};
	}

	private async deliverTaskOfferBatchWithRetry(
		transferId: string,
		push: PendingAssignmentPush,
	): Promise<boolean> {
		for (let attempt = 1; attempt <= env.WS_ASSIGNMENT_PUSH_RETRY_ATTEMPTS; attempt += 1) {
			const result = await deliverTaskOfferBatchForAssignment({
				gatewayClient: this.gatewayClient,
				batchId: push.assignmentId,
				expectedOrchestratorId: push.orchestrator.id,
				offerRegistry: this.offerRegistry,
			});
			if (result.ok) {
				recordWsPushResult(transferId, true);
				return true;
			}
			logger.warn(
				{
					transferId,
					batchId: push.assignmentId,
					hotkey: push.orchestrator.hotkey,
					attempt,
					code: result.code,
					message: result.message,
				},
				"task offer batch delivery attempt failed",
			);
			if (attempt < env.WS_ASSIGNMENT_PUSH_RETRY_ATTEMPTS) {
				await sleepMs(50 * attempt);
			}
		}

		recordWsPushResult(transferId, false);
		await this.expireTransferAssignment({
			assignmentId: push.assignmentId,
			reason: "websocket_not_ready_or_push_rejected",
		});
		this.offerRegistry.recordOverseerIntervention({
			interventionType: "assignment_push_failed",
			assignmentId: push.assignmentId,
			transferId,
			orchestratorId: push.orchestrator.id,
			chunkStart: push.chunkStart,
			chunkEnd: push.chunkEnd,
			totalChunks: push.totalChunks,
			outcome: "ws_push_failed",
			reason: "websocket_not_ready_or_push_rejected",
			metadata: { delivery_attempts: env.WS_ASSIGNMENT_PUSH_RETRY_ATTEMPTS },
		});
		logger.warn(
			{
				transferId,
				assignmentId: push.assignmentId,
				hotkey: push.orchestrator.hotkey,
				chunkStart: push.chunkStart,
			chunkEnd: push.chunkEnd,
			attempts: env.WS_ASSIGNMENT_PUSH_RETRY_ATTEMPTS,
		},
			"task offer batch delivery failed after assignment commit",
		);
		return false;
	}

	private async dispatchTaskOfferBatches(
		transferId: string,
		pushes: PendingAssignmentPush[],
	): Promise<void> {
		const concurrency = env.WS_ASSIGNMENT_PUSH_RETRY_CONCURRENCY;
		for (let offset = 0; offset < pushes.length; offset += concurrency) {
			const batch = pushes.slice(offset, offset + concurrency);
			await Promise.all(batch.map((push) => this.deliverTaskOfferBatchWithRetry(transferId, push)));
		}
	}

	async resendTaskOfferBatchFromRegistry(assignmentId: string): Promise<boolean> {
		const assignment = this.offerRegistry.getAssignment(assignmentId);
		if (!assignment || assignment.status !== "assigned") {
			return false;
		}
		const pushed = await this.deliverTaskOfferBatchWithRetry(assignment.transferId, {
			assignmentId: assignment.assignmentId,
			orchestrator: {
				id: assignment.orchestratorId,
				hotkey: assignment.orchestratorHotkey,
				uid: null,
				owner_group_id: null,
				gateway_type: assignment.workerGatewayType,
				gateway_url: assignment.workerGatewayUrl,
				prism_final_score: "0",
				prism_confidence_score: "0",
				prism_pool: "qualified",
			},
			chunkStart: assignment.chunkStart,
			chunkEnd: assignment.chunkEnd,
			totalChunks: assignment.totalChunks,
			chunkSize: assignment.chunkSize,
		});
		if (pushed) {
			recordResendPush(assignment.transferId);
		}
		return pushed;
	}

	async resendAssignmentPush(assignmentId: string): Promise<boolean> {
		return this.resendTaskOfferBatchFromRegistry(assignmentId);
	}

	async reassignStalledChunkBundle(input: {
		transferId: string;
		chunkIndices: number[];
		excludeOrchestratorIds: string[];
		reason: string;
		staleAssignmentIds?: string[];
	}): Promise<ReassignChunkBundleResult> {
		const requestedChunks = [...new Set(input.chunkIndices)].sort((a, b) => a - b);
		const chunkIndices = this.offerRegistry.filterRecoverableChunkIndices(input.transferId, requestedChunks);
		const empty: ReassignChunkBundleResult = {
			ok: true,
			assignmentIds: [],
			assignmentSlices: [],
			pendingPushes: [],
		};
		if (!chunkIndices.length) return empty;

		const transferRecord = this.offerRegistry.getTransferRecord(input.transferId);
		if (!transferRecord) {
			throw new Error(`transfer ${input.transferId} not found in runtime registry`);
		}

		if (input.staleAssignmentIds?.length) {
			for (const assignmentId of input.staleAssignmentIds) {
				const assignment = this.offerRegistry.getAssignment(assignmentId);
				if (!assignment) {
					continue;
				}
				const overlaps = chunkIndices.some(
					(index) => index >= assignment.chunkStart && index <= assignment.chunkEnd,
				);
				if (!overlaps) {
					continue;
				}
				const restartRecovered = this.offerRegistry.isRestartRecoveredAssignment(assignment.assignmentId);
				const reason = restartRecovered
					? "control_plane_restart"
					: "assignment_produced_no_tasks_before_timeout";
				const outcome = restartRecovered
					? "zero_task_assignment_requeued_after_restart"
					: "stale_assignment_timeout";
				this.offerRegistry.recordOverseerIntervention({
					interventionType: restartRecovered
						? "control_plane_restart_recovery"
						: "assignment_stale_timeout",
					transferId: input.transferId,
					assignmentId: assignment.assignmentId,
					orchestratorId: assignment.orchestratorId,
					chunkStart: assignment.chunkStart,
					chunkEnd: assignment.chunkEnd,
					totalChunks: assignment.totalChunks,
					outcome,
					reason,
					...(restartRecovered
						? {
								metadata: {
									recovery_origin: "control_plane_restart",
									prism_countable: false,
								},
							}
						: {}),
				});
				this.offerRegistry.recordRecoveryOutcomeForRange({
					transferId: input.transferId,
					chunkStart: assignment.chunkStart,
					chunkEnd: assignment.chunkEnd,
					assignmentId: assignment.assignmentId,
					orchestratorId: assignment.orchestratorId,
					reason,
					outcome: restartRecovered ? "zero_task_assignment_requeued_after_restart" : "orch_zero_tasks",
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
			assignmentIds: string[];
			assignmentSlices: AssignmentCoverageResult["assignmentSlices"];
			pendingPushes: PendingAssignmentPush[];
			partial: boolean;
			assignedChunkCount: number;
			remainingChunkCount: number;
			chunksToAssign: number[];
			interventionInputs: Array<{
				task_id: string;
				orchestrator_id: string;
				previous_worker_id: string | null;
				chunk_index: number;
				reason: string;
				outcome: string;
			}>;
		};
		type BundleBusy = {
			ok: false;
			kind: "no_orchs" | "partial_coverage";
		};

		const tryBundle = async (): Promise<BundleOk | BundleBusy> => {
			const recoverySpeeds = this.loadTransferOrchestratorSpeeds(input.transferId);
			const cumulativeExclusions =
				input.reason === "missing_chunk_coverage"
					? { orchestratorIds: input.excludeOrchestratorIds, ownerGroupIds: [] }
					: this.loadCumulativeRecoveryExclusions({
							transferId: input.transferId,
							chunkIndices,
							excludeOrchestratorIds: input.excludeOrchestratorIds,
						});

			let selection: SelectionResult;
			try {
				selection = this.selectOrchestrators(
					testMode,
					cumulativeExclusions.orchestratorIds,
					input.transferId,
					undefined,
					undefined,
					{
						recoverySelection: true,
						transferId: input.transferId,
						recoveryChunkCount: batchChunkCount,
						recoverySpeeds,
						excludeOwnerGroupIds: cumulativeExclusions.ownerGroupIds,
						requiredPool: recoveryPool,
					},
				);
				assertRequiredPoolSelection(selection, testMode, input.transferId, recoveryPool);
			} catch {
				return { ok: false, kind: "no_orchs" };
			}

			const speedByOrch = new Map(recoverySpeeds.map((row) => [row.orchestrator_id, row]));
			const speedWeights = selection.orchestrators.map((orchestrator) => {
				const stat = speedByOrch.get(orchestrator.id);
				if (stat?.median_relay_s != null && Number.isFinite(stat.median_relay_s)) {
					return buildRecoveryRelayWeight(stat.median_relay_s);
				}
				return Math.max(numericValue(orchestrator.prism_final_score), 0.0001);
			});
			const plannedSliceSizes = allocateRecoverySpeedSlices({
				orchCount: selection.orchestrators.length,
				chunkCount: batchChunkCount,
				speedWeights,
			});
			const assignableCount = plannedSliceSizes.reduce((sum, size) => sum + Math.max(0, size), 0);
			if (assignableCount === 0) {
				return { ok: false, kind: "no_orchs" };
			}

			const chunksToAssign = chunkIndices.slice(0, assignableCount);
			const partial = assignableCount < batchChunkCount;
			const virtualRows = mapVirtualBundleToRows(chunksToAssign, selection.orchestrators, plannedSliceSizes);
			if (!virtualRows.length) {
				return { ok: false, kind: "no_orchs" };
			}
			const batchRows = virtualRows.flatMap((row) => {
				const rows: VirtualAssignmentRow[] = [];
				let chunkStart = row.chunkStart;
				let remaining = row.totalChunks;
				while (remaining > 0) {
					const batchSize = Math.min(remaining, env.TRANSFER_TASK_OFFER_BATCH_SIZE);
					rows.push({
						orchestrator: row.orchestrator,
						chunkStart,
						chunkEnd: chunkStart + batchSize - 1,
						totalChunks: batchSize,
					});
					chunkStart += batchSize;
					remaining -= batchSize;
				}
				return rows;
			});

			const tasksBefore: BundleTaskBefore[] = this.offerRegistry
				.getActiveTasksForChunks(input.transferId, chunksToAssign)
				.map((task) => ({
					id: task.taskId,
					chunk_index: task.chunkIndex,
					orchestrator_id: task.orchestratorId ?? "",
					assigned_worker_id: task.workerId,
				}));

			const assignmentInputs = batchRows.map((row) => ({
				id: randomUUID(),
				transfer_id: input.transferId,
				orchestrator_id: row.orchestrator.id,
				chunk_start: row.chunkStart,
				chunk_end: row.chunkEnd,
				total_chunks: row.totalChunks,
				chunk_size: transferRecord.chunkSize,
			}));
			const insertedAssignments: InsertedAssignmentRow[] = assignmentInputs.map((row) => ({
				id: row.id,
				orchestrator_id: row.orchestrator_id,
				chunk_start: row.chunk_start,
				chunk_end: row.chunk_end,
			}));
			const assignmentByKey = new Map(
				insertedAssignments.map((row) => [
					`${row.orchestrator_id}:${row.chunk_start}:${row.chunk_end}`,
					row,
				]),
			);
			const assignmentIds: string[] = insertedAssignments.map((row) => row.id);
			const assignmentSlices: AssignmentCoverageResult["assignmentSlices"] = [];
			const pendingPushes: PendingAssignmentPush[] = [];
			const chunkOwners = new Map<number, { assignmentId: string; orchestratorId: string }>();
			for (const row of batchRows) {
				const inserted = assignmentByKey.get(`${row.orchestrator.id}:${row.chunkStart}:${row.chunkEnd}`);
				if (!inserted) continue;
				assignmentSlices.push(summarizeAssignmentSlice(row.orchestrator, row.chunkStart, row.chunkEnd, row.totalChunks));
				pendingPushes.push({
					assignmentId: inserted.id,
					orchestrator: row.orchestrator,
					chunkStart: row.chunkStart,
					chunkEnd: row.chunkEnd,
					totalChunks: row.totalChunks,
					chunkSize: transferRecord.chunkSize,
				});
				for (let chunkIndex = row.chunkStart; chunkIndex <= row.chunkEnd; chunkIndex++) {
					if (chunksToAssign.includes(chunkIndex)) {
						chunkOwners.set(chunkIndex, {
							assignmentId: inserted.id,
							orchestratorId: row.orchestrator.id,
						});
					}
				}
			}
			const outcome = interventionOutcomeForReason(input.reason);
			const interventionInputs = tasksBefore.flatMap((task) => {
				const owner = chunkOwners.get(task.chunk_index);
				if (!owner) return [];
				return [
					{
						task_id: task.id,
						orchestrator_id: task.orchestrator_id,
						previous_worker_id: task.assigned_worker_id,
						chunk_index: task.chunk_index,
						reason: input.reason,
						outcome,
					},
				];
			});

			return {
				ok: true,
				selection,
				assignmentIds,
				assignmentSlices,
				pendingPushes,
				partial,
				assignedChunkCount: assignableCount,
				remainingChunkCount: batchChunkCount - assignableCount,
				chunksToAssign,
				interventionInputs,
			};
		};

		const finishOk = (result: BundleOk): ReassignChunkBundleResult => {
			for (const intervention of result.interventionInputs) {
				this.offerRegistry.recordOverseerIntervention({
					interventionType: "task_recovery",
					transferId: input.transferId,
					taskId: intervention.task_id,
					orchestratorId: intervention.orchestrator_id,
					previousWorkerId: intervention.previous_worker_id,
					chunkIndex: intervention.chunk_index,
					reason: intervention.reason,
					outcome: intervention.outcome,
				});
			}
			const recoveryChunks = result.chunksToAssign.filter(
				(index) => !this.offerRegistry.isChunkIndexCompleted(input.transferId, index),
			);
			this.offerRegistry.expireAssignments(input.transferId, recoveryChunks, input.reason);
			const registered = this.registerRuntimeAssignments({
				transferId: input.transferId,
				totalBytes: transferRecord.totalBytes,
				totalChunks: transferTotalChunks,
				chunkSize: transferRecord.chunkSize,
				metadata: transferRecord.metadata,
				pendingPushes: result.pendingPushes,
			});
			if (!registered) {
				this.offerRegistry.recordRecoveryOutcomeForChunks({
					transferId: input.transferId,
					chunkIndices: recoveryChunks,
					reason: input.reason,
					outcome: "runtime_assignment_registration_failed",
					countRecoveryAttempt: true,
				});
				return {
					ok: false,
					assignmentIds: [],
					assignmentSlices: [],
					pendingPushes: [],
					kind: "partial_coverage",
				};
			}
			for (const push of result.pendingPushes) {
				this.offerRegistry.recordRecoveryAssignment({
					transferId: input.transferId,
					assignmentId: push.assignmentId,
					orchestratorId: push.orchestrator.id,
					chunkStart: push.chunkStart,
					chunkEnd: push.chunkEnd,
					reason: input.reason,
				});
			}
			void this.dispatchTaskOfferBatches(input.transferId, result.pendingPushes).catch((err: unknown) => {
				logger.error({ err, transferId: input.transferId }, "bundle reassignment dispatch failed");
			});
			return {
				ok: true,
				assignmentIds: result.assignmentIds,
				assignmentSlices: result.assignmentSlices,
				pendingPushes: result.pendingPushes,
				selectionRule: result.selection.selectionRule,
				partial: result.partial,
				assignedChunkCount: result.assignedChunkCount,
				remainingChunkCount: result.remainingChunkCount,
			};
		};

		const result = await tryBundle();
		if (result.ok) {
			return finishOk(result);
		}
		this.offerRegistry.recordRecoveryOutcomeForChunks({
			transferId: input.transferId,
			chunkIndices,
			reason: input.reason,
			outcome: (result as BundleBusy).kind,
			countRecoveryAttempt: true,
		});
		return {
			ok: false,
			assignmentIds: [],
			assignmentSlices: [],
			pendingPushes: [],
			kind: (result as BundleBusy).kind,
		};
	}

	async assignTransfer(transferId: string): Promise<string[]> {
		const existingSnapshot = this.offerRegistry.getTransferSnapshot(transferId);
		if (existingSnapshot) {
			return existingSnapshot.assignments.map((assignment) => assignment.assignmentId);
		}

		let transferRecord = this.offerRegistry.getTransferRecord(transferId);
		if (!transferRecord) {
			throw new Error(`transfer ${transferId} not found or not assignable in runtime registry`);
		}

		const registryMeta = parseTransferMetadata(transferRecord.metadata);
		if (
			registryMeta &&
			isSignedUrlTransferVersion(registryMeta.transfer_version) &&
			!((registryMeta.chunk_routes as unknown[] | undefined) ?? []).length
		) {
			throw new Error(`transfer ${transferId} signed URL routes missing from runtime registry`);
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
				const captured = await this.db.begin(async (sql) => {
					const selection = this.selectOrchestrators(testMode, [], transferId);
					assertRequiredPoolSelection(selection, testMode, transferId);
					const sliceSizes = allocateChunkSlices(
						selection.orchestrators,
						logicalTotalChunks,
						selection.selectionRule,
					);
					const logic = buildAssignmentLogicSummary(testMode, selection, logicalTotalChunks, sliceSizes);

					const coverage = await this.assignChunkCoverage({
						sql,
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
							"transfer assignment failed because the control plane could not cover every chunk with live orchestrator acknowledgements",
						);
						throw new Error(COVERAGE_FAILURE_MESSAGE);
					}

					return { selection, sliceSizes, logic, coverage };
				});

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
							assignmentCount: captured.coverage.assignmentIds.length,
							assignedChunkCount: captured.coverage.assignedChunkCount,
							totalChunks: logicalTotalChunks,
							sliceSizes: captured.sliceSizes.filter((size) => size > 0),
						},
						assignmentSlices: captured.coverage.assignmentSlices,
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
				this.registerRuntimeAssignments({
					transferId,
					totalBytes: Number(transfer.total_bytes),
					totalChunks: logicalTotalChunks,
					chunkSize: transfer.chunk_size,
					metadata: transfer.metadata,
					pendingPushes: captured.coverage.pendingPushes,
				});

				void this.dispatchTaskOfferBatches(transferId, captured.coverage.pendingPushes).catch((err: unknown) => {
					logger.error({ err, transferId }, "transfer assignment dispatch failed");
				});

				beginDistributePhase(transferId, captured.coverage.assignmentIds.length);
				return captured.coverage.assignmentIds;
			}

			type CapturedAssign = {
				assignmentIds: string[];
				selection: SelectionResult;
				coverage: AssignmentCoverageResult;
				sliceSizes: number[];
			};

			let captured: CapturedAssign | undefined;

			try {
				captured = await this.db.begin(async (sql) => {
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
						sql,
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

					await this.persistAssignmentPlan(sql, transferId, "qualified", slicePlan.plan);


					const capturedAssign: CapturedAssign = {
						assignmentIds: coverage.assignmentIds,
						selection,
						coverage,
						sliceSizes: slicePlan.sliceSizes,
					};
					return capturedAssign;
				});
			} catch (e) {
				if ((e as Error)?.message === "__ASSIGN_COVERAGE_FAIL__") {
					logger.error(
						{
							transferId,
							totalChunks: logicalTotalChunks,
							testMode: false,
						},
						"transfer assignment failed because the control plane could not cover every chunk with live orchestrator acknowledgements",
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
						assignmentCount: captured.coverage.assignmentIds.length,
						assignedChunkCount: captured.coverage.assignedChunkCount,
						totalChunks: logicalTotalChunks,
						sliceSizes: captured.sliceSizes.filter((size) => size > 0),
					},
					assignmentSlices: captured.coverage.assignmentSlices,
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
			this.registerRuntimeAssignments({
				transferId,
				totalBytes: Number(transfer.total_bytes),
				totalChunks: logicalTotalChunks,
				chunkSize: transfer.chunk_size,
				metadata: transfer.metadata,
				pendingPushes: captured.coverage.pendingPushes,
			});

			void this.dispatchTaskOfferBatches(transferId, captured.coverage.pendingPushes).catch((err: unknown) => {
				logger.error({ err, transferId }, "transfer assignment dispatch failed");
			});

			beginDistributePhase(transferId, captured.coverage.assignmentIds.length);
			return captured.assignmentIds;
		} catch (error) {
			if (isTerminalAssignmentFailure(error)) {
				this.failTransferAssignment(transferId, error.message);
			}
			throw error;
		}
	}

	/** @internal Unit-test hook for assignChunkCoverage. */
	async testAssignChunkCoverage(input: {
		sql: DbConn;
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

	async expireTransferAssignment(input: { assignmentId: string; reason: string }): Promise<void> {
		this.offerRegistry.updateAssignmentStatus(input.assignmentId, "expired", input.reason);
	}
}
