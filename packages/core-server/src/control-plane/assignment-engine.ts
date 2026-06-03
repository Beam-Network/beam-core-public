/**
 * Transfer assignment: selects orchestrators by PRISM tier and score, allocates chunk slices,
 * persists assignments, and notifies orchestrators over the live connection registry.
 *
 * This transparency copy inlines supporting helpers that live in separate modules in the full
 * BeamCore tree (chunking, transfer-metadata, worker eligibility, orchestrator WebSocket registry).
 */

/** Minimal WebSocket shape used by the orchestrator registry (production uses `ws`). */
interface OrchestratorWebSocket {
	readonly readyState: number;
	send(data: string): void;
}

const CONTROL_PLANE_PUBLIC_GATEWAY_URL = "http://localhost:8001";
const DEFAULT_WORKER_GATEWAY_BASE_URL = CONTROL_PLANE_PUBLIC_GATEWAY_URL;
const ZERO_TASK_ASSIGNMENT_TIMEOUT_SECONDS = 5;
const TRANSFER_RECOVERY_SPEED_WINDOW_SECONDS = 15;
const QUALIFIED_WINDOW_RATIO = 0.6;
const MIN_QUALIFIED_WINDOW_SIZE = 10;

type OrchestratorSession =
	| { kind: "direct"; ws: OrchestratorWebSocket }
	| { kind: "relay"; ws: OrchestratorWebSocket };

const orchestratorSockets = new Map<string, OrchestratorSession>();

function isOrchestratorConnected(hotkey: string): boolean {
	const session = orchestratorSockets.get(hotkey);
	return session?.ws.readyState === 1;
}

function isEligible(hotkey: string): boolean {
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

type SqlTag = <T = unknown>(strings: TemplateStringsArray, ...values: unknown[]) => Promise<T>;

export type Db = SqlTag & {
	begin?: <T>(callback: (sql: SqlTag) => Promise<T>) => Promise<T>;
	json?: (value: unknown) => unknown;
	unsafe?: (fragment: string) => unknown;
};

type DbConn = SqlTag;

export interface TransferMetadata {
	transfer_version?: "legacy" | "signed_url_v1";
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

	// Distribute leftover by largest fractional remainder; tie-break: score desc, index asc
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


function normalizeGatewayUrl(url: string | null | undefined): string | null {
	if (!url) return null;
	const trimmed = url.trim();
	if (!trimmed) return null;
	return trimmed.replace(/\/+$/, "");
}

function configuredPublicWorkerGatewayUrl(): string {
	return normalizeGatewayUrl(CONTROL_PLANE_PUBLIC_GATEWAY_URL) ?? CONTROL_PLANE_PUBLIC_GATEWAY_URL;
}

function workerGatewayUrlForType(
	type: string | null | undefined,
	url: string | null | undefined,
	fallbackUrl?: string | null | undefined,
): string | null {
	if (type === "public_worker") return configuredPublicWorkerGatewayUrl();
	return url ?? fallbackUrl ?? null;
}

export interface SessionSummary {
	workerId: string;
	gatewayMode: "public" | "orch_owned";
	gatewayUrl: string | null;
	orchestratorId: string | null;
}

export interface WorkerGatewayClient {
	getConnectedSessions(): Promise<SessionSummary[]>;
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

export interface QualifiedRingSelectionMeta {
	n: number;
	totalChunks: number;
	qualifiedWindowRatio: number;
	minQualifiedWindowSize: number;
	competitionWindowSize: number;
	cursorBeforeMod: bigint;
	cursorAfterMod: bigint;
	startIndex: number;
	windowHotkeyCount?: number;
	virtualCompetitorCount?: number;
	nOwnerGroupsInPool?: number;
}

export interface SelectionResult {
	orchestrators: OrchestratorCandidate[];
	preferredPool: "qualifying" | "qualified";
	selectionRule: AssignmentSelectionRule;
	qualifiedRing?: QualifiedRingSelectionMeta;
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

export interface RecoveryOrchestratorSpeed {
	orchestrator_id: string;
	median_relay_s: number;
	completed_count: number;
}

export function medianOf(values: number[]): number | undefined {
	if (!values.length) return undefined;
	const sorted = [...values].sort((a, b) => a - b);
	const mid = Math.floor(sorted.length / 2);
	if (sorted.length % 2 === 0) {
		return (sorted[mid - 1]! + sorted[mid]!) / 2;
	}
	return sorted[mid];
}

export function applyHotOrchestratorCarveOut(input: {
	excludedOrchestratorIds: string[];
	ownerGroupIdsByOrch: Map<string, string | null>;
	speeds: RecoveryOrchestratorSpeed[];
	activeStallOwnerIds: Set<string>;
}): { orchestratorIds: string[]; ownerGroupIds: string[] } {
	const completers = input.speeds.filter((row) => row.completed_count >= 1);
	const transferMedianP50 = medianOf(completers.map((row) => row.median_relay_s));
	const hotOrchIds = new Set(
		completers
			.filter(
				(row) =>
					transferMedianP50 !== undefined &&
					row.median_relay_s <= transferMedianP50,
			)
			.map((row) => row.orchestrator_id),
	);

	const filteredIds = input.excludedOrchestratorIds.filter(
		(id) => !hotOrchIds.has(id) || input.activeStallOwnerIds.has(id),
	);
	const ownerGroupIds = [
		...new Set(
			filteredIds
				.map((id) => input.ownerGroupIdsByOrch.get(id))
				.filter((id): id is string => Boolean(id)),
		),
	];
	return { orchestratorIds: filteredIds, ownerGroupIds };
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

export interface OwnerGroupWindowSlot {
	ownerGroupId: string | null;
	windowMemberOrchestratorIds: string[];
	windowMemberHotkeys: string[];
	memberActualPrismScores: number[];
	averagePrismScore: number;
	interGroupSliceSize: number;
	memberSliceSizes: number[];
}

export interface QualifiedAssignmentPlan {
	version: 1;
	transferId: string;
	pool: "qualified";
	selectionRule: "prism_final_score_desc";
	ring: QualifiedRingSelectionMeta;
	counts: {
		preferredHotkeys: number;
		ownerGroupsInPool: number;
		windowHotkeyCount: number;
		virtualCompetitorCount: number;
	};
	windowHotkeysOrdered: string[];
	virtualCompetitors: OwnerGroupWindowSlot[];
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

export interface QualifiedWindowSlicePlan {
	orchestrators: OrchestratorCandidate[];
	sliceSizes: number[];
	plan: QualifiedAssignmentPlan;
}

interface WindowGroup {
	ownerGroupId: string | null;
	members: OrchestratorCandidate[];
}

function compareUidAscNullsLast(a: number | null, b: number | null): number {
	if (a === null && b === null) return 0;
	if (a === null) return 1;
	if (b === null) return -1;
	return a - b;
}

function sortMembers(members: OrchestratorCandidate[]): OrchestratorCandidate[] {
	return [...members].sort(
		(a, b) =>
			compareUidAscNullsLast(a.uid, b.uid) ||
			a.hotkey.localeCompare(b.hotkey) ||
			a.id.localeCompare(b.id),
	);
}

function partitionWindowGroups(windowOrchs: OrchestratorCandidate[]): WindowGroup[] {
	const byKey = new Map<string, OrchestratorCandidate[]>();
	for (const orch of windowOrchs) {
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

function virtualCandidateForGroup(group: WindowGroup): OrchestratorCandidate {
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

export function buildQualifiedWindowSlicePlan(input: {
	transferId: string;
	windowOrchs: OrchestratorCandidate[];
	totalChunks: number;
	ring: QualifiedRingSelectionMeta;
	preferredHotkeys: number;
	ownerGroupsInPool: number;
}): QualifiedWindowSlicePlan {
	const windowGroups = partitionWindowGroups(input.windowOrchs);
	const virtualCompetitors = windowGroups.map((g) => virtualCandidateForGroup(g));
	const interGroupSlices = allocateChunkSlices(virtualCompetitors, input.totalChunks, "prism_final_score_desc");

	const deliveries: QualifiedAssignmentPlan["deliveries"] = [];
	const flatOrchs: OrchestratorCandidate[] = [];
	const flatSlices: number[] = [];
	const virtualSlots: OwnerGroupWindowSlot[] = [];

	for (let gi = 0; gi < windowGroups.length; gi++) {
		const group = windowGroups[gi]!;
		const interTotal = interGroupSlices[gi] ?? 0;
		const memberScores = group.members.map((m) => Math.max(0, numericValue(m.prism_final_score)));
		const memberSlices =
			interTotal > 0 && group.members.length > 0
				? computePrismSlices(memberScores, interTotal)
				: memberScores.map(() => 0);
		const avg = averagePrismScore(group.members);

		virtualSlots.push({
			ownerGroupId: group.ownerGroupId,
			windowMemberOrchestratorIds: group.members.map((m) => m.id),
			windowMemberHotkeys: group.members.map((m) => m.hotkey),
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

	const ring: QualifiedRingSelectionMeta = {
		...input.ring,
		windowHotkeyCount: input.windowOrchs.length,
		virtualCompetitorCount: windowGroups.length,
		nOwnerGroupsInPool: input.ownerGroupsInPool,
	};

	return {
		orchestrators: flatOrchs,
		sliceSizes: flatSlices,
		plan: {
			version: 1,
			transferId: input.transferId,
			pool: "qualified",
			selectionRule: "prism_final_score_desc",
			ring,
			counts: {
				preferredHotkeys: input.preferredHotkeys,
				ownerGroupsInPool: input.ownerGroupsInPool,
				windowHotkeyCount: input.windowOrchs.length,
				virtualCompetitorCount: windowGroups.length,
			},
			windowHotkeysOrdered: input.windowOrchs.map((o) => o.hotkey),
			virtualCompetitors: virtualSlots,
			deliveries,
		},
	};
}


function extractDestinationUrl(meta: TransferMetadata | null): string {
	const dests = (meta?.destinations as unknown[]) ?? [];
	const first = dests[0];
	if (!first || typeof first !== "object") return "";
	const maybeUrl = (first as { url?: unknown }).url;
	return typeof maybeUrl === "string" ? maybeUrl : "";
}

export interface AssignmentEngineDeps {
	db: Db;
	gatewayClient: WorkerGatewayClient;
}


interface AssignmentLogicSummary {
	transferClass: "test" | "production";
	poolDecision: string;
	rankingDecision: string;
	candidateScreening: string;
	distributionDecision: string;
	qualifiedSelectionNarrative?: string;
}

interface TransferAssignmentSlice {
	transferId: string;
	orchestratorId: string;
	chunkStart: number;
	chunkEnd: number;
	totalChunks: number;
	chunkSize: number;
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

export interface StaleAssignmentEntry {
	id: string;
	orchestratorId: string;
	chunkStart: number;
	chunkEnd: number;
}

export interface StaleAssignmentBundle {
	transferId: string;
	chunkIndices: number[];
	excludeOrchestratorIds: string[];
	staleAssignmentIds: string[];
	staleAssignments: StaleAssignmentEntry[];
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

interface TransferOrchestratorSpeed {
	orchestrator_id: string;
	median_relay_s: number | null;
	last_completed_at: Date;
	completed_count: number;
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

type AssignmentFailureType = "stale_assignment_timeout" | "ws_push_failed";

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

/** Exported for tests — qualified competition window size (plan formula). */
export function computeQualifiedCompetitionWindowSize(
	totalChunks: number,
	n: number,
	qualifiedWindowRatio: number,
	minQualifiedWindowSize: number,
): number {
	if (n <= 0 || totalChunks <= 0) return 0;
	const scaled = Math.ceil(totalChunks * qualifiedWindowRatio);
	return Math.min(n, Math.max(minQualifiedWindowSize, scaled));
}

/** Pick `competitionWindowSize` candidates from uid-sorted list after rotating ring by `startIndex`. */
export function pickQualifiedCompetitionWindow<T>(
	uidSorted: T[],
	startIndex: number,
	competitionWindowSize: number,
): T[] {
	if (!uidSorted.length || competitionWindowSize <= 0) return [];
	return rotateCandidates(uidSorted, startIndex).slice(0, competitionWindowSize);
}

export function advanceQualifiedRingCursor(cursor: bigint, windowSize: number, n: number): bigint {
	if (n <= 0) return cursor;
	const w = BigInt(windowSize);
	const nn = BigInt(n);
	return (cursor + w) % nn;
}

/** Pino/JSON cannot serialize bigint fields on assignment ring metadata. */
function qualifiedRingLogFields(ring: QualifiedRingSelectionMeta | undefined) {
	if (!ring) return undefined;
	return qualifiedRingForJson(ring);
}

function qualifiedRingForJson(ring: QualifiedRingSelectionMeta) {
	return {
		n: ring.n,
		totalChunks: ring.totalChunks,
		qualifiedWindowRatio: ring.qualifiedWindowRatio,
		minQualifiedWindowSize: ring.minQualifiedWindowSize,
		competitionWindowSize: ring.competitionWindowSize,
		cursorBeforeMod: ring.cursorBeforeMod.toString(),
		cursorAfterMod: ring.cursorAfterMod.toString(),
		startIndex: ring.startIndex,
		windowHotkeyCount: ring.windowHotkeyCount,
		virtualCompetitorCount: ring.virtualCompetitorCount,
		nOwnerGroupsInPool: ring.nOwnerGroupsInPool,
	};
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
	const ring = selection.qualifiedRing;
	const rankingDecision =
		selection.selectionRule === "prism_final_score_desc"
			? ring
				? `qualified pool: uid ordering with chunk-scaled competition window (n=${ring.n}, totalChunks=${ring.totalChunks}, ratio=${ring.qualifiedWindowRatio}, minWindow=${ring.minQualifiedWindowSize}, window=${ring.competitionWindowSize}); ring startIndex=${ring.startIndex}, cursorMod before=${ring.cursorBeforeMod} after=${ring.cursorAfterMod}; PRISM ordering within window — top: ${topRankedSummary(selection.orchestrators)}`
				: `eligible orchestrators were ranked by highest PRISM final score; top ranking: ${topRankedSummary(selection.orchestrators)}`
			: `eligible qualifying-pool orchestrators were ordered by deterministic rotation; front of rotation: ${orderedSummary(selection.orchestrators)}`;
	const distributionDecision =
		selection.selectionRule === "prism_final_score_desc"
			? `split ${totalChunks} chunks proportionally by PRISM final score across ${nonZeroSlices.length} orchestrators; slice sizes: ${nonZeroSlices.join(",")}`
			: `split ${totalChunks} chunks as evenly as possible across ${nonZeroSlices.length} orchestrators with rotated remainder priority; slice sizes: ${nonZeroSlices.join(",")}`;
	const qualifiedSelectionNarrative = ring
		? `Qualified selection used a uid-ordered ring with a chunk-scaled competition window (n=${ring.n} hotkeys, ${ring.nOwnerGroupsInPool ?? "?"} owner groups in pool). Window size ${ring.competitionWindowSize} (${ring.windowHotkeyCount ?? "?"} hotkeys, ${ring.virtualCompetitorCount ?? "?"} virtual competitors after grouping). Stage A: inter-group Hamilton on average PRISM per owner group in the window. Stage B: intra-group Hamilton on each member's actual PRISM. Ring cursor startIndex=${ring.startIndex}; mod before ${ring.cursorBeforeMod}, after ${ring.cursorAfterMod}.`
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

function assertRequiredPoolSelection(selection: SelectionResult, testMode: boolean, transferId?: string) {
	const wrongPool = selection.orchestrators.filter((candidate) => candidate.prism_pool !== selection.preferredPool);
	if (!selection.orchestrators.length || wrongPool.length > 0) {
		const transferClass = testMode ? "test" : "production";

		throw new Error(`no ready ${selection.preferredPool} orchestrators available for ${transferClass} transfers`);
	}
}

async function insertTransferAssignment(db: DbConn, slice: TransferAssignmentSlice): Promise<string> {
	const [row] = await db<{ id: string }[]>`
    INSERT INTO core.transfer_assignments
			(transfer_id, orchestrator_id, chunk_start, chunk_end, total_chunks, chunk_size, status, assigned_at)
    VALUES
      (${slice.transferId}, ${slice.orchestratorId}, ${slice.chunkStart}, ${slice.chunkEnd},
			 ${slice.totalChunks}, ${slice.chunkSize}, 'assigned', NOW())
		ON CONFLICT (transfer_id, orchestrator_id, chunk_start, chunk_end) DO UPDATE
			SET total_chunks = EXCLUDED.total_chunks,
				chunk_size = EXCLUDED.chunk_size,
				status = 'assigned',
				assigned_at = NOW()
    RETURNING id
  `;

	return row!.id;
}

async function persistAssignmentFailure(
	db: DbConn,
	input: {
		assignmentId?: string | null;
		transferId: string;
		orchestratorId: string;
		chunkStart: number;
		chunkEnd: number;
		totalChunks: number;
		failureType: AssignmentFailureType;
		reason: string;
	},
): Promise<void> {
	await db`
    INSERT INTO core.orchestrator_assignment_failures
      (assignment_id, transfer_id, orchestrator_id, chunk_start, chunk_end, total_chunks, failure_type, reason)
    VALUES
      (${input.assignmentId ?? null}, ${input.transferId}, ${input.orchestratorId}, ${input.chunkStart}, ${input.chunkEnd}, ${input.totalChunks}, ${input.failureType}, ${input.reason})
  `;
}

export class AssignmentEngine {
	private readonly db: Db;
	private readonly gatewayClient: WorkerGatewayClient;

	constructor(deps: AssignmentEngineDeps) {
		this.db = deps.db;
		this.gatewayClient = deps.gatewayClient;
	}

	private async failTransferAssignment(transferId: string, reason: string): Promise<void> {
		await this.db`
			UPDATE core.transfers
			SET status = 'failed',
					error_message = ${reason},
					completed_at = COALESCE(completed_at, NOW())
			WHERE id = ${transferId}
				AND status NOT IN ('completed', 'failed', 'cancelled')
		`;
	}

	private async markTransferInProgress(sql: DbConn, transferId: string): Promise<void> {
		const rows = await sql<{ id: string }[]>`
			UPDATE core.transfers
			SET status = 'in_progress',
				started_at = COALESCE(started_at, NOW())
			WHERE id = ${transferId}
				AND status IN ('pending', 'planning')
			RETURNING id
		`;
		if (!rows.length) {
			throw new Error(`transfer ${transferId} not found or not assignable`);
		}
	}

	private async loadCumulativeRecoveryExclusions(
		sql: DbConn,
		input: {
			transferId: string;
			chunkIndices: number[];
			excludeOrchestratorIds: string[];
		},
	): Promise<{ orchestratorIds: string[]; ownerGroupIds: string[] }> {
		const rows = await sql<{ id: string; owner_group_id: string | null }[]>`
			WITH bundled(chunk_index) AS (
				SELECT unnest(${input.chunkIndices}::int[])
			),
			previous_orchestrators AS (
				SELECT unnest(${input.excludeOrchestratorIds}::uuid[]) AS orchestrator_id
				UNION
				SELECT t.orchestrator_id
				FROM core.tasks t
				JOIN bundled b ON b.chunk_index = t.chunk_index
				WHERE t.transfer_id = ${input.transferId}
					AND t.orchestrator_id IS NOT NULL
					AND t.state NOT IN ('completed', 'cancelled')
				UNION
				SELECT i.orchestrator_id
				FROM core.orchestrator_guardrail_interventions i
				JOIN core.tasks t ON t.id = i.task_id
				JOIN bundled b ON b.chunk_index = t.chunk_index
				WHERE t.transfer_id = ${input.transferId}
					AND i.outcome IN ('stall_redistributed', 'missing_coverage_assigned')
				UNION
				SELECT f.orchestrator_id
				FROM core.orchestrator_assignment_failures f
				JOIN bundled b ON b.chunk_index BETWEEN f.chunk_start AND f.chunk_end
				WHERE f.transfer_id = ${input.transferId}
			)
			SELECT DISTINCT o.id, o.owner_group_id
			FROM core.orchestrators o
			JOIN previous_orchestrators p ON p.orchestrator_id = o.id
		`;
		const activeStallOwners = await sql<{ orchestrator_id: string }[]>`
			WITH bundled(chunk_index) AS (
				SELECT unnest(${input.chunkIndices}::int[])
			)
			SELECT DISTINCT t.orchestrator_id
			FROM core.tasks t
			JOIN bundled b ON b.chunk_index = t.chunk_index
			WHERE t.transfer_id = ${input.transferId}
				AND t.orchestrator_id IS NOT NULL
				AND t.state NOT IN ('completed', 'cancelled')
		`;
		const speeds = await this.loadTransferOrchestratorSpeeds(sql, input.transferId);
		const ownerGroupIdsByOrch = new Map(rows.map((row) => [row.id, row.owner_group_id]));
		return applyHotOrchestratorCarveOut({
			excludedOrchestratorIds: [...new Set(rows.map((row) => row.id))],
			ownerGroupIdsByOrch,
			speeds: this.toCarveOutSpeeds(speeds),
			activeStallOwnerIds: new Set(activeStallOwners.map((row) => row.orchestrator_id)),
		});
	}

	private async loadTransferOrchestratorSpeeds(
		sql: DbConn,
		transferId: string,
	): Promise<TransferOrchestratorSpeed[]> {
		return sql<TransferOrchestratorSpeed[]>`
			SELECT
				t.orchestrator_id,
				percentile_cont(0.5) WITHIN GROUP (
					ORDER BY extract(epoch FROM (t.completed_at - t.started_at))
				) FILTER (WHERE t.started_at IS NOT NULL)::float8 AS median_relay_s,
				MAX(t.completed_at) AS last_completed_at,
				count(*)::int AS completed_count
			FROM core.tasks t
			WHERE t.transfer_id = ${transferId}
				AND t.state = 'completed'
				AND t.completed_at > NOW() - (${TRANSFER_RECOVERY_SPEED_WINDOW_SECONDS} * INTERVAL '1 second')
				AND t.orchestrator_id IS NOT NULL
			GROUP BY t.orchestrator_id
		`;
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

	private async selectOrchestrators(
		sql: DbConn,
		testMode: boolean,
		excludeIds: string[] = [],
		orderSeed?: string,
		logicalTotalChunks?: number,
		qualifiedRingCursorLocked?: bigint,
		options: {
			recoverySelection?: boolean;
			transferId?: string;
			excludeOwnerGroupIds?: string[];
			recoveryChunkCount?: number;
			recoverySpeeds?: TransferOrchestratorSpeed[];
		} = {},
	): Promise<SelectionResult> {
		const preferredPool = testMode ? "qualifying" : "qualified";
		const excludeOwnerGroupIds = options.excludeOwnerGroupIds ?? [];

		const allCandidates = await sql<OrchestratorCandidate[]>`
      SELECT
        o.id,
        o.hotkey,
		g.type                          AS gateway_type,
		g.url                          AS gateway_url,
        o.uid                          AS uid,
        o.prism_final_score            AS prism_final_score,
        o.prism_confidence_score       AS prism_confidence_score,
        o.prism_pool                   AS prism_pool,
        o.owner_group_id               AS owner_group_id
      FROM core.orchestrators o
		LEFT JOIN core.gateways g ON g.id = o.gateway_id
			WHERE o.ready = TRUE
        AND (${excludeIds.length} = 0 OR o.id != ALL(${excludeIds}))
        AND (${excludeOwnerGroupIds.length} = 0 OR o.owner_group_id IS NULL OR o.owner_group_id != ALL(${excludeOwnerGroupIds}))
    `;

		const websocketReady = allCandidates.filter((candidate) => isEligible(candidate.hotkey));
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
		let qualifiedRing: QualifiedRingSelectionMeta | undefined;
		let selectionRule: AssignmentSelectionRule;

		if (options.recoverySelection && options.transferId) {
			const speeds =
				options.recoverySpeeds ??
				(await this.loadTransferOrchestratorSpeeds(sql, options.transferId));
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
			const n = preferred.length;
			if (n === 0) {
				sorted = [];
			} else {
				if (logicalTotalChunks === undefined || qualifiedRingCursorLocked === undefined) {
					throw new Error(
						"qualified pool selection requires logicalTotalChunks and locked qualified ring cursor",
					);
				}
				const uidSorted = [...preferred].sort(
					(a, b) =>
						compareUidAscNullsLast(a.uid, b.uid) ||
						a.hotkey.localeCompare(b.hotkey) ||
						a.id.localeCompare(b.id),
				);
				const ratio = QUALIFIED_WINDOW_RATIO;
				const minW = MIN_QUALIFIED_WINDOW_SIZE;
				const competitionWindowSize = computeQualifiedCompetitionWindowSize(logicalTotalChunks, n, ratio, minW);
				const nn = BigInt(n);
				const cursorBeforeMod = qualifiedRingCursorLocked % nn;
				const startIndex = Number(cursorBeforeMod);
				sorted = pickQualifiedCompetitionWindow(uidSorted, startIndex, competitionWindowSize);
				const cursorAfter = advanceQualifiedRingCursor(qualifiedRingCursorLocked, competitionWindowSize, n);
				const cursorAfterMod = cursorAfter % nn;
				qualifiedRing = {
					n,
					totalChunks: logicalTotalChunks,
					qualifiedWindowRatio: ratio,
					minQualifiedWindowSize: minW,
					competitionWindowSize,
					cursorBeforeMod,
					cursorAfterMod,
					startIndex,
					nOwnerGroupsInPool: countDistinctOwnerGroupsInPool(preferred),
				};
			}
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
		if (qualifiedRing !== undefined) {
			result.qualifiedRing = qualifiedRing;
		}
		return result;
	}

	private async persistAssignmentPlan(
		sql: DbConn,
		transferId: string,
		pool: "qualified" | "qualifying",
		plan: QualifiedAssignmentPlan,
	): Promise<void> {
		const storablePlan =
			pool === "qualified"
				? { ...plan, ring: qualifiedRingForJson(plan.ring) }
				: plan;
		await sql`
			INSERT INTO core.transfer_assignment_plans (transfer_id, pool, plan)
			VALUES (${transferId}, ${pool}, ${JSON.stringify(storablePlan as never)})
		`;
		if (pool === "qualified") {
			const orchestratorIds = [...new Set(plan.deliveries.map((delivery) => delivery.orchestratorId))];
			if (orchestratorIds.length > 0) {
				await sql`
					UPDATE core.orchestrators
					SET prism_pool_transition_pending = FALSE,
						updated_at = NOW()
					WHERE id = ANY(${orchestratorIds}::uuid[])
						AND prism_pool_transition_pending = TRUE
				`;
			}
		}
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

			const chunkStart = remainingChunkStart;
			const chunkEnd = remainingChunkStart + sliceSize - 1;
			const assignmentId = await insertTransferAssignment(input.sql, {
				transferId: input.transferId,
				orchestratorId: orchestrator.id,
				chunkStart,
				chunkEnd,
				totalChunks: sliceSize,
				chunkSize: input.chunkSize,
			});

			assignmentIds.push(assignmentId);
			pendingPushes.push({
				assignmentId,
				orchestrator,
				chunkStart,
				chunkEnd,
				totalChunks: sliceSize,
				chunkSize: input.chunkSize,
			});
			assignedChunkCount += sliceSize;
			assignmentSlices.push(summarizeAssignmentSlice(orchestrator, chunkStart, chunkEnd, sliceSize));
			remainingChunkStart += sliceSize;
			remainingChunks -= sliceSize;
		}

		return {
			assignmentIds,
			assignedChunkCount,
			assignmentSlices,
			pendingPushes,
			coveredAllChunks: remainingChunks === 0,
		};
	}

	private async dispatchAssignmentPushes(
		transferId: string,
		metadata: TransferMetadata | null,
		pushes: PendingAssignmentPush[],
	): Promise<void> {
		const destinationUrl = extractDestinationUrl(metadata);
		await Promise.all(
			pushes.map(async (push) => {
				const pushed = pushToOrchestrator(push.orchestrator.hotkey, {
					type: "transfer_assigned",
					assignment_id: push.assignmentId,
					transfer_id: transferId,
					chunk_start: push.chunkStart,
					chunk_end: push.chunkEnd,
					total_chunks: push.totalChunks,
					chunk_size: push.chunkSize,
					gateway_url:
						workerGatewayUrlForType(push.orchestrator.gateway_type, push.orchestrator.gateway_url) ??
						configuredPublicWorkerGatewayUrl(),
					destination_url: destinationUrl,
				});
				if (pushed) return;
				await this.expireTransferAssignment({
					assignmentId: push.assignmentId,
					reason: "websocket_not_ready_or_push_rejected",
				});
				await persistAssignmentFailure(this.db, {
					assignmentId: push.assignmentId,
					transferId,
					orchestratorId: push.orchestrator.id,
					chunkStart: push.chunkStart,
					chunkEnd: push.chunkEnd,
					totalChunks: push.totalChunks,
					failureType: "ws_push_failed",
					reason: "websocket_not_ready_or_push_rejected",
				});

			}),
		);
	}

	async reassignStalledChunkBundle(input: {
		transferId: string;
		chunkIndices: number[];
		excludeOrchestratorIds: string[];
		reason: string;
		staleAssignmentIds?: string[];
	}): Promise<ReassignChunkBundleResult> {
		const chunkIndices = [...new Set(input.chunkIndices)].sort((a, b) => a - b);
		const empty: ReassignChunkBundleResult = {
			ok: true,
			assignmentIds: [],
			assignmentSlices: [],
			pendingPushes: [],
		};
		if (!chunkIndices.length) return empty;

		const transfers = await this.db<{ chunk_size: number; metadata: unknown; total_chunks: number }[]>`
			SELECT chunk_size, metadata, total_chunks FROM core.transfers WHERE id = ${input.transferId} LIMIT 1
			`;
		const transfer = transfers[0];
		if (!transfer) {
			throw new Error(`transfer ${input.transferId} not found`);
		}

		const metadata = parseTransferMetadata(transfer.metadata);
		const testMode = Boolean(metadata?.test_mode);
		const transferTotalChunks = transfer.total_chunks;
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
		};
		type BundleBusy = {
			ok: false;
			kind: "no_orchs" | "partial_coverage";
		};

		const tryBundleSql = async (sql: DbConn): Promise<BundleOk | BundleBusy> => {
			if (input.staleAssignmentIds?.length) {
				await sql`
					INSERT INTO core.orchestrator_assignment_failures
						(assignment_id, transfer_id, orchestrator_id, chunk_start, chunk_end, total_chunks, failure_type, reason)
					SELECT
						ta.id,
						ta.transfer_id,
						ta.orchestrator_id,
						ta.chunk_start,
						ta.chunk_end,
						ta.total_chunks,
						'stale_assignment_timeout',
						'assignment_produced_no_tasks_before_timeout'
					FROM core.transfer_assignments ta
					WHERE ta.id = ANY(${input.staleAssignmentIds})
						AND EXISTS (
							SELECT 1
							FROM unnest(${chunkIndices}::int[]) AS bundled(chunk_index)
							WHERE bundled.chunk_index BETWEEN ta.chunk_start AND ta.chunk_end
						)
				`;
			}

			const recoverySpeeds = await this.loadTransferOrchestratorSpeeds(sql, input.transferId);

			let selection: SelectionResult;
			const cumulativeExclusions = await this.loadCumulativeRecoveryExclusions(sql, {
				transferId: input.transferId,
				chunkIndices,
				excludeOrchestratorIds: input.excludeOrchestratorIds,
			});
			try {
				selection = await this.selectOrchestrators(
					sql,
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
					},
				);
				assertRequiredPoolSelection(selection, testMode, input.transferId);
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

			await sql`
				UPDATE core.transfer_assignments
				SET status = 'expired',
					completed_at = COALESCE(completed_at, NOW())
				WHERE transfer_id = ${input.transferId}
					AND status NOT IN ('completed', 'failed', 'expired')
					AND EXISTS (
						SELECT 1
						FROM unnest(${chunksToAssign}::int[]) AS bundled(chunk_index)
						WHERE bundled.chunk_index BETWEEN chunk_start AND chunk_end
					)
			`;

			const tasksBefore = await sql<BundleTaskBefore[]>`
				SELECT id, chunk_index, orchestrator_id, assigned_worker_id
				FROM core.tasks
				WHERE transfer_id = ${input.transferId}
					AND chunk_index = ANY(${chunksToAssign})
					AND state NOT IN ('completed', 'cancelled')
				FOR UPDATE
			`;

			const assignmentInputs = virtualRows.map((row) => ({
				transfer_id: input.transferId,
				orchestrator_id: row.orchestrator.id,
				chunk_start: row.chunkStart,
				chunk_end: row.chunkEnd,
				total_chunks: row.totalChunks,
				chunk_size: transfer.chunk_size,
			}));
			const insertedAssignments = await sql<InsertedAssignmentRow[]>`
				WITH input_rows AS (
					SELECT *
					FROM jsonb_to_recordset(${JSON.stringify(assignmentInputs)}::jsonb) AS r(
						transfer_id uuid,
						orchestrator_id uuid,
						chunk_start int,
						chunk_end int,
						total_chunks int,
						chunk_size int
					)
				)
				INSERT INTO core.transfer_assignments
					(transfer_id, orchestrator_id, chunk_start, chunk_end, total_chunks, chunk_size, status, assigned_at)
				SELECT transfer_id, orchestrator_id, chunk_start, chunk_end, total_chunks, chunk_size, 'assigned', NOW()
				FROM input_rows
				ON CONFLICT (transfer_id, orchestrator_id, chunk_start, chunk_end) DO UPDATE
					SET total_chunks = EXCLUDED.total_chunks,
						chunk_size = EXCLUDED.chunk_size,
						status = 'assigned',
						assigned_at = NOW()
				RETURNING id, orchestrator_id, chunk_start, chunk_end
			`;
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
			for (const row of virtualRows) {
				const inserted = assignmentByKey.get(`${row.orchestrator.id}:${row.chunkStart}:${row.chunkEnd}`);
				if (!inserted) continue;
				assignmentSlices.push(summarizeAssignmentSlice(row.orchestrator, row.chunkStart, row.chunkEnd, row.totalChunks));
				pendingPushes.push({
					assignmentId: inserted.id,
					orchestrator: row.orchestrator,
					chunkStart: row.chunkStart,
					chunkEnd: row.chunkEnd,
					totalChunks: row.totalChunks,
					chunkSize: transfer.chunk_size,
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
			const ownerInputs = [...chunkOwners.entries()].map(([chunkIndex, owner]) => ({
				chunk_index: chunkIndex,
				assignment_id: owner.assignmentId,
				orchestrator_id: owner.orchestratorId,
			}));
			const interventionInputs = tasksBefore.flatMap((task) => {
				const owner = chunkOwners.get(task.chunk_index);
				if (!owner) return [];
				return [
					{
						task_id: task.id,
						transfer_id: input.transferId,
						orchestrator_id: task.orchestrator_id,
						previous_worker_id: task.assigned_worker_id,
						reason: input.reason,
						outcome,
					},
				];
			});
			if (interventionInputs.length) {
				await sql`
					INSERT INTO core.orchestrator_guardrail_interventions
						(task_id, transfer_id, orchestrator_id, previous_worker_id, replacement_worker_id,
						 intervention_type, reason, outcome)
					SELECT
						task_id,
						transfer_id,
						orchestrator_id,
						previous_worker_id,
						NULL::varchar,
						'beamcore_overseer_recovery',
						reason,
						outcome
					FROM jsonb_to_recordset(${JSON.stringify(interventionInputs)}::jsonb) AS r(
						task_id uuid,
						transfer_id uuid,
						orchestrator_id uuid,
						previous_worker_id varchar,
						reason text,
						outcome varchar
					)
				`;
			}
			if (ownerInputs.length) {
				await sql`
					WITH owners AS (
						SELECT *
						FROM jsonb_to_recordset(${JSON.stringify(ownerInputs)}::jsonb) AS o(
							chunk_index int,
							assignment_id uuid,
							orchestrator_id uuid
						)
					)
					UPDATE core.tasks t
					SET assignment_id = owners.assignment_id,
						orchestrator_id = owners.orchestrator_id,
						assigned_worker_id = NULL,
						state = 'pending',
						offered_at = NULL,
						accepted_at = NULL,
						started_at = NULL,
						failed_at = NULL,
						failure_reason = NULL,
						current_attempt_id = NULL
					FROM owners
					WHERE t.transfer_id = ${input.transferId}
						AND t.chunk_index = owners.chunk_index
						AND t.state NOT IN ('completed', 'cancelled')
				`;
			}

			return {
				ok: true,
				selection,
				assignmentIds,
				assignmentSlices,
				pendingPushes,
				partial,
				assignedChunkCount: assignableCount,
				remainingChunkCount: batchChunkCount - assignableCount,
			};
		};

		const finishOk = (result: BundleOk): ReassignChunkBundleResult => {
			void this.dispatchAssignmentPushes(input.transferId, metadata, result.pendingPushes).catch((err: unknown) => {

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

		const result = await this.db.begin((sql) => tryBundleSql(sql));
		if (result.ok) {
			return finishOk(result);
		}
		return {
			ok: false,
			assignmentIds: [],
			assignmentSlices: [],
			pendingPushes: [],
			kind: result.kind,
		};
	}

	async findStaleAssignmentBundles(transferIds?: string[]): Promise<StaleAssignmentBundle[]> {
		const scopedTransferIds = transferIds?.length ? transferIds : null;
		const stale = await this.db<
			{
				id: string;
				transfer_id: string;
				orchestrator_id: string;
				chunk_start: number;
				chunk_end: number;
				total_chunks: number;
				chunk_size: number;
				metadata: unknown;
			}[]
		>`
      SELECT
        ta.id,
        ta.transfer_id     AS transfer_id,
        ta.orchestrator_id AS orchestrator_id,
        ta.chunk_start     AS chunk_start,
        ta.chunk_end       AS chunk_end,
        ta.total_chunks    AS total_chunks,
        ta.chunk_size      AS chunk_size,
        t.metadata
      FROM core.transfer_assignments ta
      JOIN core.transfers t ON t.id = ta.transfer_id
      WHERE ta.status = 'assigned'
			AND ta.assigned_at < NOW() - (${ZERO_TASK_ASSIGNMENT_TIMEOUT_SECONDS} * INTERVAL '1 second')
        AND NOT EXISTS (
          SELECT 1 FROM core.tasks tk WHERE tk.assignment_id = ta.id
        )
				AND (
					t.status IN ('planning', 'in_progress')
					OR (
						t.status = 'pending'
						AND (
							EXISTS (
								SELECT 1
								FROM core.tasks tk
								WHERE tk.transfer_id = t.id
							)
							OR EXISTS (
								SELECT 1
								FROM core.transfer_assignments ta2
								WHERE ta2.transfer_id = t.id
							)
						)
					)
				)
				AND (
					${scopedTransferIds}::uuid[] IS NULL
					OR ta.transfer_id = ANY(${scopedTransferIds}::uuid[])
				)
    `;
		if (!stale.length) return [];

		const grouped = new Map<string, StaleAssignmentBundle>();
		for (const assignment of stale) {
			const chunkIndices = Array.from(
				{ length: assignment.chunk_end - assignment.chunk_start + 1 },
				(_, offset) => assignment.chunk_start + offset,
			);
			const entry: StaleAssignmentEntry = {
				id: assignment.id,
				orchestratorId: assignment.orchestrator_id,
				chunkStart: assignment.chunk_start,
				chunkEnd: assignment.chunk_end,
			};
			const bundle = grouped.get(assignment.transfer_id) ?? {
				transferId: assignment.transfer_id,
				chunkIndices: [],
				excludeOrchestratorIds: [],
				staleAssignmentIds: [],
				staleAssignments: [],
			};
			bundle.chunkIndices.push(...chunkIndices);
			bundle.excludeOrchestratorIds.push(assignment.orchestrator_id);
			bundle.staleAssignmentIds.push(assignment.id);
			bundle.staleAssignments.push(entry);
			grouped.set(assignment.transfer_id, bundle);
		}

		return [...grouped.values()].map((bundle) => ({
			transferId: bundle.transferId,
			chunkIndices: [...new Set(bundle.chunkIndices)].sort((a, b) => a - b),
			excludeOrchestratorIds: [...new Set(bundle.excludeOrchestratorIds)],
			staleAssignmentIds: bundle.staleAssignmentIds,
			staleAssignments: bundle.staleAssignments,
		}));
	}

	async collectStaleAssignmentBundles(): Promise<StaleAssignmentBundle[]> {
		return this.findStaleAssignmentBundles();
	}

	async redistributeStaleAssignments(): Promise<void> {
		const bundles = await this.findStaleAssignmentBundles();
		for (const bundle of bundles) {
			const result = await this.reassignStalledChunkBundle({
				transferId: bundle.transferId,
				chunkIndices: bundle.chunkIndices,
				excludeOrchestratorIds: bundle.excludeOrchestratorIds,
				staleAssignmentIds: bundle.staleAssignmentIds,
				reason: "stale_assignment_timeout",
			});

			if (result.ok) {

				continue;
			}

			await this.db`
				UPDATE core.transfer_assignments
				SET assigned_at = NOW()
				WHERE id = ANY(${bundle.staleAssignmentIds})
			`;

		}
	}

	async assignTransfer(transferId: string): Promise<string[]> {
		const transfers = await this.db<
			{
				id: string;
				total_chunks: number;
				chunk_size: number;
				metadata: unknown;
			}[]
		>`
      SELECT id, total_chunks AS total_chunks, chunk_size AS chunk_size, metadata
      FROM core.transfers
      WHERE id = ${transferId} AND status IN ('pending', 'planning')
      LIMIT 1
    `;
		const transfer = transfers[0];
		if (!transfer) throw new Error(`transfer ${transferId} not found or not assignable`);

		const metadata = parseTransferMetadata(transfer.metadata);
		const testMode = Boolean(metadata?.test_mode);
		const logicalTotalChunks =
			metadata?.transfer_version === "signed_url_v1" && typeof metadata.logical_chunk_count === "number"
				? metadata.logical_chunk_count
				: transfer.total_chunks;

		try {
			if (testMode) {
				const captured = await this.db.begin(async (sql) => {
					const selection = await this.selectOrchestrators(sql, testMode, [], transferId);
					assertRequiredPoolSelection(selection, testMode, transferId);
					const sliceSizes = allocateChunkSlices(
						selection.orchestrators,
						logicalTotalChunks,
						selection.selectionRule,
					);
					const logic = buildAssignmentLogicSummary(testMode, selection, logicalTotalChunks, sliceSizes);

					await this.markTransferInProgress(sql, transferId);
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

						throw new Error(COVERAGE_FAILURE_MESSAGE);
					}

					return { selection, sliceSizes, logic, coverage };
				});



				void this.dispatchAssignmentPushes(transferId, metadata, captured.coverage.pendingPushes).catch((err: unknown) => {

				});

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
					const [ringRow] = await sql<{ qualified_ring_cursor: string }[]>`
						SELECT qualified_ring_cursor FROM core.qualified_assignment_ring WHERE singleton = TRUE FOR UPDATE
					`;
					const lockedCursor = BigInt(ringRow?.qualified_ring_cursor ?? "0");

					const selection = await this.selectOrchestrators(
						sql,
						false,
						[],
						transferId,
						logicalTotalChunks,
						lockedCursor,
					);
					assertRequiredPoolSelection(selection, false, transferId);

					const q = selection.qualifiedRing;
					if (!q) {
						throw new Error("qualified assignment missing qualifiedRing metadata");
					}

					const slicePlan = buildQualifiedWindowSlicePlan({
						transferId,
						windowOrchs: selection.orchestrators,
						totalChunks: logicalTotalChunks,
						ring: q,
						preferredHotkeys: selection.counts.preferred,
						ownerGroupsInPool: q.nOwnerGroupsInPool ?? selection.counts.preferred,
					});
					selection.orchestrators = slicePlan.orchestrators;
					selection.qualifiedAssignmentPlan = slicePlan.plan;

					await this.markTransferInProgress(sql, transferId);
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

					const nextStored = advanceQualifiedRingCursor(lockedCursor, q.competitionWindowSize, q.n);
					await sql`
						UPDATE core.qualified_assignment_ring
						SET qualified_ring_cursor = ${Number(nextStored)}
						WHERE singleton = TRUE
					`;

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



			void this.dispatchAssignmentPushes(transferId, metadata, captured.coverage.pendingPushes).catch((err: unknown) => {

			});

			return captured.assignmentIds;
		} catch (error) {
			if (isTerminalAssignmentFailure(error)) {
				await this.failTransferAssignment(transferId, error.message);
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
		await this.db`
			UPDATE core.transfer_assignments
			SET status = 'expired',
				completed_at = COALESCE(completed_at, NOW())
			WHERE id = ${input.assignmentId}
				AND status NOT IN ('completed', 'failed', 'expired')
		`;
	}

	async reassignTask(taskId: string, excludeWorkerId?: string): Promise<void> {
		const tasks = await this.db<{ orchestrator_id: string }[]>`
      SELECT orchestrator_id AS orchestrator_id FROM core.tasks WHERE id = ${taskId}
    `;
		const task = tasks[0];
		if (!task) return;

		const orchs = await this.db<{ hotkey: string }[]>`
	      SELECT hotkey FROM core.orchestrators WHERE id = ${task.orchestrator_id} LIMIT 1
    `;
		if (!orchs.length) return;

		pushToOrchestrator(orchs[0]!.hotkey, {
			type: "task_reassign",
			task_id: taskId,
			...(excludeWorkerId ? { exclude_worker_id: excludeWorkerId } : {}),
		});
	}
}
