/**
 * Transfer assignment: selects orchestrators by PRISM tier and score, allocates chunk slices,
 * persists assignments, and notifies orchestrators over the live connection registry.
 *
 * This transparency copy inlines supporting helpers that live in separate modules in the full
 * BeamCore tree (`chunking`, `transfer-metadata`, worker eligibility, orchestrator WebSocket registry).
 */

/** Minimal WebSocket shape used by the orchestrator registry (production uses `ws`). */

interface OrchestratorWebSocket {
	readonly readyState: number;
	send(data: string): void;
}

/** Control-plane public gateway URL baseline for orchestrator-owned gateway detection (deployment-specific). */
const CONTROL_PLANE_PUBLIC_GATEWAY_URL = "https://gateway.invalid";

/** Seconds after assignment creation without a related task before stale redistribution (deployment-specific). */
const STALE_ASSIGNMENT_TIMEOUT_SECONDS = 900;

/** Default gateway URL embedded in orchestrator push payloads when the orchestrator row has none (deployment-specific). */
const DEFAULT_WORKER_GATEWAY_BASE_URL = "https://gateway.invalid";

/** Qualified-pool chunk scaling for competition window size (mirrors `QUALIFIED_WINDOW_RATIO` in full BeamCore). */
const QUALIFIED_WINDOW_RATIO = 0.6;
/** Floor for qualified competition window (mirrors `MIN_QUALIFIED_WINDOW_SIZE`). */
const MIN_QUALIFIED_WINDOW_SIZE = 10;

type OrchestratorSession =
	| { kind: "direct"; ws: OrchestratorWebSocket }
	| { kind: "relay"; ws: OrchestratorWebSocket };

const orchestratorSockets = new Map<string, OrchestratorSession>();

/** Live orchestrator WebSocket sessions are registered by the control-plane HTTP stack (see `orchestrator-registry` in the full BeamCore tree). */

function isOrchestratorConnected(hotkey: string): boolean {
	const session = orchestratorSockets.get(hotkey);
	return session?.ws.readyState === 1;
}

function isEligible(hotkey: string): boolean {
	return isOrchestratorConnected(hotkey);
}

function connectedHotkeys(): string[] {
	return [...orchestratorSockets.entries()].filter(([, session]) => session.ws.readyState === 1).map(([hotkey]) => hotkey);
}

function pushToOrchestrator(hotkey: string, msg: unknown): boolean {
	const session = orchestratorSockets.get(hotkey);
	if (!session || session.ws.readyState !== 1) return false;

	if (session.kind === "relay") {
		session.ws.send(
			JSON.stringify({
				type: "relay_to_orch",
				hotkey,
				payload: msg,
			}),
		);
		return true;
	}

	session.ws.send(JSON.stringify(msg));
	return true;
}

type SqlTag = <T = unknown>(strings: TemplateStringsArray, ...values: unknown[]) => Promise<T>;

/** Tagged SQL plus optional postgres-style `begin` for qualified transactional ring cursor. */
export type Db = SqlTag & { begin?: <T>(callback: (sql: SqlTag) => Promise<T>) => Promise<T> };

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

// Hamilton / largest-remainder method — guarantees sum === totalChunks, deterministic.
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

export interface SessionSummary {
	workerId: string;
	gatewayMode: "public" | "orch_owned";
	gatewayUrl: string | null;
	orchestratorId: string | null;
}

export interface WorkerGatewayClient {
	getConnectedSessions(): Promise<SessionSummary[]>;
}

interface WorkerPoolSqlRow {
	worker_id: string;
	status: string;
	region: string | null;
	bandwidth_mbps: string;
	trust_score: string;
	total_tasks: string;
	successful_tasks: string;
	bytes_relayed_total: string;
	capacity: number | null;
	min_payment_amount: string | null;
	min_payment_currency: string | null;
}

interface WorkerPoolEntry {
	worker_id: string;
	status: string;
	region: string | null;
	bandwidth_mbps: number;
	trust_score: number;
	total_tasks: number;
	successful_tasks: number;
	success_rate: number;
	bytes_relayed_total: number;
	capacity: number | null;
	min_payment_amount: number | null;
	min_payment_currency: string | null;
}

function serializeWorkerPoolRow(row: WorkerPoolSqlRow): WorkerPoolEntry {
	const totalTasks = Number(row.total_tasks);
	const successfulTasks = Number(row.successful_tasks);
	const bandwidthMbps = Number(row.bandwidth_mbps);
	const trustScore = Number(row.trust_score);
	const bytesRelayedTotal = Number(row.bytes_relayed_total);
	const successRate = totalTasks > 0 ? successfulTasks / totalTasks : 1;

	return {
		worker_id: row.worker_id,
		status: row.status,
		region: row.region,
		bandwidth_mbps: bandwidthMbps,
		trust_score: trustScore,
		total_tasks: totalTasks,
		successful_tasks: successfulTasks,
		success_rate: successRate,
		bytes_relayed_total: bytesRelayedTotal,
		capacity: row.capacity,
		min_payment_amount: row.min_payment_amount != null ? Number(row.min_payment_amount) : null,
		min_payment_currency: row.min_payment_currency,
	};
}

function normalizeGatewayUrl(url: string | null | undefined): string | null {
	if (!url) return null;
	return url.replace(/\/+$/, "");
}

function isOrchOwnedGatewayUrl(url: string | null | undefined): boolean {
	const normalized = normalizeGatewayUrl(url);
	return normalized !== null && normalized !== normalizeGatewayUrl(CONTROL_PLANE_PUBLIC_GATEWAY_URL);
}

async function isOrchOwnedGateway(db: Db, url: string | null | undefined): Promise<boolean> {
	const normalized = normalizeGatewayUrl(url);
	if (!normalized) return false;

	const rows = await db<{ type: string }[]>`
		SELECT type
		FROM core.gateways
		WHERE RTRIM(url, '/') = ${normalized}
		ORDER BY updated_at DESC NULLS LAST
		LIMIT 1
	`;
	const type = rows[0]?.type;
	if (type) return type === "orch_owned";

	return isOrchOwnedGatewayUrl(normalized);
}

async function listEligibleWorkersForOrchestrator(
	db: SqlTag,
	gatewayClient: WorkerGatewayClient,
	orchestratorId: string,
	excludeWorkerIds: string[] = [],
): Promise<WorkerPoolEntry[]> {
	const orchRows = await db<{ gatewayUrl: string | null }[]>`
		SELECT gateway_url AS gateway_url
		FROM core.orchestrators
		WHERE id = ${orchestratorId}
		LIMIT 1
	`;
	const orchGatewayUrl = orchRows[0]?.gatewayUrl ?? null;
	const ownsGateway = await isOrchOwnedGateway(db, orchGatewayUrl);
	const gatewaySessions = await gatewayClient.getConnectedSessions().catch(() => []);
	const connectedIds = gatewaySessions
		.filter((session) =>
			ownsGateway
				? session.gatewayMode === "orch_owned" &&
					session.orchestratorId === orchestratorId &&
					session.gatewayUrl === orchGatewayUrl
				: session.gatewayMode === "public",
		)
		.map((session) => session.workerId);
	if (!connectedIds.length) {
		return [];
	}

	const excludedIds = excludeWorkerIds.filter(Boolean);
	const rows = await db<WorkerPoolSqlRow[]>`
    SELECT
      w.worker_id               AS worker_id,
      w.status,
      w.region,
      w.claimed_bandwidth_mbps  AS bandwidth_mbps,
      w.trust_score             AS trust_score,
      w.total_tasks             AS total_tasks,
      w.successful_tasks        AS successful_tasks,
      w.bytes_relayed_total     AS bytes_relayed_total,
      ws.capacity,
      w.min_payment_amount      AS min_payment_amount,
      w.min_payment_currency    AS min_payment_currency
    FROM core.worker_sessions ws
    JOIN core.workers w ON w.worker_id = ws.worker_id
    WHERE w.worker_id = ANY(${connectedIds})
      AND ws.gateway_mode = ${ownsGateway ? "orch_owned" : "public"}
      ${ownsGateway ? db`AND ws.gateway_url = ${orchGatewayUrl}` : db``}
      AND (${excludedIds.length} = 0 OR NOT (w.worker_id = ANY(${excludedIds})))
			AND (
				ws.capacity IS NULL
				OR (
					SELECT COUNT(*)::INT
					FROM core.tasks tk
					WHERE tk.assigned_worker_id = w.worker_id
						AND tk.state IN ('offered', 'accepted', 'in_progress')
				) < ws.capacity
			)
      AND NOT EXISTS (
        SELECT 1 FROM core.worker_orchestrator_exclusions e
        WHERE e.worker_id = w.worker_id AND e.orchestrator_id = ${orchestratorId}
      )
		ORDER BY
			(
				SELECT COUNT(*)::INT
				FROM core.tasks tk
				WHERE tk.assigned_worker_id = w.worker_id
					AND tk.state IN ('offered', 'accepted', 'in_progress')
			) ASC,
			w.trust_score DESC,
			w.worker_id ASC
  `;

	return rows.map(serializeWorkerPoolRow);
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

interface OrchestratorCandidate {
	id: string;
	hotkey: string;
	gatewayUrl: string | null;
	uid: number | null;
	prismFinalScore: string;
	prismConfidenceScore: string;
	prismPool: "qualifying" | "qualified";
}

export interface QualifiedRingSelectionMeta {
	n: number;
	totalChunks: number;
	qualifiedWindowRatio: number;
	minQualifiedWindowSize: number;
	competitionWindowSize: number;
	cursorBeforeMod: bigint;
	cursorAfterMod: bigint;
	startIndex: number;
}

type AssignmentSelectionRule = "prism_final_score_desc" | "qualifying_equal_share_rotation";

interface SelectionResult {
	orchestrators: OrchestratorCandidate[];
	preferredPool: "qualifying" | "qualified";
	selectionRule: AssignmentSelectionRule;
	qualifiedRing?: QualifiedRingSelectionMeta;
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
		workerReadyHotkeys: string[];
		missingWebsocketHotkeys: string[];
		missingWorkerHotkeys: string[];
		connectedRegistryHotkeys: string[];
	};
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
	coveredAllChunks: boolean;
}

type AssignmentFailureType = "stale_assignment_timeout" | "ws_push_failed";

function numericValue(value: string | number | null | undefined, fallback = 0): number {
	if (typeof value === "number") return Number.isFinite(value) ? value : fallback;
	if (typeof value === "string") {
		const parsed = Number(value);
		return Number.isFinite(parsed) ? parsed : fallback;
	}
	return fallback;
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
		prismPool: candidate.prismPool,
		chunkRange: `${chunkStart}-${chunkEnd}`,
		sliceSize,
	};
}

function hashSeed(seed: string): number {
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

export function allocateChunkSlices(
	candidates: OrchestratorCandidate[],
	totalChunks: number,
	selectionRule: SelectionResult["selectionRule"],
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

	const scores = activeCandidates.map((c) => Math.max(0, numericValue(c.prismFinalScore)));
	const activeSlices = computePrismSlices(scores, totalChunks);
	for (let i = 0; i < activeCount; i++) {
		sliceSizes[i] = activeSlices[i] ?? 0;
	}
	return sliceSizes;
}

function assertRequiredPoolSelection(selection: SelectionResult, testMode: boolean) {
	const wrongPool = selection.orchestrators.filter((candidate) => candidate.prismPool !== selection.preferredPool);
	if (!selection.orchestrators.length || wrongPool.length > 0) {
		const transferClass = testMode ? "test" : "production";
		throw new Error(`no ready ${selection.preferredPool} orchestrators available for ${transferClass} transfers`);
	}
}

async function insertTransferAssignment(db: Db, slice: TransferAssignmentSlice): Promise<string> {
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
	db: Db,
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

	private async selectOrchestrators(
		sql: SqlTag,
		testMode: boolean,
		excludeIds: string[] = [],
		orderSeed?: string,
		logicalTotalChunks?: number,
		qualifiedRingCursorLocked?: bigint,
	): Promise<SelectionResult> {
		const preferredPool = testMode ? "qualifying" : "qualified";

		const allCandidates = await sql<OrchestratorCandidate[]>`
      SELECT
        o.id,
        o.hotkey,
        o.gateway_url                  AS gateway_url,
        o.uid                          AS uid,
        o.prism_final_score            AS prism_final_score,
        o.prism_confidence_score       AS prism_confidence_score,
        o.prism_pool                   AS prism_pool
      FROM core.orchestrators o
      WHERE o.status = 'active'
        AND o.ready = TRUE
        AND (${excludeIds.length} = 0 OR o.id != ALL(${excludeIds}))
    `;

		const websocketReady = allCandidates.filter((candidate) => isEligible(candidate.hotkey));
		const workerEligibility = await Promise.all(
			websocketReady.map(async (candidate) => {
				try {
					const workers = await listEligibleWorkersForOrchestrator(sql, this.gatewayClient, candidate.id);
					return { candidate, workerCount: workers.length };
				} catch {
					return { candidate, workerCount: 0 };
				}
			}),
		);
		const eligible = workerEligibility
			.filter((result) => result.workerCount > 0)
			.map((result) => result.candidate);
		const connectedRegistry = connectedHotkeys().sort((left, right) => left.localeCompare(right));
		const queriedHotkeys = allCandidates.map((candidate) => candidate.hotkey);
		const websocketReadyHotkeys = websocketReady.map((candidate) => candidate.hotkey);
		const workerReadyHotkeys = workerEligibility
			.filter((result) => result.workerCount > 0)
			.map((result) => result.candidate.hotkey);
		const missingWebsocketHotkeys = queriedHotkeys.filter((hotkey) => !websocketReadyHotkeys.includes(hotkey));
		const missingWorkerHotkeys = websocketReadyHotkeys.filter((hotkey) => !workerReadyHotkeys.includes(hotkey));

		const preferred = eligible.filter(
			(c) =>
				c.prismPool === preferredPool &&
				(preferredPool === "qualifying" || numericValue(c.prismFinalScore) > 0),
		);
		const selectionRule =
			preferredPool === "qualifying"
				? ("qualifying_equal_share_rotation" as const)
				: ("prism_final_score_desc" as const);

		let sorted: OrchestratorCandidate[];
		let qualifiedRing: QualifiedRingSelectionMeta | undefined;

		if (selectionRule === "qualifying_equal_share_rotation") {
			sorted = rotateCandidates(
				[...preferred].sort((a, b) => a.hotkey.localeCompare(b.hotkey) || a.id.localeCompare(b.id)),
				preferred.length > 0 ? hashSeed(orderSeed ?? "") % preferred.length : 0,
			);
		} else {
			const n = preferred.length;
			if (logicalTotalChunks === undefined || qualifiedRingCursorLocked === undefined) {
				throw new Error("qualified pool selection requires logicalTotalChunks and locked qualified ring cursor");
			}
			const uidSorted = [...preferred].sort(
				(a, b) =>
					compareUidAscNullsLast(a.uid, b.uid) ||
					a.hotkey.localeCompare(b.hotkey) ||
					a.id.localeCompare(b.id),
			);
			const competitionWindowSize = computeQualifiedCompetitionWindowSize(
				logicalTotalChunks,
				n,
				QUALIFIED_WINDOW_RATIO,
				MIN_QUALIFIED_WINDOW_SIZE,
			);
			const nn = BigInt(n);
			const cursorBeforeMod = qualifiedRingCursorLocked % nn;
			const startIndex = Number(cursorBeforeMod);
			sorted = pickQualifiedCompetitionWindow(uidSorted, startIndex, competitionWindowSize);
			const cursorAfterMod = advanceQualifiedRingCursor(
				qualifiedRingCursorLocked,
				competitionWindowSize,
				n,
			);
			qualifiedRing = {
				n,
				totalChunks: logicalTotalChunks,
				qualifiedWindowRatio: QUALIFIED_WINDOW_RATIO,
				minQualifiedWindowSize: MIN_QUALIFIED_WINDOW_SIZE,
				competitionWindowSize,
				cursorBeforeMod,
				cursorAfterMod,
				startIndex,
			};
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

		return {
			orchestrators: sorted,
			preferredPool,
			selectionRule,
			qualifiedRing,
			counts,
			diagnostics: {
				queriedHotkeys,
				websocketReadyHotkeys,
				workerReadyHotkeys,
				missingWebsocketHotkeys,
				missingWorkerHotkeys,
				connectedRegistryHotkeys: connectedRegistry,
			},
		};
	}

	private async assignChunkCoverage(input: {
		sql: SqlTag;
		transferId: string;
		chunkStart: number;
		totalChunks: number;
		chunkSize: number;
		metadata: TransferMetadata | null;
		orchestrators: OrchestratorCandidate[];
		selectionRule: SelectionResult["selectionRule"];
	}): Promise<AssignmentCoverageResult> {
		const assignmentIds: string[] = [];
		const assignmentSlices: Array<ReturnType<typeof summarizeAssignmentSlice>> = [];
		let assignedChunkCount = 0;
		let remainingChunkStart = input.chunkStart;
		let remainingChunks = input.totalChunks;
		const available = [...input.orchestrators];
		let plannedSliceSizes = allocateChunkSlices(available, remainingChunks, input.selectionRule);

		while (remainingChunks > 0 && available.length > 0) {
			const orchestrator = available.shift()!;
			const sliceSize = plannedSliceSizes.shift() ?? 0;
			if (sliceSize <= 0) {
				continue;
			}

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

			const pushed = pushToOrchestrator(orchestrator.hotkey, {
				type: "transfer_assigned",
				assignment_id: assignmentId,
				transfer_id: input.transferId,
				chunk_start: chunkStart,
				chunk_end: chunkEnd,
				total_chunks: sliceSize,
				chunk_size: input.chunkSize,
				gateway_url: orchestrator.gatewayUrl ?? DEFAULT_WORKER_GATEWAY_BASE_URL,
				destination_url: extractDestinationUrl(input.metadata),
			});

			if (!pushed) {
				await input.sql`DELETE FROM core.transfer_assignments WHERE id = ${assignmentId}`;
				await persistAssignmentFailure(input.sql, {
					assignmentId,
					transferId: input.transferId,
					orchestratorId: orchestrator.id,
					chunkStart,
					chunkEnd,
					totalChunks: sliceSize,
					failureType: "ws_push_failed",
					reason: "websocket_not_ready_or_push_rejected",
				});
				plannedSliceSizes = allocateChunkSlices(available, remainingChunks, input.selectionRule);
				continue;
			}

			assignmentIds.push(assignmentId);
			assignedChunkCount += sliceSize;
			assignmentSlices.push(summarizeAssignmentSlice(orchestrator, chunkStart, chunkEnd, sliceSize));
			remainingChunkStart += sliceSize;
			remainingChunks -= sliceSize;
		}

		return {
			assignmentIds,
			assignedChunkCount,
			assignmentSlices,
			coveredAllChunks: remainingChunks === 0,
		};
	}

	async redistributeStaleAssignments(): Promise<void> {
		const stale = await this.db<
			{
				id: string;
				transferId: string;
				orchestratorId: string;
				chunkStart: number;
				chunkEnd: number;
				totalChunks: number;
				chunkSize: number;
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
			AND ta.assigned_at < NOW() - (${STALE_ASSIGNMENT_TIMEOUT_SECONDS} * INTERVAL '1 second')
        AND NOT EXISTS (
          SELECT 1 FROM core.tasks tk WHERE tk.assignment_id = ta.id
        )
    `;

		for (const assignment of stale) {
			const meta = parseTransferMetadata(assignment.metadata);
			const testMode = Boolean(meta?.test_mode);

			await persistAssignmentFailure(this.db, {
				assignmentId: assignment.id,
				transferId: assignment.transferId,
				orchestratorId: assignment.orchestratorId,
				chunkStart: assignment.chunkStart,
				chunkEnd: assignment.chunkEnd,
				totalChunks: assignment.totalChunks,
				failureType: "stale_assignment_timeout",
				reason: "assignment_produced_no_tasks_before_timeout",
			});

			type RedistOk = {
				ok: true;
				selection: SelectionResult;
				redistributed: AssignmentCoverageResult;
			};
			type RedistBusy =
				| { ok: false; kind: "no_orchs" }
				| {
						ok: false;
						kind: "partial_coverage";
						selection?: SelectionResult;
						redistributed?: AssignmentCoverageResult;
				  };

			const tryRedistributeSql = async (
				sql: SqlTag,
				lockedRingCursor?: bigint,
			): Promise<RedistOk | RedistBusy> => {
				let selection: SelectionResult;
				try {
					if (testMode) {
						selection = await this.selectOrchestrators(sql, true, [assignment.orchestratorId], assignment.transferId);
					} else if (lockedRingCursor !== undefined) {
						selection = await this.selectOrchestrators(
							sql,
							false,
							[assignment.orchestratorId],
							assignment.transferId,
							assignment.totalChunks,
							lockedRingCursor,
						);
					} else {
						throw new Error("qualified redistribution requires locked ring cursor");
					}
					assertRequiredPoolSelection(selection, testMode);
				} catch {
					return { ok: false, kind: "no_orchs" };
				}

				const redistributed = await this.assignChunkCoverage({
					sql,
					transferId: assignment.transferId,
					chunkStart: assignment.chunkStart,
					totalChunks: assignment.totalChunks,
					chunkSize: assignment.chunkSize,
					metadata: meta,
					orchestrators: selection.orchestrators,
					selectionRule: selection.selectionRule,
				});

				if (!redistributed.coveredAllChunks) {
					return { ok: false, kind: "partial_coverage", selection, redistributed };
				}
				return { ok: true, selection, redistributed };
			};

			if (testMode) {
				const result = await tryRedistributeSql(this.db);
				if (!result.ok) {
					if (result.kind === "partial_coverage" && result.redistributed?.assignmentIds.length) {
						await this.db`DELETE FROM core.transfer_assignments WHERE id = ANY(${result.redistributed.assignmentIds})`;
					}
					await this.db`
						UPDATE core.transfer_assignments
						SET assigned_at = NOW()
						WHERE id = ${assignment.id}
					`;
					continue;
				}
				await this.db`
					UPDATE core.transfer_assignments
					SET status = 'expired'
					WHERE id = ${assignment.id}
				`;
				continue;
			}

			const beginFn = this.db.begin;
			if (!beginFn) {
				throw new Error(
					"AssignmentEngine: db.begin(...) is required for qualified-pool stale assignment redistribution",
				);
			}

			try {
				await beginFn.call(this.db, async (sql: SqlTag) => {
					const [ringRow] = await sql<{ qualifiedRingCursor: string }[]>`
						SELECT qualified_ring_cursor FROM core.qualified_assignment_ring WHERE singleton = TRUE FOR UPDATE
					`;
					const lockedCursor = BigInt(ringRow?.qualifiedRingCursor ?? "0");

					const outcome = await tryRedistributeSql(sql, lockedCursor);
					if (!outcome.ok) {
						throw Object.assign(new Error("__REDIST_RETRY__"), { outcome });
					}

					const q = outcome.selection.qualifiedRing;
					if (q) {
						const nextStored = advanceQualifiedRingCursor(lockedCursor, q.competitionWindowSize, q.n);
						await sql`
							UPDATE core.qualified_assignment_ring
							SET qualified_ring_cursor = ${nextStored}
							WHERE singleton = TRUE
						`;
					}

					await sql`
						UPDATE core.transfer_assignments
						SET status = 'expired'
						WHERE id = ${assignment.id}
					`;
				});
			} catch (e) {
				if ((e as Error)?.message !== "__REDIST_RETRY__") throw e;
				const tagged = e as Error & { outcome?: RedistBusy };
				const out = tagged.outcome;
				if (out?.kind === "partial_coverage" && out.redistributed?.assignmentIds.length) {
					await this.db`DELETE FROM core.transfer_assignments WHERE id = ANY(${out.redistributed.assignmentIds})`;
				}
				await this.db`
					UPDATE core.transfer_assignments
					SET assigned_at = NOW()
					WHERE id = ${assignment.id}
				`;
			}
		}
	}

	async assignTransfer(transferId: string): Promise<string[]> {
		const transfers = await this.db<
			{
				id: string;
				totalChunks: number;
				chunkSize: number;
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
				: transfer.totalChunks;

		if (testMode) {
			const selection = await this.selectOrchestrators(this.db, testMode, [], transferId);
			assertRequiredPoolSelection(selection, testMode);

			const coverage = await this.assignChunkCoverage({
				sql: this.db,
				transferId,
				chunkStart: 0,
				totalChunks: logicalTotalChunks,
				chunkSize: transfer.chunkSize,
				metadata,
				orchestrators: selection.orchestrators,
				selectionRule: selection.selectionRule,
			});

			if (!coverage.coveredAllChunks) {
				if (coverage.assignmentIds.length > 0) {
					await this.db`DELETE FROM core.transfer_assignments WHERE id = ANY(${coverage.assignmentIds})`;
				}
				throw new Error(
					"transfer assignment failed because not all chunks could be assigned to live orchestrators",
				);
			}

			await this.db`
				UPDATE core.transfers
				SET status = 'in_progress', started_at = NOW()
				WHERE id = ${transferId}
			`;

			return coverage.assignmentIds;
		}

		const beginFn = this.db.begin;
		if (!beginFn) {
			throw new Error(
				"AssignmentEngine: db.begin(...) is required for qualified-pool transfer assignment",
			);
		}

		type CapturedAssign = {
			assignmentIds: string[];
			coverage: AssignmentCoverageResult;
		};
		let captured: CapturedAssign | null = null;

		try {
			await beginFn.call(this.db, async (sql: SqlTag) => {
				const [ringRow] = await sql<{ qualifiedRingCursor: string }[]>`
					SELECT qualified_ring_cursor FROM core.qualified_assignment_ring WHERE singleton = TRUE FOR UPDATE
				`;
				const lockedCursor = BigInt(ringRow?.qualifiedRingCursor ?? "0");

				const selection = await this.selectOrchestrators(
					sql,
					false,
					[],
					transferId,
					logicalTotalChunks,
					lockedCursor,
				);
				assertRequiredPoolSelection(selection, false);

				const coverage = await this.assignChunkCoverage({
					sql,
					transferId,
					chunkStart: 0,
					totalChunks: logicalTotalChunks,
					chunkSize: transfer.chunkSize,
					metadata,
					orchestrators: selection.orchestrators,
					selectionRule: selection.selectionRule,
				});

				if (!coverage.coveredAllChunks) {
					throw new Error("__ASSIGN_COVERAGE_FAIL__");
				}

				const q = selection.qualifiedRing;
				if (!q) {
					throw new Error("qualified assignment missing qualifiedRing metadata");
				}
				const nextStored = advanceQualifiedRingCursor(lockedCursor, q.competitionWindowSize, q.n);
				await sql`
					UPDATE core.qualified_assignment_ring
					SET qualified_ring_cursor = ${nextStored}
					WHERE singleton = TRUE
				`;

				await sql`
					UPDATE core.transfers
					SET status = 'in_progress', started_at = NOW()
					WHERE id = ${transferId}
				`;

				captured = { assignmentIds: coverage.assignmentIds, coverage };
			});
		} catch (e) {
			if ((e as Error)?.message === "__ASSIGN_COVERAGE_FAIL__") {
				throw new Error(
					"transfer assignment failed because not all chunks could be assigned to live orchestrators",
				);
			}
			throw e;
		}

		if (!captured) {
			throw new Error("assignTransfer qualified path produced no captured result");
		}

		return captured.assignmentIds;
	}

	async reassignTask(taskId: string, excludeWorkerId?: string): Promise<void> {
		const tasks = await this.db<{ orchestratorId: string }[]>`
      SELECT orchestrator_id AS orchestrator_id FROM core.tasks WHERE id = ${taskId}
    `;
		const task = tasks[0];
		if (!task) return;

		const orchs = await this.db<{ hotkey: string }[]>`
      SELECT hotkey FROM core.orchestrators WHERE id = ${task.orchestratorId} LIMIT 1
    `;
		if (!orchs.length) return;

		pushToOrchestrator(orchs[0]!.hotkey, {
			type: "task_reassign",
			task_id: taskId,
			...(excludeWorkerId ? { exclude_worker_id: excludeWorkerId } : {}),
		});
	}
}
