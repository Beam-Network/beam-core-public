/**
 * PRISM score refresh job: aggregates recent proofs, tasks, and penalties, runs `computePrismScore`,
 * and persists results to qualifying/qualified metric tables and orchestrator routing fields.
 *
 * This transparency copy inlines pipeline, evidence loaders, and POP penalty materialization from beam-core.
 */

import {
	computePrismScore,
	resolveNewPool,
	type PenaltyCoefficients,
	type PrismPenaltyEvent,
	type PrismProofSample,
	type PrismTaskSample,
	type PrismScoreResult,
} from "../prism/scoring.js";

export type DbClient = <T = unknown>(
	strings: TemplateStringsArray,
	...values: unknown[]
) => Promise<T> & { json?: (value: unknown) => unknown; unsafe?: (fragment: string) => unknown };

export interface PrismScoreUpdaterContext {
	db: DbClient;
}

export type PrismScoreUpdaterJob = (ctx: PrismScoreUpdaterContext) => Promise<void>;

const PRISM_EVIDENCE_LOOKBACK_DAYS = 7;
const LOOKBACK_WINDOW = `${PRISM_EVIDENCE_LOOKBACK_DAYS} days`;
const PRISM_TARGET_GRADUATION_VERIFIED_TASKS = 120;
const PRISM_AGE_SATURATION_DAYS = 7;
const PRISM_PENALTY_COEFF_POP = 0.05;
const PRISM_PENALTY_COEFF_FRAUD = 0.05;
const PRISM_PENALTY_COEFF_SYBIL = 0.05;
const PRISM_GRADUATION_CONFIDENCE = 0.9;
const PRISM_POOL_TRANSITION_SCORE = 0.5;

const penaltyCoefficients: PenaltyCoefficients = {
	pop: PRISM_PENALTY_COEFF_POP,
	fraud: PRISM_PENALTY_COEFF_FRAUD,
	sybil: PRISM_PENALTY_COEFF_SYBIL,
};

function routingScoreForOrchestrator(
	orchestrator: OrchestratorPrismRow,
	computedFinalScore: number,
): number {
	if (orchestrator.prism_pool_transition_pending) return PRISM_POOL_TRANSITION_SCORE;
	return computedFinalScore;
}

const BATCH_SIZE = 200;

interface ChainStateRow {
	current_epoch: number | null;
}

export async function materializePopPenaltyEvents(args: {
	db: DbClient;
	currentEpoch?: number | null;
}): Promise<number> {
	const { db } = args;
	const currentEpoch =
		args.currentEpoch ??
		(
			await db<ChainStateRow[]>`
				SELECT current_epoch
				FROM core.chain_state
				ORDER BY last_synced_at DESC
				LIMIT 1
			`
		)[0]?.current_epoch ??
		null;

	if (currentEpoch == null || currentEpoch <= 0) return 0;

	const insertedRows = await db<{ worker_payment_id: string }[]>`
		WITH pending_events AS (
			SELECT
				wp.id AS worker_payment_id,
				wp.orchestrator_id,
				wp.worker_id,
				wp.task_id,
				wp.tx_hash,
				wp.epoch AS payment_epoch,
				wp.created_at AS payment_recorded_at,
				COALESCE(wp.pop_verified_at, wp.updated_at, wp.created_at) AS resolved_at,
				wp.pop_error AS verifier_error
			FROM core.worker_payments wp
			WHERE wp.pop_verified = FALSE
				AND wp.task_id IS NOT NULL
				AND wp.epoch < ${currentEpoch}
				AND NOT EXISTS (
					SELECT 1
					FROM core.pop_penalty_events ppe
					WHERE ppe.worker_payment_id = wp.id
				)
			ORDER BY COALESCE(wp.pop_verified_at, wp.updated_at, wp.created_at) ASC
			LIMIT ${BATCH_SIZE}
		)
		INSERT INTO core.pop_penalty_events
			(worker_payment_id, orchestrator_id, worker_id, task_id, tx_hash,
			 payment_epoch, payment_recorded_at, resolved_at, applied_in_epoch, verifier_error)
		SELECT
			worker_payment_id,
			orchestrator_id,
			worker_id,
			task_id,
			tx_hash,
			payment_epoch,
			payment_recorded_at,
			resolved_at,
			${currentEpoch},
			verifier_error
		FROM pending_events
		ON CONFLICT (worker_payment_id) DO NOTHING
		RETURNING worker_payment_id
	`;

	return insertedRows.length;
}



export type PrismEvidencePool = "qualifying" | "qualified";

const TEST_MODE_SQL = `COALESCE((tr.metadata->>'test_mode')::boolean, false)`;

function testModeMatches(pool: PrismEvidencePool): boolean {
	return pool === "qualifying";
}

export interface PrismProofRow {
	orchestrator_id: string;
	task_id: string | null;
	transfer_id: string | null;
	bandwidth_mbps: string;
	event_at: Date;
}

export interface PrismTaskRow {
	orchestrator_id: string;
	state: "completed" | "failed";
	failure_reason: string | null;
	event_at: Date;
}

export interface PrismPenaltyRow {
	orchestrator_id: string;
	kind: "pop" | "fraud" | "sybil";
	event_at: Date;
}

export async function loadPrismProofs(
	db: DbClient,
	lookbackWindow: string,
	pool: PrismEvidencePool,
): Promise<PrismProofRow[]> {
	const testMode = testModeMatches(pool);
	return db<PrismProofRow[]>`
		SELECT
			p.orchestrator_id,
			p.task_id,
			t.transfer_id,
			COALESCE(p.bandwidth_mbps, 0)::TEXT AS bandwidth_mbps,
			COALESCE(p.verified_at, p.published_at) AS event_at
		FROM core.proofs_of_bandwidth p
		LEFT JOIN core.tasks t ON t.id = p.task_id
		LEFT JOIN core.transfers tr ON tr.id = t.transfer_id
		WHERE COALESCE(p.verified_at, p.published_at) > NOW() - ${lookbackWindow}::INTERVAL
			AND (p.verification_passed = TRUE OR p.status = 'verified')
			AND t.transfer_id IS NOT NULL
			AND COALESCE((tr.metadata->>'test_mode')::boolean, false) = ${testMode}
	`;
}

export async function loadPrismTasks(
	db: DbClient,
	lookbackWindow: string,
	pool: PrismEvidencePool,
): Promise<PrismTaskRow[]> {
	const testMode = testModeMatches(pool);
	return db<PrismTaskRow[]>`
		SELECT t.orchestrator_id, t.state, t.failure_reason,
			COALESCE(t.completed_at, t.failed_at, t.created_at) AS event_at
		FROM core.tasks t
		INNER JOIN core.transfers tr ON tr.id = t.transfer_id
		WHERE COALESCE(t.completed_at, t.failed_at, t.created_at) > NOW() - ${lookbackWindow}::INTERVAL
			AND t.state IN ('completed', 'failed')
			AND COALESCE((tr.metadata->>'test_mode')::boolean, false) = ${testMode}
		UNION ALL
		SELECT gi.orchestrator_id, 'failed' AS state,
			CONCAT(COALESCE(gi.intervention_type, 'beamcore_guardrail_reassign'), ':', COALESCE(gi.reason, gi.outcome)) AS failure_reason,
			gi.intervened_at AS event_at
		FROM core.orchestrator_guardrail_interventions gi
		INNER JOIN core.transfers tr ON tr.id = gi.transfer_id
		WHERE gi.intervened_at > NOW() - ${lookbackWindow}::INTERVAL
			AND intervention_type = 'beamcore_guardrail_reassign'
			AND COALESCE((tr.metadata->>'test_mode')::boolean, false) = ${testMode}
		UNION ALL
		SELECT af.orchestrator_id, 'failed' AS state,
			CONCAT('assignment_failure:', COALESCE(af.failure_type, 'unknown'), ':', COALESCE(af.reason, 'unknown')) AS failure_reason,
			af.created_at AS event_at
		FROM core.orchestrator_assignment_failures af
		INNER JOIN core.transfers tr ON tr.id = af.transfer_id
		WHERE af.created_at > NOW() - ${lookbackWindow}::INTERVAL
			AND COALESCE((tr.metadata->>'test_mode')::boolean, false) = ${testMode}
	`;
}

export async function loadPrismPenaltyEvents(
	db: DbClient,
	currentEpoch: number,
	lookbackWindow: string,
	pool: PrismEvidencePool,
): Promise<PrismPenaltyRow[]> {
	const testMode = testModeMatches(pool);
	const transferScoped = db<PrismPenaltyRow[]>`
		SELECT ppe.orchestrator_id, 'pop'::text AS kind, ppe.resolved_at AS event_at
		FROM core.pop_penalty_events ppe
		INNER JOIN core.tasks t ON t.id = ppe.task_id
		INNER JOIN core.transfers tr ON tr.id = t.transfer_id
		WHERE ppe.applied_in_epoch <= ${currentEpoch}
			AND ppe.resolved_at > NOW() - ${lookbackWindow}::INTERVAL
			AND ppe.task_id IS NOT NULL
			AND COALESCE((tr.metadata->>'test_mode')::boolean, false) = ${testMode}
		UNION ALL
		SELECT t.orchestrator_id, 'sybil'::text AS kind, t.created_at AS event_at
		FROM core.sybil_violations sv
		JOIN core.tasks t ON t.assigned_worker_id = sv.worker_id
		INNER JOIN core.transfers tr ON tr.id = t.transfer_id
		WHERE sv.resolved_at IS NULL
			AND t.created_at > NOW() - ${lookbackWindow}::INTERVAL
			AND t.orchestrator_id IS NOT NULL
			AND COALESCE((tr.metadata->>'test_mode')::boolean, false) = ${testMode}
	`;
	const fraud = db<PrismPenaltyRow[]>`
		SELECT orchestrator_id, 'fraud'::text AS kind, applied_at AS event_at
		FROM core.fraud_penalties
		WHERE orchestrator_id IS NOT NULL
			AND applied_at > NOW() - ${lookbackWindow}::INTERVAL
	`;
	const [scoped, fraudRows] = await Promise.all([transferScoped, fraud]);
	return [...scoped, ...fraudRows];
}


export interface OrchestratorPrismRow {
	id: string;
	hotkey: string;
	ready: boolean;
	control_plane_connected: boolean;
	registered_at: Date;
	prism_pool: "qualifying" | "qualified";
	prism_confidence_score: string;
	prism_pool_transition_pending: boolean;
}

export function numberValue(value: string | number | null | undefined): number {
	if (typeof value === "number") return Number.isFinite(value) ? value : 0;
	if (typeof value === "string") {
		const parsed = Number(value);
		return Number.isFinite(parsed) ? parsed : 0;
	}
	return 0;
}

function verifiedTaskCountFromScore(score: PrismScoreResult): number {
	const confidence = score.scoreComponents as { confidence?: { verifiedTaskCount?: number } };
	const count = confidence.confidence?.verifiedTaskCount;
	return Number.isFinite(Number(count)) ? Number(count) : 0;
}

/** Mirrors PRISM verified task count onto core.orchestrators.total_tasks_completed. */
export async function syncOrchestratorEvidenceTotals(
	db: DbClient,
	orchestratorId: string,
	score: PrismScoreResult,
): Promise<void> {
	const verifiedTaskCount = verifiedTaskCountFromScore(score);
	await db`
		UPDATE core.orchestrators
		SET total_tasks_completed = ${verifiedTaskCount},
			updated_at = NOW()
		WHERE id = ${orchestratorId}
	`;
}

export interface EvidenceIndexes {
	proofsByOrchestrator: Map<string, PrismProofSample[]>;
	tasksByOrchestrator: Map<string, PrismTaskSample[]>;
	penaltiesByOrchestrator: Map<string, PrismPenaltyEvent[]>;
	averageMbpsByOrchestrator: Map<string, number>;
	rawReliabilityByOrchestrator: Map<string, number>;
}

export function indexPrismEvidence(
	proofs: PrismProofRow[],
	tasks: PrismTaskRow[],
	penalties: PrismPenaltyRow[],
	now: Date,
	evidenceLookbackDays: number,
): EvidenceIndexes {
	const proofsByOrchestrator = new Map<string, PrismProofSample[]>();
	const tasksByOrchestrator = new Map<string, PrismTaskSample[]>();
	const penaltiesByOrchestrator = new Map<string, PrismPenaltyEvent[]>();
	const averageMbpsByOrchestrator = new Map<string, number>();
	const rawReliabilityByOrchestrator = new Map<string, number>();

	for (const proof of proofs) {
		const sample: PrismProofSample = {
			bandwidthMbps: numberValue(proof.bandwidth_mbps),
			taskId: proof.task_id,
			transferId: proof.transfer_id,
			eventAt: proof.event_at,
		};
		const existing = proofsByOrchestrator.get(proof.orchestrator_id) ?? [];
		existing.push(sample);
		proofsByOrchestrator.set(proof.orchestrator_id, existing);
	}

	for (const [orchestratorId, orchestratorProofs] of proofsByOrchestrator.entries()) {
		const total = orchestratorProofs.reduce((sum, proof) => sum + proof.bandwidthMbps, 0);
		averageMbpsByOrchestrator.set(
			orchestratorId,
			orchestratorProofs.length ? total / orchestratorProofs.length : 0,
		);
	}

	for (const task of tasks) {
		const sample: PrismTaskSample = {
			state: task.state,
			failureReason: task.failure_reason,
			eventAt: task.event_at,
		};
		const existing = tasksByOrchestrator.get(task.orchestrator_id) ?? [];
		existing.push(sample);
		tasksByOrchestrator.set(task.orchestrator_id, existing);
	}

	for (const event of penalties) {
		const sample: PrismPenaltyEvent = { kind: event.kind, eventAt: event.event_at };
		const existing = penaltiesByOrchestrator.get(event.orchestrator_id) ?? [];
		existing.push(sample);
		penaltiesByOrchestrator.set(event.orchestrator_id, existing);
	}

	for (const orchestratorId of new Set([
		...proofsByOrchestrator.keys(),
		...tasksByOrchestrator.keys(),
	])) {
		const orchTasks = tasksByOrchestrator.get(orchestratorId) ?? [];
		rawReliabilityByOrchestrator.set(
			orchestratorId,
			computeRawReliability(orchTasks, now, evidenceLookbackDays).reliability,
		);
	}

	return {
		proofsByOrchestrator,
		tasksByOrchestrator,
		penaltiesByOrchestrator,
		averageMbpsByOrchestrator,
		rawReliabilityByOrchestrator,
	};
}

function fleetMetricRange(values: number[]): FleetMetricRange {
	if (!values.length) return { min: 0, max: 0 };
	return { min: Math.min(...values), max: Math.max(...values) };
}

export function fleetRangeForCohort(
	orchestratorIds: string[],
	indexes: EvidenceIndexes,
): { throughputRange: FleetMetricRange; reliabilityRange: FleetMetricRange } {
	const throughputValues = orchestratorIds
		.map((id) => indexes.averageMbpsByOrchestrator.get(id) ?? 0)
		.filter((value) => value > 0);
	const rawReliabilityValues = orchestratorIds
		.map((id) => indexes.rawReliabilityByOrchestrator.get(id) ?? 0)
		.filter((value) => value > 0);
	return {
		throughputRange: fleetMetricRange(throughputValues),
		reliabilityRange: fleetMetricRange(rawReliabilityValues),
	};
}

export async function persistQualifyingMetrics(
	db: DbClient,
	orchestratorId: string,
	score: PrismScoreResult,
): Promise<void> {
	await db`
		INSERT INTO core.prism_metrics_qualifying
			(orchestrator_id, confidence_score,
			 verified_transfer_count, verified_chunk_count, verified_bandwidth_mbps,
			 throughput_score, reliability_score, performance_score,
			 readiness_multiplier, penalty_multiplier,
			 prism_final_score, score_components, computed_at, updated_at)
		VALUES
			(${orchestratorId}, ${score.confidenceScore},
			 ${score.verifiedTransferCount}, ${score.verifiedChunkCount}, ${score.verifiedBandwidthMbps},
			 ${score.throughputScore}, ${score.reliabilityScore}, ${score.performanceScore},
			 ${score.readinessMultiplier}, ${score.penaltyMultiplier},
			 ${score.prismFinalScore}, ${JSON.stringify(score.scoreComponents as never)}, NOW(), NOW())
		ON CONFLICT (orchestrator_id) DO UPDATE
			SET confidence_score        = EXCLUDED.confidence_score,
				verified_transfer_count = EXCLUDED.verified_transfer_count,
				verified_chunk_count    = EXCLUDED.verified_chunk_count,
				verified_bandwidth_mbps = EXCLUDED.verified_bandwidth_mbps,
				throughput_score        = EXCLUDED.throughput_score,
				reliability_score       = EXCLUDED.reliability_score,
				performance_score       = EXCLUDED.performance_score,
				readiness_multiplier    = EXCLUDED.readiness_multiplier,
				penalty_multiplier      = EXCLUDED.penalty_multiplier,
				prism_final_score       = EXCLUDED.prism_final_score,
				score_components        = EXCLUDED.score_components,
				computed_at             = NOW(),
				updated_at              = NOW()
	`;
	await syncOrchestratorEvidenceTotals(db, orchestratorId, score);
}

export async function persistQualifiedMetrics(
	db: DbClient,
	orchestratorId: string,
	score: PrismScoreResult,
): Promise<void> {
	await db`
		INSERT INTO core.prism_metrics_qualified
			(orchestrator_id,
			 verified_transfer_count, verified_chunk_count, verified_bandwidth_mbps,
			 throughput_score, reliability_score, performance_score,
			 readiness_multiplier, penalty_multiplier,
			 prism_final_score, score_components, computed_at, updated_at)
		VALUES
			(${orchestratorId},
			 ${score.verifiedTransferCount}, ${score.verifiedChunkCount}, ${score.verifiedBandwidthMbps},
			 ${score.throughputScore}, ${score.reliabilityScore}, ${score.performanceScore},
			 ${score.readinessMultiplier}, ${score.penaltyMultiplier},
			 ${score.prismFinalScore}, ${JSON.stringify(score.scoreComponents as never)}, NOW(), NOW())
		ON CONFLICT (orchestrator_id) DO UPDATE
			SET verified_transfer_count = EXCLUDED.verified_transfer_count,
				verified_chunk_count    = EXCLUDED.verified_chunk_count,
				verified_bandwidth_mbps = EXCLUDED.verified_bandwidth_mbps,
				throughput_score        = EXCLUDED.throughput_score,
				reliability_score       = EXCLUDED.reliability_score,
				performance_score       = EXCLUDED.performance_score,
				readiness_multiplier    = EXCLUDED.readiness_multiplier,
				penalty_multiplier      = EXCLUDED.penalty_multiplier,
				prism_final_score       = EXCLUDED.prism_final_score,
				score_components        = EXCLUDED.score_components,
				computed_at             = NOW(),
				updated_at              = NOW()
	`;
	await syncOrchestratorEvidenceTotals(db, orchestratorId, score);
}

export async function applyPoolPromotion(
	db: DbClient,
	orchestratorId: string,
	transitionScore: number,
	seedScore: PrismScoreResult,
): Promise<void> {
	await db`DELETE FROM core.prism_metrics_qualifying WHERE orchestrator_id = ${orchestratorId}`;
	await persistQualifiedMetrics(db, orchestratorId, seedScore);
	await db`
		UPDATE core.orchestrators
		SET prism_pool = 'qualified',
			prism_pool_transition_pending = TRUE,
			prism_final_score = ${transitionScore},
			prism_updated_at = NOW(),
			updated_at = NOW()
		WHERE id = ${orchestratorId}
	`;
}

export async function mirrorOrchestratorRoutingScore(
	db: DbClient,
	orchestrator: OrchestratorPrismRow,
	routingScore: number,
	updateConfidence: boolean,
	confidenceScore?: number,
	newPool?: "qualifying" | "qualified",
): Promise<void> {
	if (updateConfidence && confidenceScore != null && newPool === "qualifying") {
		await db`
			UPDATE core.orchestrators
			SET prism_final_score = ${routingScore},
				prism_confidence_score = ${confidenceScore},
				prism_pool = ${newPool},
				prism_updated_at = NOW(),
				updated_at = NOW()
			WHERE id = ${orchestrator.id}
		`;
		return;
	}

	if (newPool != null) {
		await db`
			UPDATE core.orchestrators
			SET prism_final_score = ${routingScore},
				prism_pool = ${newPool},
				prism_updated_at = NOW(),
				updated_at = NOW()
			WHERE id = ${orchestrator.id}
		`;
		return;
	}

	await db`
		UPDATE core.orchestrators
		SET prism_final_score = ${routingScore},
			prism_updated_at = NOW(),
			updated_at = NOW()
		WHERE id = ${orchestrator.id}
	`;
}

export function buildScoreInput(
	orchestrator: OrchestratorPrismRow,
	indexes: EvidenceIndexes,
	throughputRange: FleetMetricRange,
	reliabilityRange: FleetMetricRange,
	penaltyCoefficients: PenaltyCoefficients,
	now: Date,
	evidenceLookbackDays: number,
	targetVerifiedTasks: number,
	ageSaturationDays: number,
	graduationConfidence: number,
) {
	return {
		averageVerifiedMbps: indexes.averageMbpsByOrchestrator.get(orchestrator.id) ?? 0,
		throughputRange,
		reliabilityRange,
		proofs: indexes.proofsByOrchestrator.get(orchestrator.id) ?? [],
		tasks: indexes.tasksByOrchestrator.get(orchestrator.id) ?? [],
		penaltyEvents: indexes.penaltiesByOrchestrator.get(orchestrator.id) ?? [],
		penaltyCoefficients,
		readiness: {
			ready: orchestrator.ready,
			controlPlaneConnected: orchestrator.control_plane_connected,
		},
		evidenceLookbackDays,
		confidenceTargets: { targetVerifiedTasks, ageSaturationDays },
		registeredAt: orchestrator.registered_at,
		now,
		graduationConfidence,
	};
}

export const prismScoreUpdater: PrismScoreUpdaterJob = async ({ db }) => {
	const now = new Date();
	const graduationConfidence = PRISM_GRADUATION_CONFIDENCE;
	const transitionScore = PRISM_POOL_TRANSITION_SCORE;

	const chainRows = await db<{ current_epoch: number | null }[]>`
		SELECT current_epoch
		FROM core.chain_state
		ORDER BY last_synced_at DESC
		LIMIT 1
	`;
	const currentEpoch = chainRows[0]?.current_epoch ?? 0;

	await materializePopPenaltyEvents({ db, currentEpoch });

	const orchestrators = await db<OrchestratorPrismRow[]>`
		SELECT
			o.id,
			o.hotkey,
			o.ready,
			o.control_plane_connected,
			o.registered_at,
			o.prism_pool,
			o.prism_confidence_score::text AS prism_confidence_score,
			o.prism_pool_transition_pending
		FROM core.orchestrators o
		WHERE o.uid IS NOT NULL
	`;

	const [testProofs, testTasks, testPenalties, prodProofs, prodTasks, prodPenalties] =
		await Promise.all([
			loadPrismProofs(db, LOOKBACK_WINDOW, "qualifying"),
			loadPrismTasks(db, LOOKBACK_WINDOW, "qualifying"),
			loadPrismPenaltyEvents(db, currentEpoch, LOOKBACK_WINDOW, "qualifying"),
			loadPrismProofs(db, LOOKBACK_WINDOW, "qualified"),
			loadPrismTasks(db, LOOKBACK_WINDOW, "qualified"),
			loadPrismPenaltyEvents(db, currentEpoch, LOOKBACK_WINDOW, "qualified"),
		]);

	const qualifyingIndexes = indexPrismEvidence(
		testProofs,
		testTasks,
		testPenalties,
		now,
		PRISM_EVIDENCE_LOOKBACK_DAYS,
	);
	const qualifiedIndexes = indexPrismEvidence(
		prodProofs,
		prodTasks,
		prodPenalties,
		now,
		PRISM_EVIDENCE_LOOKBACK_DAYS,
	);

	const qualifyingOrchestrators = orchestrators.filter((o) => o.prism_pool === "qualifying");
	const qualifiedOrchestrators = orchestrators.filter((o) => o.prism_pool === "qualified");

	const qualifyingIds = qualifyingOrchestrators.map((o) => o.id);
	const qualifiedIds = qualifiedOrchestrators.map((o) => o.id);
	const qualifyingRange = fleetRangeForCohort(qualifyingIds, qualifyingIndexes);
	const qualifiedRange = fleetRangeForCohort(qualifiedIds, qualifiedIndexes);



	const promotions: Array<{
		orchestrator: OrchestratorPrismRow;
		score: ReturnType<typeof computePrismScore>;
	}> = [];

	for (const orchestrator of qualifyingOrchestrators) {
		const scoreInput = buildScoreInput(
			orchestrator,
			qualifyingIndexes,
			qualifyingRange.throughputRange,
			qualifyingRange.reliabilityRange,
			penaltyCoefficients,
			now,
			PRISM_EVIDENCE_LOOKBACK_DAYS,
			PRISM_TARGET_GRADUATION_VERIFIED_TASKS,
			PRISM_AGE_SATURATION_DAYS,
			graduationConfidence,
		);
		const score = computePrismScore(scoreInput);
		const newPool = resolveNewPool(orchestrator.prism_pool, score.confidenceScore, graduationConfidence);

		if (newPool === "qualified") {
			promotions.push({ orchestrator, score });
			continue;
		}

		await persistQualifyingMetrics(db, orchestrator.id, score);
		const routingScore = routingScoreForOrchestrator(orchestrator, score.prismFinalScore);
		await mirrorOrchestratorRoutingScore(
			db,
			orchestrator,
			routingScore,
			true,
			score.confidenceScore,
			"qualifying",
		);
	}

	for (const { orchestrator, score } of promotions) {
		const seedScore = { ...score, prismFinalScore: transitionScore };
		await applyPoolPromotion(db, orchestrator.id, transitionScore, seedScore);
		orchestrator.prism_pool = "qualified";
		orchestrator.prism_pool_transition_pending = true;
	}

	for (const orchestrator of qualifiedOrchestrators) {
		const scoreInput = buildScoreInput(
			orchestrator,
			qualifiedIndexes,
			qualifiedRange.throughputRange,
			qualifiedRange.reliabilityRange,
			penaltyCoefficients,
			now,
			PRISM_EVIDENCE_LOOKBACK_DAYS,
			PRISM_TARGET_GRADUATION_VERIFIED_TASKS,
			PRISM_AGE_SATURATION_DAYS,
			graduationConfidence,
		);
		const score = computePrismScore(scoreInput);
		await persistQualifiedMetrics(db, orchestrator.id, score);
		const routingScore = routingScoreForOrchestrator(orchestrator, score.prismFinalScore);
		await mirrorOrchestratorRoutingScore(db, orchestrator, routingScore, false);
	}

};
