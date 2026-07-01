/**
 * PRISM score refresh job: aggregates recent tasks, bandwidth, and penalties, runs `computePrismScore`,
 * and persists results to qualifying/qualified metric tables and orchestrator routing fields.
 *
 * This transparency copy inlines pipeline and evidence loaders from beam-core.
 */

import {
	computePrismScore,
	resolveNewPool,
	withRoutingOverride,
	type FleetMetricRange,
	type PenaltyCoefficients,
	type PerformanceWeights,
	type PrismPenaltyHourBucket,
	type PrismScoreInput,
	type PrismScoreResult,
	type PrismTaskHourBucket,
	computeRawReliability,
	hourMidpoint,
	sampleWeight,
	DEFAULT_GRADUATION_CONFIDENCE,
	DEFAULT_PERFORMANCE_RELIABILITY_WEIGHT,
	DEFAULT_PERFORMANCE_THROUGHPUT_WEIGHT,
	FLEET_NORMALIZATION_FLOOR,
	HALF_LIFE_HOURS,
} from "../prism/scoring.js";

export type DbClient = <T = unknown>(
	strings: TemplateStringsArray,
	...values: unknown[]
) => Promise<T> & { json?: (value: unknown) => unknown; unsafe?: (fragment: string) => unknown };

export interface PrismScoreUpdaterContext {
	db: DbClient;
}

export type PrismScoreUpdaterJob = (ctx: PrismScoreUpdaterContext) => Promise<void>;

type Db = DbClient;

const logger = {
	info(_data: unknown, _message?: string): void {},
	debug(_data: unknown, _message?: string): void {},
	warn(_data: unknown, _message?: string): void {},
};

const OPS_SCHEDULER_DEFAULT_CORE_SERVER_URL = "http://localhost:8000";

async function postRoutingSync(path: string, body: unknown): Promise<void> {
	const secret = process.env.GATEWAY_INTERNAL_SECRET;
	if (!secret) {
		logger.debug({ path }, "skipping control-plane routing sync because GATEWAY_INTERNAL_SECRET is unset");
		return;
	}

	const coreServerUrl = process.env.CORE_SERVER_URL ?? OPS_SCHEDULER_DEFAULT_CORE_SERVER_URL;
	const url = `${coreServerUrl.replace(/\/$/, "")}${path}`;
	const response = await fetch(url, {
		method: "POST",
		headers: {
			"Content-Type": "application/json",
			"x-internal-secret": secret,
		},
		body: JSON.stringify(body),
	});
	if (!response.ok) {
		const text = await response.text().catch(() => "");
		throw new Error(`control-plane routing sync ${path} failed: ${response.status} ${text}`);
	}
}

const PRISM_DEFAULTS = {
	PRISM_EVIDENCE_LOOKBACK_DAYS: 7,
	PRISM_TARGET_GRADUATION_VERIFIED_TASKS: 120,
	PRISM_AGE_SATURATION_DAYS: 7,
	PRISM_PENALTY_COEFF_FRAUD: 0.05,
	PRISM_PENALTY_COEFF_SYBIL: 0.05,
	PRISM_GRADUATION_CONFIDENCE: 0.9,
	PRISM_POOL_TRANSITION_SCORE: 0.5,
	PRISM_PERFORMANCE_THROUGHPUT_WEIGHT: 0.4,
	PRISM_PERFORMANCE_RELIABILITY_WEIGHT: 0.6,
};
export type PrismEvidencePool = "qualifying" | "qualified";

export interface PrismHourAgg {
	orchestrator_id: string;
	bucket_start: Date;
	// assignment-derived bandwidth samples
	bandwidth_sample_count: number;
	bandwidth_mbps_sum: number;
	// proof-derived counts
	verified_task_count: number;
	verified_transfer_count: number;
	first_proof_at: Date | null;
	last_proof_at: Date | null;
	// task outcomes
	failed_task_count: number;
	overseer_intervention_count: number;
	// penalties
	fraud_penalty_count: number;
	sybil_penalty_count: number;
}

export interface PrismEvidenceTotals {
	orchestrator_id: string;
	lifetime_verified_task_count: number;
	lifetime_verified_transfer_count: number;
	last_aggregated_event_at: Date | null;
}

export async function loadPrismHourlyAggregates(
	db: Db,
	lookbackDays: number,
	pool: PrismEvidencePool,
): Promise<PrismHourAgg[]> {
	return db<PrismHourAgg[]>`
		SELECT
			orchestrator_id,
			bucket_start,
			bandwidth_sample_count::int,
			bandwidth_mbps_sum::float8 AS bandwidth_mbps_sum,
			verified_task_count::int,
			verified_transfer_count::int,
			first_proof_at,
			last_proof_at,
			failed_task_count::int,
			overseer_intervention_count::int,
			fraud_penalty_count::int,
			sybil_penalty_count::int
		FROM core.prism_evidence_hourly
		WHERE pool = ${pool}
		  AND bucket_start >= (
			date_trunc('hour', (NOW() - (${lookbackDays}::INT * INTERVAL '1 day')) AT TIME ZONE 'UTC')
			AT TIME ZONE 'UTC'
		  )
	`;
}

export async function loadPrismEvidenceTotals(
	db: Db,
	pool: PrismEvidencePool,
): Promise<PrismEvidenceTotals[]> {
	return db<PrismEvidenceTotals[]>`
		SELECT
			orchestrator_id,
			lifetime_verified_task_count::int,
			lifetime_verified_transfer_count::int,
			last_aggregated_event_at
		FROM core.prism_evidence_totals
		WHERE pool = ${pool}
	`;
}

/** Convert PrismHourAgg rows into the hourly bucket types consumed by scoring. */
export function toPrismTaskHourBuckets(agg: PrismHourAgg): PrismTaskHourBucket {
	return {
		bucketStart: agg.bucket_start,
		verified_task_count: agg.verified_task_count,
		failed_task_count: agg.failed_task_count,
		overseer_intervention_count: agg.overseer_intervention_count,
	};
}

export function toPrismPenaltyHourBuckets(agg: PrismHourAgg): PrismPenaltyHourBucket {
	return {
		bucketStart: agg.bucket_start,
		fraud_penalty_count: agg.fraud_penalty_count,
		sybil_penalty_count: agg.sybil_penalty_count,
	};
}

export const THROUGHPUT_HALF_LIFE_HOURS = 12;

export interface OrchestratorPrismRow {
	id: string;
	hotkey: string;
	ready: boolean;
	control_plane_connected: boolean;
	registered_at: Date;
	prism_age_at?: Date;
	prism_pool: "qualifying" | "qualified";
	prism_confidence_score: string;
	prism_final_score?: string;
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


export interface EvidenceIndexes {
	taskBucketsByOrchestrator: Map<string, PrismTaskHourBucket[]>;
	penaltyBucketsByOrchestrator: Map<string, PrismPenaltyHourBucket[]>;
	averageMbpsByOrchestrator: Map<string, number>;
	verifiedTaskCountByOrchestrator: Map<string, number>;
	verifiedTransferCountByOrchestrator: Map<string, number>;
	lifetimeVerifiedTaskCountByOrchestrator: Map<string, number>;
	lifetimeVerifiedTransferCountByOrchestrator: Map<string, number>;
	rawReliabilityByOrchestrator: Map<string, number>;
}


export function indexPrismEvidence(
	hourlyAggs: PrismHourAgg[],
	totals: PrismEvidenceTotals[],
	now: Date,
): EvidenceIndexes {
	const taskBucketsByOrchestrator = new Map<string, PrismTaskHourBucket[]>();
	const penaltyBucketsByOrchestrator = new Map<string, PrismPenaltyHourBucket[]>();
	const averageMbpsByOrchestrator = new Map<string, number>();
	const verifiedTaskCountByOrchestrator = new Map<string, number>();
	const verifiedTransferCountByOrchestrator = new Map<string, number>();
	const lifetimeVerifiedTaskCountByOrchestrator = new Map<string, number>();
	const lifetimeVerifiedTransferCountByOrchestrator = new Map<string, number>();
	const rawReliabilityByOrchestrator = new Map<string, number>();

	// Aggregate per-orchestrator across all hourly rows.
	const mbpsSumByOrch = new Map<string, number>();
	const mbpsSamplesByOrch = new Map<string, number>();
	const verifiedTasksByOrch = new Map<string, number>();
	const verifiedTransfersByOrch = new Map<string, number>();

	for (const agg of hourlyAggs) {
		const id = agg.orchestrator_id;

		// bandwidth
		const bandwidthWeight = sampleWeight(hourMidpoint(agg.bucket_start), now, THROUGHPUT_HALF_LIFE_HOURS);
		mbpsSumByOrch.set(id, (mbpsSumByOrch.get(id) ?? 0) + agg.bandwidth_mbps_sum * bandwidthWeight);
		mbpsSamplesByOrch.set(id, (mbpsSamplesByOrch.get(id) ?? 0) + agg.bandwidth_sample_count * bandwidthWeight);

		// verified counts (sum across buckets; hourly rows already deduplicate within each bucket)
		verifiedTasksByOrch.set(id, (verifiedTasksByOrch.get(id) ?? 0) + agg.verified_task_count);
		verifiedTransfersByOrch.set(id, (verifiedTransfersByOrch.get(id) ?? 0) + agg.verified_transfer_count);

		// task hour buckets
		const taskBuckets = taskBucketsByOrchestrator.get(id) ?? [];
		taskBuckets.push(toPrismTaskHourBuckets(agg));
		taskBucketsByOrchestrator.set(id, taskBuckets);

		// penalty hour buckets
		const penaltyBuckets = penaltyBucketsByOrchestrator.get(id) ?? [];
		penaltyBuckets.push(toPrismPenaltyHourBuckets(agg));
		penaltyBucketsByOrchestrator.set(id, penaltyBuckets);
	}

	// Compute averages
	for (const [id, sum] of mbpsSumByOrch) {
		const samples = mbpsSamplesByOrch.get(id) ?? 0;
		averageMbpsByOrchestrator.set(id, samples > 0 ? sum / samples : 0);
	}

	for (const [id, count] of verifiedTasksByOrch) {
		verifiedTaskCountByOrchestrator.set(id, count);
	}
	for (const [id, count] of verifiedTransfersByOrch) {
		verifiedTransferCountByOrchestrator.set(id, count);
	}

	// Lifetime counts from totals
	for (const row of totals) {
		lifetimeVerifiedTaskCountByOrchestrator.set(row.orchestrator_id, row.lifetime_verified_task_count);
		lifetimeVerifiedTransferCountByOrchestrator.set(row.orchestrator_id, row.lifetime_verified_transfer_count);
	}

	// Raw reliability per orchestrator
	for (const [id, buckets] of taskBucketsByOrchestrator) {
		rawReliabilityByOrchestrator.set(id, computeRawReliability(buckets, now).reliability);
	}

	return {
		taskBucketsByOrchestrator,
		penaltyBucketsByOrchestrator,
		averageMbpsByOrchestrator,
		verifiedTaskCountByOrchestrator,
		verifiedTransferCountByOrchestrator,
		lifetimeVerifiedTaskCountByOrchestrator,
		lifetimeVerifiedTransferCountByOrchestrator,
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


function tierAColumns(score: PrismScoreResult) {
	return {
		successRate: score.successRate,
		penaltyEventCount: score.penaltyEventCount,
		routingOverrideReason: score.routingOverrideReason,
		routingComputedFinalScore: score.routingComputedFinalScore,
		routingAppliedScore: score.routingAppliedScore,
		lifetimeVerifiedTaskCount: score.lifetimeVerifiedTaskCount,
	};
}


export async function persistQualifyingMetrics(
	db: Db,
	orchestratorId: string,
	score: PrismScoreResult,
): Promise<void> {
	const tierA = tierAColumns(score);
	await db`
		INSERT INTO core.prism_metrics_qualifying
			(orchestrator_id, confidence_score,
			 verified_transfer_count, verified_task_count, verified_bandwidth_mbps,
			 throughput_score, reliability_score, performance_score,
			 readiness_multiplier, penalty_multiplier,
			 prism_final_score,
			 success_rate, penalty_event_count,
			 routing_override_reason, routing_computed_final_score, routing_applied_score,
			 lifetime_verified_task_count, age_days,
			 computed_at, updated_at)
		VALUES
			(${orchestratorId}, ${score.confidenceScore},
			 ${score.verifiedTransferCount}, ${score.verifiedTaskCount}, ${score.verifiedBandwidthMbps},
			 ${score.throughputScore}, ${score.reliabilityScore}, ${score.performanceScore},
			 ${score.readinessMultiplier}, ${score.penaltyMultiplier},
			 ${score.prismFinalScore},
			 ${tierA.successRate}, ${tierA.penaltyEventCount},
			 ${tierA.routingOverrideReason}, ${tierA.routingComputedFinalScore}, ${tierA.routingAppliedScore},
			 ${tierA.lifetimeVerifiedTaskCount}, ${score.ageDays},
			 NOW(), NOW())
		ON CONFLICT (orchestrator_id) DO UPDATE
			SET confidence_score               = EXCLUDED.confidence_score,
				verified_transfer_count        = EXCLUDED.verified_transfer_count,
				verified_task_count            = EXCLUDED.verified_task_count,
				verified_bandwidth_mbps          = EXCLUDED.verified_bandwidth_mbps,
				throughput_score               = EXCLUDED.throughput_score,
				reliability_score              = EXCLUDED.reliability_score,
				performance_score              = EXCLUDED.performance_score,
				readiness_multiplier           = EXCLUDED.readiness_multiplier,
				penalty_multiplier             = EXCLUDED.penalty_multiplier,
				prism_final_score              = EXCLUDED.prism_final_score,
				success_rate                   = EXCLUDED.success_rate,
				penalty_event_count            = EXCLUDED.penalty_event_count,
				routing_override_reason        = EXCLUDED.routing_override_reason,
				routing_computed_final_score   = EXCLUDED.routing_computed_final_score,
				routing_applied_score          = EXCLUDED.routing_applied_score,
				lifetime_verified_task_count   = EXCLUDED.lifetime_verified_task_count,
				age_days                       = EXCLUDED.age_days,
				computed_at                    = NOW(),
				updated_at                     = NOW()
	`;
}


export async function persistQualifiedMetrics(
	db: Db,
	orchestratorId: string,
	score: PrismScoreResult,
): Promise<void> {
	const tierA = tierAColumns(score);
	await db`
		INSERT INTO core.prism_metrics_qualified
			(orchestrator_id,
			 verified_transfer_count, verified_task_count, verified_bandwidth_mbps,
			 throughput_score, reliability_score, performance_score,
			 readiness_multiplier, penalty_multiplier,
			 prism_final_score,
			 success_rate, penalty_event_count,
			 routing_override_reason, routing_computed_final_score, routing_applied_score,
			 lifetime_verified_task_count,
			 computed_at, updated_at)
		VALUES
			(${orchestratorId},
			 ${score.verifiedTransferCount}, ${score.verifiedTaskCount}, ${score.verifiedBandwidthMbps},
			 ${score.throughputScore}, ${score.reliabilityScore}, ${score.performanceScore},
			 ${score.readinessMultiplier}, ${score.penaltyMultiplier},
			 ${score.prismFinalScore},
			 ${tierA.successRate}, ${tierA.penaltyEventCount},
			 ${tierA.routingOverrideReason}, ${tierA.routingComputedFinalScore}, ${tierA.routingAppliedScore},
			 ${tierA.lifetimeVerifiedTaskCount},
			 NOW(), NOW())
		ON CONFLICT (orchestrator_id) DO UPDATE
			SET verified_transfer_count        = EXCLUDED.verified_transfer_count,
				verified_task_count            = EXCLUDED.verified_task_count,
				verified_bandwidth_mbps          = EXCLUDED.verified_bandwidth_mbps,
				throughput_score               = EXCLUDED.throughput_score,
				reliability_score              = EXCLUDED.reliability_score,
				performance_score              = EXCLUDED.performance_score,
				readiness_multiplier           = EXCLUDED.readiness_multiplier,
				penalty_multiplier             = EXCLUDED.penalty_multiplier,
				prism_final_score              = EXCLUDED.prism_final_score,
				success_rate                   = EXCLUDED.success_rate,
				penalty_event_count            = EXCLUDED.penalty_event_count,
				routing_override_reason        = EXCLUDED.routing_override_reason,
				routing_computed_final_score   = EXCLUDED.routing_computed_final_score,
				routing_applied_score          = EXCLUDED.routing_applied_score,
				lifetime_verified_task_count   = EXCLUDED.lifetime_verified_task_count,
				computed_at                    = NOW(),
				updated_at                     = NOW()
	`;
}


export async function applyPoolPromotion(
	db: Db,
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
	db: Db,
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


export async function clearPrismPoolTransitionPending(db: Db, orchestratorId: string): Promise<void> {
	await db`
		UPDATE core.orchestrators
		SET prism_pool_transition_pending = FALSE,
			updated_at = NOW()
		WHERE id = ${orchestratorId}
			AND prism_pool_transition_pending = TRUE
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
	performanceWeights: PerformanceWeights,
	options?: { computeConfidence?: boolean },
): PrismScoreInput {
	return {
		verifiedTaskCount: indexes.verifiedTaskCountByOrchestrator.get(orchestrator.id) ?? 0,
		verifiedTransferCount: indexes.verifiedTransferCountByOrchestrator.get(orchestrator.id) ?? 0,
		averageVerifiedMbps: indexes.averageMbpsByOrchestrator.get(orchestrator.id) ?? 0,
		taskHourBuckets: indexes.taskBucketsByOrchestrator.get(orchestrator.id) ?? [],
		penaltyHourBuckets: indexes.penaltyBucketsByOrchestrator.get(orchestrator.id) ?? [],
		lifetimeVerifiedTaskCount: indexes.lifetimeVerifiedTaskCountByOrchestrator.get(orchestrator.id) ?? 0,
		lifetimeVerifiedTransferCount: indexes.lifetimeVerifiedTransferCountByOrchestrator.get(orchestrator.id) ?? 0,
		throughputRange,
		reliabilityRange,
		penaltyCoefficients,
		readiness: {
			ready: orchestrator.ready,
			controlPlaneConnected: orchestrator.control_plane_connected,
		},
		evidenceLookbackDays,
		confidenceTargets: { targetVerifiedTasks, ageSaturationDays },
		registeredAt: orchestrator.prism_age_at ?? orchestrator.registered_at,
		now,
		graduationConfidence,
		performanceWeights,
		...(options?.computeConfidence !== undefined
			? { computeConfidence: options.computeConfidence }
			: {}),
	};
}

export interface PrismConfigRow {
	evidence_lookback_days: number;
	half_life_hours: number;
	performance_throughput_weight: number;
	performance_reliability_weight: number;
	penalty_coeff_fraud: number;
	penalty_coeff_sybil: number;
	verified_task_target: number;
	age_saturation_days: number;
	graduation_confidence: number;
	fleet_normalization_floor: number;
	config_synced_at: Date;
	updated_at: Date;
}

export async function loadPrismConfig(db: Db): Promise<PrismConfigRow | null> {
	const rows = await db<PrismConfigRow[]>`
		SELECT
			evidence_lookback_days::int AS evidence_lookback_days,
			half_life_hours::int AS half_life_hours,
			performance_throughput_weight::float8 AS performance_throughput_weight,
			performance_reliability_weight::float8 AS performance_reliability_weight,
			penalty_coeff_fraud::float8 AS penalty_coeff_fraud,
			penalty_coeff_sybil::float8 AS penalty_coeff_sybil,
			verified_task_target::int AS verified_task_target,
			age_saturation_days::int AS age_saturation_days,
			graduation_confidence::float8 AS graduation_confidence,
			fleet_normalization_floor::float8 AS fleet_normalization_floor,
			config_synced_at,
			updated_at
		FROM core.prism_config
		WHERE id = 1
	`;
	return rows[0] ?? null;
}

async function syncOrchestratorPrismRoutingToControlPlane(db: Db): Promise<void> {
	const rows = await db<
		{
			id: string;
			prism_final_score: string;
			prism_confidence_score: string;
			prism_pool: "qualifying" | "qualified";
		}[]
	>`
		SELECT
			id,
			prism_final_score::text AS prism_final_score,
			prism_confidence_score::text AS prism_confidence_score,
			prism_pool
		FROM core.orchestrators
	`;
	await postRoutingSync("/internal/routing/orchestrators-prism-sync", { rows });
	logger.debug({ count: rows.length }, "synced orchestrator PRISM routing snapshot to control plane");
}

function routingScoreForOrchestrator(
	orchestrator: OrchestratorPrismRow,
	score: PrismScoreResult,
): number {
	if (score.readinessMultiplier <= 0) {
		return score.prismFinalScore;
	}

	if (
		orchestrator.prism_pool === "qualified" &&
		orchestrator.prism_pool_transition_pending &&
		hasEmptyQualifiedEvidence(score)
	) {
		const previousScore = numberValue(orchestrator.prism_final_score);
		if (previousScore > 0) return previousScore;
		return PRISM_DEFAULTS.PRISM_POOL_TRANSITION_SCORE;
	}

	return score.prismFinalScore;
}

function hasEmptyQualifiedEvidence(score: PrismScoreResult): boolean {
	return score.verifiedTaskCount === 0
		&& score.tasksInWindow === 0
		&& score.penaltyEventCount === 0;
}

function scoreWithRoutingOverride(
	orchestrator: OrchestratorPrismRow,
	score: PrismScoreResult,
	routingScore: number,
): PrismScoreResult {
	if (routingScore === score.prismFinalScore) return score;

	return withRoutingOverride(
		score,
		"transition_pending_empty_qualified_evidence",
		score.prismFinalScore,
		routingScore,
	);
}

export const prismScoreUpdater: PrismScoreUpdaterJob = async ({ db }) => {
	const now = new Date();
	const rawConfig = await loadPrismConfig(db);
	const config = rawConfig ?? {
		evidence_lookback_days: PRISM_DEFAULTS.PRISM_EVIDENCE_LOOKBACK_DAYS,
		penalty_coeff_fraud: PRISM_DEFAULTS.PRISM_PENALTY_COEFF_FRAUD,
		penalty_coeff_sybil: PRISM_DEFAULTS.PRISM_PENALTY_COEFF_SYBIL,
		performance_throughput_weight: PRISM_DEFAULTS.PRISM_PERFORMANCE_THROUGHPUT_WEIGHT,
		performance_reliability_weight: PRISM_DEFAULTS.PRISM_PERFORMANCE_RELIABILITY_WEIGHT,
		graduation_confidence: PRISM_DEFAULTS.PRISM_GRADUATION_CONFIDENCE,
		verified_task_target: PRISM_DEFAULTS.PRISM_TARGET_GRADUATION_VERIFIED_TASKS,
		age_saturation_days: PRISM_DEFAULTS.PRISM_AGE_SATURATION_DAYS,
	};
	const lookbackDays = config.evidence_lookback_days;

	const penaltyCoefficients: PenaltyCoefficients = {
		fraud: config.penalty_coeff_fraud,
		sybil: config.penalty_coeff_sybil,
	};
	const performanceWeights: PerformanceWeights = {
		throughput: config.performance_throughput_weight,
		reliability: config.performance_reliability_weight,
	};
	const graduationConfidence = config.graduation_confidence;
	const transitionScore = PRISM_DEFAULTS.PRISM_POOL_TRANSITION_SCORE;

	const orchestrators = await db<OrchestratorPrismRow[]>`
		SELECT
			o.id,
			o.hotkey,
			o.ready,
			o.control_plane_connected,
			o.registered_at,
			COALESCE(o.uid_assigned_at, o.registered_at) AS prism_age_at,
			o.prism_pool,
			o.prism_confidence_score::text AS prism_confidence_score,
			o.prism_final_score::text AS prism_final_score,
			o.prism_pool_transition_pending
		FROM core.orchestrators o
		WHERE o.uid IS NOT NULL
	`;

	const [
		qualifyingAggs, qualifyingTotals,
		qualifiedAggs,  qualifiedTotals,
	] = await Promise.all([
		loadPrismHourlyAggregates(db, lookbackDays, "qualifying"),
		loadPrismEvidenceTotals(db, "qualifying"),
		loadPrismHourlyAggregates(db, lookbackDays, "qualified"),
		loadPrismEvidenceTotals(db, "qualified"),
	]);

	const qualifyingIndexes = indexPrismEvidence(qualifyingAggs, qualifyingTotals, now);
	const qualifiedIndexes  = indexPrismEvidence(qualifiedAggs,  qualifiedTotals,  now);

	const qualifyingOrchestrators = orchestrators.filter((o) => o.prism_pool === "qualifying");
	const qualifiedOrchestrators  = orchestrators.filter((o) => o.prism_pool === "qualified");

	const qualifyingIds = qualifyingOrchestrators.map((o) => o.id);
	const qualifiedIds  = qualifiedOrchestrators.map((o) => o.id);
	const qualifyingRange = fleetRangeForCohort(qualifyingIds, qualifyingIndexes);
	const qualifiedRange  = fleetRangeForCohort(qualifiedIds,  qualifiedIndexes);

	logger.info(
		{
			orchestratorCount: orchestrators.length,
			qualifyingCount: qualifyingOrchestrators.length,
			qualifiedCount: qualifiedOrchestrators.length,
			qualifyingThroughputRange: qualifyingRange.throughputRange,
			qualifyingReliabilityRange: qualifyingRange.reliabilityRange,
			qualifiedThroughputRange: qualifiedRange.throughputRange,
			qualifiedReliabilityRange: qualifiedRange.reliabilityRange,
			penaltyCoefficients,
			performanceWeights,
			graduationConfidence,
		},
		"PRISM scoring run started",
	);

	const promotions: Array<{
		orchestrator: OrchestratorPrismRow;
		score: PrismScoreResult;
	}> = [];

	for (const orchestrator of qualifyingOrchestrators) {
		const scoreInput = buildScoreInput(
			orchestrator,
			qualifyingIndexes,
			qualifyingRange.throughputRange,
			qualifyingRange.reliabilityRange,
			penaltyCoefficients,
			now,
			lookbackDays,
			config.verified_task_target,
			config.age_saturation_days,
			graduationConfidence,
			performanceWeights,
			{ computeConfidence: true },
		);
		const score = computePrismScore(scoreInput);
		const newPool = resolveNewPool(orchestrator.prism_pool, score.confidenceScore, graduationConfidence);

		if (newPool === "qualified") {
			promotions.push({ orchestrator, score });
			continue;
		}

		await persistQualifyingMetrics(db, orchestrator.id, score);
		const routingScore = routingScoreForOrchestrator(orchestrator, score);
		await mirrorOrchestratorRoutingScore(
			db,
			orchestrator,
			routingScore,
			true,
			score.confidenceScore,
			"qualifying",
		);
	}

	for (const { orchestrator } of promotions) {
		const qualifiedSeedInput = buildScoreInput(
			orchestrator,
			qualifiedIndexes,
			qualifiedRange.throughputRange,
			qualifiedRange.reliabilityRange,
			penaltyCoefficients,
			now,
			lookbackDays,
			config.verified_task_target,
			config.age_saturation_days,
			graduationConfidence,
			performanceWeights,
			{ computeConfidence: false },
		);
		const qualifiedSeedScore = computePrismScore(qualifiedSeedInput);
		const seedScore = { ...qualifiedSeedScore, prismFinalScore: transitionScore };
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
			lookbackDays,
			config.verified_task_target,
			config.age_saturation_days,
			graduationConfidence,
			performanceWeights,
			{ computeConfidence: false },
		);
		const score = computePrismScore(scoreInput);
		const routingScore = routingScoreForOrchestrator(orchestrator, score);
		const persistedScore = scoreWithRoutingOverride(orchestrator, score, routingScore);
		await persistQualifiedMetrics(db, orchestrator.id, persistedScore);
		await mirrorOrchestratorRoutingScore(db, orchestrator, routingScore, false);
		if (orchestrator.prism_pool_transition_pending && score.verifiedTaskCount > 0) {
			await clearPrismPoolTransitionPending(db, orchestrator.id);
		}
	}

	if (orchestrators.length > 0) {
		logger.debug(
			{
				count: orchestrators.length,
				promotions: promotions.length,
			},
			"PRISM scores refreshed",
		);
	}

	try {
		await syncOrchestratorPrismRoutingToControlPlane(db);
	} catch (err) {
		logger.warn({ err }, "failed to sync PRISM routing snapshot to control plane");
	}
};
