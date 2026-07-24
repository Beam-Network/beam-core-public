import type { JobFn } from "../runner.js";
import type { Db } from "../db/client.js";
import { logger as baseLogger } from "../logger.js";
import { env } from "../config.js";
import {
	computePrismScore,
	resolveNewPool,
	type PenaltyCoefficients,
	type PerformanceWeights,
	type PrismScoreResult,
} from "../prism/scoring.js";
import {
	loadPrismHourlyAggregates,
	loadPrismEvidenceTotals,
} from "../prism/prism-evidence.js";
import {
	applyPoolPromotion,
	buildScoreInput,
	clearPrismPoolTransitionPending,
	fleetRangeForCohort,
	indexPrismEvidence,
	indexReadinessActiveTime,
	loadReadinessEvents,
	markPrismPoolTransitionPending,
	mirrorOrchestratorRoutingScore,
	persistQualifyingMetrics,
	persistQualifiedMetrics,
	type OrchestratorPrismRow,
} from "../prism/prism-score-pipeline.js";
import { loadPrismConfig } from "../prism/prism-config.js";

const logger = baseLogger.child({ component: "prism-score-updater", job: "prism-score-updater" });

export const QUALIFIED_SEASONAL_RESET_CONFIG_KEY = "qualified_prism_seasonal_reset_at";
const QUALIFIED_SEASONAL_RESET_SCORE = 0.5;
export const QUALIFIED_SEASONAL_RESET_OVERRIDE_REASON = "seasonal_reset_pending_qualified_evidence";
const MS_PER_DAY = 24 * 60 * 60 * 1000;

interface PreparedPrismScore {
	persistedScore: PrismScoreResult;
	routingScore: number;
}

function roundScore(value: number): number {
	return Number(value.toFixed(5));
}

function latestSundayUtcResetBoundary(now: Date): Date {
	const utcDayStart = Date.UTC(now.getUTCFullYear(), now.getUTCMonth(), now.getUTCDate());
	return new Date(utcDayStart - now.getUTCDay() * MS_PER_DAY);
}

function routingOverrideScore(
	score: PrismScoreResult,
	overrideScore: number,
	reason: string,
): PreparedPrismScore {
	const prismFinalScore = roundScore(overrideScore);
	const routingScore = roundScore(prismFinalScore * score.readinessMultiplier);
	return {
		persistedScore: {
			...score,
			prismFinalScore,
			routingOverrideReason: reason,
			routingComputedFinalScore: score.prismFinalScore,
			routingAppliedScore: routingScore,
		},
		routingScore,
	};
}

function promotionSeededScore(score: PrismScoreResult): PreparedPrismScore {
	return routingOverrideScore(
		score,
		env.PRISM_POOL_TRANSITION_SCORE,
		"transition_pending_empty_qualified_task_evidence",
	);
}

function seasonalResetScore(score: PrismScoreResult): PreparedPrismScore {
	return routingOverrideScore(score, QUALIFIED_SEASONAL_RESET_SCORE, QUALIFIED_SEASONAL_RESET_OVERRIDE_REASON);
}

function prepareScoreForRouting(
	orchestrator: OrchestratorPrismRow,
	score: PrismScoreResult,
	options: { allowQualifiedTransitionScore?: boolean; applyQualifiedSeasonalReset?: boolean } = {},
): PreparedPrismScore {
	if (
		orchestrator.prism_pool === "qualified" &&
		options.applyQualifiedSeasonalReset === true
	) {
		return seasonalResetScore(score);
	}

	if (
		orchestrator.prism_pool === "qualified" &&
		options.allowQualifiedTransitionScore === true &&
		hasEmptyQualifiedTaskEvidence(score)
	) {
		return promotionSeededScore(score);
	}

	return {
		persistedScore: score,
		routingScore: score.prismFinalScore,
	};
}

function hasEmptyQualifiedTaskEvidence(score: PrismScoreResult): boolean {
	return score.verifiedTaskCount === 0
		&& score.verifiedTransferCount === 0
		&& score.verifiedBandwidthMbps === 0;
}

function hasPositiveQualifiedTaskEvidence(score: PrismScoreResult): boolean {
	return !hasEmptyQualifiedTaskEvidence(score);
}

function hasEmptyLifetimeEvidence(input: { lifetimeVerifiedTaskCount: number; lifetimeVerifiedTransferCount: number }): boolean {
	return input.lifetimeVerifiedTaskCount === 0 && input.lifetimeVerifiedTransferCount === 0;
}

function hasEvidenceAtOrAfter(latestEventAt: Date | undefined, boundary: Date): boolean {
	return latestEventAt != null && latestEventAt.getTime() >= boundary.getTime();
}

async function advanceQualifiedSeasonalResetCheckpoint(
	db: Db,
	scheduledResetAt: Date,
): Promise<{ resetAt: Date; advanced: boolean; previousResetAt: Date | null }> {
	const currentRows = await db<{ reset_at: Date }[]>`
		SELECT value::timestamptz AS reset_at
		FROM core.system_config
		WHERE key = ${QUALIFIED_SEASONAL_RESET_CONFIG_KEY}
		LIMIT 1
	`;
	const previousResetAt = currentRows[0]?.reset_at ?? null;
	if (previousResetAt != null && previousResetAt.getTime() >= scheduledResetAt.getTime()) {
		return { resetAt: previousResetAt, advanced: false, previousResetAt };
	}

	const resetAt = scheduledResetAt;
	const updatedRows = await db<{ reset_at: Date }[]>`
		INSERT INTO core.system_config (key, value, updated_at)
		VALUES (${QUALIFIED_SEASONAL_RESET_CONFIG_KEY}, ${resetAt.toISOString()}, NOW())
		ON CONFLICT (key) DO UPDATE
			SET value = EXCLUDED.value,
				updated_at = NOW()
			WHERE core.system_config.value::timestamptz < EXCLUDED.value::timestamptz
		RETURNING value::timestamptz AS reset_at
	`;
	if (updatedRows[0]?.reset_at != null) {
		return { resetAt: updatedRows[0].reset_at, advanced: true, previousResetAt };
	}

	const latestRows = await db<{ reset_at: Date }[]>`
		SELECT value::timestamptz AS reset_at
		FROM core.system_config
		WHERE key = ${QUALIFIED_SEASONAL_RESET_CONFIG_KEY}
		LIMIT 1
	`;
	return { resetAt: latestRows[0]?.reset_at ?? resetAt, advanced: false, previousResetAt };
}

export async function refreshPrismScores(db: Db): Promise<void> {
	const now = new Date();
	const qualifiedScheduledResetAt = latestSundayUtcResetBoundary(now);
	const qualifiedSeasonalResetCheckpoint = await advanceQualifiedSeasonalResetCheckpoint(
		db,
		qualifiedScheduledResetAt,
	);
	const rawConfig = await loadPrismConfig(db);
	const config = rawConfig ?? {
		evidence_lookback_days: env.PRISM_EVIDENCE_LOOKBACK_DAYS,
		penalty_coeff_fraud: env.PRISM_PENALTY_COEFF_FRAUD,
		penalty_coeff_integrity_chunk_mismatch: env.PRISM_PENALTY_COEFF_INTEGRITY_CHUNK_MISMATCH,
		penalty_coeff_sybil: env.PRISM_PENALTY_COEFF_SYBIL,
		performance_throughput_weight: env.PRISM_PERFORMANCE_THROUGHPUT_WEIGHT,
		performance_reliability_weight: env.PRISM_PERFORMANCE_RELIABILITY_WEIGHT,
		graduation_confidence: env.PRISM_GRADUATION_CONFIDENCE,
		verified_task_target: env.PRISM_TARGET_GRADUATION_VERIFIED_TASKS,
		age_saturation_days: env.PRISM_AGE_SATURATION_DAYS,
	};
	const lookbackDays = config.evidence_lookback_days;

	const penaltyCoefficients: PenaltyCoefficients = {
		fraud: config.penalty_coeff_fraud,
		integrityChunkMismatch: config.penalty_coeff_integrity_chunk_mismatch,
		sybil: config.penalty_coeff_sybil,
	};
	const performanceWeights: PerformanceWeights = {
		throughput: config.performance_throughput_weight,
		reliability: config.performance_reliability_weight,
	};
	const graduationConfidence = config.graduation_confidence;

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
		readinessEvents,
		qualifyingAggs, qualifyingTotals,
		qualifiedAggs,  qualifiedTotals,
	] = await Promise.all([
		loadReadinessEvents(db, lookbackDays, now),
		loadPrismHourlyAggregates(db, lookbackDays, "qualifying"),
		loadPrismEvidenceTotals(db, "qualifying"),
		loadPrismHourlyAggregates(db, lookbackDays, "qualified"),
		loadPrismEvidenceTotals(db, "qualified"),
	]);

	const qualifyingIndexes = indexPrismEvidence(qualifyingAggs, qualifyingTotals, now);
	const qualifiedIndexes  = indexPrismEvidence(qualifiedAggs,  qualifiedTotals,  now);
	const readinessActiveTimeByOrchestrator = indexReadinessActiveTime(orchestrators, readinessEvents, now, lookbackDays);

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
			qualifiedScheduledResetAt: qualifiedScheduledResetAt.toISOString(),
			qualifiedSeasonalResetAt: qualifiedSeasonalResetCheckpoint.resetAt.toISOString(),
			qualifiedSeasonalResetAdvanced: qualifiedSeasonalResetCheckpoint.advanced,
			previousQualifiedSeasonalResetAt: qualifiedSeasonalResetCheckpoint.previousResetAt?.toISOString() ?? null,
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
			{
				computeConfidence: true,
				...(readinessActiveTimeByOrchestrator.has(orchestrator.id)
					? { readinessActiveTimeMultiplier: readinessActiveTimeByOrchestrator.get(orchestrator.id)! }
					: {}),
			},
		);
		const score = computePrismScore(scoreInput);
		const newPool = resolveNewPool(orchestrator.prism_pool, score.confidenceScore, graduationConfidence);

		if (newPool === "qualified") {
			promotions.push({ orchestrator, score });
			continue;
		}

		await persistQualifyingMetrics(db, orchestrator.id, score);
		const { routingScore } = prepareScoreForRouting(orchestrator, score);
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
		const promotionReadinessMultiplier = orchestrator.ready && orchestrator.control_plane_connected ? 1 : 0;
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
			{
				computeConfidence: false,
				readinessActiveTimeMultiplier: promotionReadinessMultiplier,
			},
		);
		const qualifiedSeedScore = computePrismScore(qualifiedSeedInput);
		const { persistedScore, routingScore } = promotionSeededScore(qualifiedSeedScore);
		await applyPoolPromotion(db, orchestrator, routingScore, persistedScore);
		orchestrator.prism_pool = "qualified";
		orchestrator.prism_pool_transition_pending = true;
	}

	let seasonalResetOverrides = 0;
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
			{
				computeConfidence: false,
				...(readinessActiveTimeByOrchestrator.has(orchestrator.id)
					? { readinessActiveTimeMultiplier: readinessActiveTimeByOrchestrator.get(orchestrator.id)! }
					: {}),
			},
		);
		const score = computePrismScore(scoreInput);
		const emptyQualifiedEvidence = hasEmptyQualifiedTaskEvidence(score);
		const emptyLifetimeEvidence = hasEmptyLifetimeEvidence(scoreInput);
		const transitionAlreadyPending = orchestrator.prism_pool_transition_pending === true;
		const allowQualifiedTransitionScore = emptyQualifiedEvidence
			&& (
				emptyLifetimeEvidence
				|| transitionAlreadyPending
			);
		const applyQualifiedSeasonalReset = !hasEvidenceAtOrAfter(
			qualifiedIndexes.latestEventAtByOrchestrator.get(orchestrator.id),
			qualifiedSeasonalResetCheckpoint.resetAt,
		);
		if (applyQualifiedSeasonalReset) seasonalResetOverrides += 1;
		const { persistedScore, routingScore } = prepareScoreForRouting(orchestrator, score, {
			allowQualifiedTransitionScore,
			applyQualifiedSeasonalReset,
		});
		await persistQualifiedMetrics(db, orchestrator.id, persistedScore);
		await mirrorOrchestratorRoutingScore(db, orchestrator, routingScore, false);
		if (allowQualifiedTransitionScore && !orchestrator.prism_pool_transition_pending) {
			await markPrismPoolTransitionPending(db, orchestrator.id);
			orchestrator.prism_pool_transition_pending = true;
		} else if (orchestrator.prism_pool_transition_pending && (!allowQualifiedTransitionScore || hasPositiveQualifiedTaskEvidence(score))) {
			await clearPrismPoolTransitionPending(db, orchestrator.id);
			orchestrator.prism_pool_transition_pending = false;
		}
	}

	if (orchestrators.length > 0) {
		logger.debug(
			{
				count: orchestrators.length,
				promotions: promotions.length,
				seasonalResetOverrides,
			},
			"PRISM scores refreshed",
		);
	}

	try {
		const { syncOrchestratorPrismRoutingToTransferRuntime } = await import("../lib/transfer-runtime-routing-sync.js");
		await syncOrchestratorPrismRoutingToTransferRuntime(db);
	} catch (err) {
		logger.warn({ err }, "failed to sync PRISM routing snapshot to transfer runtime");
	}
}

export const prismScoreUpdater: JobFn = async ({ db }) => {
	await refreshPrismScores(db);
};
