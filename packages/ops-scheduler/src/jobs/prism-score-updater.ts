import type { JobFn } from "../runner.js";
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
	mirrorOrchestratorRoutingScore,
	persistQualifyingMetrics,
	persistQualifiedMetrics,
	type OrchestratorPrismRow,
} from "../prism/prism-score-pipeline.js";
import { loadPrismConfig } from "../prism/prism-config.js";

const logger = baseLogger.child({ component: "prism-score-updater", job: "prism-score-updater" });

interface PreparedPrismScore {
	persistedScore: PrismScoreResult;
	routingScore: number;
}

function roundScore(value: number): number {
	return Number(value.toFixed(5));
}

function promotionSeededScore(score: PrismScoreResult): PreparedPrismScore {
	const promotionScore = env.PRISM_POOL_TRANSITION_SCORE;
	const routingScore = roundScore(promotionScore * score.readinessMultiplier);

	return {
		persistedScore: {
			...score,
			prismFinalScore: promotionScore,
			routingOverrideReason: "transition_pending_empty_qualified_task_evidence",
			routingComputedFinalScore: score.prismFinalScore,
			routingAppliedScore: routingScore,
		},
		routingScore,
	};
}

function prepareScoreForRouting(
	orchestrator: OrchestratorPrismRow,
	score: PrismScoreResult,
): PreparedPrismScore {
	if (
		orchestrator.prism_pool === "qualified" &&
		orchestrator.prism_pool_transition_pending &&
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

export const prismScoreUpdater: JobFn = async ({ db }) => {
	const now = new Date();
	const rawConfig = await loadPrismConfig(db);
	const config = rawConfig ?? {
		evidence_lookback_days: env.PRISM_EVIDENCE_LOOKBACK_DAYS,
		penalty_coeff_fraud: env.PRISM_PENALTY_COEFF_FRAUD,
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
		const { persistedScore, routingScore } = prepareScoreForRouting(orchestrator, score);
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
		const { syncOrchestratorPrismRoutingToTransferRuntime } = await import("../lib/transfer-runtime-routing-sync.js");
		await syncOrchestratorPrismRoutingToTransferRuntime(db);
	} catch (err) {
		logger.warn({ err }, "failed to sync PRISM routing snapshot to transfer runtime");
	}
};
