

import {
	computePrismScore,
	resolveNewPool,
	type PrismScoreInput,
	type PrismScoreResult,
} from "../prism/scoring.js";

const QUALIFIED_RESET_SCORE = 0.5;

export const QUALIFIED_SEASONAL_RESET_OVERRIDE_REASON = "seasonal_reset_pending_qualified_evidence";
export const TRANSITION_OVERRIDE_REASON = "transition_pending_empty_qualified_task_evidence";

export interface OrchestratorPrismSnapshot {
	id: string;
	pool: "qualifying" | "qualified";
	ready: boolean;
	controlPlaneConnected: boolean;
	transitionPending: boolean;
	latestQualifiedEvidenceAt: Date | null;
}

export interface PrismRefreshConfig {
	graduationConfidence: number;
	transitionScore: number;
}

export interface PrismRefreshState {
	now: Date;
	manualQualifiedResetAt: Date | null;
	config: PrismRefreshConfig;
	orchestrators: OrchestratorPrismSnapshot[];

	buildScoreInput(
		orchestrator: OrchestratorPrismSnapshot,
		pool: "qualifying" | "qualified",
		options: { computeConfidence: boolean; readinessActiveTimeMultiplier?: number },
	): PrismScoreInput;
}

export interface PrismRefreshSink {
	persistQualifying(orchestratorId: string, score: PrismScoreResult, routingScore: number): Promise<void>;
	promote(
		orchestratorId: string,
		score: PrismScoreResult,
		routingScore: number,
	): Promise<void>;
	persistQualified(orchestratorId: string, score: PrismScoreResult, routingScore: number): Promise<void>;
	setTransitionPending(orchestratorId: string, pending: boolean): Promise<void>;

	publishRoutingSnapshot(): Promise<void>;
}

export interface PrismRefreshSummary {
	processed: number;
	promotions: number;
	qualifiedResetOverrides: number;
	routingSnapshotPublished: boolean;
}

export interface PreparedPrismScore {
	persistedScore: PrismScoreResult;
	routingScore: number;
}

function roundScore(value: number): number {
	return Number(value.toFixed(5));
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

function hasEmptyQualifiedEvidence(score: PrismScoreResult): boolean {
	return score.verifiedTaskCount === 0
		&& score.verifiedTransferCount === 0
		&& score.verifiedBandwidthMbps === 0;
}

function hasEmptyLifetimeEvidence(input: PrismScoreInput): boolean {
	return input.lifetimeVerifiedTaskCount === 0
		&& input.lifetimeVerifiedTransferCount === 0;
}

function hasEvidenceAtOrAfter(evidenceAt: Date | null, boundary: Date): boolean {
	return evidenceAt !== null && evidenceAt.getTime() >= boundary.getTime();
}

export function prepareQualifiedScore(input: {
	score: PrismScoreResult;
	transitionScore: number;
	allowTransitionOverride: boolean;
	applyResetOverride: boolean;
}): PreparedPrismScore {
	if (input.applyResetOverride) {
		return routingOverrideScore(
			input.score,
			QUALIFIED_RESET_SCORE,
			QUALIFIED_SEASONAL_RESET_OVERRIDE_REASON,
		);
	}
	if (input.allowTransitionOverride && hasEmptyQualifiedEvidence(input.score)) {
		return routingOverrideScore(input.score, input.transitionScore, TRANSITION_OVERRIDE_REASON);
	}
	return { persistedScore: input.score, routingScore: input.score.prismFinalScore };
}

export async function refreshPrismScores(
	state: PrismRefreshState,
	sink: PrismRefreshSink,
): Promise<PrismRefreshSummary> {
	const resetBoundary = state.manualQualifiedResetAt;
	const qualifying = state.orchestrators.filter((orchestrator) => orchestrator.pool === "qualifying");
	const qualified = state.orchestrators.filter((orchestrator) => orchestrator.pool === "qualified");
	const promotions: OrchestratorPrismSnapshot[] = [];

	for (const orchestrator of qualifying) {
		const scoreInput = state.buildScoreInput(orchestrator, "qualifying", { computeConfidence: true });
		const score = computePrismScore(scoreInput);
		const nextPool = resolveNewPool(
			orchestrator.pool,
			score.confidenceScore,
			state.config.graduationConfidence,
		);
		if (nextPool === "qualified") {
			promotions.push(orchestrator);
			continue;
		}
		await sink.persistQualifying(orchestrator.id, score, score.prismFinalScore);
	}

	for (const orchestrator of promotions) {
		const currentlyReady = orchestrator.ready && orchestrator.controlPlaneConnected ? 1 : 0;
		const qualifiedInput = state.buildScoreInput(orchestrator, "qualified", {
			computeConfidence: false,
			readinessActiveTimeMultiplier: currentlyReady,
		});
		const qualifiedScore = computePrismScore(qualifiedInput);
		const seeded = routingOverrideScore(
			qualifiedScore,
			state.config.transitionScore,
			TRANSITION_OVERRIDE_REASON,
		);
		await sink.promote(orchestrator.id, seeded.persistedScore, seeded.routingScore);
		orchestrator.pool = "qualified";
		orchestrator.transitionPending = true;
	}

	let qualifiedResetOverrides = 0;
	for (const orchestrator of qualified) {
		const scoreInput = state.buildScoreInput(orchestrator, "qualified", { computeConfidence: false });
		const score = computePrismScore(scoreInput);
		const emptyEvidence = hasEmptyQualifiedEvidence(score);
		const allowTransitionOverride = emptyEvidence
			&& (hasEmptyLifetimeEvidence(scoreInput) || orchestrator.transitionPending);
		const applyResetOverride = resetBoundary !== null
			&& !hasEvidenceAtOrAfter(
				orchestrator.latestQualifiedEvidenceAt,
				resetBoundary,
			);
		if (applyResetOverride) qualifiedResetOverrides += 1;
		const prepared = prepareQualifiedScore({
			score,
			transitionScore: state.config.transitionScore,
			allowTransitionOverride,
			applyResetOverride,
		});
		await sink.persistQualified(orchestrator.id, prepared.persistedScore, prepared.routingScore);

		const shouldRemainPending = allowTransitionOverride;
		if (shouldRemainPending !== orchestrator.transitionPending) {
			await sink.setTransitionPending(orchestrator.id, shouldRemainPending);
			orchestrator.transitionPending = shouldRemainPending;
		}
	}

	let routingSnapshotPublished = true;
	try {
		await sink.publishRoutingSnapshot();
	} catch {
		routingSnapshotPublished = false;
	}

	return {
		processed: state.orchestrators.length,
		promotions: promotions.length,
		qualifiedResetOverrides,
		routingSnapshotPublished,
	};
}
