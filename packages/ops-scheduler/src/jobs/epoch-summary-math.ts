/**
 * Pure helpers for epoch summary weights (testable, no DB).
 * raw_i = prism_final_score_i x task_done_count_i; tiered weights sum to 1 when raw work exists.
 */

export const PRISM_WEIGHT_FORMULA_VERSION = "tiered_prism_final_score_x_tasks_v1";

export type EmissionTier = "A" | "B" | "C";

const TIER_SHARES: Record<EmissionTier, number> = {
	A: 0.8,
	B: 0.15,
	C: 0.05,
};

export interface TieredWeightInput {
	uid: number | null;
	hotkey: string;
	prismFinalScore: number;
	taskDoneCount: number;
	rawScore: number;
}

export interface TieredWeightResult {
	weights: number[];
	sumRaw: number;
	tiers: EmissionTier[];
	tierCounts: Record<EmissionTier, number>;
	tierRawTotals: Record<EmissionTier, number>;
	effectiveTierShares: Record<EmissionTier, number>;
}

export function computeRawScore(prismFinalScore: number, taskDoneCount: number): number {
	const p = Number.isFinite(prismFinalScore) ? prismFinalScore : 0;
	const t = Number.isFinite(taskDoneCount) ? taskDoneCount : 0;
	return Math.max(0, p * t);
}

function safeScore(value: number): number {
	return Number.isFinite(value) ? Math.max(0, value) : 0;
}

function compareUidAscNullsLast(a: number | null, b: number | null): number {
	if (a === null && b === null) return 0;
	if (a === null) return 1;
	if (b === null) return -1;
	return a - b;
}

function computeTierCounts(count: number): Record<EmissionTier, number> {
	if (count <= 0) return { A: 0, B: 0, C: 0 };
	const a = Math.min(count, Math.ceil(count * 0.1));
	const b = Math.min(count - a, Math.ceil(count * 0.3));
	return { A: a, B: b, C: count - a - b };
}

function tierForRank(rank: number, counts: Record<EmissionTier, number>): EmissionTier {
	if (rank < counts.A) return "A";
	if (rank < counts.A + counts.B) return "B";
	return "C";
}

/**
 * Maps base raw scores to tiered normalized weights.
 *
 * Tier membership is based on base_raw descending with deterministic tie-breakers.
 * Tier B/C shares roll up to Tier A when that tier has no positive raw score.
 */
export function normalizeWeightsWithEmissionTiers(candidates: TieredWeightInput[]): TieredWeightResult {
	const n = candidates.length;
	const weights = new Array<number>(n).fill(0);
	const tiers = new Array<EmissionTier>(n).fill("C");
	const tierCounts = computeTierCounts(n);
	const tierRawTotals: Record<EmissionTier, number> = { A: 0, B: 0, C: 0 };
	const effectiveTierShares: Record<EmissionTier, number> = { ...TIER_SHARES };

	const ranked = candidates
		.map((candidate, index) => ({
			...candidate,
			index,
			rawScore: safeScore(candidate.rawScore),
			prismFinalScore: safeScore(candidate.prismFinalScore),
			taskDoneCount: safeScore(candidate.taskDoneCount),
		}))
		.sort(
			(left, right) =>
				right.rawScore - left.rawScore ||
				right.prismFinalScore - left.prismFinalScore ||
				right.taskDoneCount - left.taskDoneCount ||
				compareUidAscNullsLast(left.uid, right.uid) ||
				left.hotkey.localeCompare(right.hotkey) ||
				left.index - right.index,
		);

	for (let rank = 0; rank < ranked.length; rank++) {
		const tier = tierForRank(rank, tierCounts);
		const candidate = ranked[rank]!;
		tiers[candidate.index] = tier;
		tierRawTotals[tier] += candidate.rawScore;
	}

	const sumRaw = tierRawTotals.A + tierRawTotals.B + tierRawTotals.C;
	if (n === 0 || sumRaw <= 0 || tierRawTotals.A <= 0) {
		return { weights, sumRaw, tiers, tierCounts, tierRawTotals, effectiveTierShares };
	}

	for (const tier of ["B", "C"] as const) {
		if (tierCounts[tier] === 0 || tierRawTotals[tier] <= 0) {
			effectiveTierShares.A += TIER_SHARES[tier];
			effectiveTierShares[tier] = 0;
		}
	}

	let lastPositiveIndex = -1;
	for (const candidate of ranked) {
		if (candidate.rawScore <= 0) continue;
		const tier = tiers[candidate.index]!;
		const tierShare = effectiveTierShares[tier];
		const tierRawTotal = tierRawTotals[tier];
		if (tierShare <= 0 || tierRawTotal <= 0) continue;
		weights[candidate.index] = tierShare * (candidate.rawScore / tierRawTotal);
		lastPositiveIndex = candidate.index;
	}

	if (lastPositiveIndex >= 0) {
		const total = sumWeights(weights);
		weights[lastPositiveIndex] = Math.max(0, weights[lastPositiveIndex]! + (1 - total));
	}

	return { weights, sumRaw, tiers, tierCounts, tierRawTotals, effectiveTierShares };
}

export function sumWeights(weights: number[]): number {
	return weights.reduce((s, w) => s + w, 0);
}
