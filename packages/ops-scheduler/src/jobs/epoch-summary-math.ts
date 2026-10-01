/**
 * Pure helpers for epoch summary weights (testable, no DB).
 * raw_i = verified_uploaded_mib_i * penalty_multiplier_i * fraud_report_reward_i, where verified_uploaded_mib_i is
 * the completed production upload MiB total in the PRISM evidence window; tiered
 * weights sum to 1 when raw work exists.
 */

export const PRISM_WEIGHT_FORMULA_VERSION = "tiered_weight_verified_uploaded_mib_x_penalty_x_fraud_report_reward_v4";
export const FRAUD_REPORT_BONUS_PERCENTAGES = { critical: 30, high: 20, medium: 10, low: 5 } as const;

export function fraudReportReward(bonusPercentages: readonly number[]): number {
  return 1 + Math.min(100, bonusPercentages.reduce((sum, bonus) => sum + (Number.isFinite(bonus) ? Math.max(0, bonus) : 0), 0)) / 100;
}

export type EmissionTier = "A" | "B" | "C" | "D" | "E";

const TIER_SHARES: Record<EmissionTier, number> = {
	A: 0.5,
	B: 0.35,
	C: 0.1,
	D: 0.04,
	E: 0.01,
};

export interface TieredWeightInput {
	uid: number | null;
	hotkey: string;
	prismFinalScore: number;
	verifiedUploadedMib: number;
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

export function computeRawScore(verifiedUploadedMib: number, penaltyMultiplier: number, fraudReportRewardMultiplier: number): number {
	const m = Number.isFinite(verifiedUploadedMib) ? verifiedUploadedMib : 0;
	const p = Number.isFinite(penaltyMultiplier) ? penaltyMultiplier : 0;
  const reward = Number.isFinite(fraudReportRewardMultiplier) ? Math.max(1, Math.min(2, fraudReportRewardMultiplier)) : 1;
  return Math.max(0, m) * Math.max(0, p) * reward;
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
	if (count <= 0) return { A: 0, B: 0, C: 0, D: 0, E: 0 };
	const a = Math.min(count, 30);
	const b = Math.min(count - a, 30);
	const c = Math.min(count - a - b, 30);
	const d = Math.min(count - a - b - c, 30);
	return { A: a, B: b, C: c, D: d, E: count - a - b - c - d };
}

function tierForRank(rank: number, counts: Record<EmissionTier, number>): EmissionTier {
	if (rank < counts.A) return "A";
	if (rank < counts.A + counts.B) return "B";
	if (rank < counts.A + counts.B + counts.C) return "C";
	if (rank < counts.A + counts.B + counts.C + counts.D) return "D";
	return "E";
}

/**
 * Maps base raw scores to tiered normalized weights.
 *
 * Tier membership is based on base_raw descending with deterministic tie-breakers.
 * Tier B/C/D/E shares roll up to Tier A when that tier has no positive raw score.
 */
export function normalizeWeightsWithEmissionTiers(candidates: TieredWeightInput[]): TieredWeightResult {
	const n = candidates.length;
	const weights = new Array<number>(n).fill(0);
	const tiers = new Array<EmissionTier>(n).fill("E");
	const tierCounts = computeTierCounts(n);
	const tierRawTotals: Record<EmissionTier, number> = { A: 0, B: 0, C: 0, D: 0, E: 0 };
	const effectiveTierShares: Record<EmissionTier, number> = { ...TIER_SHARES };

	const ranked = candidates
		.map((candidate, index) => ({
			...candidate,
			index,
			rawScore: safeScore(candidate.rawScore),
			prismFinalScore: safeScore(candidate.prismFinalScore),
			verifiedUploadedMib: safeScore(candidate.verifiedUploadedMib),
		}))
		.sort(
			(left, right) =>
				right.rawScore - left.rawScore ||
				right.prismFinalScore - left.prismFinalScore ||
				right.verifiedUploadedMib - left.verifiedUploadedMib ||
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

	const sumRaw = tierRawTotals.A + tierRawTotals.B + tierRawTotals.C + tierRawTotals.D + tierRawTotals.E;
	if (n === 0 || sumRaw <= 0 || tierRawTotals.A <= 0) {
		return { weights, sumRaw, tiers, tierCounts, tierRawTotals, effectiveTierShares };
	}

	for (const tier of ["B", "C", "D", "E"] as const) {
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

export interface RewardSnapshotInput extends TieredWeightInput {
  penaltyMultiplier: number;
  rewardIds: readonly string[];
  normalizedWeight: number;
}

export interface AwardContribution {
  id: string;
  hotkey: string;
  netuid: number;
  bonusPercentage: number;
  appliedAt: Date;
  expiresAt: Date;
  revokedAt: Date | null;
}

/** Trusted services supply award records and evaluation time, independent of participant claims. */
export function recalculateSnapshotAfterRewardExpiry(
  snapshot: readonly RewardSnapshotInput[], awards: readonly AwardContribution[], netuid: number, evaluatedAt: Date,
) {
  const active = new Map(awards.filter((award) => award.netuid === netuid
    && award.appliedAt <= evaluatedAt && award.expiresAt > evaluatedAt
    && (award.revokedAt === null || award.revokedAt > evaluatedAt)).map((award) => [award.id, award]));
  let adjusted = false;
  const rows = snapshot.map((row) => {
    // Only captured IDs can contribute: newer grants never attach to historical work.
    const included = row.rewardIds.flatMap((id) => {
      const award = active.get(id);
      return award?.hotkey === row.hotkey ? [award] : [];
    });
    if (included.length !== row.rewardIds.length) adjusted = true;
    const reward = fraudReportReward(included.map((award) => award.bonusPercentage));
    return { ...row, rewardIds: included.map((award) => award.id), fraudReportReward: reward,
      rawScore: computeRawScore(row.verifiedUploadedMib, row.penaltyMultiplier, reward) };
  });
  const weights = adjusted ? normalizeWeightsWithEmissionTiers(rows).weights : snapshot.map((row) => row.normalizedWeight);
  return {
    source: adjusted ? "epoch_summary_reward_expiry_adjusted" : "epoch_summary",
    rewardEvaluatedAt: evaluatedAt,
    weights,
    uint16Weights: weights.map((weight) => Math.max(0, Math.min(65535, Math.floor(weight * 65535)))),
    rows: rows.map((row, index) => ({ ...row, normalizedWeight: weights[index]! })),
  };
}
