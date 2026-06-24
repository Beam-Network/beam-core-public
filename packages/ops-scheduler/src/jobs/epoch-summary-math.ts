/**
 * Pure helpers for final epoch summary weights.
 *
 * raw_i = prism_final_score_i x task_done_count_i
 * normalized weights sum to 1 when at least one raw score is positive.
 */

export function computeRawScore(prismFinalScore: number, taskDoneCount: number): number {
	const p = Number.isFinite(prismFinalScore) ? prismFinalScore : 0;
	const t = Number.isFinite(taskDoneCount) ? taskDoneCount : 0;
	return p * t;
}

/**
 * Maps raw scores to normalized weights. When sum(raw) > 0, first n-1 weights are raw/sumRaw,
 * the last is 1 - acc so the total is exactly 1 (floating-point safe).
 */
export function normalizeWeightsFromRawScores(rawScores: number[]): {
	weights: number[];
	sumRaw: number;
} {
	const n = rawScores.length;
	const sumRaw = rawScores.reduce((s, r) => s + r, 0);
	if (n === 0 || sumRaw <= 0) {
		return { weights: rawScores.map(() => 0), sumRaw };
	}
	const weights = new Array<number>(n).fill(0);
	let acc = 0;
	for (let i = 0; i < n - 1; i++) {
		const w = rawScores[i]! / sumRaw;
		weights[i] = w;
		acc += w;
	}
	weights[n - 1] = Math.max(0, 1 - acc);
	return { weights, sumRaw };
}

export function sumWeights(weights: number[]): number {
	return weights.reduce((s, w) => s + w, 0);
}