

import {
	computeRawScore,
	normalizeWeightsWithEmissionTiers,
	PRISM_WEIGHT_FORMULA_VERSION,
	type EmissionTier,
} from "./epoch-summary-math.js";

const MAX_UINT16 = 65_535;
const BLOCK_MS = 12_000;
const BLOCKS_PER_DAY = 7_200;
const MAX_BACKFILL_EPOCHS = 50;

export interface ChainEpochSnapshot {
	currentEpoch: number;
	currentBlock: number;
	blocksPerEpoch: number;
	lastSyncedAt: Date;
}

export interface EpochTaskWindow {
	epoch: number;
	start: Date;
	end: Date;
}

export interface EpochTaskCount {
	epoch: number;
	orchestratorId: string;
	completedTaskCount: number;
}

export interface QualifiedOrchestratorWeightInput {
	id: string;
	hotkey: string;
	uid: number | null;
	stakeTao: number;
	throughputScore: number;
	reliabilityScore: number;
	performanceScore: number;
	readinessActiveTimeMultiplier: number;
	penaltyMultiplier: number;
	taskDoneCount: number;
	verifiedUploadedMib: number;
}

export interface EpochSummaryRow extends QualifiedOrchestratorWeightInput {
	epoch: number;
	prismFinalScore: number;
	epochTaskDoneCount: number;
	rawWeight: number;
	normalizedWeight: number;
	uint16Weight: number;
	tier: EmissionTier;
}

export interface EpochSummaryTotals {
	epoch: number;
	sumRawWeight: number;
	sumTaskDoneCount: number;
	sumEpochTaskDoneCount: number;
	sumVerifiedUploadedMib: number;
	qualifiedOrchestratorCount: number;
	allWeightsZero: boolean;
	currentBlock: number;
	blocksPerEpoch: number;
	formulaVersion: string;
}

export interface EpochSummaryRepository {
	loadChainSnapshot(): Promise<ChainEpochSnapshot | null>;
	loadLastRecordedEpoch(): Promise<number | null>;
	loadQualifiedOrchestrators(): Promise<QualifiedOrchestratorWeightInput[]>;
	countCompletedProductionTasks(windows: readonly EpochTaskWindow[]): Promise<EpochTaskCount[]>;
	transaction<T>(operation: (store: EpochSummaryStore) => Promise<T>): Promise<T>;
}

export interface EpochSummaryStore {
	backfillEmptyTotals(epochs: readonly number[], chain: ChainEpochSnapshot): Promise<void>;
	upsertRows(rows: readonly EpochSummaryRow[]): Promise<void>;
	upsertTotals(totals: EpochSummaryTotals): Promise<void>;
	refreshStoredTaskCounts(epoch: number, counts: ReadonlyMap<string, number>): Promise<void>;
	pruneBeforeEpoch(firstRetainedEpoch: number): Promise<void>;
}

export interface EpochMaterializationResult {
	epoch: number;
	rows: EpochSummaryRow[];
	totals: EpochSummaryTotals;
	backfilledEpochs: number[];
	refreshedPreviousEpoch: boolean;
}

function toUint16Weight(normalizedWeight: number): number {
	return Math.max(0, Math.min(MAX_UINT16, Math.floor(normalizedWeight * MAX_UINT16)));
}

export function epochRetentionCount(blocksPerEpoch: number): number {
	return Math.ceil((30 * BLOCKS_PER_DAY) / Math.max(1, blocksPerEpoch));
}

function addMilliseconds(date: Date, milliseconds: number): Date {
	return new Date(date.getTime() + milliseconds);
}

export function buildEpochTaskWindows(
	chain: ChainEpochSnapshot,
	now: Date,
	includePrevious: boolean,
): EpochTaskWindow[] {
	const blocksPerEpoch = Math.max(1, chain.blocksPerEpoch);
	const epochStartBlock = chain.currentEpoch * blocksPerEpoch;
	const blocksSinceStart = Math.max(0, chain.currentBlock - epochStartBlock);
	const currentStart = addMilliseconds(chain.lastSyncedAt, -blocksSinceStart * BLOCK_MS);
	const projectedEnd = addMilliseconds(currentStart, blocksPerEpoch * BLOCK_MS);
	const currentEnd = projectedEnd.getTime() < now.getTime() ? projectedEnd : now;
	const windows: EpochTaskWindow[] = [];
	if (currentEnd.getTime() > currentStart.getTime()) {
		windows.push({ epoch: chain.currentEpoch, start: currentStart, end: currentEnd });
	}
	if (includePrevious && chain.currentEpoch > 0) {
		const previousStart = addMilliseconds(currentStart, -blocksPerEpoch * BLOCK_MS);
		windows.push({ epoch: chain.currentEpoch - 1, start: previousStart, end: currentStart });
	}
	return windows;
}

function taskCountMap(counts: readonly EpochTaskCount[], epoch: number): Map<string, number> {
	return new Map(
		counts
			.filter((count) => count.epoch === epoch)
			.map((count) => [count.orchestratorId, Math.max(0, count.completedTaskCount)]),
	);
}

export function buildEpochSummary(input: {
	epoch: number;
	currentBlock: number;
	blocksPerEpoch: number;
	orchestrators: readonly QualifiedOrchestratorWeightInput[];
	epochTaskCounts: ReadonlyMap<string, number>;
}): { rows: EpochSummaryRow[]; totals: EpochSummaryTotals } {
	const rawScores = input.orchestrators.map((orchestrator) =>
		computeRawScore(orchestrator.verifiedUploadedMib, orchestrator.penaltyMultiplier),
	);
	const prismFinalScores = input.orchestrators.map((orchestrator) =>
		Math.max(
			0,
			orchestrator.performanceScore
				* orchestrator.readinessActiveTimeMultiplier
				* orchestrator.penaltyMultiplier,
		),
	);
	const normalized = normalizeWeightsWithEmissionTiers(
		input.orchestrators.map((orchestrator, index) => ({
			uid: orchestrator.uid,
			hotkey: orchestrator.hotkey,
			prismFinalScore: prismFinalScores[index] ?? 0,
			verifiedUploadedMib: orchestrator.verifiedUploadedMib,
			rawScore: rawScores[index] ?? 0,
		})),
	);
	const rows = input.orchestrators.map((orchestrator, index): EpochSummaryRow => ({
		...orchestrator,
		epoch: input.epoch,
		prismFinalScore: prismFinalScores[index] ?? 0,
		epochTaskDoneCount: input.epochTaskCounts.get(orchestrator.id) ?? 0,
		rawWeight: rawScores[index] ?? 0,
		normalizedWeight: normalized.weights[index] ?? 0,
		uint16Weight: toUint16Weight(normalized.weights[index] ?? 0),
		tier: normalized.tiers[index] ?? "E",
	}));
	return {
		rows,
		totals: {
			epoch: input.epoch,
			sumRawWeight: normalized.sumRaw,
			sumTaskDoneCount: rows.reduce((sum, row) => sum + row.taskDoneCount, 0),
			sumEpochTaskDoneCount: rows.reduce((sum, row) => sum + row.epochTaskDoneCount, 0),
			sumVerifiedUploadedMib: rows.reduce((sum, row) => sum + row.verifiedUploadedMib, 0),
			qualifiedOrchestratorCount: rows.length,
			allWeightsZero: normalized.sumRaw === 0,
			currentBlock: input.currentBlock,
			blocksPerEpoch: input.blocksPerEpoch,
			formulaVersion: PRISM_WEIGHT_FORMULA_VERSION,
		},
	};
}

export async function materializeCurrentEpoch(
	repository: EpochSummaryRepository,
	now = new Date(),
): Promise<EpochMaterializationResult | null> {
	const chain = await repository.loadChainSnapshot();
	if (chain === null) return null;
	const lastRecordedEpoch = await repository.loadLastRecordedEpoch();
	const refreshPrevious = chain.currentEpoch > 0
		&& (lastRecordedEpoch === null || lastRecordedEpoch < chain.currentEpoch);
	const windows = buildEpochTaskWindows(chain, now, refreshPrevious);
	const [orchestrators, counts] = await Promise.all([
		repository.loadQualifiedOrchestrators(),
		repository.countCompletedProductionTasks(windows),
	]);
	const currentCounts = taskCountMap(counts, chain.currentEpoch);
	const previousCounts = taskCountMap(counts, chain.currentEpoch - 1);
	const backfillStart = lastRecordedEpoch === null
		? chain.currentEpoch
		: Math.max(lastRecordedEpoch + 1, chain.currentEpoch - MAX_BACKFILL_EPOCHS);
	const backfilledEpochs: number[] = [];
	for (let epoch = backfillStart; epoch < chain.currentEpoch; epoch += 1) {
		backfilledEpochs.push(epoch);
	}
	const summary = buildEpochSummary({
		epoch: chain.currentEpoch,
		currentBlock: chain.currentBlock,
		blocksPerEpoch: chain.blocksPerEpoch,
		orchestrators,
		epochTaskCounts: currentCounts,
	});

	await repository.transaction(async (store) => {
		if (backfilledEpochs.length) await store.backfillEmptyTotals(backfilledEpochs, chain);
		await store.upsertRows(summary.rows);
		await store.upsertTotals(summary.totals);
		if (refreshPrevious) {
			await store.refreshStoredTaskCounts(chain.currentEpoch - 1, previousCounts);
		}
		const firstRetainedEpoch = Math.max(
			0,
			chain.currentEpoch - epochRetentionCount(chain.blocksPerEpoch),
		);
		await store.pruneBeforeEpoch(firstRetainedEpoch);
	});

	return {
		epoch: chain.currentEpoch,
		rows: summary.rows,
		totals: summary.totals,
		backfilledEpochs,
		refreshedPreviousEpoch: refreshPrevious,
	};
}
