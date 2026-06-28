/**
 * Final validator weight materialization job.
 *
 * Public transparency copy of BeamCore's epoch-summary scheduler job. It converts
 * qualified PRISM scores plus completed production task counts into the
 * materialized UID/weight vector validators read from BeamCore.
 */

import { computeRawScore, normalizeWeightsWithEmissionTiers, PRISM_WEIGHT_FORMULA_VERSION } from "./epoch-summary-math.js";

export type SqlClient = (<T = unknown>(
	strings: TemplateStringsArray,
	...values: unknown[]
) => Promise<T>) & {
	json: (value: unknown) => unknown;
};

export type DbClient = SqlClient & {
	begin<T>(fn: (sql: SqlClient) => Promise<T>): Promise<T>;
};

export interface EpochSummaryContext {
	db: DbClient;
}

export type EpochSummaryJob = (ctx: EpochSummaryContext) => Promise<void>;

const logger = {
	info(_data: unknown, _message?: string): void {},
};

const PUBLIC_DEFAULT_METAGRAPH_NETUID = 105;
const MAX_UINT16 = 65_535;
const BLOCKS_PER_SECOND = 1 / 12;
const SECONDS_PER_DAY = 86_400;
const BLOCKS_PER_DAY = BLOCKS_PER_SECOND * SECONDS_PER_DAY; // 7200

function envNumber(name: string, fallback: number): number {
	const parsed = Number(process.env[name]);
	return Number.isFinite(parsed) ? parsed : fallback;
}

const env = {
	METAGRAPH_NETUID: envNumber("METAGRAPH_NETUID", PUBLIC_DEFAULT_METAGRAPH_NETUID),
};

function toUint16Weight(normalizedWeight: number): number {
	return Math.max(0, Math.min(MAX_UINT16, Math.floor(normalizedWeight * MAX_UINT16)));
}

function epochRetentionCount(blocksPerEpoch: number): number {
	return Math.ceil((30 * BLOCKS_PER_DAY) / Math.max(1, blocksPerEpoch));
}

interface OrchRow {
	id: string;
	hotkey: string;
	uid: number | null;
	stake_tao: string;
	prism_final_score: string;
	prism_pool: string | null;
	throughput_score: string;
	reliability_score: string;
	performance_score: string;
	penalty_multiplier: string;
	task_done_count: string;
}

function estimateEpochStartedAt(chain: { current_block: string; blocks_per_epoch: number; last_synced_at: Date }): Date {
	const currentBlock = Number(chain.current_block);
	const blocksPerEpoch = Math.max(1, Number(chain.blocks_per_epoch || 1));
	const blockOffset = Number.isFinite(currentBlock) ? currentBlock % blocksPerEpoch : 0;
	return new Date(chain.last_synced_at.getTime() - blockOffset * 12_000);
}

export const epochSummary: EpochSummaryJob = async ({ db }) => {
	const chainRows = await db<
		{ current_epoch: number | null; current_block: string; blocks_per_epoch: number; last_synced_at: Date }[]
	>`
		SELECT
			current_epoch,
			current_block::text AS current_block,
			blocks_per_epoch,
			last_synced_at
		FROM core.chain_state
		WHERE netuid = ${env.METAGRAPH_NETUID}
		ORDER BY last_synced_at DESC
		LIMIT 1
	`;
	const chain = chainRows[0];
	if (!chain || chain.current_epoch == null) return;
	const epoch = chain.current_epoch;
	const epochStartedAt = estimateEpochStartedAt(chain);

	// Backfill any epochs skipped during downtime/restarts with empty-weight rows.
	const lastEpochRows = await db<{ last_epoch: number | null }[]>`
		SELECT MAX(epoch) AS last_epoch
		FROM core.epoch_summary_totals
		WHERE netuid = ${env.METAGRAPH_NETUID}
	`;
	const lastRecordedEpoch = lastEpochRows[0]?.last_epoch ?? null;
	if (lastRecordedEpoch != null && epoch > lastRecordedEpoch + 1) {
		const MAX_BACKFILL = 50;
		const startEpoch = Math.max(lastRecordedEpoch + 1, epoch - MAX_BACKFILL);
		for (let e = startEpoch; e < epoch; e++) {
			await db`
				INSERT INTO core.epoch_summary_totals
					(netuid, epoch, sum_raw_weight, sum_task_done_count, qualified_orchestrator_count,
					 all_weights_zero, current_block, blocks_per_epoch,
					 formula_version, source, created_at, updated_at)
				VALUES
					(${env.METAGRAPH_NETUID}, ${e}, 0, 0, 0,
					 TRUE, ${Number(chain.current_block)}, ${chain.blocks_per_epoch},
					 ${PRISM_WEIGHT_FORMULA_VERSION}, 'epoch_summary_backfill', NOW(), NOW())
				ON CONFLICT (netuid, epoch) DO NOTHING
			`;
		}
		logger.info(
			{ backfilled: epoch - startEpoch, from: startEpoch, to: epoch - 1 },
			"backfilled missing epoch summary totals",
		);
	}

	const rows = await db<OrchRow[]>`
		SELECT
			o.id,
			o.hotkey,
			o.uid,
			COALESCE(ns.stake, 0)::text AS stake_tao,
			COALESCE(pmq.prism_final_score, o.prism_final_score, 0)::text AS prism_final_score,
			o.prism_pool::text AS prism_pool,
			COALESCE(pmq.throughput_score,   0)::text AS throughput_score,
			COALESCE(pmq.reliability_score,  0)::text AS reliability_score,
			COALESCE(pmq.performance_score,  0)::text AS performance_score,
			COALESCE(pmq.penalty_multiplier, 1)::text AS penalty_multiplier,
			COALESCE(epoch_tasks.task_done_count, 0)::text AS task_done_count
		FROM core.orchestrators o
		INNER JOIN core.prism_metrics_qualified pmq ON pmq.orchestrator_id = o.id
		LEFT JOIN core.neuron_state ns ON ns.netuid = ${env.METAGRAPH_NETUID} AND ns.hotkey = o.hotkey
		LEFT JOIN (
			SELECT
				t.orchestrator_id,
				COUNT(DISTINCT t.id) AS task_done_count
			FROM core.tasks t
			WHERE t.state = 'completed'
			  AND t.completed_at IS NOT NULL
			  AND t.completed_at >= ${epochStartedAt}
			  AND t.test_mode = FALSE
			GROUP BY t.orchestrator_id
		) epoch_tasks ON epoch_tasks.orchestrator_id = o.id
		WHERE o.uid IS NOT NULL
		  AND o.prism_pool = 'qualified'
		ORDER BY o.uid ASC, o.hotkey ASC
	`;

	const rawScores = rows.map((r) => {
		const prism = Number(r.prism_final_score);
		const taskDone = Number(r.task_done_count);
		const raw = computeRawScore(prism, taskDone);
		return { row: r, prism, taskDone, raw };
	});

	const tierInputs = rawScores.map(({ row, prism, taskDone, raw }) => ({
		uid: row.uid,
		hotkey: row.hotkey,
		prismFinalScore: prism,
		taskDoneCount: taskDone,
		rawScore: raw,
	}));
	const { weights: normalizedList, sumRaw } = normalizeWeightsWithEmissionTiers(tierInputs);

	const orchestrators = rawScores.map(({ row, prism, taskDone, raw }, idx) => ({
		id: row.id,
		hotkey: row.hotkey,
		uid: row.uid,
		stake_tao: Number(row.stake_tao),
		prism_final_score: prism,
		prism_pool: row.prism_pool,
		throughput_score: Number(row.throughput_score),
		reliability_score: Number(row.reliability_score),
		performance_score: Number(row.performance_score),
		penalty_multiplier: Number(row.penalty_multiplier),
		task_done_count: taskDone,
		raw_weight: raw,
		normalized_weight: normalizedList[idx] ?? 0,
		uint16_weight: toUint16Weight(normalizedList[idx] ?? 0),
	}));

	const sumTaskDone = orchestrators.reduce((s, o) => s + o.task_done_count, 0);
	const allWeightsZero = sumRaw === 0;
	const retentionCount = epochRetentionCount(chain.blocks_per_epoch);

	await db.begin(async (sql) => {
		// Upsert per-orchestrator rows for the current epoch.
		if (orchestrators.length > 0) {
			await sql`
				INSERT INTO core.epoch_summary
					(netuid, epoch, orchestrator_id, orchestrator_uid, orchestrator_hotkey,
					 stake_tao, prism_final_score, task_done_count,
					 raw_weight, normalized_weight, uint16_weight,
					 throughput_score, reliability_score, performance_score, penalty_multiplier,
					 prism_pool, formula_version, source, created_at, updated_at)
				SELECT
					${env.METAGRAPH_NETUID},
					${epoch},
					x.id::UUID,
					x.uid::INT,
					x.hotkey,
					x.stake_tao::NUMERIC(20,6),
					x.prism_final_score::NUMERIC(6,5),
					x.task_done_count::INT,
					x.raw_weight::NUMERIC(12,6),
					x.normalized_weight::NUMERIC(12,10),
					x.uint16_weight::INT,
					x.throughput_score::NUMERIC(6,5),
					x.reliability_score::NUMERIC(6,5),
					x.performance_score::NUMERIC(6,5),
					x.penalty_multiplier::NUMERIC(6,5),
					x.prism_pool,
					${PRISM_WEIGHT_FORMULA_VERSION},
					'epoch_summary',
					NOW(),
					NOW()
				FROM jsonb_to_recordset(${sql.json(orchestrators as never)}) AS x(
					id TEXT, hotkey TEXT, uid INT,
					stake_tao FLOAT8, prism_final_score FLOAT8, task_done_count INT,
					raw_weight FLOAT8, normalized_weight FLOAT8, uint16_weight INT,
					throughput_score FLOAT8, reliability_score FLOAT8,
					performance_score FLOAT8, penalty_multiplier FLOAT8,
					prism_pool TEXT
				)
				ON CONFLICT (netuid, epoch, orchestrator_id) DO UPDATE SET
					orchestrator_uid    = EXCLUDED.orchestrator_uid,
					orchestrator_hotkey = EXCLUDED.orchestrator_hotkey,
					stake_tao           = EXCLUDED.stake_tao,
					prism_final_score   = EXCLUDED.prism_final_score,
					task_done_count     = EXCLUDED.task_done_count,
					raw_weight          = EXCLUDED.raw_weight,
					normalized_weight   = EXCLUDED.normalized_weight,
					uint16_weight       = EXCLUDED.uint16_weight,
					throughput_score    = EXCLUDED.throughput_score,
					reliability_score   = EXCLUDED.reliability_score,
					performance_score   = EXCLUDED.performance_score,
					penalty_multiplier  = EXCLUDED.penalty_multiplier,
					prism_pool          = EXCLUDED.prism_pool,
					formula_version     = EXCLUDED.formula_version,
					source              = EXCLUDED.source,
					updated_at          = NOW()
			`;
		}

		// Upsert epoch totals.
		await sql`
			INSERT INTO core.epoch_summary_totals
				(netuid, epoch, sum_raw_weight, sum_task_done_count, qualified_orchestrator_count,
				 all_weights_zero, current_block, blocks_per_epoch,
				 formula_version, source, created_at, updated_at)
			VALUES
				(${env.METAGRAPH_NETUID}, ${epoch},
				 ${sumRaw}, ${sumTaskDone}, ${orchestrators.length},
				 ${allWeightsZero}, ${Number(chain.current_block)}, ${chain.blocks_per_epoch},
				 ${PRISM_WEIGHT_FORMULA_VERSION}, 'epoch_summary', NOW(), NOW())
			ON CONFLICT (netuid, epoch) DO UPDATE SET
				sum_raw_weight               = EXCLUDED.sum_raw_weight,
				sum_task_done_count          = EXCLUDED.sum_task_done_count,
				qualified_orchestrator_count = EXCLUDED.qualified_orchestrator_count,
				all_weights_zero             = EXCLUDED.all_weights_zero,
				current_block                = EXCLUDED.current_block,
				blocks_per_epoch             = EXCLUDED.blocks_per_epoch,
				formula_version              = EXCLUDED.formula_version,
				source                       = EXCLUDED.source,
				updated_at                   = NOW()
		`;

		// Retention: remove epochs older than about 30 days.
		await sql`
			DELETE FROM core.epoch_summary
			WHERE netuid = ${env.METAGRAPH_NETUID}
			  AND epoch < ${epoch - retentionCount}
		`;
		await sql`
			DELETE FROM core.epoch_summary_totals
			WHERE netuid = ${env.METAGRAPH_NETUID}
			  AND epoch < ${epoch - retentionCount}
		`;
	});

	logger.info(
		{ epoch, orch_count: orchestrators.length, sum_raw: sumRaw, all_weights_zero: allWeightsZero },
		"epoch summary updated",
	);
};