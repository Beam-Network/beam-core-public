/**
 * PRISM score refresh job: aggregates recent proofs, tasks, and penalties, runs `computePrismScore`,
 * and persists results to `core.beam_prism_metrics` and `core.orchestrators`.
 *
 * This transparency copy is self-contained (database client and job types are declared below).
 */

import {
	computePrismScore,
	percentile,
	type PrismPenaltySnapshot,
	type PrismProofSample,
	type PrismTaskSample,
} from "../prism/scoring.js";

export type DbClient = <T = unknown>(
	strings: TemplateStringsArray,
	...values: unknown[]
) => Promise<T>;

export interface PrismScoreUpdaterContext {
	db: DbClient;
}

export type PrismScoreUpdaterJob = (ctx: PrismScoreUpdaterContext) => Promise<void>;

const LOOKBACK_WINDOW = "14 days";
const PENALTY_LOOKBACK_WINDOW = "30 days";
const MATERIALIZE_BATCH_SIZE = 200;

interface OrchestratorRow {
	id: string;
	hotkey: string;
	ready: boolean;
	registeredAt: Date;
	connectedWorkers: number;
}

interface ProofRow {
	orchestratorId: string;
	transferId: string | null;
	bandwidthMbps: string;
	eventAt: Date;
}

interface TaskRow {
	orchestratorId: string;
	state: "completed" | "failed";
	failureReason: string | null;
	eventAt: Date;
}

interface PaymentPenaltyRow {
	orchestratorId: string;
	failedPopPayments: number;
}

interface FraudPenaltyRow {
	orchestratorId: string;
	fraudPenaltyEvents: number;
}

interface SybilPenaltyRow {
	orchestratorId: string;
	sybilViolationEvents: number;
}

interface GuardrailPenaltyRow {
	orchestratorId: string;
	guardrailInterventionEvents: number;
}

interface PreviousMetricRow {
	orchestratorId: string;
	prismFinalScore: string;
	confidenceScore: string;
	throughputScore: string;
	reliabilityScore: string;
	performanceScore: string;
	readinessMultiplier: string;
	penaltyMultiplier: string;
}

interface PreviousMetricValues {
	prism_final_score: number;
	confidence_score: number;
	throughput_score: number;
	reliability_score: number;
	performance_score: number;
	readiness_multiplier: number;
	penalty_multiplier: number;
}

type Trend = "up" | "down" | "flat";
type ScoreTrends = Record<keyof PreviousMetricValues, Trend>;

/** Match persisted NUMERIC(6,5): compare rounded values so trends align with DB storage and UI. */
function roundPrismMetric(n: number): number {
	return Math.round(n * 1e5) / 1e5;
}

function computeTrend(previous: number | undefined, next: number): Trend {
	if (previous == null) return "flat";
	const p = roundPrismMetric(Number(previous));
	const n = roundPrismMetric(Number(next));
	if (p === n) return "flat";
	return n > p ? "up" : "down";
}

function numberValue(value: string | number | null | undefined): number {
	if (typeof value === "number") return Number.isFinite(value) ? value : 0;
	if (typeof value === "string") {
		const parsed = Number(value);
		return Number.isFinite(parsed) ? parsed : 0;
	}
	return 0;
}

async function materializePopPenaltyEvents(args: {
	db: DbClient;
	currentEpoch?: number | null;
}): Promise<number> {
	const { db } = args;
	const currentEpoch =
		args.currentEpoch ??
		(
			await db<{ currentEpoch: number | null }[]>`
				SELECT current_epoch AS "currentEpoch"
				FROM core.chain_state
				ORDER BY last_synced_at DESC
				LIMIT 1
			`
		)[0]?.currentEpoch ??
		null;

	if (currentEpoch == null || currentEpoch <= 0) return 0;

	const insertedRows = await db<{ workerPaymentId: string }[]>`
		WITH pending_events AS (
			SELECT
				wp.id AS "workerPaymentId",
				wp.orchestrator_id AS "orchestratorId",
				wp.worker_id AS "workerId",
				wp.task_id AS "taskId",
				wp.tx_hash AS "txHash",
				wp.epoch AS "paymentEpoch",
				wp.created_at AS "paymentRecordedAt",
				COALESCE(wp.pop_verified_at, wp.updated_at, wp.created_at) AS "resolvedAt",
				wp.pop_error AS "verifierError"
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
			LIMIT ${MATERIALIZE_BATCH_SIZE}
		)
		INSERT INTO core.pop_penalty_events
			(worker_payment_id, orchestrator_id, worker_id, task_id, tx_hash,
			 payment_epoch, payment_recorded_at, resolved_at, applied_in_epoch, verifier_error)
		SELECT
			"workerPaymentId",
			"orchestratorId",
			"workerId",
			"taskId",
			"txHash",
			"paymentEpoch",
			"paymentRecordedAt",
			"resolvedAt",
			${currentEpoch},
			"verifierError"
		FROM pending_events
		ON CONFLICT (worker_payment_id) DO NOTHING
		RETURNING worker_payment_id AS "workerPaymentId"
	`;

	return insertedRows.length;
}

export const prismScoreUpdater: PrismScoreUpdaterJob = async ({ db }) => {
	const now = new Date();
	const chainRows = await db<{ currentEpoch: number | null }[]>`
		SELECT current_epoch AS "currentEpoch"
		FROM core.chain_state
		ORDER BY last_synced_at DESC
		LIMIT 1
	`;
	const currentEpoch = chainRows[0]?.currentEpoch ?? 0;

	await materializePopPenaltyEvents({ db, currentEpoch });

	const previousMetrics = await db<PreviousMetricRow[]>`
		SELECT
			orchestrator_id        AS "orchestratorId",
			prism_final_score      AS "prismFinalScore",
			confidence_score       AS "confidenceScore",
			throughput_score       AS "throughputScore",
			reliability_score      AS "reliabilityScore",
			performance_score      AS "performanceScore",
			readiness_multiplier   AS "readinessMultiplier",
			penalty_multiplier     AS "penaltyMultiplier"
		FROM core.beam_prism_metrics
	`;

	const previousByOrchestrator = new Map<string, PreviousMetricValues>(
		previousMetrics.map((row) => [
			row.orchestratorId,
			{
				prism_final_score: numberValue(row.prismFinalScore),
				confidence_score: numberValue(row.confidenceScore),
				throughput_score: numberValue(row.throughputScore),
				reliability_score: numberValue(row.reliabilityScore),
				performance_score: numberValue(row.performanceScore),
				readiness_multiplier: numberValue(row.readinessMultiplier),
				penalty_multiplier: numberValue(row.penaltyMultiplier),
			},
		]),
	);

	const [orchestrators, proofs, tasks, paymentPenalties, fraudPenalties, sybilPenalties, guardrailPenalties] =
		await Promise.all([
			db<OrchestratorRow[]>`
        SELECT
          o.id,
          o.hotkey,
          o.ready,
          o.registered_at AS "registeredAt",
          (
            SELECT COUNT(*)::INT
            FROM core.worker_sessions ws
            JOIN core.workers w ON w.worker_id = ws.worker_id
            WHERE w.status IN ('registered', 'active')
              AND NOT EXISTS (
                SELECT 1 FROM core.worker_orchestrator_exclusions e
                WHERE e.worker_id = w.worker_id
                  AND e.orchestrator_id = o.id
              )
          ) AS "connectedWorkers"
        FROM core.orchestrators o
      `,
			db<ProofRow[]>`
        SELECT
          p.orchestrator_id AS "orchestratorId",
          t.transfer_id     AS "transferId",
          COALESCE(p.bandwidth_mbps, 0)::TEXT AS "bandwidthMbps",
          COALESCE(p.verified_at, p.published_at) AS "eventAt"
        FROM core.proofs_of_bandwidth p
        LEFT JOIN core.tasks t ON t.id = p.task_id
        WHERE COALESCE(p.verified_at, p.published_at) > NOW() - ${LOOKBACK_WINDOW}::INTERVAL
          AND (p.verification_passed = TRUE OR p.status = 'verified')
      `,
			db<TaskRow[]>`
        SELECT
          orchestrator_id AS "orchestratorId",
          state,
          failure_reason AS "failureReason",
          COALESCE(completed_at, failed_at, created_at) AS "eventAt"
        FROM core.tasks
        WHERE COALESCE(completed_at, failed_at, created_at) > NOW() - ${LOOKBACK_WINDOW}::INTERVAL
          AND state IN ('completed', 'failed')
        UNION ALL
        SELECT
          orchestrator_id AS "orchestratorId",
          'failed' AS state,
          CONCAT('beamcore_guardrail_reassign:', COALESCE(reason, outcome)) AS "failureReason",
          intervened_at AS "eventAt"
        FROM core.orchestrator_guardrail_interventions
        WHERE intervened_at > NOW() - ${LOOKBACK_WINDOW}::INTERVAL
			UNION ALL
			SELECT
			  orchestrator_id AS "orchestratorId",
			  'failed' AS state,
			  CONCAT('assignment_failure:', COALESCE(failure_type, 'unknown'), ':', COALESCE(reason, 'unknown')) AS "failureReason",
			  created_at AS "eventAt"
			FROM core.orchestrator_assignment_failures
			WHERE created_at > NOW() - ${LOOKBACK_WINDOW}::INTERVAL
      `,
			db<PaymentPenaltyRow[]>`
        SELECT
          orchestrator_id AS "orchestratorId",
					COUNT(*)::INT AS "failedPopPayments"
        FROM core.pop_penalty_events
        WHERE applied_in_epoch <= ${currentEpoch}
          AND resolved_at > NOW() - ${PENALTY_LOOKBACK_WINDOW}::INTERVAL
        GROUP BY orchestrator_id
      `,
			db<FraudPenaltyRow[]>`
        SELECT
          orchestrator_id AS "orchestratorId",
          COUNT(*)::INT AS "fraudPenaltyEvents"
        FROM core.fraud_penalties
        WHERE orchestrator_id IS NOT NULL
          AND applied_at > NOW() - ${PENALTY_LOOKBACK_WINDOW}::INTERVAL
        GROUP BY orchestrator_id
      `,
			db<SybilPenaltyRow[]>`
        SELECT
          t.orchestrator_id AS "orchestratorId",
          COUNT(DISTINCT sv.id)::INT AS "sybilViolationEvents"
        FROM core.sybil_violations sv
        JOIN core.tasks t ON t.assigned_worker_id = sv.worker_id
        WHERE sv.resolved_at IS NULL
          AND t.created_at > NOW() - ${PENALTY_LOOKBACK_WINDOW}::INTERVAL
        GROUP BY t.orchestrator_id
      `,
			db<GuardrailPenaltyRow[]>`
        SELECT
          orchestrator_id AS "orchestratorId",
          COUNT(*)::INT AS "guardrailInterventionEvents"
        FROM core.orchestrator_guardrail_interventions
        WHERE intervened_at > NOW() - ${PENALTY_LOOKBACK_WINDOW}::INTERVAL
        GROUP BY orchestrator_id
      `,
		]);

	const proofsByOrchestrator = new Map<string, PrismProofSample[]>();
	const tasksByOrchestrator = new Map<string, PrismTaskSample[]>();
	const penaltyByOrchestrator = new Map<string, PrismPenaltySnapshot>();
	const averageMbpsByOrchestrator = new Map<string, number>();

	for (const proof of proofs) {
		const sample: PrismProofSample = {
			bandwidthMbps: numberValue(proof.bandwidthMbps),
			transferId: proof.transferId,
			eventAt: proof.eventAt,
		};
		const existing = proofsByOrchestrator.get(proof.orchestratorId) ?? [];
		existing.push(sample);
		proofsByOrchestrator.set(proof.orchestratorId, existing);
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
			failureReason: task.failureReason,
			eventAt: task.eventAt,
		};
		const existing = tasksByOrchestrator.get(task.orchestratorId) ?? [];
		existing.push(sample);
		tasksByOrchestrator.set(task.orchestratorId, existing);
	}

	for (const row of paymentPenalties) {
		penaltyByOrchestrator.set(row.orchestratorId, {
			failedPopPayments: row.failedPopPayments,
			fraudPenaltyEvents: 0,
			sybilViolationEvents: 0,
			guardrailInterventionEvents: 0,
		});
	}

	for (const row of fraudPenalties) {
		const existing = penaltyByOrchestrator.get(row.orchestratorId) ?? {
			failedPopPayments: 0,
			fraudPenaltyEvents: 0,
			sybilViolationEvents: 0,
			guardrailInterventionEvents: 0,
		};
		existing.fraudPenaltyEvents = row.fraudPenaltyEvents;
		penaltyByOrchestrator.set(row.orchestratorId, existing);
	}

	for (const row of sybilPenalties) {
		const existing = penaltyByOrchestrator.get(row.orchestratorId) ?? {
			failedPopPayments: 0,
			fraudPenaltyEvents: 0,
			sybilViolationEvents: 0,
			guardrailInterventionEvents: 0,
		};
		existing.sybilViolationEvents = row.sybilViolationEvents;
		penaltyByOrchestrator.set(row.orchestratorId, existing);
	}

	for (const row of guardrailPenalties) {
		const existing = penaltyByOrchestrator.get(row.orchestratorId) ?? {
			failedPopPayments: 0,
			fraudPenaltyEvents: 0,
			sybilViolationEvents: 0,
			guardrailInterventionEvents: 0,
		};
		existing.guardrailInterventionEvents = row.guardrailInterventionEvents;
		penaltyByOrchestrator.set(row.orchestratorId, existing);
	}

	const throughputValues = [...averageMbpsByOrchestrator.values()].filter((value) => value > 0);
	const throughputBounds = {
		p25: percentile(throughputValues, 0.25),
		p90: percentile(throughputValues, 0.9),
	};

	for (const orchestrator of orchestrators) {
		const scoreInput = {
			averageVerifiedMbps: averageMbpsByOrchestrator.get(orchestrator.id) ?? 0,
			throughputBounds,
			proofs: proofsByOrchestrator.get(orchestrator.id) ?? [],
			tasks: tasksByOrchestrator.get(orchestrator.id) ?? [],
			penalties: penaltyByOrchestrator.get(orchestrator.id) ?? {
				failedPopPayments: 0,
				fraudPenaltyEvents: 0,
				sybilViolationEvents: 0,
				guardrailInterventionEvents: 0,
			},
			readiness: {
				ready: orchestrator.ready,
				connectedWorkers: orchestrator.connectedWorkers,
			},
			registeredAt: orchestrator.registeredAt,
			now,
		};
		const score = computePrismScore(scoreInput);

		const previous = previousByOrchestrator.get(orchestrator.id);
		const scoreTrends: ScoreTrends = {
			prism_final_score: computeTrend(previous?.prism_final_score, score.prismFinalScore),
			confidence_score: computeTrend(previous?.confidence_score, score.confidenceScore),
			throughput_score: computeTrend(previous?.throughput_score, score.throughputScore),
			reliability_score: computeTrend(previous?.reliability_score, score.reliabilityScore),
			performance_score: computeTrend(previous?.performance_score, score.performanceScore),
			readiness_multiplier: computeTrend(previous?.readiness_multiplier, score.readinessMultiplier),
			penalty_multiplier: computeTrend(previous?.penalty_multiplier, score.penaltyMultiplier),
		};

		await db`
      INSERT INTO core.beam_prism_metrics
        (orchestrator_id, prism_pool, confidence_score,
         verified_transfer_count, verified_chunk_count, verified_bandwidth_mbps,
         throughput_score, reliability_score, performance_score,
         readiness_multiplier, penalty_multiplier,
         prism_final_score, score_components, score_trends, computed_at, updated_at)
      VALUES
        (${orchestrator.id}, ${score.prismPool}, ${score.confidenceScore},
         ${score.verifiedTransferCount}, ${score.verifiedChunkCount}, ${score.verifiedBandwidthMbps},
         ${score.throughputScore}, ${score.reliabilityScore}, ${score.performanceScore},
         ${score.readinessMultiplier}, ${score.penaltyMultiplier},
         ${score.prismFinalScore}, ${JSON.stringify(score.scoreComponents)}, ${JSON.stringify(scoreTrends)}, NOW(), NOW())
      ON CONFLICT (orchestrator_id) DO UPDATE
        SET prism_pool              = EXCLUDED.prism_pool,
            confidence_score        = EXCLUDED.confidence_score,
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
            score_trends            = EXCLUDED.score_trends,
            computed_at             = NOW(),
            updated_at              = NOW()
    `;

		await db`
      UPDATE core.orchestrators
      SET prism_final_score            = ${score.prismFinalScore},
          prism_updated_at             = CASE WHEN prism_updated_at > NOW() THEN prism_updated_at ELSE NOW() END,
          prism_confidence_score       = ${score.confidenceScore},
          prism_pool                   = CASE
                                           WHEN prism_updated_at > NOW()
                                           THEN prism_pool
                                           ELSE ${score.prismPool}
                                         END,
          success_rate                 = ${score.successRate},
          updated_at                   = NOW()
      WHERE id = ${orchestrator.id}
    `;
	}
};
