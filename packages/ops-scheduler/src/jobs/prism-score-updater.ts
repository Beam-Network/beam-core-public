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
	registered_at: Date;
	connected_workers: number;
}

interface ProofRow {
	orchestrator_id: string;
	transfer_id: string | null;
	bandwidth_mbps: string;
	event_at: Date;
}

interface TaskRow {
	orchestrator_id: string;
	state: "completed" | "failed";
	failure_reason: string | null;
	event_at: Date;
}

interface PaymentPenaltyRow {
	orchestrator_id: string;
	failed_pop_payments: number;
}

interface FraudPenaltyRow {
	orchestrator_id: string;
	fraud_penalty_events: number;
}

interface SybilPenaltyRow {
	orchestrator_id: string;
	sybil_violation_events: number;
}

interface GuardrailPenaltyRow {
	orchestrator_id: string;
	guardrail_intervention_events: number;
}

interface PreviousMetricRow {
	orchestrator_id: string;
	prism_final_score: string;
	confidence_score: string;
	throughput_score: string;
	reliability_score: string;
	performance_score: string;
	readiness_multiplier: string;
	penalty_multiplier: string;
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
			await db<{ current_epoch: number | null }[]>`
				SELECT current_epoch
				FROM core.chain_state
				ORDER BY last_synced_at DESC
				LIMIT 1
			`
		)[0]?.current_epoch ??
		null;

	if (currentEpoch == null || currentEpoch <= 0) return 0;

	const insertedRows = await db<{ worker_payment_id: string }[]>`
		WITH pending_events AS (
			SELECT
				wp.id AS worker_payment_id,
				wp.orchestrator_id AS orchestrator_id,
				wp.worker_id AS worker_id,
				wp.task_id AS task_id,
				wp.tx_hash AS tx_hash,
				wp.epoch AS payment_epoch,
				wp.created_at AS payment_recorded_at,
				COALESCE(wp.pop_verified_at, wp.updated_at, wp.created_at) AS resolved_at,
				wp.pop_error AS verifier_error
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
			worker_payment_id,
			orchestrator_id,
			worker_id,
			task_id,
			tx_hash,
			payment_epoch,
			payment_recorded_at,
			resolved_at,
			${currentEpoch},
			verifier_error
		FROM pending_events
		ON CONFLICT (worker_payment_id) DO NOTHING
		RETURNING worker_payment_id
	`;

	return insertedRows.length;
}

export const prismScoreUpdater: PrismScoreUpdaterJob = async ({ db }) => {
	const now = new Date();
	const chainRows = await db<{ current_epoch: number | null }[]>`
		SELECT current_epoch
		FROM core.chain_state
		ORDER BY last_synced_at DESC
		LIMIT 1
	`;
	const currentEpoch = chainRows[0]?.current_epoch ?? 0;

	await materializePopPenaltyEvents({ db, currentEpoch });

	const previousMetrics = await db<PreviousMetricRow[]>`
		SELECT
			orchestrator_id,
			prism_final_score,
			confidence_score,
			throughput_score,
			reliability_score,
			performance_score,
			readiness_multiplier,
			penalty_multiplier
		FROM core.beam_prism_metrics
	`;

	const previousByOrchestrator = new Map<string, PreviousMetricValues>(
		previousMetrics.map((row) => [
			row.orchestrator_id,
			{
				prism_final_score: numberValue(row.prism_final_score),
				confidence_score: numberValue(row.confidence_score),
				throughput_score: numberValue(row.throughput_score),
				reliability_score: numberValue(row.reliability_score),
				performance_score: numberValue(row.performance_score),
				readiness_multiplier: numberValue(row.readiness_multiplier),
				penalty_multiplier: numberValue(row.penalty_multiplier),
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
					o.registered_at,
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
					) AS connected_workers
        FROM core.orchestrators o
      `,
			db<ProofRow[]>`
        SELECT
					p.orchestrator_id,
					t.transfer_id,
					COALESCE(p.bandwidth_mbps, 0)::TEXT AS bandwidth_mbps,
					COALESCE(p.verified_at, p.published_at) AS event_at
        FROM core.proofs_of_bandwidth p
        LEFT JOIN core.tasks t ON t.id = p.task_id
        WHERE COALESCE(p.verified_at, p.published_at) > NOW() - ${LOOKBACK_WINDOW}::INTERVAL
          AND (p.verification_passed = TRUE OR p.status = 'verified')
      `,
			db<TaskRow[]>`
        SELECT
					orchestrator_id,
          state,
					failure_reason,
					COALESCE(completed_at, failed_at, created_at) AS event_at
        FROM core.tasks
        WHERE COALESCE(completed_at, failed_at, created_at) > NOW() - ${LOOKBACK_WINDOW}::INTERVAL
          AND state IN ('completed', 'failed')
        UNION ALL
        SELECT
					orchestrator_id,
          'failed' AS state,
					CONCAT('beamcore_guardrail_reassign:', COALESCE(reason, outcome)) AS failure_reason,
					intervened_at AS event_at
        FROM core.orchestrator_guardrail_interventions
        WHERE intervened_at > NOW() - ${LOOKBACK_WINDOW}::INTERVAL
			UNION ALL
			SELECT
			  orchestrator_id,
			  'failed' AS state,
			  CONCAT('assignment_failure:', COALESCE(failure_type, 'unknown'), ':', COALESCE(reason, 'unknown')) AS failure_reason,
			  created_at AS event_at
			FROM core.orchestrator_assignment_failures
			WHERE created_at > NOW() - ${LOOKBACK_WINDOW}::INTERVAL
      `,
			db<PaymentPenaltyRow[]>`
        SELECT
			  orchestrator_id,
					COUNT(*)::INT AS failed_pop_payments
        FROM core.pop_penalty_events
        WHERE applied_in_epoch <= ${currentEpoch}
          AND resolved_at > NOW() - ${PENALTY_LOOKBACK_WINDOW}::INTERVAL
        GROUP BY orchestrator_id
      `,
			db<FraudPenaltyRow[]>`
        SELECT
			  orchestrator_id,
					COUNT(*)::INT AS fraud_penalty_events
        FROM core.fraud_penalties
        WHERE orchestrator_id IS NOT NULL
          AND applied_at > NOW() - ${PENALTY_LOOKBACK_WINDOW}::INTERVAL
        GROUP BY orchestrator_id
      `,
			db<SybilPenaltyRow[]>`
        SELECT
			  t.orchestrator_id,
					COUNT(DISTINCT sv.id)::INT AS sybil_violation_events
        FROM core.sybil_violations sv
        JOIN core.tasks t ON t.assigned_worker_id = sv.worker_id
        WHERE sv.resolved_at IS NULL
          AND t.created_at > NOW() - ${PENALTY_LOOKBACK_WINDOW}::INTERVAL
        GROUP BY t.orchestrator_id
      `,
			db<GuardrailPenaltyRow[]>`
        SELECT
			  orchestrator_id,
					COUNT(*)::INT AS guardrail_intervention_events
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
			bandwidthMbps: numberValue(proof.bandwidth_mbps),
			transferId: proof.transfer_id,
			eventAt: proof.event_at,
		};
		const existing = proofsByOrchestrator.get(proof.orchestrator_id) ?? [];
		existing.push(sample);
		proofsByOrchestrator.set(proof.orchestrator_id, existing);
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
			failureReason: task.failure_reason,
			eventAt: task.event_at,
		};
		const existing = tasksByOrchestrator.get(task.orchestrator_id) ?? [];
		existing.push(sample);
		tasksByOrchestrator.set(task.orchestrator_id, existing);
	}

	for (const row of paymentPenalties) {
		penaltyByOrchestrator.set(row.orchestrator_id, {
			failedPopPayments: row.failed_pop_payments,
			fraudPenaltyEvents: 0,
			sybilViolationEvents: 0,
			guardrailInterventionEvents: 0,
		});
	}

	for (const row of fraudPenalties) {
		const existing = penaltyByOrchestrator.get(row.orchestrator_id) ?? {
			failedPopPayments: 0,
			fraudPenaltyEvents: 0,
			sybilViolationEvents: 0,
			guardrailInterventionEvents: 0,
		};
		existing.fraudPenaltyEvents = row.fraud_penalty_events;
		penaltyByOrchestrator.set(row.orchestrator_id, existing);
	}

	for (const row of sybilPenalties) {
		const existing = penaltyByOrchestrator.get(row.orchestrator_id) ?? {
			failedPopPayments: 0,
			fraudPenaltyEvents: 0,
			sybilViolationEvents: 0,
			guardrailInterventionEvents: 0,
		};
		existing.sybilViolationEvents = row.sybil_violation_events;
		penaltyByOrchestrator.set(row.orchestrator_id, existing);
	}

	for (const row of guardrailPenalties) {
		const existing = penaltyByOrchestrator.get(row.orchestrator_id) ?? {
			failedPopPayments: 0,
			fraudPenaltyEvents: 0,
			sybilViolationEvents: 0,
			guardrailInterventionEvents: 0,
		};
		existing.guardrailInterventionEvents = row.guardrail_intervention_events;
		penaltyByOrchestrator.set(row.orchestrator_id, existing);
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
				connectedWorkers: orchestrator.connected_workers,
			},
			registeredAt: orchestrator.registered_at,
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
