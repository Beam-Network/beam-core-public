export interface PrismProofSample {
	bandwidthMbps: number;
	taskId?: string | null;
	transferId: string | null;
	eventAt: Date;
}

export interface PrismTaskSample {
	state: "completed" | "failed";
	failureReason: string | null;
	eventAt: Date;
}

export interface PrismPenaltySnapshot {
	failedPopPayments: number;
	fraudPenaltyEvents: number;
	sybilViolationEvents: number;
	guardrailInterventionEvents: number;
}

export interface PrismReadinessSnapshot {
	ready: boolean;
	connectedWorkers: number;
}

export interface PrismScoreInput {
	averageVerifiedMbps: number;
	throughputBounds: { p25: number; p90: number };
	proofs: PrismProofSample[];
	tasks: PrismTaskSample[];
	penalties: PrismPenaltySnapshot;
	readiness: PrismReadinessSnapshot;
	evidenceLookbackDays?: number;
	confidenceTargets?: {
		targetVerifiedTasks?: number;
	};
	registeredAt: Date;
	now: Date;
}

export interface PrismScoreResult {
	throughputScore: number;
	reliabilityScore: number;
	performanceScore: number;
	readinessMultiplier: number;
	penaltyMultiplier: number;
	confidenceScore: number;
	prismFinalScore: number;
	verifiedTransferCount: number;
	verifiedChunkCount: number;
	verifiedBandwidthMbps: number;
	prismPool: "qualifying" | "qualified";
	successRate: number;
	scoreComponents: Record<string, unknown>;
}

const DEFAULT_EVIDENCE_LOOKBACK_DAYS = 10;
const PAYMENT_LOOKBACK_DAYS = 30;
const HALF_LIFE_HOURS = 72;
const GRADUATION_CONFIDENCE = 0.5;
const DEFAULT_TARGET_GRADUATION_VERIFIED_TASKS = 20;
const AGE_SATURATION_DAYS = 21;

function clamp(value: number, min = 0, max = 1): number {
	return Math.min(max, Math.max(min, value));
}

function round(value: number, digits = 5): number {
	return Number(value.toFixed(digits));
}

export function percentile(values: number[], ratio: number): number {
	if (!values.length) return 0;
	const sorted = [...values].sort((left, right) => left - right);
	const index = (sorted.length - 1) * ratio;
	const lower = Math.floor(index);
	const upper = Math.ceil(index);
	if (lower === upper) return sorted[lower]!;
	const weight = index - lower;
	return sorted[lower]! * (1 - weight) + sorted[upper]! * weight;
}

function sampleWeight(sampleAt: Date, now: Date): number {
	const ageMs = Math.max(0, now.getTime() - sampleAt.getTime());
	const ageHours = ageMs / (1000 * 60 * 60);
	return Math.pow(0.5, ageHours / HALF_LIFE_HOURS);
}

function normalizeThroughput(
	averageVerifiedMbps: number,
	bounds: { p25: number; p90: number },
): { score: number; floor: number; ceiling: number } {
	const floor = bounds.p25;
	const ceiling = Math.max(bounds.p90, floor + 1);
	if (averageVerifiedMbps <= 0) {
		return { score: 0, floor, ceiling };
	}

	return {
		score: clamp((averageVerifiedMbps - floor) / (ceiling - floor)),
		floor,
		ceiling,
	};
}

function readinessMultiplier(readiness: PrismReadinessSnapshot): { multiplier: number; reason: string } {
	if (!readiness.ready) return { multiplier: 0, reason: "not_ready" };
	if (readiness.connectedWorkers <= 0) return { multiplier: 0, reason: "no_connected_workers" };
	if (readiness.connectedWorkers === 1) return { multiplier: 0.7, reason: "single_worker" };
	if (readiness.connectedWorkers === 2) return { multiplier: 0.85, reason: "two_workers" };
	return { multiplier: 1, reason: "fully_ready" };
}

function penaltyMultiplier(snapshot: PrismPenaltySnapshot): { multiplier: number; pressure: number } {
	const pressure =
		snapshot.failedPopPayments * 0.15 +
		snapshot.fraudPenaltyEvents * 0.1 +
		snapshot.sybilViolationEvents * 0.1 +
		snapshot.guardrailInterventionEvents * 0.08;

	return {
		multiplier: clamp(1 - pressure, 0.2, 1),
		pressure,
	};
}

function confidenceScore(
	verifiedTaskCount: number,
	successRate: number,
	confidenceTargets: { targetVerifiedTasks: number },
	registeredAt: Date,
	now: Date,
): {
	score: number;
	ageDays: number;
	verifiedTaskCount: number;
	verifiedTaskTarget: number;
	verifiedTaskRatio: number;
	ageRatio: number;
	maturityFactor: number;
	successRate: number;
} {
	const verifiedTaskTarget = Math.max(1, confidenceTargets.targetVerifiedTasks);
	const ageDays = Math.max(0, now.getTime() - registeredAt.getTime()) / (1000 * 60 * 60 * 24);
	const verifiedTaskRatio = clamp(verifiedTaskCount / verifiedTaskTarget);
	const ageRatio = clamp(ageDays / AGE_SATURATION_DAYS);
	const maturityFactor = clamp(0.8 + 0.2 * ageRatio);
	return {
		score: clamp(verifiedTaskRatio * successRate * maturityFactor),
		ageDays,
		verifiedTaskCount,
		verifiedTaskTarget,
		verifiedTaskRatio,
		ageRatio,
		maturityFactor,
		successRate,
	};
}

function reliabilityScore(
	tasks: PrismTaskSample[],
	now: Date,
): {
	reliability: number;
	successRate: number;
	completedWeight: number;
	failedWeight: number;
	reassignmentWeight: number;
	reassignmentRate: number;
	totalWeight: number;
} {
	let completedWeight = 0;
	let failedWeight = 0;
	let reassignmentWeight = 0;

	for (const task of tasks) {
		const weight = sampleWeight(task.eventAt, now);
		if (task.state === "completed") {
			completedWeight += weight;
			continue;
		}

		failedWeight += weight;
		const reason = (task.failureReason ?? "").toLowerCase();
		if (reason.includes("reassign") || reason.includes("reject") || reason.includes("stale")) {
			reassignmentWeight += weight;
		}
	}

	const totalWeight = completedWeight + failedWeight;
	if (totalWeight <= 0) {
		return {
			reliability: 0,
			successRate: 0,
			completedWeight,
			failedWeight,
			reassignmentWeight,
			reassignmentRate: 0,
			totalWeight,
		};
	}

	const successRate = completedWeight / totalWeight;
	const reassignmentRate = failedWeight > 0 ? reassignmentWeight / failedWeight : 0;
	const reliability = clamp(successRate * 0.7 + (1 - reassignmentRate) * 0.3);

	return {
		reliability,
		successRate,
		completedWeight,
		failedWeight,
		reassignmentWeight,
		reassignmentRate,
		totalWeight,
	};
}

export function computePrismScore(input: PrismScoreInput): PrismScoreResult {
	const evidenceLookbackDays = input.evidenceLookbackDays ?? DEFAULT_EVIDENCE_LOOKBACK_DAYS;
	const boundedProofs = input.proofs.filter((proof) => {
		return (
			proof.eventAt.getTime() >=
			input.now.getTime() - evidenceLookbackDays * 24 * 60 * 60 * 1000
		);
	});
	const boundedTasks = input.tasks.filter((task) => {
		return (
			task.eventAt.getTime() >=
			input.now.getTime() - evidenceLookbackDays * 24 * 60 * 60 * 1000
		);
	});

	const throughput = normalizeThroughput(input.averageVerifiedMbps, input.throughputBounds);
	const reliability = reliabilityScore(boundedTasks, input.now);
	const performanceScore = clamp(throughput.score * 0.55 + reliability.reliability * 0.45);
	const readiness = readinessMultiplier(input.readiness);
	const penalty = penaltyMultiplier(input.penalties);
	const confidenceTargets = {
		targetVerifiedTasks:
			input.confidenceTargets?.targetVerifiedTasks ?? DEFAULT_TARGET_GRADUATION_VERIFIED_TASKS,
	};

	const verifiedTransferCount = new Set(
		boundedProofs
			.map((proof) => proof.transferId)
			.filter((transferId): transferId is string => Boolean(transferId)),
	).size;
	const verifiedTaskCount = new Set(
		boundedProofs
			.map((proof) => proof.taskId)
			.filter((taskId): taskId is string => Boolean(taskId)),
	).size;
	const verifiedChunkCount = boundedProofs.length;

	const confidence = confidenceScore(
		verifiedTaskCount,
		reliability.successRate,
		confidenceTargets,
		input.registeredAt,
		input.now,
	);
	const prismPool = confidence.score > GRADUATION_CONFIDENCE ? "qualified" : "qualifying";
	const finalScore = clamp(performanceScore * readiness.multiplier * penalty.multiplier);

	return {
		throughputScore: round(throughput.score),
		reliabilityScore: round(reliability.reliability),
		performanceScore: round(performanceScore),
		readinessMultiplier: round(readiness.multiplier),
		penaltyMultiplier: round(penalty.multiplier),
		confidenceScore: round(confidence.score, 4),
		prismFinalScore: round(finalScore),
		verifiedTransferCount,
		verifiedChunkCount,
		verifiedBandwidthMbps: round(input.averageVerifiedMbps, 2),
		prismPool,
		successRate: round(reliability.successRate, 4),
		scoreComponents: {
			lookbackDays: evidenceLookbackDays,
			paymentLookbackDays: PAYMENT_LOOKBACK_DAYS,
			halfLifeHours: HALF_LIFE_HOURS,
			sampleCounts: {
				proofsInWindow: boundedProofs.length,
				tasksInWindow: boundedTasks.length,
			},
			throughputBounds: {
				p25: round(input.throughputBounds.p25, 2),
				p90: round(input.throughputBounds.p90, 2),
			},
			throughput: {
				averageVerifiedMbps: round(input.averageVerifiedMbps, 2),
				normalizedScore: round(throughput.score),
				floor: round(throughput.floor, 2),
				ceiling: round(throughput.ceiling, 2),
			},
			reliability: {
				completedWeight: round(reliability.completedWeight),
				failedWeight: round(reliability.failedWeight),
				reassignmentWeight: round(reliability.reassignmentWeight),
				totalWeight: round(reliability.totalWeight),
				reassignmentRate: round(reliability.reassignmentRate, 4),
				successRate: round(reliability.successRate, 4),
				reliabilityScore: round(reliability.reliability),
			},
			penalties: {
				...input.penalties,
				pressure: round(penalty.pressure),
				multiplier: round(penalty.multiplier),
			},
			readiness: {
				...input.readiness,
				multiplier: round(readiness.multiplier),
				reason: readiness.reason,
			},
			confidence: {
				ageDays: round(confidence.ageDays, 2),
				verifiedTaskCount: confidence.verifiedTaskCount,
				verifiedTaskTarget: round(confidence.verifiedTaskTarget, 4),
				verifiedTaskRatio: round(confidence.verifiedTaskRatio, 4),
				ageRatio: round(confidence.ageRatio, 4),
				ageSaturationDays: AGE_SATURATION_DAYS,
				maturityFactor: round(confidence.maturityFactor, 4),
				successRate: round(confidence.successRate, 4),
				score: round(confidence.score, 4),
				graduationConfidence: GRADUATION_CONFIDENCE,
			},
			final: {
				performanceScore: round(performanceScore),
				readinessMultiplier: round(readiness.multiplier),
				penaltyMultiplier: round(penalty.multiplier),
				prismFinalScore: round(finalScore),
			},
			verifiedTransferCount,
			verifiedChunkCount,
		},
	};
}
