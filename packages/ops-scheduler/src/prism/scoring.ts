/**
 * PRISM score computation for orchestrator throughput, reliability, readiness, penalties, and confidence.
 *
 * Self-contained transparency copy of the production scoring module in beam-core.
 */

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

export type PrismPenaltyKind = "pop" | "fraud" | "sybil";

export interface PrismPenaltyEvent {
	kind: PrismPenaltyKind;
	eventAt: Date;
}

export interface PenaltyCoefficients {
	pop: number;
	fraud: number;
	sybil: number;
}

export interface PrismReadinessSnapshot {
	ready: boolean;
	controlPlaneConnected: boolean;
}

export interface FleetMetricRange {
	min: number;
	max: number;
}

export interface PerformanceWeights {
	throughput: number;
	reliability: number;
}

export interface PrismScoreInput {
	averageVerifiedMbps: number;
	throughputRange: FleetMetricRange;
	reliabilityRange: FleetMetricRange;
	proofs: PrismProofSample[];
	tasks: PrismTaskSample[];
	penaltyEvents: PrismPenaltyEvent[];
	penaltyCoefficients: PenaltyCoefficients;
	readiness: PrismReadinessSnapshot;
	evidenceLookbackDays?: number;
	confidenceTargets?: {
		targetVerifiedTasks?: number;
		ageSaturationDays?: number;
	};
	registeredAt: Date;
	now: Date;
	graduationConfidence?: number;
	performanceWeights?: PerformanceWeights;
}

export interface PrismScoreResult {
	throughputScore: number;
	reliabilityScore: number;
	rawReliability: number;
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

const DEFAULT_EVIDENCE_LOOKBACK_DAYS = 7;
const HALF_LIFE_HOURS = 72;
export const DEFAULT_GRADUATION_CONFIDENCE = 0.9;

export function resolveNewPool(
	previousPool: "qualifying" | "qualified",
	confidenceScore: number,
	graduationConfidence = DEFAULT_GRADUATION_CONFIDENCE,
): "qualifying" | "qualified" {
	if (previousPool === "qualified") return "qualified";
	return confidenceScore >= graduationConfidence ? "qualified" : "qualifying";
}
const DEFAULT_TARGET_GRADUATION_VERIFIED_TASKS = 120;
const DEFAULT_AGE_SATURATION_DAYS = 7;
export const DEFAULT_PERFORMANCE_THROUGHPUT_WEIGHT = 0.4;
export const DEFAULT_PERFORMANCE_RELIABILITY_WEIGHT = 0.6;
/** Lowest normalized fleet score for orchestrators with positive evidence (linear spread up to 1). */
export const FLEET_NORMALIZATION_FLOOR = 0.2;

function clamp(value: number, min = 0, max = 1): number {
	return Math.min(max, Math.max(min, value));
}

function round(value: number, digits = 5): number {
	return Number(value.toFixed(digits));
}

export function sampleWeight(sampleAt: Date, now: Date, halfLifeHours = HALF_LIFE_HOURS): number {
	const ageMs = Math.max(0, now.getTime() - sampleAt.getTime());
	const ageHours = ageMs / (1000 * 60 * 60);
	return Math.pow(0.5, ageHours / halfLifeHours);
}

export function normalizeFleetMetric(
	value: number,
	range: FleetMetricRange,
): { score: number; floor: number; ceiling: number } {
	const floor = range.min;
	const ceiling = Math.max(range.max, floor + 1e-9);
	if (value <= 0) {
		return { score: 0, floor, ceiling };
	}
	if (ceiling <= floor) {
		return { score: 1, floor, ceiling };
	}
	const linear = (value - floor) / (ceiling - floor);
	const span = 1 - FLEET_NORMALIZATION_FLOOR;
	return {
		score: clamp(FLEET_NORMALIZATION_FLOOR + span * linear),
		floor,
		ceiling,
	};
}

function filterByLookback<T extends { eventAt: Date }>(
	samples: T[],
	now: Date,
	lookbackDays: number,
): T[] {
	const cutoff = now.getTime() - lookbackDays * 24 * 60 * 60 * 1000;
	return samples.filter((sample) => sample.eventAt.getTime() >= cutoff);
}

export function computeRawReliability(
	tasks: PrismTaskSample[],
	now: Date,
	evidenceLookbackDays = DEFAULT_EVIDENCE_LOOKBACK_DAYS,
): ReturnType<typeof reliabilityScore> {
	const boundedTasks = filterByLookback(tasks, now, evidenceLookbackDays);
	return reliabilityScore(boundedTasks, now);
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

function readinessMultiplier(readiness: PrismReadinessSnapshot): { multiplier: number; reason: string } {
	if (!readiness.controlPlaneConnected) return { multiplier: 0, reason: "disconnected" };
	if (!readiness.ready) return { multiplier: 0, reason: "not_ready" };
	return { multiplier: 1, reason: "ready" };
}

function decayedPenaltyPressure(
	events: PrismPenaltyEvent[],
	coefficients: PenaltyCoefficients,
	now: Date,
): { multiplier: number; pressure: number; eventCount: number } {
	let pressure = 0;
	for (const event of events) {
		const coeff = coefficients[event.kind];
		pressure += coeff * sampleWeight(event.eventAt, now);
	}
	return {
		pressure,
		multiplier: clamp(1 - pressure, 0.2, 1),
		eventCount: events.length,
	};
}

function confidenceScore(
	verifiedTaskCount: number,
	successRate: number,
	confidenceTargets: { targetVerifiedTasks: number; ageSaturationDays: number },
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
	const ageSaturationDays = Math.max(1, confidenceTargets.ageSaturationDays);
	const ageDays = Math.max(0, now.getTime() - registeredAt.getTime()) / (1000 * 60 * 60 * 24);
	const verifiedTaskRatio = clamp(verifiedTaskCount / verifiedTaskTarget);
	const ageRatio = clamp(ageDays / ageSaturationDays);
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

function resolvePerformanceWeights(input: PrismScoreInput): PerformanceWeights {
	return {
		throughput: input.performanceWeights?.throughput ?? DEFAULT_PERFORMANCE_THROUGHPUT_WEIGHT,
		reliability: input.performanceWeights?.reliability ?? DEFAULT_PERFORMANCE_RELIABILITY_WEIGHT,
	};
}

export function computePrismScore(input: PrismScoreInput): PrismScoreResult {
	const evidenceLookbackDays = input.evidenceLookbackDays ?? DEFAULT_EVIDENCE_LOOKBACK_DAYS;
	const boundedProofs = filterByLookback(input.proofs, input.now, evidenceLookbackDays);
	const boundedTasks = filterByLookback(input.tasks, input.now, evidenceLookbackDays);
	const boundedPenalties = filterByLookback(input.penaltyEvents, input.now, evidenceLookbackDays);
	const performanceWeights = resolvePerformanceWeights(input);

	const throughput = normalizeFleetMetric(input.averageVerifiedMbps, input.throughputRange);
	const reliability = reliabilityScore(boundedTasks, input.now);
	const normalizedReliability = normalizeFleetMetric(reliability.reliability, input.reliabilityRange);
	const performanceScore = clamp(
		throughput.score * performanceWeights.throughput +
			normalizedReliability.score * performanceWeights.reliability,
	);
	const readiness = readinessMultiplier(input.readiness);
	const penalty = decayedPenaltyPressure(boundedPenalties, input.penaltyCoefficients, input.now);
	const confidenceTargets = {
		targetVerifiedTasks:
			input.confidenceTargets?.targetVerifiedTasks ?? DEFAULT_TARGET_GRADUATION_VERIFIED_TASKS,
		ageSaturationDays:
			input.confidenceTargets?.ageSaturationDays ?? DEFAULT_AGE_SATURATION_DAYS,
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
	const graduationConfidence = input.graduationConfidence ?? DEFAULT_GRADUATION_CONFIDENCE;
	const prismPool = resolveNewPool("qualifying", confidence.score, graduationConfidence);
	const finalScore = clamp(performanceScore * readiness.multiplier * penalty.multiplier);

	return {
		throughputScore: round(throughput.score),
		reliabilityScore: round(normalizedReliability.score),
		rawReliability: round(reliability.reliability),
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
			halfLifeHours: HALF_LIFE_HOURS,
			performanceWeights: {
				throughput: performanceWeights.throughput,
				reliability: performanceWeights.reliability,
			},
			fleetNormalization: {
				floor: FLEET_NORMALIZATION_FLOOR,
				ceiling: 1,
			},
			sampleCounts: {
				proofsInWindow: boundedProofs.length,
				tasksInWindow: boundedTasks.length,
				penaltyEventsInWindow: boundedPenalties.length,
			},
			throughputRange: {
				min: round(input.throughputRange.min, 2),
				max: round(input.throughputRange.max, 2),
			},
			reliabilityRange: {
				min: round(input.reliabilityRange.min, 4),
				max: round(input.reliabilityRange.max, 4),
			},
			throughput: {
				averageVerifiedMbps: round(input.averageVerifiedMbps, 2),
				normalizedScore: round(throughput.score),
				floor: round(throughput.floor, 2),
				ceiling: round(throughput.ceiling, 2),
			},
			reliability: {
				rawReliability: round(reliability.reliability),
				normalizedScore: round(normalizedReliability.score),
				completedWeight: round(reliability.completedWeight),
				failedWeight: round(reliability.failedWeight),
				reassignmentWeight: round(reliability.reassignmentWeight),
				totalWeight: round(reliability.totalWeight),
				reassignmentRate: round(reliability.reassignmentRate, 4),
				successRate: round(reliability.successRate, 4),
			},
			penalties: {
				coefficients: { ...input.penaltyCoefficients },
				eventCount: penalty.eventCount,
				pressure: round(penalty.pressure),
				multiplier: round(penalty.multiplier),
			},
			readiness: {
				ready: input.readiness.ready,
				controlPlaneConnected: input.readiness.controlPlaneConnected,
				multiplier: round(readiness.multiplier),
				reason: readiness.reason,
			},
			confidence: {
				ageDays: round(confidence.ageDays, 2),
				verifiedTaskCount: confidence.verifiedTaskCount,
				verifiedTaskTarget: round(confidence.verifiedTaskTarget, 4),
				verifiedTaskRatio: round(confidence.verifiedTaskRatio, 4),
				ageRatio: round(confidence.ageRatio, 4),
				ageSaturationDays: confidenceTargets.ageSaturationDays,
				maturityFactor: round(confidence.maturityFactor, 4),
				successRate: round(confidence.successRate, 4),
				score: round(confidence.score, 4),
				graduationConfidence,
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
