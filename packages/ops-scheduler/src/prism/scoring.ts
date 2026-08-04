

export interface PrismTaskHourBucket {
	bucketStart: Date;
	verified_task_count: number;
	failed_task_count: number;
	overseer_intervention_count: number;
}

export interface PrismPenaltyHourBucket {
	bucketStart: Date;
	fraud_penalty_count: number;
	integrity_chunk_mismatch_count: number;
	sybil_penalty_count: number;
}

export interface PenaltyCoefficients {
	fraud: number;
	integrityChunkMismatch: number;
	sybil: number;
}

export interface PrismReadinessSnapshot {
	ready: boolean;
	controlPlaneConnected: boolean;
	activeTimeMultiplier?: number;
}

export interface PrismReadinessEvent {
	eventAt: Date;
	ready: boolean;
	controlPlaneConnected: boolean;
	active?: boolean;
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

	verifiedTaskCount: number;
	verifiedUploadedMib: number;
	verifiedTransferCount: number;
	averageVerifiedMbps: number;

	taskHourBuckets: PrismTaskHourBucket[];
	penaltyHourBuckets: PrismPenaltyHourBucket[];

	lifetimeVerifiedTaskCount: number;
	lifetimeVerifiedTransferCount: number;

	throughputRange: FleetMetricRange;
	reliabilityRange: FleetMetricRange;

	penaltyCoefficients: PenaltyCoefficients;
	penaltyHalfLifeHours?: number;
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
	computeConfidence?: boolean;
}

export interface PrismScoreResult {
	throughputScore: number;
	reliabilityScore: number;
	rawReliability: number;
	performanceScore: number;
	readinessMultiplier: number;
	readinessActiveTimeMultiplier: number;
	penaltyMultiplier: number;
	confidenceScore: number;
	prismFinalScore: number;
	verifiedTransferCount: number;
	verifiedTaskCount: number;
	verifiedUploadedMib: number;
	lifetimeVerifiedTaskCount: number;
	verifiedBandwidthMbps: number;
	prismPool: "qualifying" | "qualified";
	successRate: number;
	penaltyEventCount: number;
	ageDays: number;
	tasksInWindow: number;
	routingOverrideReason: string | null;
	routingComputedFinalScore: number | null;
	routingAppliedScore: number | null;
}

export const DEFAULT_EVIDENCE_LOOKBACK_DAYS = 7;
export const RELIABILITY_HALF_LIFE_HOURS = 24;
export const DEFAULT_PENALTY_HALF_LIFE_HOURS = 0;
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

export const FLEET_NORMALIZATION_FLOOR = 0.2;

function clamp(value: number, min = 0, max = 1): number {
	return Math.min(max, Math.max(min, value));
}

function round(value: number, digits = 5): number {
	return Number(value.toFixed(digits));
}

function readinessEventActive(event: PrismReadinessEvent): boolean {
	return event.active ?? (event.ready && event.controlPlaneConnected);
}

export function computeReadinessActiveTimeMultiplier(input: {
	events: PrismReadinessEvent[];
	current: PrismReadinessSnapshot;
	lookbackDays: number;
	registeredAt?: Date;
	now: Date;
}): number {
	const windowMs = Math.max(1, input.lookbackDays) * 24 * 60 * 60 * 1000;
	const nowMs = input.now.getTime();
	const lookbackStartMs = nowMs - windowMs;
	const registrationMs = input.registeredAt?.getTime();
	const hasRegistrationCap = Number.isFinite(registrationMs) && registrationMs! > lookbackStartMs;
	const baseWindowStartMs = hasRegistrationCap
		? Math.min(nowMs, registrationMs!)
		: lookbackStartMs;
	const events = input.events
		.filter((event) => Number.isFinite(event.eventAt.getTime()) && event.eventAt.getTime() <= nowMs)
		.sort((left, right) => left.eventAt.getTime() - right.eventAt.getTime());

	if (events.length === 0 || baseWindowStartMs >= nowMs) {
		return input.current.ready && input.current.controlPlaneConnected ? 1 : 0;
	}

	let effectiveWindowStartMs = baseWindowStartMs;
	let state: boolean | null = null;
	let startIndex = 0;
	for (const [index, event] of events.entries()) {
		const eventMs = event.eventAt.getTime();
		if (eventMs <= baseWindowStartMs) {
			state = readinessEventActive(event);
			startIndex = index + 1;
			continue;
		}
		if (state === null) {
			effectiveWindowStartMs = eventMs;
			state = readinessEventActive(event);
			startIndex = index + 1;
		}
		break;
	}

	if (state === null || effectiveWindowStartMs >= nowMs) {
		return input.current.ready && input.current.controlPlaneConnected ? 1 : 0;
	}

	const denominatorMs = Math.max(1, nowMs - effectiveWindowStartMs);
	let cursorMs = effectiveWindowStartMs;
	let activeMs = 0;
	for (const event of events.slice(startIndex)) {
		const eventMs = event.eventAt.getTime();
		if (eventMs <= effectiveWindowStartMs) continue;
		if (eventMs > nowMs) break;
		if (state) activeMs += Math.max(0, eventMs - cursorMs);
		state = readinessEventActive(event);
		cursorMs = Math.max(cursorMs, eventMs);
	}

	if (state) activeMs += Math.max(0, nowMs - cursorMs);
	return clamp(activeMs / denominatorMs);
}

export function sampleWeight(sampleAt: Date, now: Date, halfLifeHours: number): number {
	const ageMs = Math.max(0, now.getTime() - sampleAt.getTime());
	const ageHours = ageMs / (1000 * 60 * 60);
	return Math.pow(0.5, ageHours / halfLifeHours);
}

export function hourMidpoint(bucketStart: Date): Date {
	const d = new Date(bucketStart);
	d.setUTCMinutes(30, 0, 0);
	return d;
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

export function computeRawReliability(
	buckets: PrismTaskHourBucket[],
	now: Date,
): ReturnType<typeof reliabilityScore> {
	return reliabilityScore(buckets, now);
}

function reliabilityScore(
	buckets: PrismTaskHourBucket[],
	now: Date,
): {
	reliability: number;
	successRate: number;
	verifiedWeight: number;
	failedWeight: number;
	overseerInterventionWeight: number;
	totalWeight: number;
} {
	let verifiedWeight = 0;
	let failedTaskWeight = 0;
	let overseerInterventionWeight = 0;

	for (const bucket of buckets) {
		const weight = sampleWeight(hourMidpoint(bucket.bucketStart), now, RELIABILITY_HALF_LIFE_HOURS);
		verifiedWeight += bucket.verified_task_count * weight;
		failedTaskWeight += bucket.failed_task_count * weight;
		overseerInterventionWeight += bucket.overseer_intervention_count * weight;
	}

	const failedWeight = Math.max(failedTaskWeight, overseerInterventionWeight);
	const totalWeight = verifiedWeight + failedWeight;
	if (totalWeight <= 0) {
		return {
			reliability: 0,
			successRate: 0,
			verifiedWeight,
			failedWeight,
			overseerInterventionWeight,
			totalWeight,
		};
	}

	const successRate = verifiedWeight / totalWeight;
	const reliability = clamp(successRate);

	return {
		reliability,
		successRate,
		verifiedWeight,
		failedWeight,
		overseerInterventionWeight,
		totalWeight,
	};
}

function readinessMultiplier(readiness: PrismReadinessSnapshot): {
	multiplier: number;
	activeTimeMultiplier: number;
	currentMultiplier: number;
	reason: string;
} {
	const activeTimeMultiplier = clamp(readiness.activeTimeMultiplier ?? 1);
	if (!readiness.controlPlaneConnected) {
		return { multiplier: 0, activeTimeMultiplier, currentMultiplier: 0, reason: "disconnected" };
	}
	if (!readiness.ready) {
		return { multiplier: 0, activeTimeMultiplier, currentMultiplier: 0, reason: "not_ready" };
	}
	return {
		multiplier: activeTimeMultiplier,
		activeTimeMultiplier,
		currentMultiplier: 1,
		reason: "ready",
	};
}

function penaltyPressure(
	buckets: PrismPenaltyHourBucket[],
	coefficients: PenaltyCoefficients,
	penaltyHalfLifeHours: number,
	now: Date,
): { multiplier: number; pressure: number; eventCount: number } {
	let pressure = 0;
	let eventCount = 0;
	const halfLifeHours = Math.max(0, penaltyHalfLifeHours);
	for (const bucket of buckets) {
		const weight = halfLifeHours > 0
			? sampleWeight(hourMidpoint(bucket.bucketStart), now, halfLifeHours)
			: 1;
		pressure += coefficients.fraud * bucket.fraud_penalty_count * weight;
		pressure += coefficients.integrityChunkMismatch * bucket.integrity_chunk_mismatch_count * weight;
		pressure += coefficients.sybil * bucket.sybil_penalty_count * weight;
		eventCount += bucket.fraud_penalty_count + bucket.integrity_chunk_mismatch_count + bucket.sybil_penalty_count;
	}
	return {
		pressure,
		multiplier: clamp(1 - pressure, 0, 1),
		eventCount,
	};
}

function confidenceScore(
	lifetimeVerifiedTaskCount: number,
	successRate: number,
	confidenceTargets: { targetVerifiedTasks: number; ageSaturationDays: number },
	registeredAt: Date,
	now: Date,
): {
	score: number;
	ageDays: number;
	verifiedTaskTarget: number;
	verifiedTaskRatio: number;
	ageRatio: number;
	maturityFactor: number;
	successRate: number;
} {
	const verifiedTaskTarget = Math.max(1, confidenceTargets.targetVerifiedTasks);
	const ageSaturationDays = Math.max(1, confidenceTargets.ageSaturationDays);
	const ageDays = Math.max(0, now.getTime() - registeredAt.getTime()) / (1000 * 60 * 60 * 24);
	const verifiedTaskRatio = clamp(lifetimeVerifiedTaskCount / verifiedTaskTarget);
	const ageRatio = clamp(ageDays / ageSaturationDays);
	const maturityFactor = clamp(0.8 + 0.2 * ageRatio);
	return {
		score: clamp(verifiedTaskRatio * successRate * maturityFactor),
		ageDays,
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
	const performanceWeights = resolvePerformanceWeights(input);

	const throughput = normalizeFleetMetric(input.averageVerifiedMbps, input.throughputRange);
	const reliability = reliabilityScore(input.taskHourBuckets, input.now);
	const normalizedReliability = normalizeFleetMetric(reliability.reliability, input.reliabilityRange);
	const performanceScore = clamp(
		throughput.score * performanceWeights.throughput +
			normalizedReliability.score * performanceWeights.reliability,
	);
	const readiness = readinessMultiplier(input.readiness);
	const penalty = penaltyPressure(
		input.penaltyHourBuckets,
		input.penaltyCoefficients,
		input.penaltyHalfLifeHours ?? DEFAULT_PENALTY_HALF_LIFE_HOURS,
		input.now,
	);
	const confidenceTargets = {
		targetVerifiedTasks:
			input.confidenceTargets?.targetVerifiedTasks ?? DEFAULT_TARGET_GRADUATION_VERIFIED_TASKS,
		ageSaturationDays:
			input.confidenceTargets?.ageSaturationDays ?? DEFAULT_AGE_SATURATION_DAYS,
	};

	const computeConfidence = input.computeConfidence ?? true;
	const confidence = computeConfidence
		? confidenceScore(
				input.lifetimeVerifiedTaskCount,
				reliability.successRate,
				confidenceTargets,
				input.registeredAt,
				input.now,
			)
		: {
				score: 0,
				ageDays: Math.max(0, input.now.getTime() - input.registeredAt.getTime()) / (1000 * 60 * 60 * 24),
				verifiedTaskTarget: confidenceTargets.targetVerifiedTasks,
				verifiedTaskRatio: 0,
				ageRatio: 0,
				maturityFactor: 0,
				successRate: reliability.successRate,
			};
	const graduationConfidence = input.graduationConfidence ?? DEFAULT_GRADUATION_CONFIDENCE;
	const prismPool = resolveNewPool("qualifying", confidence.score, graduationConfidence);
	const finalScore = clamp(performanceScore * readiness.multiplier * penalty.multiplier);

	const windowTaskCounts = input.taskHourBuckets.reduce(
		(sum, b) => ({
			verified: sum.verified + b.verified_task_count,
			failed: sum.failed + b.failed_task_count,
			overseer: sum.overseer + b.overseer_intervention_count,
		}),
		{ verified: 0, failed: 0, overseer: 0 },
	);
	const tasksInWindow =
		windowTaskCounts.verified + Math.max(windowTaskCounts.failed, windowTaskCounts.overseer);

	return {
		throughputScore: round(throughput.score),
		reliabilityScore: round(normalizedReliability.score),
		rawReliability: round(reliability.reliability),
		performanceScore: round(performanceScore),
		readinessMultiplier: round(readiness.multiplier),
		readinessActiveTimeMultiplier: round(readiness.activeTimeMultiplier),
		penaltyMultiplier: round(penalty.multiplier),
		confidenceScore: round(confidence.score, 4),
		prismFinalScore: round(finalScore),
		verifiedTransferCount: input.verifiedTransferCount,
		verifiedTaskCount: input.verifiedTaskCount,
		verifiedUploadedMib: Math.max(0, Math.floor(input.verifiedUploadedMib)),
		lifetimeVerifiedTaskCount: input.lifetimeVerifiedTaskCount,
		verifiedBandwidthMbps: round(input.averageVerifiedMbps, 2),
		prismPool,
		successRate: round(reliability.successRate, 4),
		penaltyEventCount: penalty.eventCount,
		ageDays: round(confidence.ageDays, 2),
		tasksInWindow,
		routingOverrideReason: null,
		routingComputedFinalScore: null,
		routingAppliedScore: null,
	};
}

export function withRoutingOverride(
	score: PrismScoreResult,
	reason: string,
	computedFinalScore: number,
	appliedScore: number,
): PrismScoreResult {
	return {
		...score,
		prismFinalScore: round(appliedScore),
		routingOverrideReason: reason,
		routingComputedFinalScore: round(computedFinalScore),
		routingAppliedScore: round(appliedScore),
	};
}
