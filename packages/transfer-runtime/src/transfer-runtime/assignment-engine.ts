

import { randomInt, randomUUID } from "node:crypto";
import { performance } from "node:perf_hooks";

export type AssignmentAllocationRule =
	| "qualifying_equal_share_rotation"
	| "prism_final_score_desc";

export type DispatchWaveKind = "initial" | "recovery" | "permit_split";

export interface OrchestratorCandidate {
	id: string;
	hotkey: string;
	uid: number | null;
	prismFinalScore: number;
	ownerGroupId?: string | null;
}

export interface CandidateAllocation {
	orchestrator: OrchestratorCandidate;
	chunkCount: number;
}

export interface QualifiedAllocationPlan {
	allocationRule: "prism_final_score_desc";
	allocationOrder: string[];
	ownerGroups: Array<{
		ownerGroupId: string | null;
		averagePrismScore: number;
		chunkCount: number;
		members: Array<{
			hotkey: string;
			prismFinalScore: number;
			chunkCount: number;
		}>;
	}>;
	allocations: CandidateAllocation[];
}

export interface RecoveryChunkAssignment {
	chunkIndex: number;
	orchestrator: OrchestratorCandidate;
}

export interface TaskOfferWaveBatch {
	batchId: string;
	orchestratorId: string;
	orchestratorHotkey: string;
	waveKind: DispatchWaveKind;
	taskIds: string[];
}

export interface PreparedTaskOfferPayload {
	payloadId: string;
	batchId: string;
	transferId: string;
	orchestratorHotkey: string;
	subject: string;
	encodedPayload: Uint8Array;
	signedUrlsExpireAt: Date | null;
}

export interface PreparedTaskOfferBatch {
	batch: TaskOfferWaveBatch;
	preparedAt: Date;
	payloads: PreparedTaskOfferPayload[];
}

export interface PublishResult {
	payloadId: string;
	batchId: string;
	orchestratorHotkey: string;
	published: boolean;
	offeredAt?: Date;
	sentMonotonicMs?: number;
	publishMs?: number;
	reason?: string;
}

export interface ActivationDescriptor {
	payloadId: string;
	batchId: string;
	offeredAt: Date;
}

export interface WaveFailure {
	stage: "preparation" | "preflight" | "publish" | "activation" | "flush";
	batchId?: string;
	payloadId?: string;
	reason: string;
	deliveryUncertain: boolean;
}

export interface WaveDispatchResult {
	waveId: string;
	waveKind: DispatchWaveKind;
	participantOrder: string[];
	preparationMs: number;
	publishLoopMs: number;
	publishSpreadMs: number;
	activationMs: number;
	flushAttempted: boolean;
	flushMs: number;
	flushOk: boolean;
	payloadOrder: Array<{
		payloadId: string;
		batchId: string;
		orchestratorHotkey: string;
		encodedBytes: number;
	}>;
	results: PublishResult[];
	failures: WaveFailure[];
}

export interface PreparedWaveRegistry {

	preflight(payloads: readonly PreparedTaskOfferPayload[]): Array<{
		payloadId: string;
		reason: string;
	}>;

	bulkActivate(descriptors: readonly ActivationDescriptor[]): {
		failures: Array<{ payloadId: string; reason: string }>;
	};

	settleUndispatched(payloadIds: readonly string[], reason: string): void;

	settleUndispatchedBatches(batchIds: readonly string[], reason: string): void;
}

export interface PreparedWaveTransport {
	isReady(): boolean;
	isParticipantRegistered(hotkey: string): boolean;
	publish(subject: string, payload: Uint8Array): void;
	flush(): Promise<void>;
}

export interface DispatchWaveDependencies {
	prepareBatch(batch: TaskOfferWaveBatch): Promise<PreparedTaskOfferBatch>;
	registry: PreparedWaveRegistry;
	transport: PreparedWaveTransport;
	maxPayloadBytes: number;
	minimumSignedUrlTtlMs: number;
}

function safeScore(value: number): number {
	return Number.isFinite(value) ? Math.max(0, value) : 0;
}

export function cryptographicShuffle<T>(values: readonly T[]): T[] {
	const output = [...values];
	for (let index = output.length - 1; index > 0; index -= 1) {
		const swapIndex = randomInt(index + 1);
		[output[index], output[swapIndex]] = [output[swapIndex]!, output[index]!];
	}
	return output;
}

function shuffleEqualRuns<T>(
	values: readonly T[],
	isTie: (left: T, right: T) => boolean,
): T[] {
	const output: T[] = [];
	let start = 0;
	while (start < values.length) {
		let end = start + 1;
		while (end < values.length && isTie(values[start]!, values[end]!)) end += 1;
		const run = values.slice(start, end);
		output.push(...(run.length > 1 ? cryptographicShuffle(run) : run));
		start = end;
	}
	return output;
}

export function computePrismSlices(scores: readonly number[], totalChunks: number): number[] {
	if (!scores.length || totalChunks <= 0) return scores.map(() => 0);
	const normalized = scores.map(safeScore);
	const totalScore = normalized.reduce((sum, score) => sum + score, 0);
	if (totalScore <= 0) {
		const base = Math.floor(totalChunks / normalized.length);
		const remainder = totalChunks % normalized.length;
		const winners = new Set(cryptographicShuffle(normalized.map((_, index) => index)).slice(0, remainder));
		return normalized.map((_, index) => base + (winners.has(index) ? 1 : 0));
	}

	const quotas = normalized.map((score) => (score / totalScore) * totalChunks);
	const slices = quotas.map(Math.floor);
	let remaining = totalChunks - slices.reduce((sum, count) => sum + count, 0);
	const remainders = shuffleEqualRuns(
		quotas
			.map((quota, index) => ({ index, fraction: quota - Math.floor(quota) }))
			.sort((left, right) => right.fraction - left.fraction),
		(left, right) => left.fraction === right.fraction,
	);
	for (const entry of remainders) {
		if (remaining <= 0) break;
		slices[entry.index]! += 1;
		remaining -= 1;
	}
	return slices;
}

export function allocateEqualShare(
	candidates: readonly OrchestratorCandidate[],
	totalChunks: number,
): CandidateAllocation[] {
	if (!candidates.length || totalChunks <= 0) return [];
	const randomized = cryptographicShuffle(candidates);
	const active = randomized.slice(0, Math.min(randomized.length, totalChunks));
	const base = Math.floor(totalChunks / active.length);
	const remainder = totalChunks % active.length;
	return active.map((orchestrator, index) => ({
		orchestrator,
		chunkCount: base + (index < remainder ? 1 : 0),
	}));
}

export function buildQualifiedAllocation(
	candidates: readonly OrchestratorCandidate[],
	totalChunks: number,
): QualifiedAllocationPlan {
	const ordered = shuffleEqualRuns(
		candidates
			.filter((candidate) => safeScore(candidate.prismFinalScore) > 0)
			.sort((left, right) => safeScore(right.prismFinalScore) - safeScore(left.prismFinalScore)),
		(left, right) => safeScore(left.prismFinalScore) === safeScore(right.prismFinalScore),
	);
	const grouped = new Map<string, OrchestratorCandidate[]>();
	for (const candidate of ordered) {
		const key = candidate.ownerGroupId ?? `single:${candidate.id}`;
		const members = grouped.get(key) ?? [];
		members.push(candidate);
		grouped.set(key, members);
	}
	const groups = [...grouped.entries()].map(([key, members]) => ({
		ownerGroupId: key.startsWith("single:") ? null : key,
		members,
		averagePrismScore:
			members.reduce((sum, member) => sum + safeScore(member.prismFinalScore), 0) / members.length,
	}));
	const groupSlices = computePrismSlices(groups.map((group) => group.averagePrismScore), totalChunks);
	const allocations: CandidateAllocation[] = [];
	const ownerGroups = groups.map((group, groupIndex) => {
		const chunkCount = groupSlices[groupIndex] ?? 0;
		const memberSlices = computePrismSlices(
			group.members.map((member) => member.prismFinalScore),
			chunkCount,
		);
		const members = group.members.map((member, memberIndex) => {
			const memberChunkCount = memberSlices[memberIndex] ?? 0;
			if (memberChunkCount > 0) allocations.push({ orchestrator: member, chunkCount: memberChunkCount });
			return {
				hotkey: member.hotkey,
				prismFinalScore: safeScore(member.prismFinalScore),
				chunkCount: memberChunkCount,
			};
		});
		return {
			ownerGroupId: group.ownerGroupId,
			averagePrismScore: group.averagePrismScore,
			chunkCount,
			members,
		};
	});
	return {
		allocationRule: "prism_final_score_desc",
		allocationOrder: ordered.map((candidate) => candidate.hotkey),
		ownerGroups,
		allocations,
	};
}

export function allocateRecoveryChunks(input: {
	chunkIndices: readonly number[];
	candidates: readonly OrchestratorCandidate[];
	targetChunkCounts: readonly number[];
	excludedOrchestratorIdsByChunk: ReadonlyMap<number, readonly string[]>;
	excludedOwnerGroupIdsByChunk: ReadonlyMap<number, readonly string[]>;
}): RecoveryChunkAssignment[] {
	const assigned = new Map<string, number>();
	const output: RecoveryChunkAssignment[] = [];
	for (const chunkIndex of cryptographicShuffle([...new Set(input.chunkIndices)])) {
		const excludedIds = new Set(input.excludedOrchestratorIdsByChunk.get(chunkIndex) ?? []);
		const excludedGroups = new Set(input.excludedOwnerGroupIdsByChunk.get(chunkIndex) ?? []);
		const eligible = input.candidates.filter((candidate) =>
			!excludedIds.has(candidate.id)
			&& (candidate.ownerGroupId == null || !excludedGroups.has(candidate.ownerGroupId)),
		);
		if (!eligible.length) continue;
		let lowestLoad = Number.POSITIVE_INFINITY;
		let leastLoaded: OrchestratorCandidate[] = [];
		for (const candidate of eligible) {
			const index = input.candidates.indexOf(candidate);
			const target = Math.max(1, input.targetChunkCounts[index] ?? 0);
			const load = ((assigned.get(candidate.id) ?? 0) + 1) / target;
			if (load < lowestLoad) {
				lowestLoad = load;
				leastLoaded = [candidate];
			} else if (load === lowestLoad) {
				leastLoaded.push(candidate);
			}
		}
		const winner = leastLoaded[randomInt(leastLoaded.length)]!;
		assigned.set(winner.id, (assigned.get(winner.id) ?? 0) + 1);
		output.push({ chunkIndex, orchestrator: winner });
	}
	return output;
}

export function randomizeTaskOfferWaveBatches(
	batches: readonly TaskOfferWaveBatch[],
): { participantOrder: string[]; batches: TaskOfferWaveBatch[] } {
	const byParticipant = new Map<string, TaskOfferWaveBatch[]>();
	for (const batch of batches) {
		const participantBatches = byParticipant.get(batch.orchestratorHotkey) ?? [];
		participantBatches.push(batch);
		byParticipant.set(batch.orchestratorHotkey, participantBatches);
	}
	const participantOrder = cryptographicShuffle([...byParticipant.keys()]);
	const ordered: TaskOfferWaveBatch[] = [];
	const rounds = Math.max(0, ...participantOrder.map((hotkey) => byParticipant.get(hotkey)?.length ?? 0));
	for (let round = 0; round < rounds; round += 1) {
		for (const hotkey of participantOrder) {
			const batch = byParticipant.get(hotkey)?.[round];
			if (batch) ordered.push(batch);
		}
	}
	return { participantOrder, batches: ordered };
}

function roundRobinPreparedPayloads(
	prepared: readonly PreparedTaskOfferBatch[],
	participantOrder: readonly string[],
): PreparedTaskOfferPayload[] {
	const byParticipant = new Map<string, PreparedTaskOfferPayload[]>();
	for (const batch of prepared) {
		const payloads = byParticipant.get(batch.batch.orchestratorHotkey) ?? [];
		payloads.push(...batch.payloads);
		byParticipant.set(batch.batch.orchestratorHotkey, payloads);
	}
	const ordered: PreparedTaskOfferPayload[] = [];
	const rounds = Math.max(0, ...participantOrder.map((hotkey) => byParticipant.get(hotkey)?.length ?? 0));
	for (let round = 0; round < rounds; round += 1) {
		for (const hotkey of participantOrder) {
			const payload = byParticipant.get(hotkey)?.[round];
			if (payload) ordered.push(payload);
		}
	}
	return ordered;
}

function publishPreparedBurst(
	payloads: readonly PreparedTaskOfferPayload[],
	transport: PreparedWaveTransport,
): {
	results: PublishResult[];
	publishLoopMs: number;
	publishSpreadMs: number;
	flush: () => Promise<{ attempted: boolean; ok: boolean; flushMs: number }>;
} {
	const results = new Array<PublishResult>(payloads.length);
	let firstSentMonotonicMs: number | undefined;
	let lastSentMonotonicMs: number | undefined;
	const publishLoopStartedAt = performance.now();

	for (let index = 0; index < payloads.length; index += 1) {
		const payload = payloads[index]!;
		const offeredAt = new Date();
		const sentMonotonicMs = performance.now();
		try {
			transport.publish(payload.subject, payload.encodedPayload);
			results[index] = {
				payloadId: payload.payloadId,
				batchId: payload.batchId,
				orchestratorHotkey: payload.orchestratorHotkey,
				published: true,
				offeredAt,
				sentMonotonicMs,
				publishMs: performance.now() - sentMonotonicMs,
			};
			firstSentMonotonicMs ??= sentMonotonicMs;
			lastSentMonotonicMs = sentMonotonicMs;
		} catch {
			results[index] = {
				payloadId: payload.payloadId,
				batchId: payload.batchId,
				orchestratorHotkey: payload.orchestratorHotkey,
				published: false,
				reason: "nats_publish_failed",
				publishMs: performance.now() - sentMonotonicMs,
			};
		}
	}

	const publishLoopMs = performance.now() - publishLoopStartedAt;
	const publishSpreadMs = firstSentMonotonicMs === undefined || lastSentMonotonicMs === undefined
		? 0
		: Math.max(0, lastSentMonotonicMs - firstSentMonotonicMs);
	let flushPromise: Promise<{ attempted: boolean; ok: boolean; flushMs: number }> | null = null;
	return {
		results,
		publishLoopMs,
		publishSpreadMs,
		flush: () => {
			if (flushPromise) return flushPromise;
			if (!results.some((result) => result.published)) {
				flushPromise = Promise.resolve({ attempted: false, ok: false, flushMs: 0 });
				return flushPromise;
			}
			const startedAt = performance.now();
			flushPromise = transport.flush()
				.then(() => ({ attempted: true, ok: true, flushMs: performance.now() - startedAt }))
				.catch(() => ({ attempted: true, ok: false, flushMs: performance.now() - startedAt }));
			return flushPromise;
		},
	};
}

function preflightFailureReason(
	payload: PreparedTaskOfferPayload,
	dependencies: DispatchWaveDependencies,
	now: Date,
): string | null {
	if (!dependencies.transport.isReady()) return "nats_transport_unavailable";
	if (!dependencies.transport.isParticipantRegistered(payload.orchestratorHotkey)) {
		return "participant_not_registered";
	}
	if (payload.encodedPayload.byteLength > dependencies.maxPayloadBytes) return "payload_too_large";
	if (
		payload.signedUrlsExpireAt !== null
		&& payload.signedUrlsExpireAt.getTime() - now.getTime() < dependencies.minimumSignedUrlTtlMs
	) {
		return "signed_url_expired_or_stale";
	}
	return null;
}

export async function dispatchParticipantNeutralWave(
	batches: readonly TaskOfferWaveBatch[],
	dependencies: DispatchWaveDependencies,
): Promise<WaveDispatchResult> {
	const waveId = randomUUID();
	const waveKind = batches[0]?.waveKind ?? "initial";
	const waveStartedAt = performance.now();
	const randomized = randomizeTaskOfferWaveBatches(batches);
	const settled = await Promise.allSettled(
		randomized.batches.map((batch) => dependencies.prepareBatch(batch)),
	);
	const prepared: PreparedTaskOfferBatch[] = [];
	const failures: WaveFailure[] = [];
	const failedPreparationBatchIds: string[] = [];
	for (let index = 0; index < settled.length; index += 1) {
		const result = settled[index]!;
		const batch = randomized.batches[index]!;
		if (result.status === "fulfilled") {
			prepared.push(result.value);
		} else {
			failedPreparationBatchIds.push(batch.batchId);
			failures.push({
				stage: "preparation",
				batchId: batch.batchId,
				reason: "batch_preparation_failed",
				deliveryUncertain: false,
			});
		}
	}
	if (failedPreparationBatchIds.length) {
		dependencies.registry.settleUndispatchedBatches(
			failedPreparationBatchIds,
			"batch_preparation_failed",
		);
	}
	const preparationMs = performance.now() - waveStartedAt;
	const orderedPayloads = roundRobinPreparedPayloads(prepared, randomized.participantOrder);
	const registryFailures = new Map(
		dependencies.registry.preflight(orderedPayloads).map((failure) => [failure.payloadId, failure.reason]),
	);
	const preflightAt = new Date();
	const publishable: PreparedTaskOfferPayload[] = [];
	for (const payload of orderedPayloads) {
		const reason = registryFailures.get(payload.payloadId)
			?? preflightFailureReason(payload, dependencies, preflightAt);
		if (reason) {
			failures.push({
				stage: "preflight",
				batchId: payload.batchId,
				payloadId: payload.payloadId,
				reason,
				deliveryUncertain: false,
			});
		} else {
			publishable.push(payload);
		}
	}

	const burst = publishPreparedBurst(publishable, dependencies.transport);
	for (const result of burst.results) {
		if (!result.published) {
			failures.push({
				stage: "publish",
				batchId: result.batchId,
				payloadId: result.payloadId,
				reason: result.reason ?? "nats_publish_failed",
				deliveryUncertain: false,
			});
		}
	}

	const activationStartedAt = performance.now();
	const activation = dependencies.registry.bulkActivate(
		burst.results.flatMap((result) =>
			result.published && result.offeredAt
				? [{ payloadId: result.payloadId, batchId: result.batchId, offeredAt: result.offeredAt }]
				: [],
		),
	);
	const activationMs = performance.now() - activationStartedAt;
	for (const failure of activation.failures) {
		failures.push({
			stage: "activation",
			payloadId: failure.payloadId,
			reason: failure.reason,
			deliveryUncertain: true,
		});
	}

	const certainlyUndispatched = [...new Set(failures
		.filter((failure) => !failure.deliveryUncertain && failure.payloadId)
		.map((failure) => failure.payloadId!))];
	if (certainlyUndispatched.length) {
		dependencies.registry.settleUndispatched(certainlyUndispatched, "wave_dispatch_failed_before_send");
	}

	const flush = await burst.flush();
	if (!flush.ok && burst.results.some((result) => result.published)) {
		failures.push({
			stage: "flush",
			reason: "nats_flush_failed",
			deliveryUncertain: true,
		});
	}

	return {
		waveId,
		waveKind,
		participantOrder: randomized.participantOrder,
		preparationMs,
		publishLoopMs: burst.publishLoopMs,
		publishSpreadMs: burst.publishSpreadMs,
		activationMs,
		flushAttempted: flush.attempted,
		flushMs: flush.flushMs,
		flushOk: flush.ok,
		payloadOrder: publishable.map((payload) => ({
			payloadId: payload.payloadId,
			batchId: payload.batchId,
			orchestratorHotkey: payload.orchestratorHotkey,
			encodedBytes: payload.encodedPayload.byteLength,
		})),
		results: burst.results,
		failures,
	};
}
