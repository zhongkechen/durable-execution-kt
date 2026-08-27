package io.github.zhongkechen.durable.internal

import io.github.zhongkechen.durable.ChildFailureException
import io.github.zhongkechen.durable.ChildOptions
import io.github.zhongkechen.durable.CallbackFailureException
import io.github.zhongkechen.durable.CallbackHandle
import io.github.zhongkechen.durable.CallbackOptions
import io.github.zhongkechen.durable.CallbackSubmitterScope
import io.github.zhongkechen.durable.CallbackWaitOptions
import io.github.zhongkechen.durable.ConditionDecision
import io.github.zhongkechen.durable.ConditionFailureException
import io.github.zhongkechen.durable.ConditionOptions
import io.github.zhongkechen.durable.ConditionScope
import io.github.zhongkechen.durable.DeliverySemantics
import io.github.zhongkechen.durable.DurableFuture
import io.github.zhongkechen.durable.DurableLogger
import io.github.zhongkechen.durable.InvokeFailureException
import io.github.zhongkechen.durable.InvokeOptions
import io.github.zhongkechen.durable.JsonSerde
import io.github.zhongkechen.durable.MapOptions
import io.github.zhongkechen.durable.MapResult
import io.github.zhongkechen.durable.ItemResult
import io.github.zhongkechen.durable.Nesting
import io.github.zhongkechen.durable.ParallelOptions
import io.github.zhongkechen.durable.ParallelResult
import io.github.zhongkechen.durable.RetryDecision
import io.github.zhongkechen.durable.Serde
import io.github.zhongkechen.durable.StepFailureException
import io.github.zhongkechen.durable.StepInterruptedException
import io.github.zhongkechen.durable.StepOptions
import io.github.zhongkechen.durable.StepScope
import io.github.zhongkechen.durable.TypeRef
import io.github.zhongkechen.durable.typeRef
import java.time.Instant
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CompletableDeferred

internal class ExecutionSuspended(
    val operationId: String,
    val resumeAt: Instant? = null,
) : Error("Execution suspended at operation $operationId")

/**
 * Executes operations inside one deterministic context.
 */
internal class OperationRuntime(
    val executionArn: String,
    val isReplaying: Boolean,
    private val ledger: ReplayLedger,
    private val checkpoints: CheckpointCoordinator,
    private val parentId: String? = null,
    private val ids: OperationIdSequence = OperationIdSequence(parentId),
    private val defaultSerde: Serde = JsonSerde(),
) {
    val logger: DurableLogger = DurableLogger(executionArn, parentId)

    suspend fun <T> step(
        name: String?,
        type: TypeRef<T>,
        options: StepOptions = StepOptions(),
        block: suspend StepScope.() -> T,
    ): T {
        val identity = reserve(name, OperationKind.STEP, "Step")
        val existing = ledger.find(identity)
        val attempt = (existing?.attempt ?: 0) + 1

        when (existing?.status) {
            CheckpointStatus.SUCCEEDED ->
                return decodeResult(existing, type, options.serde)
            CheckpointStatus.FAILED,
            CheckpointStatus.TIMED_OUT,
            CheckpointStatus.STOPPED,
            CheckpointStatus.CANCELLED,
            -> throw stepFailure(existing)
            CheckpointStatus.PENDING ->
                throw ExecutionSuspended(identity.id, existing.nextAttemptAt)
            CheckpointStatus.STARTED -> {
                if (options.delivery == DeliverySemantics.AT_MOST_ONCE_PER_RETRY) {
                    return failStep(
                        identity,
                        StepInterruptedException(identity.id),
                        attempt,
                        options,
                    )
                }
            }
            CheckpointStatus.READY,
            null,
            -> checkpoints.checkpoint(
                CheckpointCommand(identity, CheckpointAction.START),
            )
            CheckpointStatus.UNKNOWN ->
                error("Operation ${identity.id} has an unsupported checkpoint status")
        }

        return try {
            val value =
                block(
                    StepScopeImpl(
                        attempt,
                        DurableLogger(executionArn, identity.id, identity.name, attempt),
                    ),
                )
            val serde = options.serde ?: defaultSerde
            val payload = serde.encode(value)
            val normalized = serde.decode(payload, type)
            val replayChildren = payload.encodeToByteArray().size >= LARGE_CONTEXT_RESULT_BYTES
            checkpoints.checkpoint(
                CheckpointCommand(
                    identity = identity,
                    action = CheckpointAction.SUCCEED,
                    payload = if (replayChildren) "" else payload,
                    replayChildren = replayChildren,
                ),
            )
            normalized
        } catch (suspension: ExecutionSuspended) {
            throw suspension
        } catch (error: Throwable) {
            failStep(identity, error, attempt, options)
        }
    }

    suspend fun wait(
        duration: Duration,
        name: String?,
    ) {
        require(duration.isPositive()) { "Wait duration must be positive" }
        val identity = reserve(name, OperationKind.WAIT, "Wait")
        val existing = ledger.find(identity)
        when (existing?.status) {
            CheckpointStatus.SUCCEEDED -> return
            CheckpointStatus.STARTED,
            CheckpointStatus.PENDING,
            CheckpointStatus.READY,
            -> throw ExecutionSuspended(identity.id, existing.nextAttemptAt)
            CheckpointStatus.FAILED,
            CheckpointStatus.TIMED_OUT,
            CheckpointStatus.STOPPED,
            CheckpointStatus.CANCELLED,
            -> error("Wait ${identity.id} ended with ${existing.status}")
            CheckpointStatus.UNKNOWN ->
                error("Wait ${identity.id} has an unsupported checkpoint status")
            null -> {
                checkpoints.checkpoint(
                    CheckpointCommand(
                        identity = identity,
                        action = CheckpointAction.START,
                        waitDuration = duration,
                    ),
                )
                throw ExecutionSuspended(identity.id)
            }
        }
    }

    suspend fun <I, O> invoke(
        name: String,
        functionName: String,
        input: I,
        outputType: TypeRef<O>,
        options: InvokeOptions = InvokeOptions(),
    ): O {
        val identity = reserve(name, OperationKind.INVOKE, "ChainedInvoke")
        val existing = ledger.find(identity)
        when (existing?.status) {
            CheckpointStatus.SUCCEEDED ->
                return decodeResult(existing, outputType, options.resultSerde)
            CheckpointStatus.FAILED,
            CheckpointStatus.TIMED_OUT,
            CheckpointStatus.STOPPED,
            CheckpointStatus.CANCELLED,
            -> throw InvokeFailureException(
                identity.id,
                RuntimeException(existing.error?.message ?: "Checkpointed invocation failure"),
            )
            CheckpointStatus.STARTED,
            CheckpointStatus.PENDING,
            CheckpointStatus.READY,
            -> throw ExecutionSuspended(identity.id)
            CheckpointStatus.UNKNOWN ->
                error("Invocation ${identity.id} has an unsupported checkpoint status")
            null -> {
                val payload = (options.payloadSerde ?: defaultSerde).encode(input)
                checkpoints.checkpoint(
                    CheckpointCommand(
                        identity = identity,
                        action = CheckpointAction.START,
                        payload = payload,
                        targetFunction = functionName,
                        tenantId = options.tenantId,
                    ),
                )
                throw ExecutionSuspended(identity.id)
            }
        }
    }

    suspend fun <T> callback(
        name: String?,
        type: TypeRef<T>,
        options: CallbackOptions = CallbackOptions(),
    ): CallbackHandle<T> {
        val identity = reserve(name, OperationKind.CALLBACK, "Callback")
        var existing = ledger.find(identity)
        if (existing == null) {
            checkpoints.checkpoint(
                CheckpointCommand(
                    identity = identity,
                    action = CheckpointAction.START,
                    callbackTimeout = options.timeout,
                    heartbeatTimeout = options.heartbeatTimeout,
                ),
            )
            existing = ledger.snapshot()[identity.id]
        }
        val callbackId =
            existing?.callbackId
                ?: error("Callback ${identity.id} did not receive a callback ID")
        return RuntimeCallback(
            identity = identity,
            id = callbackId,
            type = type,
            serde = options.serde ?: defaultSerde,
            ledger = ledger,
        )
    }

    suspend fun <T> waitForCallback(
        name: String?,
        type: TypeRef<T>,
        options: CallbackWaitOptions = CallbackWaitOptions(),
        submitter: suspend CallbackSubmitterScope.() -> Unit,
    ): T {
        val identity = reserve(name, OperationKind.CONTEXT, "WaitForCallback")
        val existing = ledger.find(identity)
        if (existing?.status == CheckpointStatus.SUCCEEDED) {
            return decodeResult(existing, type, options.callback.serde)
        }
        if (existing != null && existing.status.terminal) {
            throw CallbackFailureException(
                identity.id,
                RuntimeException(existing.error?.message ?: "Checkpointed callback wait failure"),
            )
        }
        if (existing == null) {
            checkpoints.checkpoint(CheckpointCommand(identity, CheckpointAction.START))
        }

        val childRuntime =
            OperationRuntime(
                executionArn = executionArn,
                isReplaying = existing != null,
                ledger = ledger,
                checkpoints = checkpoints,
                parentId = identity.id,
                ids = OperationIdSequence(identity.id),
                defaultSerde = defaultSerde,
            )
        return try {
            val callback = childRuntime.callback(null, type, options.callback)
            childRuntime.step(
                name = null,
                type = typeRef<Unit>(),
                options = options.submitter,
            ) {
                submitter(
                    CallbackSubmitterScopeImpl(
                        callback.id,
                        attempt,
                        DurableLogger(executionArn, identity.id, name, attempt),
                    ),
                )
            }
            val result = callback.await()
            val serde = options.callback.serde ?: defaultSerde
            val payload = serde.encode(result)
            val normalized = serde.decode(payload, type)
            checkpoints.checkpoint(
                CheckpointCommand(
                    identity = identity,
                    action = CheckpointAction.SUCCEED,
                    payload = payload,
                ),
            )
            normalized
        } catch (suspension: ExecutionSuspended) {
            throw suspension
        } catch (error: Throwable) {
            checkpoints.checkpoint(
                CheckpointCommand(
                    identity = identity,
                    action = CheckpointAction.FAIL,
                    error = error.toCheckpointError(),
                ),
            )
            throw CallbackFailureException(identity.id, error)
        }
    }

    suspend fun <T> waitForCondition(
        name: String?,
        type: TypeRef<T>,
        options: ConditionOptions<T>,
        check: suspend ConditionScope<T>.() -> ConditionDecision<T>,
    ): T {
        val identity = reserve(name, OperationKind.STEP, "WaitForCondition")
        val existing = ledger.find(identity)
        when (existing?.status) {
            CheckpointStatus.SUCCEEDED ->
                return decodeResult(existing, type, options.serde)
            CheckpointStatus.FAILED,
            CheckpointStatus.TIMED_OUT,
            CheckpointStatus.STOPPED,
            CheckpointStatus.CANCELLED,
            -> throw ConditionFailureException(
                identity.id,
                RuntimeException(existing.error?.message ?: "Checkpointed condition failure"),
            )
            CheckpointStatus.PENDING ->
                throw ExecutionSuspended(identity.id, existing.nextAttemptAt)
            CheckpointStatus.STARTED,
            CheckpointStatus.READY,
            null,
            -> Unit
            CheckpointStatus.UNKNOWN ->
                error("Condition ${identity.id} has an unsupported checkpoint status")
        }

        val attempt = (existing?.attempt ?: 0) + 1
        val state =
            if (existing?.resultPayload != null) {
                (options.serde ?: defaultSerde).decode(existing.resultPayload, type)
            } else {
                options.initialState
            }
        @Suppress("UNCHECKED_CAST")
        val currentState = state as T

        if (existing == null || existing.status == CheckpointStatus.READY) {
            checkpoints.checkpoint(CheckpointCommand(identity, CheckpointAction.START))
        }

        return try {
            when (
                val decision =
                    check(
                        ConditionScopeImpl(
                            currentState,
                            attempt,
                            DurableLogger(executionArn, identity.id, identity.name, attempt),
                        ),
                    )
            ) {
                is ConditionDecision.Complete -> {
                    val serde = options.serde ?: defaultSerde
                    val payload = serde.encode(decision.result)
                    val normalized = serde.decode(payload, type)
                    checkpoints.checkpoint(
                        CheckpointCommand(
                            identity = identity,
                            action = CheckpointAction.SUCCEED,
                            payload = payload,
                        ),
                    )
                    normalized
                }
                is ConditionDecision.Continue -> {
                    if (options.maximumAttempts != null && attempt >= options.maximumAttempts) {
                        throw IllegalStateException(
                            "Condition ${identity.id} exhausted ${options.maximumAttempts} attempts",
                        )
                    }
                    val serde = options.serde ?: defaultSerde
                    val payload = serde.encode(decision.state)
                    val normalized = serde.decode(payload, type)
                    val delay = options.delay(normalized, attempt)
                    require(delay.isPositive()) { "Condition retry delay must be positive" }
                    val resumeAt = Instant.now().plusMillis(delay.inWholeMilliseconds)
                    checkpoints.checkpoint(
                        CheckpointCommand(
                            identity = identity,
                            action = CheckpointAction.RETRY,
                            payload = payload,
                            retryDelay = delay,
                        ),
                    )
                    throw ExecutionSuspended(identity.id, resumeAt)
                }
            }
        } catch (suspension: ExecutionSuspended) {
            throw suspension
        } catch (error: Throwable) {
            checkpoints.checkpoint(
                CheckpointCommand(
                    identity = identity,
                    action = CheckpointAction.FAIL,
                    error = error.toCheckpointError(),
                ),
            )
            throw ConditionFailureException(identity.id, error)
        }
    }

    suspend fun <T> child(
        name: String,
        type: TypeRef<T>,
        options: ChildOptions = ChildOptions(),
        block: suspend OperationRuntime.() -> T,
    ): T {
        val identity = reserve(name, OperationKind.CONTEXT, "RunInChildContext")
        return runContext(identity, type, options, block)
    }

    suspend fun <I, O> map(
        name: String?,
        items: Collection<I>,
        outputType: TypeRef<O>,
        options: MapOptions<I> = MapOptions(),
        block: suspend OperationRuntime.(item: I, index: Int) -> O,
    ): MapResult<O> {
        val identity = reserve(name, OperationKind.CONTEXT, "Map")
        val existing = ledger.find(identity)
        if (existing?.status == CheckpointStatus.SUCCEEDED && !existing.replayChildren) {
            if (options.resultSerde != null) {
                @Suppress("UNCHECKED_CAST")
                return options.resultSerde
                    .decode(existing.resultPayload ?: error("Map result payload is missing"), typeRef<MapResult<Any?>>())
                    as MapResult<O>
            }
            return decodeMap(existing, outputType, options)
        }
        if (existing != null && existing.status.terminal && existing.status != CheckpointStatus.SUCCEEDED) {
            throw childFailure(existing)
        }
        if (existing == null) {
            checkpoints.checkpoint(CheckpointCommand(identity, CheckpointAction.START))
        }

        val itemIds = OperationIdSequence(identity.id)
        val work =
            items.mapIndexed { index, item ->
                val itemName = options.itemName?.invoke(item, index)
                val itemIdentity =
                    OperationIdentity(
                        id = itemIds.next(),
                        name = itemName,
                        kind = OperationKind.CONTEXT,
                        subtype = "MapIteration",
                        parentId = identity.id,
                    )
                BatchWork(index, itemName) {
                    if (options.nesting == Nesting.FLAT) {
                        val flatRuntime =
                            OperationRuntime(
                                executionArn = executionArn,
                                isReplaying = ledger.snapshot().containsKey(itemIdentity.id),
                                ledger = ledger,
                                checkpoints = checkpoints,
                                parentId = identity.id,
                                ids = OperationIdSequence(itemIdentity.id),
                                defaultSerde = defaultSerde,
                            )
                        block(flatRuntime, item, index)
                    } else {
                        runContext(
                            identity = itemIdentity,
                            type = outputType,
                            options = ChildOptions(serde = options.itemSerde),
                        ) {
                            block(this, item, index)
                        }
                    }
                }
            }
        val maximumConcurrency = options.maximumConcurrency ?: maxOf(1, work.size)
        val outcome = executeBatch(work, maximumConcurrency, options.completion)
        val result = MapResult(outcome.completion, outcome.items)
        if (existing?.replayChildren != true) {
            val payload =
                options.resultSerde?.encode(result)
                    ?: defaultSerde.encode(encodeMap(result, options))
            val replayChildren = payload.encodeToByteArray().size >= LARGE_CONTEXT_RESULT_BYTES
            checkpoints.checkpoint(
                CheckpointCommand(
                    identity = identity,
                    action = CheckpointAction.SUCCEED,
                    payload = if (replayChildren) "" else payload,
                    replayChildren = replayChildren,
                ),
            )
        }
        return result
    }

    suspend fun parallel(
        name: String,
        options: ParallelOptions = ParallelOptions(),
        register: RuntimeParallelScope.() -> Unit,
    ): ParallelResult {
        val branches = RuntimeParallelScope().apply(register)
        val identity = reserve(name, OperationKind.CONTEXT, "Parallel")
        val existing = ledger.find(identity)
        if (existing?.status == CheckpointStatus.SUCCEEDED && !existing.replayChildren) {
            val result = decodeParallel(existing, branches)
            branches.complete(result.items)
            return result
        }
        if (existing != null && existing.status.terminal && existing.status != CheckpointStatus.SUCCEEDED) {
            throw childFailure(existing)
        }
        if (existing == null) {
            checkpoints.checkpoint(CheckpointCommand(identity, CheckpointAction.START))
        }

        val branchIds = OperationIdSequence(identity.id)
        val work =
            branches.registered.mapIndexed { index, branch ->
                val branchIdentity =
                    OperationIdentity(
                        id = branchIds.next(),
                        name = branch.name,
                        kind = OperationKind.CONTEXT,
                        subtype = "ParallelBranch",
                        parentId = identity.id,
                    )
                BatchWork<Any?>(index, branch.name) {
                    if (options.nesting == Nesting.FLAT) {
                        val flatRuntime =
                            OperationRuntime(
                                executionArn = executionArn,
                                isReplaying = ledger.snapshot().containsKey(branchIdentity.id),
                                ledger = ledger,
                                checkpoints = checkpoints,
                                parentId = identity.id,
                                ids = OperationIdSequence(branchIdentity.id),
                                defaultSerde = defaultSerde,
                            )
                        branch.execute(flatRuntime)
                    } else {
                        runContext(
                            identity = branchIdentity,
                            type = branch.type,
                            options = ChildOptions(serde = branch.serde ?: options.itemSerde),
                        ) {
                            branch.execute(this)
                        }
                    }
                }
            }
        val maximumConcurrency = options.maximumConcurrency ?: maxOf(1, work.size)
        val outcome = executeBatch(work, maximumConcurrency, options.completion)
        val result = ParallelResult(outcome.completion, outcome.items)
        branches.complete(result.items)
        checkpoints.checkpoint(
            CheckpointCommand(
                identity = identity,
                action = CheckpointAction.SUCCEED,
                payload = defaultSerde.encode(encodeParallel(result, branches, options)),
            ),
        )
        return result
    }

    private suspend fun <T> runContext(
        identity: OperationIdentity,
        type: TypeRef<T>,
        options: ChildOptions,
        block: suspend OperationRuntime.() -> T,
    ): T {
        val existing = ledger.find(identity)
        if (existing?.status == CheckpointStatus.SUCCEEDED && !existing.replayChildren) {
            return decodeResult(existing, type, options.serde)
        }
        if (existing != null && existing.status.terminal && existing.status != CheckpointStatus.SUCCEEDED) {
            throw childFailure(existing)
        }
        if (existing != null &&
            existing.status != CheckpointStatus.STARTED &&
            !(existing.status == CheckpointStatus.SUCCEEDED && existing.replayChildren)
        ) {
            error("Child context ${identity.id} has unsupported status ${existing.status}")
        }

        if (existing == null && !options.virtual) {
            checkpoints.checkpoint(
                CheckpointCommand(identity, CheckpointAction.START),
            )
        }

        val childRuntime =
            OperationRuntime(
                executionArn = executionArn,
                isReplaying = existing != null,
                ledger = ledger,
                checkpoints = checkpoints,
                parentId = if (options.virtual) parentId else identity.id,
                ids = OperationIdSequence(if (options.virtual) parentId else identity.id),
                defaultSerde = defaultSerde,
            )

        return try {
            val value = block(childRuntime)
            if (options.virtual || existing?.replayChildren == true) return value
            val serde = options.serde ?: defaultSerde
            val payload = serde.encode(value)
            val normalized = serde.decode(payload, type)
            checkpoints.checkpoint(
                CheckpointCommand(
                    identity = identity,
                    action = CheckpointAction.SUCCEED,
                    payload = payload,
                ),
            )
            normalized
        } catch (suspension: ExecutionSuspended) {
            throw suspension
        } catch (error: Throwable) {
            if (!options.virtual) {
                checkpoints.checkpoint(
                    CheckpointCommand(
                        identity = identity,
                        action = CheckpointAction.FAIL,
                        error = error.toCheckpointError(),
                    ),
                )
            }
            throw ChildFailureException(identity.id, error)
        }
    }

    private fun reserve(
        name: String?,
        kind: OperationKind,
        subtype: String,
    ): OperationIdentity =
        OperationIdentity(
            id = ids.next(),
            name = name,
            kind = kind,
            subtype = subtype,
            parentId = parentId,
        )

    private suspend fun <T> failStep(
        identity: OperationIdentity,
        error: Throwable,
        attempt: Int,
        options: StepOptions,
    ): T =
        when (val decision = options.retry.decide(error, attempt)) {
            RetryDecision.Fail -> {
                checkpoints.checkpoint(
                    CheckpointCommand(
                        identity = identity,
                        action = CheckpointAction.FAIL,
                        error = error.toCheckpointError(),
                    ),
                )
                throw StepFailureException(identity.id, error)
            }
            is RetryDecision.Retry -> {
                val delay = maxOf(decision.delay, 1.seconds)
                val resumeAt = Instant.now().plusMillis(delay.inWholeMilliseconds)
                checkpoints.checkpoint(
                    CheckpointCommand(
                        identity = identity,
                        action = CheckpointAction.RETRY,
                        error = error.toCheckpointError(),
                        retryDelay = delay,
                    ),
                )
                throw ExecutionSuspended(identity.id, resumeAt)
            }
        }

    private fun <T> decodeResult(
        record: OperationRecord,
        type: TypeRef<T>,
        serde: Serde?,
    ): T {
        val payload = record.resultPayload ?: "null"
        return (serde ?: defaultSerde).decode(payload, type)
    }

    private fun stepFailure(record: OperationRecord): StepFailureException =
        StepFailureException(
            record.identity.id,
            RuntimeException(record.error?.message ?: "Checkpointed step failure"),
        )

    private fun childFailure(record: OperationRecord): ChildFailureException =
        ChildFailureException(
            record.identity.id,
            RuntimeException(record.error?.message ?: "Checkpointed child failure"),
        )

    private fun <O> encodeMap(
        result: MapResult<O>,
        options: MapOptions<*>,
    ): BatchCheckpoint =
        BatchCheckpoint(
            completion = result.completion.name,
            items =
                result.items.map { item ->
                    when (item) {
                        is ItemResult.Success ->
                            BatchCheckpointItem(
                                index = item.index,
                                name = item.name,
                                status = "SUCCEEDED",
                                payload = (options.itemSerde ?: defaultSerde).encode(item.value),
                            )
                        is ItemResult.Failure ->
                            BatchCheckpointItem(
                                index = item.index,
                                name = item.name,
                                status = "FAILED",
                                error = item.error.message,
                            )
                        is ItemResult.Skipped ->
                            BatchCheckpointItem(
                                index = item.index,
                                name = item.name,
                                status = "SKIPPED",
                            )
                    }
                },
        )

    private fun <O> decodeMap(
        record: OperationRecord,
        outputType: TypeRef<O>,
        options: MapOptions<*>,
    ): MapResult<O> {
        val checkpoint =
            defaultSerde.decode(
                record.resultPayload ?: error("Map result payload is missing"),
                typeRef<BatchCheckpoint>(),
            )
        val itemSerde = options.itemSerde ?: defaultSerde
        val items =
            checkpoint.items.map { item ->
                when (item.status) {
                    "SUCCEEDED" ->
                        ItemResult.Success(
                            item.index,
                            item.name,
                            itemSerde.decode(item.payload ?: "null", outputType),
                        )
                    "FAILED" ->
                        ItemResult.Failure(
                            item.index,
                            item.name,
                            RuntimeException(item.error ?: "Checkpointed map item failure"),
                        )
                    else -> ItemResult.Skipped(item.index, item.name)
                }
            }
        return MapResult(
            completion =
                io.github.zhongkechen.durable.BatchCompletion.valueOf(checkpoint.completion),
            items = items,
        )
    }

    private fun encodeParallel(
        result: ParallelResult,
        branches: RuntimeParallelScope,
        options: ParallelOptions,
    ): BatchCheckpoint =
        BatchCheckpoint(
            completion = result.completion.name,
            items =
                result.items.map { item ->
                    val branch = branches.registered[item.index]
                    when (item) {
                        is ItemResult.Success ->
                            BatchCheckpointItem(
                                index = item.index,
                                name = item.name,
                                status = "SUCCEEDED",
                                payload = (branch.serde ?: options.itemSerde ?: defaultSerde).encode(item.value),
                            )
                        is ItemResult.Failure ->
                            BatchCheckpointItem(
                                index = item.index,
                                name = item.name,
                                status = "FAILED",
                                error = item.error.message,
                            )
                        is ItemResult.Skipped ->
                            BatchCheckpointItem(
                                index = item.index,
                                name = item.name,
                                status = "SKIPPED",
                            )
                    }
                },
        )

    private fun decodeParallel(
        record: OperationRecord,
        branches: RuntimeParallelScope,
    ): ParallelResult {
        val checkpoint =
            defaultSerde.decode(
                record.resultPayload ?: error("Parallel result payload is missing"),
                typeRef<BatchCheckpoint>(),
            )
        val items =
            checkpoint.items.map { item ->
                val branch = branches.registered[item.index]
                when (item.status) {
                    "SUCCEEDED" ->
                        ItemResult.Success(
                            item.index,
                            item.name,
                            (branch.serde ?: defaultSerde).decode(item.payload ?: "null", branch.type),
                        )
                    "FAILED" ->
                        ItemResult.Failure(
                            item.index,
                            item.name,
                            RuntimeException(item.error ?: "Checkpointed parallel branch failure"),
                        )
                    else -> ItemResult.Skipped(item.index, item.name)
                }
            }
        return ParallelResult(
            completion =
                io.github.zhongkechen.durable.BatchCompletion.valueOf(checkpoint.completion),
            items = items,
        )
    }

    private data class StepScopeImpl(
        override val attempt: Int,
        override val logger: DurableLogger,
    ) : StepScope

    private data class CallbackSubmitterScopeImpl(
        override val callbackId: String,
        override val attempt: Int,
        override val logger: DurableLogger,
    ) : CallbackSubmitterScope

    private data class ConditionScopeImpl<T>(
        override val state: T,
        override val attempt: Int,
        override val logger: DurableLogger,
    ) : ConditionScope<T>
}

private data class BatchCheckpoint(
    val completion: String,
    val items: List<BatchCheckpointItem>,
)

private data class BatchCheckpointItem(
    val index: Int,
    val name: String?,
    val status: String,
    val payload: String? = null,
    val error: String? = null,
)

internal class RuntimeParallelScope {
    internal data class RegisteredBranch(
        val name: String,
        val type: TypeRef<Any?>,
        val serde: Serde?,
        val execute: suspend OperationRuntime.() -> Any?,
        val completion: CompletableDeferred<Any?>,
    )

    internal val registered: MutableList<RegisteredBranch> = mutableListOf()

    fun <T> branch(
        name: String,
        type: TypeRef<T>,
        serde: Serde? = null,
        block: suspend OperationRuntime.() -> T,
    ): DurableFuture<T> {
        val completion = CompletableDeferred<Any?>()
        @Suppress("UNCHECKED_CAST")
        registered +=
            RegisteredBranch(
                name = name,
                type = type as TypeRef<Any?>,
                serde = serde,
                execute = block as suspend OperationRuntime.() -> Any?,
                completion = completion,
            )
        return RuntimeFuture(completion)
    }

    internal fun complete(items: List<ItemResult<Any?>>) {
        items.forEach { item ->
            val completion = registered[item.index].completion
            when (item) {
                is ItemResult.Success -> completion.complete(item.value)
                is ItemResult.Failure -> completion.completeExceptionally(item.error)
                is ItemResult.Skipped ->
                    completion.completeExceptionally(
                        IllegalStateException("Parallel branch ${item.name ?: item.index} was skipped"),
                    )
            }
        }
    }
}

private class RuntimeFuture<T>(
    private val deferred: CompletableDeferred<Any?>,
) : DurableFuture<T> {
    @Suppress("UNCHECKED_CAST")
    override suspend fun await(): T = deferred.await() as T
}

private class RuntimeCallback<T>(
    private val identity: OperationIdentity,
    override val id: String,
    private val type: TypeRef<T>,
    private val serde: Serde,
    private val ledger: ReplayLedger,
) : CallbackHandle<T> {
    override suspend fun await(): T {
        val record = ledger.snapshot()[identity.id]
            ?: error("Callback ${identity.id} is missing from checkpoint state")
        return when (record.status) {
            CheckpointStatus.SUCCEEDED -> serde.decode(record.resultPayload ?: "null", type)
            CheckpointStatus.FAILED,
            CheckpointStatus.TIMED_OUT,
            CheckpointStatus.STOPPED,
            CheckpointStatus.CANCELLED,
            -> throw CallbackFailureException(
                identity.id,
                RuntimeException(record.error?.message ?: "Checkpointed callback failure"),
            )
            CheckpointStatus.STARTED,
            CheckpointStatus.PENDING,
            CheckpointStatus.READY,
            -> throw ExecutionSuspended(identity.id)
            CheckpointStatus.UNKNOWN ->
                error("Callback ${identity.id} has an unsupported checkpoint status")
        }
    }
}

private fun Throwable.toCheckpointError(): CheckpointError =
    CheckpointError(
        type = this::class.qualifiedName,
        message = message,
        stack =
            stackTrace.map { frame ->
                "${frame.className}|${frame.methodName}|${frame.fileName.orEmpty()}|${frame.lineNumber}"
            },
    )

private const val LARGE_CONTEXT_RESULT_BYTES: Int = 256 * 1024
