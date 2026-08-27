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
import io.github.zhongkechen.durable.FunctionAttemptEnded
import io.github.zhongkechen.durable.FunctionAttemptStarted
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
import io.github.zhongkechen.durable.extension.ExtensionCallbackConfig
import io.github.zhongkechen.durable.extension.ExtensionChildOperation
import io.github.zhongkechen.durable.extension.ExtensionContextConfig
import io.github.zhongkechen.durable.extension.ExtensionContextFailure
import io.github.zhongkechen.durable.extension.ExtensionContextReplay
import io.github.zhongkechen.durable.extension.ExtensionContextResult
import io.github.zhongkechen.durable.extension.ExtensionInvokeConfig
import io.github.zhongkechen.durable.extension.ExtensionRetryDecision
import io.github.zhongkechen.durable.extension.ExtensionStepConfig
import io.github.zhongkechen.durable.extension.ExtensionStepResult
import io.github.zhongkechen.durable.typeRef
import java.time.Instant
import java.util.concurrent.CancellationException
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.Deferred

internal class ExecutionSuspended(
    val operationId: String,
    val resumeAt: Instant? = null,
) : Error("Execution suspended at operation $operationId")

internal data class ReservedOperation(
    val id: String,
    val name: String?,
    val parentId: String?,
) {
    fun identity(
        kind: OperationKind,
        subtype: String,
    ): OperationIdentity =
        OperationIdentity(
            id = id,
            name = name,
            kind = kind,
            subtype = subtype,
            parentId = parentId,
        )
}

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
    private val plugins: PluginDispatcher = PluginDispatcher(emptyList()),
) {
    val logger: DurableLogger = DurableLogger(executionArn, parentId)

    suspend fun <T> step(
        name: String?,
        type: TypeRef<T>,
        options: StepOptions = StepOptions(),
        combineStartAndTerminal: Boolean = false,
        startedThisInvocation: Boolean = false,
        block: suspend StepScope.() -> T,
    ): T =
        step(
            identity = reserveOperation(name).identity(OperationKind.STEP, "Step"),
            type = type,
            options = options,
            combineStartAndTerminal = combineStartAndTerminal,
            startedThisInvocation = startedThisInvocation,
            block = block,
        )

    internal suspend fun <T> step(
        identity: OperationIdentity,
        type: TypeRef<T>,
        options: StepOptions = StepOptions(),
        combineStartAndTerminal: Boolean = false,
        startedThisInvocation: Boolean = false,
        notifyObservation: Boolean = true,
        block: suspend StepScope.() -> T,
    ): T {
        val existing = ledger.find(identity)
        if (notifyObservation) notifyObserved(identity, existing)
        val attempt = (existing?.attempt ?: 0) + 1
        var startCheckpoint: Deferred<Unit>? = null
        var startCommand: CheckpointCommand? = null

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
                if (!startedThisInvocation &&
                    options.delivery == DeliverySemantics.AT_MOST_ONCE_PER_RETRY
                ) {
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
            -> {
                val command = CheckpointCommand(identity, CheckpointAction.START)
                if (options.delivery == DeliverySemantics.AT_MOST_ONCE_PER_RETRY) {
                    checkpoints.checkpoint(command)
                } else if (combineStartAndTerminal) {
                    startCommand = command
                } else {
                    startCheckpoint = checkpoints.checkpointAsync(command)
                }
            }
            CheckpointStatus.UNKNOWN ->
                error("Operation ${identity.id} has an unsupported checkpoint status")
        }

        val value =
            try {
                runUserFunction(identity, attempt, replayingChildren = false) {
                    block(
                        StepScopeImpl(
                            attempt,
                            DurableLogger(executionArn, identity.id, identity.name, attempt),
                        ),
                    )
                }
            } catch (suspension: ExecutionSuspended) {
                throw suspension
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (error: Throwable) {
                return failStep(
                    identity,
                    error,
                    attempt,
                    options,
                    startCheckpoint,
                    startCommand,
                )
            }

        val serde = options.serde ?: defaultSerde
        val payload = serde.encode(value)
        val normalized = serde.decode(payload, type)
        checkpointTerminal(
            startCommand,
            CheckpointCommand(
                identity = identity,
                action = CheckpointAction.SUCCEED,
                payload = payload,
            ),
        )
        startCheckpoint?.await()
        return normalized
    }

    internal suspend fun <T> extensionStep(
        identity: OperationIdentity,
        type: TypeRef<T>,
        config: ExtensionStepConfig<T>,
        function: suspend StepScope.(state: T?) -> ExtensionStepResult<T>,
    ): T {
        val existing = ledger.find(identity)
        notifyObserved(identity, existing)
        val attempt = (existing?.attempt ?: 0) + 1
        val serde = config.serde ?: defaultSerde
        val state =
            if (existing?.resultPayload != null) {
                serde.decode(existing.resultPayload, type)
            } else {
                config.initialState
            }
        var startCheckpoint: Deferred<Unit>? = null

        when (existing?.status) {
            CheckpointStatus.SUCCEEDED ->
                return decodeResult(existing, type, config.serde)
            CheckpointStatus.FAILED,
            CheckpointStatus.TIMED_OUT,
            CheckpointStatus.STOPPED,
            CheckpointStatus.CANCELLED,
            -> throw stepFailure(existing)
            CheckpointStatus.PENDING ->
                throw ExecutionSuspended(identity.id, existing.nextAttemptAt)
            CheckpointStatus.STARTED -> {
                if (config.delivery == DeliverySemantics.AT_MOST_ONCE_PER_RETRY) {
                    return failExtensionStep(
                        identity = identity,
                        type = type,
                        config = config,
                        state = state,
                        attempt = attempt,
                        error = StepInterruptedException(identity.id),
                    )
                }
            }
            CheckpointStatus.READY,
            null,
            -> {
                val start = CheckpointCommand(identity, CheckpointAction.START)
                if (config.delivery == DeliverySemantics.AT_MOST_ONCE_PER_RETRY) {
                    checkpoints.checkpoint(start)
                } else {
                    startCheckpoint = checkpoints.checkpointAsync(start)
                }
            }
            CheckpointStatus.UNKNOWN ->
                error("Operation ${identity.id} has an unsupported checkpoint status")
        }

        val outcome =
            try {
                runUserFunction(identity, attempt, replayingChildren = false) {
                    function(
                        StepScopeImpl(
                            attempt,
                            DurableLogger(executionArn, identity.id, identity.name, attempt),
                        ),
                        state,
                    )
                }
            } catch (suspension: ExecutionSuspended) {
                throw suspension
            } catch (cancelled: CancellationException) {
                throw cancelled
            } catch (error: Throwable) {
                return failExtensionStep(
                    identity = identity,
                    type = type,
                    config = config,
                    state = state,
                    attempt = attempt,
                    error = error,
                    startCheckpoint = startCheckpoint,
                )
            }

        return when (outcome) {
            is ExtensionStepResult.Succeeded -> {
                val payload = serde.encode(outcome.value)
                val normalized = serde.decode(payload, type)
                checkpoints.checkpoint(
                    CheckpointCommand(
                        identity = identity,
                        action = CheckpointAction.SUCCEED,
                        payload = payload,
                    ),
                )
                startCheckpoint?.await()
                normalized
            }
            is ExtensionStepResult.Retry ->
                retryExtensionStep(
                    identity = identity,
                    type = type,
                    serde = serde,
                    state = outcome.state,
                    delay = outcome.delay,
                    startCheckpoint = startCheckpoint,
                )
            is ExtensionStepResult.RetryAfterNormalization -> {
                val payload = serde.encode(outcome.state)
                val normalized = serde.decode(payload, type)
                retryExtensionStep(
                    identity = identity,
                    type = type,
                    serde = serde,
                    state = normalized,
                    delay = outcome.delay(normalized),
                    startCheckpoint = startCheckpoint,
                )
            }
        }
    }

    suspend fun wait(
        duration: Duration,
        name: String?,
    ) {
        wait(
            identity = reserveOperation(name).identity(OperationKind.WAIT, "Wait"),
            duration = duration,
        )
    }

    internal suspend fun wait(
        identity: OperationIdentity,
        duration: Duration,
    ) {
        require(duration.isPositive()) { "Wait duration must be positive" }
        val existing = ledger.find(identity)
        notifyObserved(identity, existing)
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
    ): O =
        invoke(
            identity = reserveOperation(name).identity(OperationKind.INVOKE, "ChainedInvoke"),
            functionName = functionName,
            input = input,
            outputType = outputType,
            config =
                ExtensionInvokeConfig(
                    payloadSerde = options.payloadSerde,
                    resultSerde = options.resultSerde,
                    tenantId = options.tenantId,
                ),
        )

    internal suspend fun <I, O> invoke(
        identity: OperationIdentity,
        functionName: String,
        input: I,
        outputType: TypeRef<O>,
        config: ExtensionInvokeConfig,
    ): O {
        val existing = ledger.find(identity)
        notifyObserved(identity, existing)
        when (existing?.status) {
            CheckpointStatus.SUCCEEDED ->
                return decodeResult(existing, outputType, config.resultSerde)
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
                val payload = (config.payloadSerde ?: defaultSerde).encode(input)
                checkpoints.checkpoint(
                    CheckpointCommand(
                        identity = identity,
                        action = CheckpointAction.START,
                        payload = payload,
                        targetFunction = functionName,
                        tenantId = config.tenantId,
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
    ): CallbackHandle<T> =
        callback(
            identity = reserveOperation(name).identity(OperationKind.CALLBACK, "Callback"),
            type = type,
            config =
                ExtensionCallbackConfig(
                    timeout = options.timeout,
                    heartbeatTimeout = options.heartbeatTimeout,
                    serde = options.serde,
                ),
        )

    internal suspend fun <T> callback(
        identity: OperationIdentity,
        type: TypeRef<T>,
        config: ExtensionCallbackConfig,
        notifyObservation: Boolean = true,
    ): CallbackHandle<T> {
        var existing = ledger.find(identity)
        if (notifyObservation) notifyObserved(identity, existing)
        if (existing == null) {
            checkpoints.checkpoint(
                CheckpointCommand(
                    identity = identity,
                    action = CheckpointAction.START,
                    callbackTimeout = config.timeout,
                    heartbeatTimeout = config.heartbeatTimeout,
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
            serde = config.serde ?: defaultSerde,
            ledger = ledger,
            terminalWhenObserved = existing.status.terminal,
        )
    }

    suspend fun <T> waitForCallback(
        name: String?,
        type: TypeRef<T>,
        options: CallbackWaitOptions = CallbackWaitOptions(),
        submitter: suspend CallbackSubmitterScope.() -> Unit,
    ): T {
        val identity =
            reserveOperation(name).identity(OperationKind.CONTEXT, "WaitForCallback")
        val existing = ledger.find(identity)
        notifyObserved(identity, existing)
        if (existing?.status == CheckpointStatus.SUCCEEDED) {
            return decodeResult(existing, type, options.callback.serde)
        }
        if (existing != null && existing.status.terminal) {
            throw CallbackFailureException(
                identity.id,
                RuntimeException(existing.error?.message ?: "Checkpointed callback wait failure"),
            )
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
                plugins = plugins,
            )
        val callbackIdentity =
            childRuntime
                .reserveOperation(null)
                .identity(OperationKind.CALLBACK, "Callback")
        val submitterIdentity =
            childRuntime
                .reserveOperation(null)
                .identity(OperationKind.STEP, "Step")
        val callbackExisting = ledger.find(callbackIdentity)
        val submitterExisting = ledger.find(submitterIdentity)
        notifyObserved(callbackIdentity, callbackExisting)
        notifyObserved(submitterIdentity, submitterExisting)
        val starts = mutableListOf<CheckpointCommand>()
        if (existing == null) {
            starts += CheckpointCommand(identity, CheckpointAction.START)
        }
        if (callbackExisting == null) {
            starts +=
                CheckpointCommand(
                    identity = callbackIdentity,
                    action = CheckpointAction.START,
                    callbackTimeout = options.callback.timeout,
                    heartbeatTimeout = options.callback.heartbeatTimeout,
                )
        }
        if (submitterExisting == null) {
            starts += CheckpointCommand(submitterIdentity, CheckpointAction.START)
        }
        if (starts.isNotEmpty()) checkpoints.checkpoint(starts)

        return try {
            val callback =
                childRuntime.callback(
                    identity = callbackIdentity,
                    type = type,
                    config =
                        ExtensionCallbackConfig(
                            timeout = options.callback.timeout,
                            heartbeatTimeout = options.callback.heartbeatTimeout,
                            serde = options.callback.serde,
                        ),
                    notifyObservation = false,
                )
            childRuntime.step(
                identity = submitterIdentity,
                type = typeRef<Unit>(),
                options = options.submitter,
                startedThisInvocation = submitterExisting == null,
                notifyObservation = false,
            ) {
                submitter(
                    CallbackSubmitterScopeImpl(
                        callback.id,
                        attempt,
                        DurableLogger(executionArn, identity.id, name, attempt),
                    ),
                )
            }
            if (!(callback as RuntimeCallback<T>).terminalWhenObserved) {
                throw ExecutionSuspended(callback.operationId)
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
        } catch (cancelled: CancellationException) {
            throw cancelled
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
        val identity =
            reserveOperation(name).identity(OperationKind.STEP, "WaitForCondition")
        val existing = ledger.find(identity)
        notifyObserved(identity, existing)
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
                    runUserFunction(identity, attempt, replayingChildren = false) {
                        check(
                            ConditionScopeImpl(
                                currentState,
                                attempt,
                                DurableLogger(executionArn, identity.id, identity.name, attempt),
                            ),
                        )
                    }
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
        } catch (cancelled: CancellationException) {
            throw cancelled
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
    ): T =
        context(
            identity = reserveOperation(name).identity(OperationKind.CONTEXT, "RunInChildContext"),
            type = type,
            options = options,
            block = block,
        )

    internal suspend fun <T> context(
        identity: OperationIdentity,
        type: TypeRef<T>,
        options: ChildOptions = ChildOptions(),
        block: suspend OperationRuntime.() -> T,
    ): T = runContext(identity, type, options, block)

    internal suspend fun <T> extensionContext(
        identity: OperationIdentity,
        type: TypeRef<T>,
        config: ExtensionContextConfig,
        block: suspend (
            childRuntime: OperationRuntime,
            replay: ExtensionContextReplay<T>,
        ) -> ExtensionContextResult<T>,
    ): T {
        val existing = ledger.find(identity)
        notifyObserved(identity, existing)
        if (existing?.status == CheckpointStatus.SUCCEEDED && !existing.replayChildren) {
            return decodeResult(existing, type, config.serde)
        }
        if (existing != null && existing.status.terminal && existing.status != CheckpointStatus.SUCCEEDED) {
            throw childFailure(existing)
        }
        if (existing != null &&
            existing.status != CheckpointStatus.STARTED &&
            !(existing.status == CheckpointStatus.SUCCEEDED && existing.replayChildren)
        ) {
            error("Extension context ${identity.id} has unsupported status ${existing.status}")
        }
        if (existing == null && !config.virtual) {
            checkpoints.checkpoint(CheckpointCommand(identity, CheckpointAction.START))
        }

        val childRuntime =
            OperationRuntime(
                executionArn = executionArn,
                isReplaying = existing != null,
                ledger = ledger,
                checkpoints = checkpoints,
                parentId = if (config.virtual) parentId else identity.id,
                ids = OperationIdSequence(if (config.virtual) parentId else identity.id),
                defaultSerde = defaultSerde,
                plugins = plugins,
            )
        val serde = config.serde ?: defaultSerde
        val replayState =
            existing
                ?.takeIf { it.replayChildren && !it.resultPayload.isNullOrEmpty() }
                ?.let { serde.decode(it.resultPayload!!, type) }
        val replay =
            ExtensionContextReplay(
                replayingChildren = existing?.replayChildren == true,
                replayState = replayState,
            )

        return try {
            val outcome =
                if (config.emitFunctionEvents) {
                    runUserFunction(
                        identity = identity,
                        attempt = null,
                        replayingChildren = replay.replayingChildren,
                    ) {
                        block(childRuntime, replay)
                    }
                } else {
                    block(childRuntime, replay)
                }
            if (config.virtual || existing?.replayChildren == true) return outcome.result

            val fullPayload = serde.encode(outcome.result)
            val normalized = serde.decode(fullPayload, type)
            val replayChildren =
                when (outcome) {
                    is ExtensionContextResult.Completed -> false
                    is ExtensionContextResult.ReplayChildren -> true
                    is ExtensionContextResult.ReplayChildrenAboveSize ->
                        fullPayload.encodeToByteArray().size >= outcome.thresholdBytes
                }
            val checkpointPayload =
                if (replayChildren) {
                    val state =
                        when (outcome) {
                            is ExtensionContextResult.ReplayChildren -> outcome.replayState
                            is ExtensionContextResult.ReplayChildrenAboveSize -> outcome.replayState
                            is ExtensionContextResult.Completed -> null
                        }
                    if (state == null) "" else serde.encode(state)
                } else {
                    fullPayload
                }
            checkpoints.checkpoint(
                CheckpointCommand(
                    identity = identity,
                    action = CheckpointAction.SUCCEED,
                    payload = checkpointPayload,
                    replayChildren = replayChildren,
                ),
            )
            normalized
        } catch (suspension: ExecutionSuspended) {
            throw suspension
        } catch (cancelled: CancellationException) {
            throw cancelled
        } catch (error: Throwable) {
            val failure =
                ExtensionContextFailure(
                    contextName = identity.name,
                    subtype = identity.subtype,
                    originalException = error,
                    childOperations =
                        ledger
                            .snapshot()
                            .values
                            .filter { it.identity.parentId == identity.id }
                            .map {
                                ExtensionChildOperation(
                                    id = it.identity.id,
                                    name = it.identity.name,
                                    type = it.identity.kind.name,
                                    subtype = it.identity.subtype,
                                    status = it.status.name,
                                )
                            },
                )
            val translated =
                config.errorHandler?.invoke(failure)
                    ?: ChildFailureException(identity.id, error)
            if (!config.virtual) {
                checkpoints.checkpoint(
                    CheckpointCommand(
                        identity = identity,
                        action = CheckpointAction.FAIL,
                        error = translated.toCheckpointError(),
                    ),
                )
            }
            throw translated
        }
    }

    suspend fun <I, O> map(
        name: String?,
        items: Collection<I>,
        outputType: TypeRef<O>,
        options: MapOptions<I> = MapOptions(),
        block: suspend OperationRuntime.(item: I, index: Int) -> O,
    ): MapResult<O> {
        val identity = reserveOperation(name).identity(OperationKind.CONTEXT, "Map")
        val existing = ledger.find(identity)
        notifyObserved(identity, existing)
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
                                plugins = plugins,
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
        val identity = reserveOperation(name).identity(OperationKind.CONTEXT, "Parallel")
        val existing = ledger.find(identity)
        notifyObserved(identity, existing)
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
                                plugins = plugins,
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
        notifyObserved(identity, existing)
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
                plugins = plugins,
            )

        return try {
            val value =
                runUserFunction(
                    identity,
                    attempt = null,
                    replayingChildren = existing != null || existing?.replayChildren == true,
                ) {
                    block(childRuntime)
                }
            if (options.virtual || existing?.replayChildren == true) return value
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
        } catch (cancelled: CancellationException) {
            throw cancelled
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

    internal fun reserveOperation(
        name: String?,
        localOperationId: String? = null,
    ): ReservedOperation =
        ReservedOperation(
            id = if (localOperationId == null) ids.next() else ids.next(localOperationId),
            name = name,
            parentId = parentId,
        )

    private suspend fun <T> runUserFunction(
        identity: OperationIdentity,
        attempt: Int?,
        replayingChildren: Boolean,
        block: suspend () -> T,
    ): T {
        val startedAt = Instant.now()
        val operation =
            identity.newSnapshot(
                ledger.wasPresentAtInvocationStart(identity.id),
            ).copy(attempt = attempt)
        plugins.functionStarted(
            FunctionAttemptStarted(
                operation = operation,
                startedAt = startedAt,
                replayingChildren = replayingChildren,
            ),
        )
        return try {
            block().also {
                plugins.functionEnded(
                    FunctionAttemptEnded(
                        operation = operation,
                        startedAt = startedAt,
                        endedAt = Instant.now(),
                        replayingChildren = replayingChildren,
                        succeeded = true,
                        error = null,
                    ),
                )
            }
        } catch (error: Throwable) {
            plugins.functionEnded(
                FunctionAttemptEnded(
                    operation = operation,
                    startedAt = startedAt,
                    endedAt = Instant.now(),
                    replayingChildren = replayingChildren,
                    succeeded = false,
                    error = error,
                ),
            )
            throw error
        }
    }

    private fun notifyObserved(
        identity: OperationIdentity,
        existing: OperationRecord?,
    ) {
        if (existing == null || !existing.status.terminal) {
            plugins.operationStarted(
                existing?.toSnapshot(true) ?: identity.newSnapshot(),
            )
        }
    }

    private suspend fun <T> failStep(
        identity: OperationIdentity,
        error: Throwable,
        attempt: Int,
        options: StepOptions,
        startCheckpoint: Deferred<Unit>? = null,
        startCommand: CheckpointCommand? = null,
    ): T =
        when (val decision = options.retry.decide(error, attempt)) {
            RetryDecision.Fail -> {
                checkpointTerminal(
                    startCommand,
                    CheckpointCommand(
                        identity = identity,
                        action = CheckpointAction.FAIL,
                        error = error.toCheckpointError(),
                    ),
                )
                startCheckpoint?.await()
                throw StepFailureException(identity.id, error)
            }
            is RetryDecision.Retry -> {
                val delay = maxOf(decision.delay, 1.seconds)
                val resumeAt = Instant.now().plusMillis(delay.inWholeMilliseconds)
                checkpointTerminal(
                    startCommand,
                    CheckpointCommand(
                        identity = identity,
                        action = CheckpointAction.RETRY,
                        error = error.toCheckpointError(),
                        retryDelay = delay,
                    ),
                )
                startCheckpoint?.await()
                throw ExecutionSuspended(identity.id, resumeAt)
            }
        }

    private suspend fun <T> failExtensionStep(
        identity: OperationIdentity,
        type: TypeRef<T>,
        config: ExtensionStepConfig<T>,
        state: T?,
        attempt: Int,
        error: Throwable,
        startCheckpoint: Deferred<Unit>? = null,
    ): T =
        when (
            val decision =
                config.retry?.decide(error, state, attempt)
                    ?: ExtensionRetryDecision.DoNotRetry
        ) {
            ExtensionRetryDecision.DoNotRetry -> {
                checkpoints.checkpoint(
                    CheckpointCommand(
                        identity = identity,
                        action = CheckpointAction.FAIL,
                        error = error.toCheckpointError(),
                    ),
                )
                startCheckpoint?.await()
                throw StepFailureException(identity.id, error)
            }
            is ExtensionRetryDecision.Retry ->
                retryExtensionStep(
                    identity = identity,
                    type = type,
                    serde = config.serde ?: defaultSerde,
                    state = decision.state,
                    delay = decision.delay,
                    error = error,
                    startCheckpoint = startCheckpoint,
                )
        }

    private suspend fun <T> retryExtensionStep(
        identity: OperationIdentity,
        type: TypeRef<T>,
        serde: Serde,
        state: T?,
        delay: Duration,
        error: Throwable? = null,
        startCheckpoint: Deferred<Unit>? = null,
    ): T {
        require(delay.isPositive()) { "Retry delay must be positive" }
        val effectiveDelay = maxOf(delay, 1.seconds)
        val payload =
            state?.let {
                serde.encode(it).also { encoded ->
                    serde.decode(encoded, type)
                }
            }
        val resumeAt = Instant.now().plusMillis(effectiveDelay.inWholeMilliseconds)
        checkpoints.checkpoint(
            CheckpointCommand(
                identity = identity,
                action = CheckpointAction.RETRY,
                payload = payload,
                error = error?.takeIf { payload == null }?.toCheckpointError(),
                retryDelay = effectiveDelay,
            ),
        )
        startCheckpoint?.await()
        throw ExecutionSuspended(identity.id, resumeAt)
    }

    private suspend fun checkpointTerminal(
        start: CheckpointCommand?,
        terminal: CheckpointCommand,
    ) {
        if (start == null) {
            checkpoints.checkpoint(terminal)
        } else {
            checkpoints.checkpoint(listOf(start, terminal))
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
    internal val terminalWhenObserved: Boolean,
) : CallbackHandle<T> {
    internal val operationId: String
        get() = identity.id

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
