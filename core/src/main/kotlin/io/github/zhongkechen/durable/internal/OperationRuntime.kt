package io.github.zhongkechen.durable.internal

import io.github.zhongkechen.durable.ChildFailureException
import io.github.zhongkechen.durable.ChildOptions
import io.github.zhongkechen.durable.CallbackFailureException
import io.github.zhongkechen.durable.CallbackHandle
import io.github.zhongkechen.durable.CallbackOptions
import io.github.zhongkechen.durable.DeliverySemantics
import io.github.zhongkechen.durable.InvokeFailureException
import io.github.zhongkechen.durable.InvokeOptions
import io.github.zhongkechen.durable.JsonSerde
import io.github.zhongkechen.durable.RetryDecision
import io.github.zhongkechen.durable.Serde
import io.github.zhongkechen.durable.StepFailureException
import io.github.zhongkechen.durable.StepInterruptedException
import io.github.zhongkechen.durable.StepOptions
import io.github.zhongkechen.durable.StepScope
import io.github.zhongkechen.durable.TypeRef
import java.time.Instant
import kotlin.time.Duration

internal class ExecutionSuspended(
    val operationId: String,
    val resumeAt: Instant? = null,
) : RuntimeException("Execution suspended at operation $operationId")

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
            val value = block(StepScopeImpl(attempt))
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

    suspend fun <T> child(
        name: String,
        type: TypeRef<T>,
        options: ChildOptions = ChildOptions(),
        block: suspend OperationRuntime.() -> T,
    ): T {
        val identity = reserve(name, OperationKind.CONTEXT, "RunInChildContext")
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
                val resumeAt = Instant.now().plusMillis(decision.delay.inWholeMilliseconds)
                checkpoints.checkpoint(
                    CheckpointCommand(
                        identity = identity,
                        action = CheckpointAction.RETRY,
                        error = error.toCheckpointError(),
                        retryDelay = decision.delay,
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

    private data class StepScopeImpl(
        override val attempt: Int,
    ) : StepScope
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
