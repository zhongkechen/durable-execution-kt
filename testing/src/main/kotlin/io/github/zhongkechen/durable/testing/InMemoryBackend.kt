package io.github.zhongkechen.durable.testing

import io.github.zhongkechen.durable.BackendAction
import io.github.zhongkechen.durable.BackendCheckpoint
import io.github.zhongkechen.durable.BackendError
import io.github.zhongkechen.durable.BackendOperation
import io.github.zhongkechen.durable.BackendOperationStatus
import io.github.zhongkechen.durable.BackendOperationType
import io.github.zhongkechen.durable.BackendState
import io.github.zhongkechen.durable.BackendUpdate
import io.github.zhongkechen.durable.DurableBackend
import java.time.Instant
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicInteger

public class InMemoryDurableBackend : DurableBackend {
    private val operations = ConcurrentHashMap<String, BackendOperation>()
    private val tokenSequence = AtomicInteger()
    private val callbacks = ConcurrentHashMap<String, String>()

    override fun checkpoint(
        executionArn: String,
        checkpointToken: String,
        updates: List<BackendUpdate>,
    ): BackendCheckpoint {
        updates.forEach(::apply)
        return BackendCheckpoint(
            checkpointToken = "local-${tokenSequence.incrementAndGet()}",
            state = BackendState(snapshot()),
        )
    }

    override fun getState(
        executionArn: String,
        checkpointToken: String,
        marker: String?,
    ): BackendState = BackendState(snapshot())

    public fun initializeExecution(
        executionId: String,
        inputPayload: String,
    ) {
        operations[executionId] =
            BackendOperation(
                id = executionId,
                name = executionId,
                type = BackendOperationType.EXECUTION,
                subtype = "Execution",
                parentId = null,
                status = BackendOperationStatus.STARTED,
                startedAt = Instant.now(),
                resultPayload = inputPayload,
            )
    }

    public fun advanceExternalOperations() {
        operations.replaceAll { _, operation ->
            when {
                operation.type == BackendOperationType.WAIT &&
                    operation.status == BackendOperationStatus.STARTED ->
                    operation.copy(
                        status = BackendOperationStatus.SUCCEEDED,
                        endedAt = Instant.now(),
                    )
                operation.status == BackendOperationStatus.PENDING ->
                    operation.copy(status = BackendOperationStatus.READY)
                else -> operation
            }
        }
    }

    public fun completeCallback(
        callbackId: String,
        payload: String,
    ) {
        val operationId =
            callbacks[callbackId] ?: error("Unknown callback ID: $callbackId")
        operations.compute(operationId) { _, operation ->
            requireNotNull(operation).copy(
                status = BackendOperationStatus.SUCCEEDED,
                endedAt = Instant.now(),
                resultPayload = payload,
            )
        }
    }

    public fun failCallback(
        callbackId: String,
        error: BackendError,
    ) {
        val operationId =
            callbacks[callbackId] ?: error("Unknown callback ID: $callbackId")
        operations.compute(operationId) { _, operation ->
            requireNotNull(operation).copy(
                status = BackendOperationStatus.FAILED,
                endedAt = Instant.now(),
                error = error,
            )
        }
    }

    public fun snapshot(): List<BackendOperation> =
        operations.values.sortedBy { it.startedAt ?: Instant.EPOCH }

    private fun apply(update: BackendUpdate) {
        val previous = operations[update.id]
        operations[update.id] =
            when (update.action) {
                BackendAction.START -> {
                    val callbackId =
                        if (update.type == BackendOperationType.CALLBACK) {
                            previous?.callbackId ?: UUID.randomUUID().toString()
                        } else {
                            null
                        }
                    if (callbackId != null) callbacks[callbackId] = update.id
                    BackendOperation(
                        id = update.id,
                        name = update.name,
                        type = update.type,
                        subtype = update.subtype,
                        parentId = update.parentId,
                        status = BackendOperationStatus.STARTED,
                        startedAt = previous?.startedAt ?: Instant.now(),
                        attempt = previous?.attempt ?: 0,
                        resultPayload = previous?.resultPayload,
                        callbackId = callbackId,
                    )
                }
                BackendAction.SUCCEED ->
                    base(update, previous).copy(
                        status = BackendOperationStatus.SUCCEEDED,
                        endedAt = Instant.now(),
                        resultPayload = update.payload,
                        replayChildren = update.replayChildren,
                    )
                BackendAction.FAIL ->
                    base(update, previous).copy(
                        status = BackendOperationStatus.FAILED,
                        endedAt = Instant.now(),
                        error = update.error,
                    )
                BackendAction.RETRY ->
                    base(update, previous).copy(
                        status = BackendOperationStatus.PENDING,
                        attempt = (previous?.attempt ?: 0) + 1,
                        resultPayload = update.payload,
                        error = update.error,
                        nextAttemptAt =
                            update.retryDelay?.let {
                                Instant.now().plusMillis(it.inWholeMilliseconds)
                            },
                    )
            }
    }

    private fun base(
        update: BackendUpdate,
        previous: BackendOperation?,
    ): BackendOperation =
        previous
            ?: BackendOperation(
                id = update.id,
                name = update.name,
                type = update.type,
                subtype = update.subtype,
                parentId = update.parentId,
                status = BackendOperationStatus.UNKNOWN,
                startedAt = Instant.now(),
            )
}
