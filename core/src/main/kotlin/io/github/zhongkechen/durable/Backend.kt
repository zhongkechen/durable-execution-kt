package io.github.zhongkechen.durable

import io.github.zhongkechen.durable.internal.CheckpointAction
import io.github.zhongkechen.durable.internal.CheckpointCommand
import io.github.zhongkechen.durable.internal.CheckpointError
import io.github.zhongkechen.durable.internal.CheckpointReply
import io.github.zhongkechen.durable.internal.CheckpointStatus
import io.github.zhongkechen.durable.internal.LambdaDurableService
import io.github.zhongkechen.durable.internal.OperationIdentity
import io.github.zhongkechen.durable.internal.OperationKind
import io.github.zhongkechen.durable.internal.OperationRecord
import io.github.zhongkechen.durable.internal.ServicePage
import java.time.Instant
import kotlin.time.Duration
import software.amazon.awssdk.services.lambda.LambdaClient

public enum class BackendOperationType {
    EXECUTION,
    STEP,
    WAIT,
    INVOKE,
    CALLBACK,
    CONTEXT,
}

public enum class BackendOperationStatus {
    PENDING,
    STARTED,
    READY,
    SUCCEEDED,
    FAILED,
    TIMED_OUT,
    STOPPED,
    CANCELLED,
    UNKNOWN,
}

public enum class BackendAction {
    START,
    SUCCEED,
    FAIL,
    RETRY,
}

public data class BackendError(
    val type: String?,
    val message: String?,
    val data: String? = null,
    val stack: List<String> = emptyList(),
)

public data class BackendOperation(
    val id: String,
    val name: String?,
    val type: BackendOperationType,
    val subtype: String,
    val parentId: String?,
    val status: BackendOperationStatus,
    val startedAt: Instant? = null,
    val endedAt: Instant? = null,
    val attempt: Int? = null,
    val resultPayload: String? = null,
    val error: BackendError? = null,
    val nextAttemptAt: Instant? = null,
    val replayChildren: Boolean = false,
    val callbackId: String? = null,
)

public data class BackendUpdate(
    val id: String,
    val name: String?,
    val type: BackendOperationType,
    val subtype: String,
    val parentId: String?,
    val action: BackendAction,
    val payload: String? = null,
    val error: BackendError? = null,
    val retryDelay: Duration? = null,
    val waitDuration: Duration? = null,
    val targetFunction: String? = null,
    val tenantId: String? = null,
    val callbackTimeout: Duration? = null,
    val heartbeatTimeout: Duration? = null,
    val replayChildren: Boolean = false,
)

public data class BackendState(
    val operations: List<BackendOperation>,
    val nextMarker: String? = null,
)

public data class BackendCheckpoint(
    val checkpointToken: String,
    val state: BackendState?,
)

public interface DurableBackend {
    public fun checkpoint(
        executionArn: String,
        checkpointToken: String,
        updates: List<BackendUpdate>,
    ): BackendCheckpoint

    public fun getState(
        executionArn: String,
        checkpointToken: String,
        marker: String?,
    ): BackendState
}

public class LambdaBackend(
    client: LambdaClient = LambdaClient.create(),
) : DurableBackend {
    private val service = LambdaDurableService(client)

    override fun checkpoint(
        executionArn: String,
        checkpointToken: String,
        updates: List<BackendUpdate>,
    ): BackendCheckpoint =
        service
            .checkpoint(executionArn, checkpointToken, updates.map(BackendUpdate::toInternal))
            .toPublic()

    override fun getState(
        executionArn: String,
        checkpointToken: String,
        marker: String?,
    ): BackendState = service.getState(executionArn, checkpointToken, marker).toPublic()
}

internal fun BackendUpdate.toInternal(): CheckpointCommand =
    CheckpointCommand(
        identity =
            OperationIdentity(
                id = id,
                name = name,
                kind = type.toInternal(),
                subtype = subtype,
                parentId = parentId,
            ),
        action = CheckpointAction.valueOf(action.name),
        payload = payload,
        error = error?.toInternal(),
        retryDelay = retryDelay,
        waitDuration = waitDuration,
        targetFunction = targetFunction,
        tenantId = tenantId,
        callbackTimeout = callbackTimeout,
        heartbeatTimeout = heartbeatTimeout,
        replayChildren = replayChildren,
    )

internal fun BackendOperation.toInternal(): OperationRecord =
    OperationRecord(
        identity =
            OperationIdentity(
                id = id,
                name = name,
                kind = type.toInternal(),
                subtype = subtype,
                parentId = parentId,
            ),
        status = CheckpointStatus.valueOf(status.name),
        startedAt = startedAt,
        endedAt = endedAt,
        attempt = attempt,
        resultPayload = resultPayload,
        error = error?.toInternal(),
        nextAttemptAt = nextAttemptAt,
        replayChildren = replayChildren,
        callbackId = callbackId,
    )

internal fun CheckpointCommand.toPublic(): BackendUpdate =
    BackendUpdate(
        id = identity.id,
        name = identity.name,
        type = identity.kind.toPublic(),
        subtype = identity.subtype,
        parentId = identity.parentId,
        action = BackendAction.valueOf(action.name),
        payload = payload,
        error = error?.toPublic(),
        retryDelay = retryDelay,
        waitDuration = waitDuration,
        targetFunction = targetFunction,
        tenantId = tenantId,
        callbackTimeout = callbackTimeout,
        heartbeatTimeout = heartbeatTimeout,
        replayChildren = replayChildren,
    )

internal fun OperationRecord.toPublic(): BackendOperation =
    BackendOperation(
        id = identity.id,
        name = identity.name,
        type = identity.kind.toPublic(),
        subtype = identity.subtype,
        parentId = identity.parentId,
        status = BackendOperationStatus.valueOf(status.name),
        startedAt = startedAt,
        endedAt = endedAt,
        attempt = attempt,
        resultPayload = resultPayload,
        error = error?.toPublic(),
        nextAttemptAt = nextAttemptAt,
        replayChildren = replayChildren,
        callbackId = callbackId,
    )

internal fun BackendState.toInternal(): ServicePage =
    ServicePage(operations.map(BackendOperation::toInternal), nextMarker)

internal fun ServicePage.toPublic(): BackendState =
    BackendState(operations.map(OperationRecord::toPublic), nextMarker)

internal fun BackendCheckpoint.toInternal(): CheckpointReply =
    CheckpointReply(checkpointToken, state?.toInternal())

internal fun CheckpointReply.toPublic(): BackendCheckpoint =
    BackendCheckpoint(checkpointToken, state?.toPublic())

private fun BackendOperationType.toInternal(): OperationKind = OperationKind.valueOf(name)

private fun OperationKind.toPublic(): BackendOperationType = BackendOperationType.valueOf(name)

private fun BackendError.toInternal(): CheckpointError = CheckpointError(type, message, data, stack)

private fun CheckpointError.toPublic(): BackendError = BackendError(type, message, data, stack)
