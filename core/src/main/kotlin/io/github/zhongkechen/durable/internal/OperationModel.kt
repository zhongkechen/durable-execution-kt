package io.github.zhongkechen.durable.internal

import java.time.Instant

internal enum class OperationKind {
    EXECUTION,
    STEP,
    WAIT,
    INVOKE,
    CALLBACK,
    CONTEXT,
}

internal enum class CheckpointStatus {
    PENDING,
    STARTED,
    READY,
    SUCCEEDED,
    FAILED,
    TIMED_OUT,
    STOPPED,
    CANCELLED,
    UNKNOWN,
    ;

    val terminal: Boolean
        get() =
            this == SUCCEEDED ||
                this == FAILED ||
                this == TIMED_OUT ||
                this == STOPPED ||
                this == CANCELLED
}

internal data class OperationIdentity(
    val id: String,
    val name: String?,
    val kind: OperationKind,
    val subtype: String,
    val parentId: String?,
)

internal data class CheckpointError(
    val type: String?,
    val message: String?,
    val data: String? = null,
    val stack: List<String> = emptyList(),
)

internal data class OperationRecord(
    val identity: OperationIdentity,
    val status: CheckpointStatus,
    val startedAt: Instant? = null,
    val endedAt: Instant? = null,
    val attempt: Int? = null,
    val resultPayload: String? = null,
    val error: CheckpointError? = null,
    val nextAttemptAt: Instant? = null,
    val replayChildren: Boolean = false,
)
