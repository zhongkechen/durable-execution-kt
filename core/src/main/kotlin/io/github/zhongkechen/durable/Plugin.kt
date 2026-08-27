package io.github.zhongkechen.durable

import java.time.Instant

public data class OperationSnapshot(
    val id: String,
    val name: String?,
    val type: String,
    val subtype: String?,
    val parentId: String?,
    val status: String?,
    val startedAt: Instant?,
    val endedAt: Instant?,
    val attempt: Int?,
    val replay: Boolean,
    val error: Throwable? = null,
    val resultPayload: String? = null,
)

public data class InvocationStarted(
    val requestId: String?,
    val executionArn: String,
    val firstInvocation: Boolean,
    val executionStartedAt: Instant,
    val operations: Map<String, OperationSnapshot>,
    val updatedOperations: Map<String, OperationSnapshot>,
    val input: Any?,
)

public data class InvocationEnded(
    val requestId: String?,
    val executionArn: String,
    val firstInvocation: Boolean,
    val executionStartedAt: Instant,
    val operations: Map<String, OperationSnapshot>,
    val status: ExecutionStatus,
    val error: Throwable?,
    val input: Any?,
    val result: Any?,
)

public data class FunctionAttemptStarted(
    val operation: OperationSnapshot,
    val startedAt: Instant,
    val replayingChildren: Boolean,
)

public data class FunctionAttemptEnded(
    val operation: OperationSnapshot,
    val startedAt: Instant,
    val endedAt: Instant,
    val replayingChildren: Boolean,
    val succeeded: Boolean,
    val error: Throwable?,
)

public interface DurablePlugin {
    public fun invocationStarted(info: InvocationStarted): Unit = Unit

    public fun invocationEnded(info: InvocationEnded): Unit = Unit

    public fun operationStarted(operation: OperationSnapshot): Unit = Unit

    public fun operationEnded(operation: OperationSnapshot): Unit = Unit

    public fun operationsChanged(
        executionArn: String,
        changed: Map<String, OperationSnapshot>,
        all: Map<String, OperationSnapshot>,
    ): Unit = Unit

    public fun functionStarted(info: FunctionAttemptStarted): Unit = Unit

    public fun functionEnded(info: FunctionAttemptEnded): Unit = Unit
}
