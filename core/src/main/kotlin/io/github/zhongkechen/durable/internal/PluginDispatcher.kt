package io.github.zhongkechen.durable.internal

import io.github.zhongkechen.durable.DurablePlugin
import io.github.zhongkechen.durable.FunctionAttemptEnded
import io.github.zhongkechen.durable.FunctionAttemptStarted
import io.github.zhongkechen.durable.InvocationEnded
import io.github.zhongkechen.durable.InvocationStarted
import io.github.zhongkechen.durable.OperationSnapshot

internal class PluginDispatcher(
    plugins: List<DurablePlugin>,
) {
    private val plugins = plugins.toList()

    fun invocationStarted(info: InvocationStarted) = dispatch { it.invocationStarted(info) }

    fun invocationEnded(info: InvocationEnded) = dispatch { it.invocationEnded(info) }

    fun operationStarted(operation: OperationSnapshot) = dispatch { it.operationStarted(operation) }

    fun operationEnded(operation: OperationSnapshot) = dispatch { it.operationEnded(operation) }

    fun operationsChanged(
        executionArn: String,
        changed: Map<String, OperationSnapshot>,
        all: Map<String, OperationSnapshot>,
    ) = dispatch { it.operationsChanged(executionArn, changed, all) }

    fun functionStarted(info: FunctionAttemptStarted) = dispatch { it.functionStarted(info) }

    fun functionEnded(info: FunctionAttemptEnded) = dispatch { it.functionEnded(info) }

    private inline fun dispatch(call: (DurablePlugin) -> Unit) {
        plugins.forEach { plugin ->
            try {
                call(plugin)
            } catch (_: Throwable) {
                // Instrumentation failures are isolated from user execution.
            }
        }
    }
}

internal fun OperationRecord.toSnapshot(replay: Boolean): OperationSnapshot =
    OperationSnapshot(
        id = identity.id,
        name = identity.name,
        type = identity.kind.name,
        subtype = identity.subtype,
        parentId = identity.parentId,
        status = status.name,
        startedAt = startedAt,
        endedAt = endedAt,
        attempt = attempt,
        replay = replay,
        error = error?.let { RuntimeException(it.message ?: it.type ?: "Operation failure") },
        resultPayload = resultPayload,
    )

internal fun OperationIdentity.newSnapshot(replay: Boolean = false): OperationSnapshot =
    OperationSnapshot(
        id = id,
        name = name,
        type = kind.name,
        subtype = subtype,
        parentId = parentId,
        status = null,
        startedAt = null,
        endedAt = null,
        attempt = null,
        replay = replay,
    )
