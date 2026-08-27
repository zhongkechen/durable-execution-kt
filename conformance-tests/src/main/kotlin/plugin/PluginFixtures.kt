package plugin

import io.github.zhongkechen.durable.*


public open class ConformanceLoggingPlugin(
    private val label: String,
) : DurablePlugin {
    private var executionArn: String? = null

    override fun invocationStarted(info: InvocationStarted) {
        executionArn = info.executionArn
        emit(
            "plugin" to label,
            "hook" to "invocation-start",
            "first" to info.firstInvocation,
            "durableExecutionArn" to executionArn,
        )
    }

    override fun invocationEnded(info: InvocationEnded) {
        emit(
            "plugin" to label,
            "hook" to "invocation-end",
            "status" to info.status.name,
            "durableExecutionArn" to executionArn,
        )
    }

    override fun operationStarted(operation: OperationSnapshot) {
        if (!operation.isStep()) return
        emit(
            "plugin" to label,
            "hook" to "operation-start",
            "op" to operation.id,
            "durableExecutionArn" to executionArn,
        )
    }

    override fun operationEnded(operation: OperationSnapshot) {
        if (!operation.isStep()) return
        emit(
            "plugin" to label,
            "hook" to "operation-end",
            "op" to operation.id,
            "status" to operation.status,
            "durableExecutionArn" to executionArn,
        )
    }

    override fun functionStarted(info: FunctionAttemptStarted) {
        if (!info.operation.isStep() || info.operation.attempt == null) return
        emit(
            "plugin" to label,
            "hook" to "attempt-start",
            "n" to info.operation.attempt,
            "op" to info.operation.id,
            "durableExecutionArn" to executionArn,
        )
    }

    override fun functionEnded(info: FunctionAttemptEnded) {
        if (!info.operation.isStep() || info.operation.attempt == null) return
        emit(
            "plugin" to label,
            "hook" to "attempt-end",
            "n" to info.operation.attempt,
            "outcome" to if (info.succeeded) "SUCCEEDED" else "FAILED",
            "op" to info.operation.id,
            "durableExecutionArn" to executionArn,
        )
    }
}

public class FaultyConformancePlugin : DurablePlugin {
    private var executionArn: String? = null

    override fun invocationStarted(info: InvocationStarted) {
        executionArn = info.executionArn
        fail("invocation-start")
    }

    override fun invocationEnded(info: InvocationEnded) = fail("invocation-end")

    override fun operationStarted(operation: OperationSnapshot) {
        if (operation.isStep()) fail("operation-start")
    }

    override fun operationEnded(operation: OperationSnapshot) {
        if (operation.isStep()) fail("operation-end")
    }

    override fun functionStarted(info: FunctionAttemptStarted) {
        if (info.operation.isStep()) fail("attempt-start")
    }

    override fun functionEnded(info: FunctionAttemptEnded) {
        if (info.operation.isStep()) fail("attempt-end")
    }

    private fun fail(hook: String): Nothing {
        emit(
            "plugin" to "CONFPLUGIN-FAULTY",
            "hook" to hook,
            "durableExecutionArn" to executionArn,
        )
        error("faulty $hook")
    }
}

internal fun OperationSnapshot.isStep(): Boolean = type == "STEP"

internal fun OperationSnapshot.isWait(): Boolean = type == "WAIT"

internal fun OperationSnapshot.isContext(): Boolean = type == "CONTEXT"

internal fun OperationSnapshot.isBranch(): Boolean = subtype.equals("ParallelBranch", ignoreCase = true)

internal fun emit(vararg fields: Pair<String, Any?>) {
    println(
        fields
            .filter { it.second != null }
            .joinToString(prefix = "{", postfix = "}") { (key, value) ->
                val rendered =
                    when (value) {
                        is Boolean, is Number -> value.toString()
                        else -> "\"${value.toString().escapeJson()}\""
                    }
                "\"${key.escapeJson()}\": $rendered"
            },
    )
}

private fun String.escapeJson(): String =
    buildString(length) {
        for (character in this@escapeJson) {
            when (character) {
                '\\' -> append("\\\\")
                '"' -> append("\\\"")
                '\b' -> append("\\b")
                '\u000c' -> append("\\f")
                '\n' -> append("\\n")
                '\r' -> append("\\r")
                '\t' -> append("\\t")
                else -> append(character)
            }
        }
    }
