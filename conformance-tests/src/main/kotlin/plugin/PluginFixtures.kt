package plugin

import software.amazon.lambda.durable.plugin.DurableExecutionPlugin
import software.amazon.lambda.durable.plugin.InvocationEndInfo
import software.amazon.lambda.durable.plugin.InvocationInfo
import software.amazon.lambda.durable.plugin.OperationEndInfo
import software.amazon.lambda.durable.plugin.OperationInfo
import software.amazon.lambda.durable.plugin.UserFunctionEndInfo
import software.amazon.lambda.durable.plugin.UserFunctionStartInfo

public class ConformanceLoggingPlugin(
    private val label: String,
) : DurableExecutionPlugin {
    @Volatile
    private var executionArn: String? = null

    override fun onInvocationStart(info: InvocationInfo) {
        executionArn = info.durableExecutionArn()
        emit(
            "plugin" to label,
            "hook" to "invocation-start",
            "first" to info.isFirstInvocation,
            "durableExecutionArn" to executionArn,
        )
    }

    override fun onInvocationEnd(info: InvocationEndInfo) {
        emit(
            "plugin" to label,
            "hook" to "invocation-end",
            "status" to info.invocationStatus().name,
            "durableExecutionArn" to executionArn,
        )
    }

    override fun onOperationStart(info: OperationInfo) {
        if (!info.type().isStep()) return
        emit(
            "plugin" to label,
            "hook" to "operation-start",
            "op" to info.id(),
            "durableExecutionArn" to executionArn,
        )
    }

    override fun onOperationEnd(info: OperationEndInfo) {
        if (!info.type().isStep()) return
        emit(
            "plugin" to label,
            "hook" to "operation-end",
            "op" to info.id(),
            "status" to info.status(),
            "durableExecutionArn" to executionArn,
        )
    }

    override fun onUserFunctionStart(info: UserFunctionStartInfo) {
        val attempt = info.attempt() ?: return
        if (!info.type().isStep()) return
        emit(
            "plugin" to label,
            "hook" to "attempt-start",
            "n" to attempt,
            "op" to info.id(),
            "durableExecutionArn" to executionArn,
        )
    }

    override fun onUserFunctionEnd(info: UserFunctionEndInfo) {
        val attempt = info.attempt() ?: return
        if (!info.type().isStep()) return
        emit(
            "plugin" to label,
            "hook" to "attempt-end",
            "n" to attempt,
            "outcome" to if (info.succeeded()) "SUCCEEDED" else "FAILED",
            "op" to info.id(),
            "durableExecutionArn" to executionArn,
        )
    }
}

public class FaultyConformancePlugin : DurableExecutionPlugin {
    @Volatile
    private var executionArn: String? = null

    override fun onInvocationStart(info: InvocationInfo) {
        executionArn = info.durableExecutionArn()
        failAfterLog("invocation-start")
    }

    override fun onInvocationEnd(info: InvocationEndInfo) {
        failAfterLog("invocation-end")
    }

    override fun onOperationStart(info: OperationInfo) {
        if (info.type().isStep()) failAfterLog("operation-start")
    }

    override fun onOperationEnd(info: OperationEndInfo) {
        if (info.type().isStep()) failAfterLog("operation-end")
    }

    override fun onUserFunctionStart(info: UserFunctionStartInfo) {
        if (info.type().isStep()) failAfterLog("attempt-start")
    }

    override fun onUserFunctionEnd(info: UserFunctionEndInfo) {
        if (info.type().isStep()) failAfterLog("attempt-end")
    }

    private fun failAfterLog(hook: String): Nothing {
        emit(
            "plugin" to "CONFPLUGIN-FAULTY",
            "hook" to hook,
            "durableExecutionArn" to executionArn,
        )
        error("faulty $hook")
    }
}

private fun String?.isStep(): Boolean = this == "STEP"

private fun emit(vararg fields: Pair<String, Any?>) {
    println(
        fields
            .filter { it.second != null }
            .joinToString(prefix = "{", postfix = "}") { (key, value) ->
                val encoded =
                    when (value) {
                        is Boolean, is Number -> value.toString()
                        else -> "\"${value.toString().jsonEscaped()}\""
                    }
                "\"${key.jsonEscaped()}\": $encoded"
            },
    )
}

private fun String.jsonEscaped(): String =
    buildString(length) {
        for (character in this@jsonEscaped) {
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
