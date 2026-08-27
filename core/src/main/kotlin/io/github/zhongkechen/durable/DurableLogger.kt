package io.github.zhongkechen.durable

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.slf4j.MDC

public class DurableLogger internal constructor(
    private val executionArn: String,
    private val operationId: String? = null,
    private val operationName: String? = null,
    private val attempt: Int? = null,
    private val delegate: Logger = LoggerFactory.getLogger("durable-execution"),
) {
    public fun trace(
        message: String,
        vararg arguments: Any?,
    ): Unit = withContext { delegate.trace(message, *arguments) }

    public fun debug(
        message: String,
        vararg arguments: Any?,
    ): Unit = withContext { delegate.debug(message, *arguments) }

    public fun info(
        message: String,
        vararg arguments: Any?,
    ): Unit = withContext { delegate.info(message, *arguments) }

    public fun warn(
        message: String,
        vararg arguments: Any?,
    ): Unit = withContext { delegate.warn(message, *arguments) }

    public fun error(
        message: String,
        throwable: Throwable? = null,
    ): Unit =
        withContext {
            if (throwable == null) delegate.error(message) else delegate.error(message, throwable)
        }

    private inline fun withContext(block: () -> Unit) {
        val previous =
            mapOf(
                "executionArn" to MDC.get("executionArn"),
                "operationId" to MDC.get("operationId"),
                "operationName" to MDC.get("operationName"),
                "attempt" to MDC.get("attempt"),
            )
        try {
            MDC.put("executionArn", executionArn)
            operationId?.let { MDC.put("operationId", it) }
            operationName?.let { MDC.put("operationName", it) }
            attempt?.let { MDC.put("attempt", it.toString()) }
            block()
        } finally {
            previous.forEach { (key, value) ->
                if (value == null) MDC.remove(key) else MDC.put(key, value)
            }
        }
    }
}
