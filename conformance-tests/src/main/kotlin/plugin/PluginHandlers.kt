// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package plugin

import java.util.Locale
import kotlin.time.Duration.Companion.seconds
import software.amazon.lambda.durable.DurableFuture
import software.amazon.lambda.durable.kotlin.KotlinDurableContext
import software.amazon.lambda.durable.kotlin.KotlinDurableHandler
import software.amazon.lambda.durable.kotlin.KotlinDurableRuntime
import software.amazon.lambda.durable.kotlin.RetryJitter
import software.amazon.lambda.durable.kotlin.RetryPolicy
import software.amazon.lambda.durable.kotlin.await
import software.amazon.lambda.durable.plugin.DurableExecutionPlugin
import software.amazon.lambda.durable.plugin.InvocationEndInfo
import software.amazon.lambda.durable.plugin.InvocationInfo
import software.amazon.lambda.durable.plugin.InvocationStatus
import software.amazon.lambda.durable.plugin.OperationChangeInfo
import software.amazon.lambda.durable.plugin.OperationEndInfo
import software.amazon.lambda.durable.plugin.OperationInfo
import software.amazon.lambda.durable.plugin.UserFunctionEndInfo
import software.amazon.lambda.durable.plugin.UserFunctionStartInfo

public class PluginInvocationLifecycle :
    KotlinDurableHandler<String, String>(
        config(ConformanceLoggingPlugin("CONFPLUGIN")),
    ) {
    override suspend fun handle(input: String, context: KotlinDurableContext): String =
        context.step("greet") {
            javaContext.logger.info("Greeting step running for: {}", input)
            "Hello, $input!"
        }
}

public class PluginOperationLifecycle :
    KotlinDurableHandler<String, String>(
        config(ConformanceLoggingPlugin("CONFPLUGIN")),
    ) {
    override suspend fun handle(input: String, context: KotlinDurableContext): String =
        context.step("greet") { "Hello, $input!" }
}

public class PluginAttemptHooksRetry :
    KotlinDurableHandler<Any?, String>(
        config(ConformanceLoggingPlugin("CONFPLUGIN")),
    ) {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String =
        context.step(
            name = "retry-step",
            retry = RetryPolicy.fixed(maxAttempts = 3, delay = 1.seconds),
        ) {
            if (attempt < 2) error("Attempt $attempt failed")
            "Operation succeeded"
        }
}

public class PluginErrorIsolation :
    KotlinDurableHandler<String, String>(
        config(FaultyConformancePlugin()),
    ) {
    override suspend fun handle(input: String, context: KotlinDurableContext): String =
        context.step("greet") { "Hello, $input!" }
}

public class PluginMultiplePlugins :
    KotlinDurableHandler<String, String>(
        config(InvocationLoggingPlugin("CONFPLUGIN-A"), InvocationLoggingPlugin("CONFPLUGIN-B")),
    ) {
    override suspend fun handle(input: String, context: KotlinDurableContext): String =
        context.step("greet") { "Hello, $input!" }

    private class InvocationLoggingPlugin(
        private val prefix: String,
    ) : DurableExecutionPlugin {
        @Volatile
        private var executionArn: String? = null

        override fun onInvocationStart(info: InvocationInfo) {
            executionArn = info.durableExecutionArn()
            println("""{"plugin": "$prefix", "hook": "invocation-start"${arnField(executionArn)}}""")
        }

        override fun onInvocationEnd(info: InvocationEndInfo) {
            println(
                """{"plugin": "$prefix", "hook": "invocation-end", "status": "${info.invocationStatus().name}"${arnField(executionArn)}}""",
            )
        }
    }
}

public class PluginFirstInvocationFlag :
    KotlinDurableHandler<Any?, String>(
        config(ConformanceLoggingPlugin("CONFPLUGIN")),
    ) {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String {
        context.wait(2.seconds)
        return "Wait completed"
    }
}

public class PluginTerminalFailure :
    KotlinDurableHandler<Any?, String>(
        config(ConformanceLoggingPlugin("CONFPLUGIN")),
    ) {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String =
        context.step(
            name = "failing-step",
            retry = RetryPolicy.none,
        ) {
            error("Something went wrong")
        }
}

public class PluginNestedParentLinkage :
    KotlinDurableHandler<String, String>(
        config(ParentLinkagePlugin()),
    ) {
    override suspend fun handle(input: String, context: KotlinDurableContext): String =
        context.childContext("child") {
            step<String>("greet") { "Hello, $input!" }
        }

    private class ParentLinkagePlugin : DurableExecutionPlugin {
        @Volatile
        private var executionArn: String? = null

        override fun onInvocationStart(info: InvocationInfo) {
            executionArn = info.durableExecutionArn()
        }

        override fun onOperationEnd(info: OperationEndInfo) {
            println(
                """{"plugin": "CONFPLUGIN", "hook": "operation-end", "op": "${info.id()}", "parent": "${parentOrNone(info.parentId())}", "status": "${info.status()}"${arnField(executionArn)}}""",
            )
        }
    }
}

public class PluginOperationChange :
    KotlinDurableHandler<String, String>(
        config(ChangePlugin()),
    ) {
    override suspend fun handle(input: String, context: KotlinDurableContext): String =
        context.step("greet") { "Hello, $input!" }

    private class ChangePlugin : DurableExecutionPlugin {
        @Volatile
        private var executionArn: String? = null

        override fun onInvocationStart(info: InvocationInfo) {
            executionArn = info.durableExecutionArn()
        }

        override fun onOperationChange(info: OperationChangeInfo) {
            info.updatedOperations().values
                .filter { isStepChange(it.type()) }
                .forEach { item ->
                    val status = item.status()?.toString() ?: "NONE"
                    println(
                        """{"plugin": "CONFPLUGIN", "hook": "operation-change", "op": "${item.id()}", "status": "$status", "in_full_map": ${info.operations().containsKey(item.id())}${arnField(executionArn)}}""",
                    )
                }
        }
    }
}

public class PluginExternalUpdateOnInvoke :
    KotlinDurableHandler<Any?, String>(
        config(UpdatedOnInvokePlugin()),
    ) {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String {
        context.wait(2.seconds)
        return "Wait completed"
    }

    private class UpdatedOnInvokePlugin : DurableExecutionPlugin {
        @Volatile
        private var executionArn: String? = null

        @Volatile
        private var firstInvocation: Boolean = false

        override fun onInvocationStart(info: InvocationInfo) {
            executionArn = info.durableExecutionArn()
            firstInvocation = info.isFirstInvocation
        }

        override fun onOperationEnd(info: OperationEndInfo) {
            if (!isWait(info.type()) || !info.isReplay) return
            println(
                """{"plugin": "CONFPLUGIN", "hook": "updated-on-invoke", "op": "${info.id()}", "status": "${info.status()}", "first": $firstInvocation${arnField(executionArn)}}""",
            )
        }
    }
}

public class PluginWaitOperationHooks :
    KotlinDurableHandler<Any?, String>(
        config(WaitHooksPlugin()),
    ) {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String {
        context.wait(2.seconds)
        return "Wait completed"
    }

    private class WaitHooksPlugin : DurableExecutionPlugin {
        @Volatile
        private var executionArn: String? = null

        override fun onInvocationStart(info: InvocationInfo) {
            executionArn = info.durableExecutionArn()
        }

        override fun onOperationStart(info: OperationInfo) {
            if (!isWait(info.type())) return
            println(
                """{"plugin": "CONFPLUGIN", "hook": "operation-start", "op": "${info.id()}", "type": "${info.type().uppercase(Locale.ROOT)}"${arnField(executionArn)}}""",
            )
        }

        override fun onOperationEnd(info: OperationEndInfo) {
            if (!isWait(info.type())) return
            println(
                """{"plugin": "CONFPLUGIN", "hook": "operation-end", "op": "${info.id()}", "type": "${info.type().uppercase(Locale.ROOT)}", "status": "${info.status()}"${arnField(executionArn)}}""",
            )
        }
    }
}

public class PluginParallelBranchHooks :
    KotlinDurableHandler<Any?, List<String>>(
        config(BranchHooksPlugin()),
    ) {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): List<String> {
        val futures = mutableListOf<DurableFuture<String>>()
        context.parallel(name = "parallel", maxConcurrency = 1) {
            futures += branch<String>("branch-0") { "task-1" }
            futures += branch<String>("branch-1") { "task-2" }
        }
        return futures.map { it.await() }
    }

    private class BranchHooksPlugin : DurableExecutionPlugin {
        @Volatile
        private var executionArn: String? = null

        override fun onInvocationStart(info: InvocationInfo) {
            executionArn = info.durableExecutionArn()
        }

        override fun onUserFunctionStart(info: UserFunctionStartInfo) {
            if (!isBranch(info.subType())) return
            println(
                """{"plugin": "CONFPLUGIN", "hook": "fn-start", "op": "${info.id()}", "parent": "${parentOrNone(info.parentId())}"${arnField(executionArn)}}""",
            )
        }

        override fun onUserFunctionEnd(info: UserFunctionEndInfo) {
            if (!isBranch(info.subType())) return
            val outcome = if (info.succeeded()) "SUCCEEDED" else "FAILED"
            println(
                """{"plugin": "CONFPLUGIN", "hook": "fn-end", "op": "${info.id()}", "parent": "${parentOrNone(info.parentId())}", "outcome": "$outcome"${arnField(executionArn)}}""",
            )
        }
    }
}

public class PluginReplayFlags :
    KotlinDurableHandler<Any?, String>(
        config(ReplayFlagPlugin()),
    ) {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String {
        context.step<String>("step-a") { "a" }
        return context.step(
            name = "step-b",
            retry =
                RetryPolicy.exponential(
                    maxAttempts = 3,
                    initialDelay = 1.seconds,
                    maxDelay = 10.seconds,
                    backoffRate = 1.0,
                    jitter = RetryJitter.NONE,
                ),
        ) {
            if (attempt < 2) error("Attempt $attempt failed")
            "Operation succeeded"
        }
    }

    private class ReplayFlagPlugin : DurableExecutionPlugin {
        @Volatile
        private var executionArn: String? = null

        override fun onInvocationStart(info: InvocationInfo) {
            executionArn = info.durableExecutionArn()
        }

        override fun onOperationStart(info: OperationInfo) {
            if (!isStep(info.type())) return
            println(
                """{"plugin": "CONFPLUGIN", "hook": "operation-start", "op": "${info.id()}", "replay": ${info.isReplay}${arnField(executionArn)}}""",
            )
        }

        override fun onOperationEnd(info: OperationEndInfo) {
            if (!isStep(info.type())) return
            println(
                """{"plugin": "CONFPLUGIN", "hook": "operation-end", "op": "${info.id()}", "status": "${info.status()}"${arnField(executionArn)}}""",
            )
        }
    }
}

public class PluginRetryExhaustion :
    KotlinDurableHandler<Any?, String>(
        config(AttemptPlugin()),
    ) {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String =
        context.step(
            name = "always-fails",
            retry =
                RetryPolicy.exponential(
                    maxAttempts = 2,
                    initialDelay = 1.seconds,
                    maxDelay = 10.seconds,
                    backoffRate = 1.0,
                    jitter = RetryJitter.NONE,
                ),
        ) {
            error("boom")
        }

    private class AttemptPlugin : DurableExecutionPlugin {
        @Volatile
        private var executionArn: String? = null

        override fun onInvocationStart(info: InvocationInfo) {
            executionArn = info.durableExecutionArn()
        }

        override fun onUserFunctionStart(info: UserFunctionStartInfo) {
            if (!isStep(info.type()) || info.attempt() == null) return
            println(
                """{"plugin": "CONFPLUGIN", "hook": "attempt-start", "n": ${info.attempt()}, "op": "${info.id()}"${arnField(executionArn)}}""",
            )
        }

        override fun onUserFunctionEnd(info: UserFunctionEndInfo) {
            if (!isStep(info.type()) || info.attempt() == null) return
            val outcome = if (info.succeeded()) "SUCCEEDED" else "FAILED"
            println(
                """{"plugin": "CONFPLUGIN", "hook": "attempt-end", "n": ${info.attempt()}, "outcome": "$outcome", "op": "${info.id()}"${arnField(executionArn)}}""",
            )
        }

        override fun onOperationEnd(info: OperationEndInfo) {
            if (!isStep(info.type())) return
            println(
                """{"plugin": "CONFPLUGIN", "hook": "operation-end", "op": "${info.id()}", "status": "${info.status()}"${arnField(executionArn)}}""",
            )
        }
    }
}

public class PluginSuspensionInvocationEnd :
    KotlinDurableHandler<Any?, String>(
        config(InvocationEndPlugin()),
    ) {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String {
        context.wait(2.seconds)
        return "Wait completed"
    }

    private class InvocationEndPlugin : DurableExecutionPlugin {
        @Volatile
        private var executionArn: String? = null

        override fun onInvocationStart(info: InvocationInfo) {
            executionArn = info.durableExecutionArn()
            println(
                """{"plugin": "CONFPLUGIN", "hook": "invocation-start", "first": ${info.isFirstInvocation}${arnField(executionArn)}}""",
            )
        }

        override fun onInvocationEnd(info: InvocationEndInfo) {
            val status = info.invocationStatus()
            val terminal = status == InvocationStatus.SUCCEEDED || status == InvocationStatus.FAILED
            println(
                """{"plugin": "CONFPLUGIN", "hook": "invocation-end", "first": ${info.isFirstInvocation}, "terminal": $terminal, "status": "${status.name}"${arnField(executionArn)}}""",
            )
        }
    }
}

public class PluginFaultyAndHealthy :
    KotlinDurableHandler<String, String>(
        config(FaultyPeerPlugin(), HealthyPeerPlugin()),
    ) {
    override suspend fun handle(input: String, context: KotlinDurableContext): String =
        context.step("greet") { "Hello, $input!" }

    private class FaultyPeerPlugin : DurableExecutionPlugin {
        @Volatile
        private var executionArn: String? = null

        override fun onInvocationStart(info: InvocationInfo) {
            executionArn = info.durableExecutionArn()
            logAndThrow("invocation-start")
        }

        override fun onOperationStart(info: OperationInfo) {
            if (isStep(info.type())) logAndThrow("operation-start")
        }

        override fun onUserFunctionStart(info: UserFunctionStartInfo) {
            if (isStep(info.type()) && info.attempt() != null) logAndThrow("attempt-start")
        }

        override fun onUserFunctionEnd(info: UserFunctionEndInfo) {
            if (isStep(info.type()) && info.attempt() != null) logAndThrow("attempt-end")
        }

        override fun onOperationEnd(info: OperationEndInfo) {
            if (isStep(info.type())) logAndThrow("operation-end")
        }

        override fun onInvocationEnd(info: InvocationEndInfo) {
            logAndThrow("invocation-end")
        }

        private fun logAndThrow(hook: String): Nothing {
            println("""{"plugin": "CONFPLUGIN-FAULTY", "hook": "$hook"${arnField(executionArn)}}""")
            error("faulty $hook")
        }
    }

    private class HealthyPeerPlugin : DurableExecutionPlugin {
        @Volatile
        private var executionArn: String? = null

        override fun onInvocationStart(info: InvocationInfo) {
            executionArn = info.durableExecutionArn()
            println(
                """{"plugin": "CONFPLUGIN-HEALTHY", "hook": "invocation-start", "first": ${info.isFirstInvocation}${arnField(executionArn)}}""",
            )
        }

        override fun onInvocationEnd(info: InvocationEndInfo) {
            println(
                """{"plugin": "CONFPLUGIN-HEALTHY", "hook": "invocation-end", "status": "${info.invocationStatus().name}"${arnField(executionArn)}}""",
            )
        }

        override fun onOperationStart(info: OperationInfo) {
            if (!isStep(info.type())) return
            println(
                """{"plugin": "CONFPLUGIN-HEALTHY", "hook": "operation-start", "op": "${info.id()}"${arnField(executionArn)}}""",
            )
        }

        override fun onUserFunctionStart(info: UserFunctionStartInfo) {
            if (!isStep(info.type()) || info.attempt() == null) return
            println(
                """{"plugin": "CONFPLUGIN-HEALTHY", "hook": "attempt-start", "op": "${info.id()}"${arnField(executionArn)}}""",
            )
        }

        override fun onUserFunctionEnd(info: UserFunctionEndInfo) {
            if (!isStep(info.type()) || info.attempt() == null) return
            val outcome = if (info.succeeded()) "SUCCEEDED" else "FAILED"
            println(
                """{"plugin": "CONFPLUGIN-HEALTHY", "hook": "attempt-end", "op": "${info.id()}", "outcome": "$outcome"${arnField(executionArn)}}""",
            )
        }

        override fun onOperationEnd(info: OperationEndInfo) {
            if (!isStep(info.type())) return
            println(
                """{"plugin": "CONFPLUGIN-HEALTHY", "hook": "operation-end", "op": "${info.id()}", "status": "${info.status()}"${arnField(executionArn)}}""",
            )
        }
    }
}

public class PluginOperationEndPayload :
    KotlinDurableHandler<Any?, String>(
        config(OperationEndPayloadPlugin()),
    ) {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String {
        context.step<String>("task-a") { "task-a" }
        return context.step(
            name = "task-b",
            retry = RetryPolicy.none,
        ) {
            error("boom")
        }
    }

    private class OperationEndPayloadPlugin : DurableExecutionPlugin {
        @Volatile
        private var executionArn: String? = null

        override fun onInvocationStart(info: InvocationInfo) {
            executionArn = info.durableExecutionArn()
        }

        override fun onOperationEnd(info: OperationEndInfo) {
            if (!isStep(info.type())) return
            println(
                jsonObject(
                    "plugin" to "CONFPLUGIN",
                    "hook" to "operation-end",
                    "op" to info.id(),
                    "status" to info.status(),
                    "result" to (info.result() ?: "NONE"),
                    "error" to (info.error()?.message ?: "NONE"),
                    "durableExecutionArn" to executionArn,
                ),
            )
        }
    }
}

public class PluginPendingWaitReplay :
    KotlinDurableHandler<Any?, List<String>>(
        config(PendingWaitPlugin()),
    ) {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): List<String> {
        val futures = mutableListOf<DurableFuture<String>>()
        context.parallel(name = "waits", maxConcurrency = 2) {
            futures += branch<String>("short-branch") {
                wait(name = "short", duration = 2.seconds)
                "short-done"
            }
            futures += branch<String>("long-branch") {
                wait(name = "long", duration = 8.seconds)
                "long-done"
            }
        }
        return futures.map { it.await() }
    }

    private class PendingWaitPlugin : DurableExecutionPlugin {
        @Volatile
        private var executionArn: String? = null

        override fun onInvocationStart(info: InvocationInfo) {
            executionArn = info.durableExecutionArn()
        }

        override fun onOperationStart(info: OperationInfo) {
            if (!isWait(info.type())) return
            println(
                jsonObject(
                    "plugin" to "CONFPLUGIN",
                    "hook" to "operation-start",
                    "type" to info.type().uppercase(Locale.ROOT),
                    "name" to info.name(),
                    "replay" to info.isReplay,
                    "pending" to (info.endTimestamp() == null),
                    "durableExecutionArn" to executionArn,
                ),
            )
        }

        override fun onOperationEnd(info: OperationEndInfo) {
            if (!isWait(info.type())) return
            println(
                jsonObject(
                    "plugin" to "CONFPLUGIN",
                    "hook" to "operation-end",
                    "type" to info.type().uppercase(Locale.ROOT),
                    "name" to info.name(),
                    "status" to info.status(),
                    "durableExecutionArn" to executionArn,
                ),
            )
        }
    }
}

public class PluginInvocationInfoFields :
    KotlinDurableHandler<String, String>(
        config(InvocationFieldsPlugin()),
    ) {
    override suspend fun handle(input: String, context: KotlinDurableContext): String {
        context.wait(2.seconds)
        return "done-$input"
    }

    private class InvocationFieldsPlugin : DurableExecutionPlugin {
        @Volatile
        private var executionArn: String? = null

        override fun onInvocationStart(info: InvocationInfo) {
            executionArn = info.durableExecutionArn()
            println(
                jsonObject(
                    "plugin" to "CONFPLUGIN",
                    "hook" to "invocation-start",
                    "isFirstInvocation" to info.isFirstInvocation,
                    "requestId" to info.requestId(),
                    "operationsCount" to info.operations().size,
                    "updatedOperationsCount" to info.updatedOperations().size,
                    "executionStartTimestamp" to info.executionStartTime().toString(),
                    "durableExecutionArn" to executionArn,
                ),
            )
        }

        override fun onInvocationEnd(info: InvocationEndInfo) {
            val status = info.invocationStatus()
            println(
                jsonObject(
                    "plugin" to "CONFPLUGIN",
                    "hook" to "invocation-end",
                    "isFirstInvocation" to info.isFirstInvocation,
                    "requestId" to info.requestId(),
                    "operationsCount" to info.operations().size,
                    "executionStartTimestamp" to info.executionStartTime()?.toString(),
                    "status" to status.name,
                    "terminal" to (status == InvocationStatus.SUCCEEDED || status == InvocationStatus.FAILED),
                    "executionError" to info.executionError()?.message,
                    "durableExecutionArn" to executionArn,
                ),
            )
        }
    }
}

public class PluginOperationInfoFields :
    KotlinDurableHandler<Any?, String>(
        config(OperationFieldsPlugin()),
    ) {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String =
        context.step("greet") { "task-a" }

    private class OperationFieldsPlugin : DurableExecutionPlugin {
        @Volatile
        private var executionArn: String? = null

        override fun onInvocationStart(info: InvocationInfo) {
            executionArn = info.durableExecutionArn()
        }

        override fun onOperationStart(info: OperationInfo) {
            if (!isStep(info.type())) return
            println(
                jsonObject(
                    "plugin" to "CONFPLUGIN",
                    "hook" to "operation-start",
                    "id" to info.id(),
                    "name" to info.name(),
                    "type" to info.type().uppercase(Locale.ROOT),
                    "subType" to info.subType(),
                    "parentId" to info.parentId(),
                    "status" to info.status(),
                    "startTimestamp" to info.startTimestamp()?.toString(),
                    "endTimestamp" to info.endTimestamp()?.toString(),
                    "isReplay" to info.isReplay,
                    "durableExecutionArn" to executionArn,
                ),
            )
        }

        override fun onOperationEnd(info: OperationEndInfo) {
            if (!isStep(info.type())) return
            println(
                jsonObject(
                    "plugin" to "CONFPLUGIN",
                    "hook" to "operation-end",
                    "id" to info.id(),
                    "name" to info.name(),
                    "type" to info.type().uppercase(Locale.ROOT),
                    "subType" to info.subType(),
                    "parentId" to info.parentId(),
                    "status" to info.status(),
                    "startTimestamp" to info.startTimestamp()?.toString(),
                    "endTimestamp" to info.endTimestamp()?.toString(),
                    "error" to info.error()?.message,
                    "result" to info.result(),
                    "attempt" to info.attempt(),
                    "isReplay" to info.isReplay,
                    "durableExecutionArn" to executionArn,
                ),
            )
        }
    }
}

public class PluginAttemptInfoFields :
    KotlinDurableHandler<Any?, String>(
        config(AttemptFieldsPlugin()),
    ) {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String =
        context.step(
            name = "flaky",
            retry = RetryPolicy.fixed(maxAttempts = 2, delay = 1.seconds),
        ) {
            if (attempt == 1) error("first attempt failed")
            "ok"
        }

    private class AttemptFieldsPlugin : DurableExecutionPlugin {
        @Volatile
        private var executionArn: String? = null

        override fun onInvocationStart(info: InvocationInfo) {
            executionArn = info.durableExecutionArn()
        }

        override fun onUserFunctionStart(info: UserFunctionStartInfo) {
            if (!isStep(info.type()) || info.attempt() == null) return
            println(
                jsonObject(
                    "plugin" to "CONFPLUGIN",
                    "hook" to "attempt-start",
                    "id" to info.id(),
                    "name" to info.name(),
                    "type" to info.type().uppercase(Locale.ROOT),
                    "subType" to info.subType(),
                    "parentId" to info.parentId(),
                    "attempt" to info.attempt(),
                    "startTimestamp" to info.startTimestamp().toString(),
                    "isReplay" to info.isReplay,
                    "isReplayingChildren" to info.isReplayingChildren,
                    "durableExecutionArn" to executionArn,
                ),
            )
        }

        override fun onUserFunctionEnd(info: UserFunctionEndInfo) {
            if (!isStep(info.type()) || info.attempt() == null) return
            println(
                jsonObject(
                    "plugin" to "CONFPLUGIN",
                    "hook" to "attempt-end",
                    "id" to info.id(),
                    "name" to info.name(),
                    "type" to info.type().uppercase(Locale.ROOT),
                    "subType" to info.subType(),
                    "parentId" to info.parentId(),
                    "attempt" to info.attempt(),
                    "startTimestamp" to info.startTimestamp().toString(),
                    "endTimestamp" to info.endTimestamp().toString(),
                    "isReplay" to info.isReplay,
                    "isReplayingChildren" to info.isReplayingChildren,
                    "outcome" to if (info.succeeded()) "SUCCEEDED" else "FAILED",
                    "error" to info.error()?.message,
                    "durableExecutionArn" to executionArn,
                ),
            )
        }
    }
}

public class PluginOperationChangeFields :
    KotlinDurableHandler<Any?, String>(
        config(OperationChangeFieldsPlugin()),
    ) {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String =
        context.step("greet") { "task-a" }

    private class OperationChangeFieldsPlugin : DurableExecutionPlugin {
        override fun onOperationChange(info: OperationChangeInfo) {
            info.updatedOperations().values
                .filter { isStepChange(it.type()) }
                .forEach { item ->
                    println(
                        jsonObject(
                            "plugin" to "CONFPLUGIN",
                            "hook" to "operation-change",
                            "executionArn" to info.durableExecutionArn(),
                            "updatedOperationsCount" to info.updatedOperations().size,
                            "operationsCount" to info.operations().size,
                            "inFullMap" to info.operations().containsKey(item.id()),
                            "id" to item.id(),
                            "name" to item.name(),
                            "type" to item.type()?.uppercase(Locale.ROOT),
                            "subType" to item.subType(),
                            "parentId" to item.parentId(),
                            "status" to item.status()?.toString(),
                            "startTimestamp" to item.startTimestamp()?.toString(),
                            "endTimestamp" to item.endTimestamp()?.toString(),
                            "error" to item.error()?.message,
                            "result" to item.result(),
                            "attempt" to item.attempt(),
                            "isReplay" to item.isReplay,
                            "durableExecutionArn" to info.durableExecutionArn(),
                        ),
                    )
                }
        }
    }
}

public class PluginContextInfoFields :
    KotlinDurableHandler<Any?, List<String>>(
        config(ContextFieldsPlugin()),
    ) {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): List<String> {
        val futures = mutableListOf<DurableFuture<String>>()
        context.parallel(name = "ctx", maxConcurrency = 1) {
            futures += branch<String>("branch-a") {
                step<String>("inner") { "x" }
                wait(2.seconds)
                "a-done"
            }
            futures += branch<String>("branch-b") { "b-done" }
        }
        return futures.map { it.await() }
    }

    private class ContextFieldsPlugin : DurableExecutionPlugin {
        @Volatile
        private var executionArn: String? = null

        override fun onInvocationStart(info: InvocationInfo) {
            executionArn = info.durableExecutionArn()
        }

        override fun onOperationStart(info: OperationInfo) {
            if (!isContext(info.type())) return
            println(
                jsonObject(
                    "plugin" to "CONFPLUGIN",
                    "hook" to "operation-start",
                    "id" to info.id(),
                    "name" to info.name(),
                    "type" to info.type(),
                    "subType" to info.subType(),
                    "parentId" to info.parentId(),
                    "isReplay" to info.isReplay,
                    "durableExecutionArn" to executionArn,
                ),
            )
        }

        override fun onUserFunctionStart(info: UserFunctionStartInfo) {
            if (!isContext(info.type())) return
            println(
                jsonObject(
                    "plugin" to "CONFPLUGIN",
                    "hook" to "fn-start",
                    "id" to info.id(),
                    "name" to info.name(),
                    "type" to info.type(),
                    "subType" to info.subType(),
                    "parentId" to info.parentId(),
                    "isReplay" to info.isReplay,
                    "isReplayingChildren" to info.isReplayingChildren,
                    "durableExecutionArn" to executionArn,
                ),
            )
        }
    }
}

private fun config(vararg plugins: DurableExecutionPlugin) =
    KotlinDurableRuntime.config {
        withPlugins(*plugins)
    }

private fun isStep(type: String?): Boolean = type == "STEP"

private fun isWait(type: String?): Boolean = type == "WAIT"

private fun isContext(type: String?): Boolean = type.equals("CONTEXT", ignoreCase = true)

private fun isStepChange(type: String?): Boolean = type.equals("STEP", ignoreCase = true)

private fun isBranch(subType: String?): Boolean = subType == "ParallelBranch"

private fun parentOrNone(parentId: String?): String = parentId ?: "NONE"

private fun arnField(executionArn: String?): String =
    executionArn?.let { """, "durableExecutionArn": "$it"""" } ?: ""

private fun jsonObject(vararg fields: Pair<String, Any?>): String =
    fields
        .filter { it.second != null }
        .joinToString(prefix = "{", postfix = "}") { (name, value) ->
            val rendered =
                when (value) {
                    is Boolean, is Number -> value.toString()
                    else -> """"${jsonEscape(value.toString())}""""
                }
            """"${jsonEscape(name)}": $rendered"""
        }

private fun jsonEscape(value: String): String =
    buildString(value.length) {
        value.forEach { char ->
            when (char) {
                '\\' -> append("\\\\")
                '"' -> append("\\\"")
                '\b' -> append("\\b")
                '\u000c' -> append("\\f")
                '\n' -> append("\\n")
                '\r' -> append("\\r")
                '\t' -> append("\\t")
                else -> append(char)
            }
        }
    }
