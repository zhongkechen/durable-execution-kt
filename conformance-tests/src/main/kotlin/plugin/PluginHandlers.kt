package plugin

import io.github.zhongkechen.durable.CallbackFailureException
import io.github.zhongkechen.durable.CompletionPolicy
import io.github.zhongkechen.durable.DurableContext
import io.github.zhongkechen.durable.DurableFuture
import io.github.zhongkechen.durable.DurableHandler
import io.github.zhongkechen.durable.DurablePlugin
import io.github.zhongkechen.durable.DurableRuntimeConfig
import io.github.zhongkechen.durable.ExecutionStatus
import io.github.zhongkechen.durable.FunctionAttemptEnded
import io.github.zhongkechen.durable.FunctionAttemptStarted
import io.github.zhongkechen.durable.InvocationEnded
import io.github.zhongkechen.durable.InvocationStarted
import io.github.zhongkechen.durable.OperationSnapshot
import io.github.zhongkechen.durable.ParallelOptions
import io.github.zhongkechen.durable.RetryJitter
import io.github.zhongkechen.durable.RetryPolicy
import io.github.zhongkechen.durable.StepOptions
import io.github.zhongkechen.durable.child
import io.github.zhongkechen.durable.step
import io.github.zhongkechen.durable.typeRef
import kotlin.time.Duration.Companion.seconds

public class PluginInvocationLifecycle(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(
        typeRef(),
        typeRef(),
        config.withPlugins(ConformanceLoggingPlugin("CONFPLUGIN")),
    ) {
    override suspend fun handle(input: String, context: DurableContext): String =
        context.step("greet") {
            logger.info("Greeting step running for: {}", input)
            "Hello, $input!"
        }
}

public class PluginOperationLifecycle(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(
        typeRef(),
        typeRef(),
        config.withPlugins(ConformanceLoggingPlugin("CONFPLUGIN")),
    ) {
    override suspend fun handle(input: String, context: DurableContext): String =
        context.step("greet") { "Hello, $input!" }
}

public class PluginAttemptHooksRetry(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(
        typeRef(),
        typeRef(),
        config.withPlugins(ConformanceLoggingPlugin("CONFPLUGIN")),
    ) {
    override suspend fun handle(input: Any?, context: DurableContext): String =
        context.step(
            "retry-step",
            StepOptions(retry = RetryPolicy.fixed(maxAttempts = 3, delay = 1.seconds)),
        ) {
            if (attempt < 2) error("Attempt $attempt failed")
            "Operation succeeded"
        }
}

public class PluginErrorIsolation(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(
        typeRef(),
        typeRef(),
        config.withPlugins(FaultyConformancePlugin()),
    ) {
    override suspend fun handle(input: String, context: DurableContext): String =
        context.step("greet") { "Hello, $input!" }
}

public class PluginMultiplePlugins(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(
        typeRef(),
        typeRef(),
        config.withPlugins(InvocationOnlyPlugin("CONFPLUGIN-A"), InvocationOnlyPlugin("CONFPLUGIN-B")),
    ) {
    override suspend fun handle(input: String, context: DurableContext): String =
        context.step("greet") { "Hello, $input!" }
}

public class PluginFirstInvocationFlag(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(
        typeRef(),
        typeRef(),
        config.withPlugins(ConformanceLoggingPlugin("CONFPLUGIN")),
    ) {
    override suspend fun handle(input: Any?, context: DurableContext): String {
        context.wait(2.seconds)
        return "Wait completed"
    }
}

public class PluginTerminalFailure(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(
        typeRef(),
        typeRef(),
        config.withPlugins(ConformanceLoggingPlugin("CONFPLUGIN")),
    ) {
    override suspend fun handle(input: Any?, context: DurableContext): String =
        context.step("failing-step", StepOptions(retry = RetryPolicy.none)) {
            error("Something went wrong")
        }
}

public class PluginOperationChange(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(
        typeRef(),
        typeRef(),
        config.withPlugins(SimpleChangePlugin()),
    ) {
    override suspend fun handle(input: String, context: DurableContext): String =
        context.step("greet") { "Hello, $input!" }
}

public class PluginExternalUpdateOnInvoke(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(
        typeRef(),
        typeRef(),
        config.withPlugins(ExternalUpdatePlugin()),
    ) {
    override suspend fun handle(input: Any?, context: DurableContext): String {
        context.wait(2.seconds)
        return "Wait completed"
    }
}

public class PluginWaitOperationHooks(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(
        typeRef(),
        typeRef(),
        config.withPlugins(WaitHooksPlugin()),
    ) {
    override suspend fun handle(input: Any?, context: DurableContext): String {
        context.wait(2.seconds)
        return "Wait completed"
    }
}

public class PluginNestedParentLinkage(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(
        typeRef(),
        typeRef(),
        config.withPlugins(ParentLinkPlugin()),
    ) {
    override suspend fun handle(input: String, context: DurableContext): String =
        context.child("child") {
            step<String>("greet") { "Hello, $input!" }
        }
}

public class PluginParallelBranchHooks(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, List<String>>(
        typeRef(),
        typeRef(),
        config.withPlugins(BranchHooksPlugin()),
    ) {
    override suspend fun handle(input: Any?, context: DurableContext): List<String> {
        val futures = mutableListOf<DurableFuture<String>>()
        context.parallel("parallel", ParallelOptions(maximumConcurrency = 1)) {
            futures += branch("branch-0", typeRef()) { "task-1" }
            futures += branch("branch-1", typeRef()) { "task-2" }
        }
        return futures.map { it.await() }
    }
}

public class PluginReplayFlags(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(
        typeRef(),
        typeRef(),
        config.withPlugins(ReplayPlugin()),
    ) {
    override suspend fun handle(input: Any?, context: DurableContext): String {
        context.step<String>("step-a") { "a" }
        return context.step(
            "step-b",
            StepOptions(
                retry =
                    RetryPolicy.exponential(
                        maxAttempts = 3,
                        initialDelay = 1.seconds,
                        maximumDelay = 10.seconds,
                        multiplier = 1.0,
                        jitter = RetryJitter.NONE,
                    ),
            ),
        ) {
            if (attempt < 2) error("Attempt $attempt failed")
            "Operation succeeded"
        }
    }
}

public class PluginRetryExhaustion(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(
        typeRef(),
        typeRef(),
        config.withPlugins(AttemptLifecyclePlugin()),
    ) {
    override suspend fun handle(input: Any?, context: DurableContext): String =
        context.step(
            "always-fails",
            StepOptions(
                retry =
                    RetryPolicy.exponential(
                        maxAttempts = 2,
                        initialDelay = 1.seconds,
                        maximumDelay = 10.seconds,
                        multiplier = 1.0,
                        jitter = RetryJitter.NONE,
                    ),
            ),
        ) {
            error("boom")
        }
}

public class PluginSuspensionInvocationEnd(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(
        typeRef(),
        typeRef(),
        config.withPlugins(InvocationShapePlugin(simple = true)),
    ) {
    override suspend fun handle(input: Any?, context: DurableContext): String {
        context.wait(2.seconds)
        return "Wait completed"
    }
}

public class PluginFaultyAndHealthy(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(
        typeRef(),
        typeRef(),
        config.withPlugins(FaultyPeerPlugin(), HealthyPeerPlugin()),
    ) {
    override suspend fun handle(input: String, context: DurableContext): String =
        context.step("greet") { "Hello, $input!" }
}

public class PluginOperationEndPayload(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(
        typeRef(),
        typeRef(),
        config.withPlugins(OperationPayloadPlugin()),
    ) {
    override suspend fun handle(input: Any?, context: DurableContext): String {
        context.step<String>("task-a") { "task-a" }
        return context.step("task-b", StepOptions(retry = RetryPolicy.none)) {
            error("boom")
        }
    }
}

public class PluginPendingWaitReplay(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, List<String>>(
        typeRef(),
        typeRef(),
        config.withPlugins(PendingWaitPlugin()),
    ) {
    override suspend fun handle(input: Any?, context: DurableContext): List<String> {
        val futures = mutableListOf<DurableFuture<String>>()
        context.parallel("waits", ParallelOptions(maximumConcurrency = 2)) {
            futures +=
                branch("short-branch", typeRef()) {
                    wait(2.seconds, "short")
                    "short-done"
                }
            futures +=
                branch("long-branch", typeRef()) {
                    wait(8.seconds, "long")
                    "long-done"
                }
        }
        return futures.map { it.await() }
    }
}

public class PluginInvocationInfoFields(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(
        typeRef(),
        typeRef(),
        config.withPlugins(InvocationShapePlugin(simple = false)),
    ) {
    override suspend fun handle(input: String, context: DurableContext): String {
        context.wait(2.seconds)
        return "done-$input"
    }
}

public class PluginOperationInfoFields(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(
        typeRef(),
        typeRef(),
        config.withPlugins(OperationFieldsPlugin()),
    ) {
    override suspend fun handle(input: Any?, context: DurableContext): String =
        context.step("greet") { "task-a" }
}

public class PluginAttemptInfoFields(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(
        typeRef(),
        typeRef(),
        config.withPlugins(AttemptFieldsPlugin()),
    ) {
    override suspend fun handle(input: Any?, context: DurableContext): String =
        context.step(
            "flaky",
            StepOptions(retry = RetryPolicy.fixed(maxAttempts = 2, delay = 1.seconds)),
        ) {
            if (attempt == 1) error("first attempt failed")
            "ok"
        }
}

public class PluginOperationChangeFields(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(
        typeRef(),
        typeRef(),
        config.withPlugins(ChangeFieldsPlugin()),
    ) {
    override suspend fun handle(input: Any?, context: DurableContext): String =
        context.step("greet") { "task-a" }
}

public class PluginContextInfoFields(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, List<String>>(
        typeRef(),
        typeRef(),
        config.withPlugins(ContextFieldsPlugin()),
    ) {
    override suspend fun handle(input: Any?, context: DurableContext): List<String> {
        val futures = mutableListOf<DurableFuture<String>>()
        context.parallel("ctx", ParallelOptions(maximumConcurrency = 1)) {
            futures +=
                branch("branch-a", typeRef()) {
                    step<String>("inner") { "x" }
                    wait(2.seconds)
                    "a-done"
                }
            futures += branch("branch-b", typeRef()) { "b-done" }
        }
        return futures.map { it.await() }
    }
}

private abstract class ArnPlugin : DurablePlugin {
    protected var executionArn: String? = null

    override fun invocationStarted(info: InvocationStarted) {
        executionArn = info.executionArn
    }

    protected fun fields(vararg values: Pair<String, Any?>) {
        emit(*values, "durableExecutionArn" to executionArn)
    }
}

private class InvocationOnlyPlugin(private val label: String) : ArnPlugin() {
    override fun invocationStarted(info: InvocationStarted) {
        super.invocationStarted(info)
        fields("plugin" to label, "hook" to "invocation-start")
    }

    override fun invocationEnded(info: InvocationEnded) {
        fields("plugin" to label, "hook" to "invocation-end", "status" to info.status.name)
    }
}

private class SimpleChangePlugin : ArnPlugin() {
    override fun operationsChanged(
        executionArn: String,
        changed: Map<String, OperationSnapshot>,
        all: Map<String, OperationSnapshot>,
    ) {
        changed.values.filter(OperationSnapshot::isStep).forEach {
            fields(
                "plugin" to "CONFPLUGIN",
                "hook" to "operation-change",
                "op" to it.id,
                "status" to it.status,
                "in_full_map" to all.containsKey(it.id),
            )
        }
    }
}

private class ExternalUpdatePlugin : ArnPlugin() {
    override fun invocationStarted(info: InvocationStarted) {
        super.invocationStarted(info)
        info.updatedOperations.values.filter(OperationSnapshot::isWait).forEach {
            fields(
                "plugin" to "CONFPLUGIN",
                "hook" to "updated-on-invoke",
                "op" to it.id,
                "status" to it.status,
                "first" to info.firstInvocation,
            )
        }
    }
}

private class WaitHooksPlugin : ArnPlugin() {
    override fun operationStarted(operation: OperationSnapshot) {
        if (operation.isWait()) {
            fields("plugin" to "CONFPLUGIN", "hook" to "operation-start", "op" to operation.id, "type" to "WAIT")
        }
    }

    override fun operationEnded(operation: OperationSnapshot) {
        if (operation.isWait()) {
            fields(
                "plugin" to "CONFPLUGIN",
                "hook" to "operation-end",
                "op" to operation.id,
                "type" to "WAIT",
                "status" to operation.status,
            )
        }
    }
}

private class ParentLinkPlugin : ArnPlugin() {
    override fun operationEnded(operation: OperationSnapshot) {
        if (operation.isStep()) {
            fields(
                "plugin" to "CONFPLUGIN",
                "hook" to "operation-end",
                "op" to operation.id,
                "parent" to (operation.parentId ?: "NONE"),
                "status" to operation.status,
            )
        }
    }
}

private class BranchHooksPlugin : ArnPlugin() {
    override fun functionStarted(info: FunctionAttemptStarted) {
        if (info.operation.isBranch()) {
            fields(
                "plugin" to "CONFPLUGIN",
                "hook" to "fn-start",
                "op" to info.operation.id,
                "parent" to (info.operation.parentId ?: "NONE"),
            )
        }
    }

    override fun functionEnded(info: FunctionAttemptEnded) {
        if (info.operation.isBranch()) {
            fields(
                "plugin" to "CONFPLUGIN",
                "hook" to "fn-end",
                "op" to info.operation.id,
                "parent" to (info.operation.parentId ?: "NONE"),
                "outcome" to if (info.succeeded) "SUCCEEDED" else "FAILED",
            )
        }
    }
}

private class ReplayPlugin : ArnPlugin() {
    override fun operationStarted(operation: OperationSnapshot) {
        if (operation.isStep()) {
            fields(
                "plugin" to "CONFPLUGIN",
                "hook" to "operation-start",
                "op" to operation.id,
                "replay" to operation.replay,
            )
        }
    }

    override fun operationEnded(operation: OperationSnapshot) {
        if (operation.isStep()) {
            fields(
                "plugin" to "CONFPLUGIN",
                "hook" to "operation-end",
                "op" to operation.id,
                "status" to operation.status,
            )
        }
    }
}

private class AttemptLifecyclePlugin : ArnPlugin() {
    override fun functionStarted(info: FunctionAttemptStarted) {
        if (info.operation.isStep()) {
            fields("plugin" to "CONFPLUGIN", "hook" to "attempt-start", "n" to info.operation.attempt, "op" to info.operation.id)
        }
    }

    override fun functionEnded(info: FunctionAttemptEnded) {
        if (info.operation.isStep()) {
            fields(
                "plugin" to "CONFPLUGIN",
                "hook" to "attempt-end",
                "n" to info.operation.attempt,
                "outcome" to if (info.succeeded) "SUCCEEDED" else "FAILED",
                "op" to info.operation.id,
            )
        }
    }

    override fun operationEnded(operation: OperationSnapshot) {
        if (operation.isStep()) {
            fields("plugin" to "CONFPLUGIN", "hook" to "operation-end", "op" to operation.id, "status" to operation.status)
        }
    }
}

private class InvocationShapePlugin(private val simple: Boolean) : ArnPlugin() {
    override fun invocationStarted(info: InvocationStarted) {
        super.invocationStarted(info)
        if (simple) {
            fields("plugin" to "CONFPLUGIN", "hook" to "invocation-start", "first" to info.firstInvocation)
        } else {
            fields(
                "plugin" to "CONFPLUGIN",
                "hook" to "invocation-start",
                "isFirstInvocation" to info.firstInvocation,
                "requestId" to info.requestId,
                "operationsCount" to info.operations.size,
                "updatedOperationsCount" to info.updatedOperations.size,
                "executionStartTimestamp" to info.executionStartedAt.toString(),
            )
        }
    }

    override fun invocationEnded(info: InvocationEnded) {
        val terminal = info.status == ExecutionStatus.SUCCEEDED || info.status == ExecutionStatus.FAILED
        if (simple) {
            fields(
                "plugin" to "CONFPLUGIN",
                "hook" to "invocation-end",
                "first" to info.firstInvocation,
                "terminal" to terminal,
                "status" to info.status.name,
            )
        } else {
            fields(
                "plugin" to "CONFPLUGIN",
                "hook" to "invocation-end",
                "isFirstInvocation" to info.firstInvocation,
                "requestId" to info.requestId,
                "operationsCount" to info.operations.size,
                "executionStartTimestamp" to info.executionStartedAt.toString(),
                "status" to info.status.name,
                "terminal" to terminal,
                "executionError" to info.error?.message,
            )
        }
    }
}

private class FaultyPeerPlugin : ArnPlugin() {
    override fun invocationStarted(info: InvocationStarted) {
        super.invocationStarted(info)
        fail("invocation-start")
    }

    override fun operationStarted(operation: OperationSnapshot) {
        if (operation.isStep()) fail("operation-start")
    }

    override fun functionStarted(info: FunctionAttemptStarted) {
        if (info.operation.isStep()) fail("attempt-start")
    }

    override fun functionEnded(info: FunctionAttemptEnded) {
        if (info.operation.isStep()) fail("attempt-end")
    }

    override fun operationEnded(operation: OperationSnapshot) {
        if (operation.isStep()) fail("operation-end")
    }

    override fun invocationEnded(info: InvocationEnded) = fail("invocation-end")

    private fun fail(hook: String): Nothing {
        fields("plugin" to "CONFPLUGIN-FAULTY", "hook" to hook)
        error("faulty $hook")
    }
}

private class HealthyPeerPlugin : ConformanceLoggingPlugin("CONFPLUGIN-HEALTHY")

private class OperationPayloadPlugin : ArnPlugin() {
    override fun operationEnded(operation: OperationSnapshot) {
        if (operation.isStep()) {
            fields(
                "plugin" to "CONFPLUGIN",
                "hook" to "operation-end",
                "op" to operation.id,
                "status" to operation.status,
                "result" to (operation.resultPayload ?: "NONE"),
                "error" to (operation.error?.message ?: "NONE"),
            )
        }
    }
}

private class PendingWaitPlugin : ArnPlugin() {
    override fun operationStarted(operation: OperationSnapshot) {
        if (operation.isWait()) {
            fields(
                "plugin" to "CONFPLUGIN",
                "hook" to "operation-start",
                "type" to "WAIT",
                "name" to operation.name,
                "replay" to operation.replay,
                "pending" to (operation.endedAt == null),
            )
        }
    }

    override fun operationEnded(operation: OperationSnapshot) {
        if (operation.isWait()) {
            fields(
                "plugin" to "CONFPLUGIN",
                "hook" to "operation-end",
                "type" to "WAIT",
                "name" to operation.name,
                "status" to operation.status,
            )
        }
    }
}

private class OperationFieldsPlugin : ArnPlugin() {
    override fun operationStarted(operation: OperationSnapshot) {
        if (operation.isStep()) fields(*operation.fields("operation-start"))
    }

    override fun operationEnded(operation: OperationSnapshot) {
        if (operation.isStep()) fields(*operation.fields("operation-end"))
    }
}

private class AttemptFieldsPlugin : ArnPlugin() {
    override fun functionStarted(info: FunctionAttemptStarted) {
        if (!info.operation.isStep()) return
        fields(
            *info.operation.fields("attempt-start"),
            "startTimestamp" to info.startedAt.toString(),
            "isReplay" to info.operation.replay,
        )
    }

    override fun functionEnded(info: FunctionAttemptEnded) {
        if (!info.operation.isStep()) return
        fields(
            *info.operation.fields("attempt-end"),
            "startTimestamp" to info.startedAt.toString(),
            "endTimestamp" to info.endedAt.toString(),
            "isReplay" to info.operation.replay,
            "outcome" to if (info.succeeded) "SUCCEEDED" else "FAILED",
            "error" to info.error?.message,
        )
    }
}

private class ChangeFieldsPlugin : ArnPlugin() {
    override fun operationsChanged(
        executionArn: String,
        changed: Map<String, OperationSnapshot>,
        all: Map<String, OperationSnapshot>,
    ) {
        changed.values.filter(OperationSnapshot::isStep).forEach {
            fields(
                "plugin" to "CONFPLUGIN",
                "hook" to "operation-change",
                "executionArn" to executionArn,
                "updatedOperationsCount" to changed.size,
                "operationsCount" to all.size,
                "inFullMap" to all.containsKey(it.id),
                *it.fields(null),
            )
        }
    }
}

private class ContextFieldsPlugin : ArnPlugin() {
    override fun operationStarted(operation: OperationSnapshot) {
        if (operation.isContext()) fields(*operation.fields("operation-start"))
    }

    override fun functionStarted(info: FunctionAttemptStarted) {
        if (!info.operation.isContext()) return
        fields(
            *info.operation.fields("fn-start"),
            "isReplayingChildren" to info.replayingChildren,
        )
    }
}

private fun OperationSnapshot.fields(hook: String?): Array<Pair<String, Any?>> =
    arrayOf(
        "plugin" to "CONFPLUGIN",
        "hook" to hook,
        "id" to id,
        "name" to name,
        "type" to type,
        "subType" to subtype,
        "parentId" to parentId,
        "status" to status,
        "startTimestamp" to startedAt?.toString(),
        "endTimestamp" to endedAt?.toString(),
        "error" to error?.message,
        "result" to resultPayload,
        "attempt" to attempt,
        "isReplay" to replay,
    )

private fun DurableRuntimeConfig.withPlugins(vararg plugins: DurablePlugin): DurableRuntimeConfig =
    copy(plugins = plugins.toList())
