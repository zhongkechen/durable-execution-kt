package io.github.zhongkechen.durable.internal

import io.github.zhongkechen.durable.CallbackHandle
import io.github.zhongkechen.durable.CallbackOptions
import io.github.zhongkechen.durable.CallbackSubmitterScope
import io.github.zhongkechen.durable.CallbackWaitOptions
import io.github.zhongkechen.durable.ChildOptions
import io.github.zhongkechen.durable.ConditionDecision
import io.github.zhongkechen.durable.ConditionOptions
import io.github.zhongkechen.durable.ConditionScope
import io.github.zhongkechen.durable.DurableContext
import io.github.zhongkechen.durable.DurableFuture
import io.github.zhongkechen.durable.DurableLogger
import io.github.zhongkechen.durable.InvokeOptions
import io.github.zhongkechen.durable.MapOptions
import io.github.zhongkechen.durable.MapResult
import io.github.zhongkechen.durable.ParallelOptions
import io.github.zhongkechen.durable.ParallelResult
import io.github.zhongkechen.durable.ParallelScope
import io.github.zhongkechen.durable.Serde
import io.github.zhongkechen.durable.StepOptions
import io.github.zhongkechen.durable.StepScope
import io.github.zhongkechen.durable.TypeRef
import kotlin.time.Duration

internal class RuntimeDurableContext(
    private val runtime: OperationRuntime,
) : DurableContext {
    override val executionArn: String
        get() = runtime.executionArn

    override val isReplaying: Boolean
        get() = runtime.isReplaying

    override val logger: DurableLogger
        get() = runtime.logger

    override suspend fun <T> step(
        name: String?,
        type: TypeRef<T>,
        options: StepOptions,
        block: suspend StepScope.() -> T,
    ): T = runtime.step(name, type, options, block)

    override suspend fun wait(
        duration: Duration,
        name: String?,
    ) {
        runtime.wait(duration, name)
    }

    override suspend fun <I, O> invoke(
        name: String,
        functionName: String,
        input: I,
        outputType: TypeRef<O>,
        options: InvokeOptions,
    ): O = runtime.invoke(name, functionName, input, outputType, options)

    override suspend fun <T> callback(
        name: String?,
        type: TypeRef<T>,
        options: CallbackOptions,
    ): CallbackHandle<T> = runtime.callback(name, type, options)

    override suspend fun <T> child(
        name: String,
        type: TypeRef<T>,
        options: ChildOptions,
        block: suspend DurableContext.() -> T,
    ): T =
        runtime.child(name, type, options) {
            block(RuntimeDurableContext(this))
        }

    override suspend fun <I, O> map(
        name: String?,
        items: Collection<I>,
        outputType: TypeRef<O>,
        options: MapOptions<I>,
        block: suspend DurableContext.(item: I, index: Int) -> O,
    ): MapResult<O> =
        runtime.map(name, items, outputType, options) { item, index ->
            block(RuntimeDurableContext(this), item, index)
        }

    override suspend fun parallel(
        name: String,
        options: ParallelOptions,
        block: ParallelScope.() -> Unit,
    ): ParallelResult =
        runtime.parallel(name, options) {
            block(RuntimeParallelAdapter(this))
        }

    override suspend fun <T> waitForCallback(
        name: String?,
        type: TypeRef<T>,
        options: CallbackWaitOptions,
        submitter: suspend CallbackSubmitterScope.() -> Unit,
    ): T = runtime.waitForCallback(name, type, options, submitter)

    override suspend fun <T> waitForCondition(
        name: String?,
        type: TypeRef<T>,
        options: ConditionOptions<T>,
        check: suspend ConditionScope<T>.() -> ConditionDecision<T>,
    ): T = runtime.waitForCondition(name, type, options, check)
}

private class RuntimeParallelAdapter(
    private val delegate: RuntimeParallelScope,
) : ParallelScope {
    override fun <T> branch(
        name: String,
        type: TypeRef<T>,
        serde: Serde?,
        block: suspend DurableContext.() -> T,
    ): DurableFuture<T> =
        delegate.branch(name, type, serde) {
            block(RuntimeDurableContext(this))
        }
}
