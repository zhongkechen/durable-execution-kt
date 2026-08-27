package io.github.zhongkechen.durable

import io.github.zhongkechen.durable.extension.ExtensionCallbackConfig
import io.github.zhongkechen.durable.extension.ExtensionContextConfig
import io.github.zhongkechen.durable.extension.ExtensionContextResult
import io.github.zhongkechen.durable.extension.ExtensionInvokeConfig
import io.github.zhongkechen.durable.extension.ExtensionRetryDecision
import io.github.zhongkechen.durable.extension.ExtensionRetryStrategy
import io.github.zhongkechen.durable.extension.ExtensionStepConfig
import io.github.zhongkechen.durable.extension.ExtensionStepResult
import io.github.zhongkechen.durable.extension.currentExtensionContext
import io.github.zhongkechen.durable.internal.RuntimeParallelScope
import io.github.zhongkechen.durable.internal.currentRuntimeExtensionContext
import io.github.zhongkechen.durable.internal.withRuntime
import kotlin.time.Duration

public interface DurableFuture<out T> {
    public suspend fun await(): T
}

public interface CallbackHandle<out T> : DurableFuture<T> {
    public val id: String
}

public interface StepScope {
    public val attempt: Int
    public val logger: DurableLogger
}

public interface CallbackSubmitterScope : StepScope {
    public val callbackId: String
}

public interface ConditionScope<T> : StepScope {
    public val state: T
}

public interface ParallelScope {
    public fun <T> branch(
        name: String,
        type: TypeRef<T>,
        serde: Serde? = null,
        block: suspend () -> T,
    ): DurableFuture<T>
}

/** The current durable execution ARN. */
public suspend fun currentExecutionArn(): String =
    currentExtensionContext().executionArn

/** Whether the current durable scope is replaying checkpointed work. */
public suspend fun isReplaying(): Boolean =
    currentExtensionContext().isReplaying

/** Replay-aware logger for the current durable scope. */
public suspend fun durableLogger(): DurableLogger =
    currentExtensionContext().logger

public suspend fun <T> step(
    name: String? = null,
    type: TypeRef<T>,
    options: StepOptions = StepOptions(),
    block: suspend StepScope.() -> T,
): T {
    val retry =
        ExtensionRetryStrategy<T> { error, state, attempt ->
            when (val decision = options.retry.decide(error, attempt)) {
                RetryDecision.Fail -> ExtensionRetryDecision.DoNotRetry
                is RetryDecision.Retry ->
                    ExtensionRetryDecision.Retry(
                        state = state,
                        delay = decision.delay,
                    )
            }
        }
    return currentExtensionContext()
        .reserve(name)
        .step(
            subtype = "Step",
            type = type,
            config =
                ExtensionStepConfig(
                    serde = options.serde,
                    retry = retry,
                    delivery = options.delivery,
                ),
        ) { ExtensionStepResult.Succeeded(block()) }
}

public suspend inline fun <reified T> step(
    name: String? = null,
    options: StepOptions = StepOptions(),
    noinline block: suspend StepScope.() -> T,
): T = step(name, typeRef(), options, block)

public suspend fun wait(
    duration: Duration,
    name: String? = null,
) {
    currentExtensionContext()
        .reserve(name)
        .wait("Wait", duration)
}

public suspend fun <I, O> invoke(
    name: String,
    functionName: String,
    input: I,
    outputType: TypeRef<O>,
    options: InvokeOptions = InvokeOptions(),
): O =
    currentExtensionContext()
        .reserve(name)
        .invoke(
            subtype = "ChainedInvoke",
            functionName = functionName,
            input = input,
            outputType = outputType,
            config =
                ExtensionInvokeConfig(
                    payloadSerde = options.payloadSerde,
                    resultSerde = options.resultSerde,
                    tenantId = options.tenantId,
                ),
        )

public suspend inline fun <I, reified O> invoke(
    name: String,
    functionName: String,
    input: I,
    options: InvokeOptions = InvokeOptions(),
): O = invoke(name, functionName, input, typeRef(), options)

public suspend fun <T> createCallback(
    name: String? = null,
    type: TypeRef<T>,
    options: CallbackOptions = CallbackOptions(),
): CallbackHandle<T> =
    currentExtensionContext()
        .reserve(name)
        .createCallback(
            subtype = "Callback",
            type = type,
            config =
                ExtensionCallbackConfig(
                    timeout = options.timeout,
                    heartbeatTimeout = options.heartbeatTimeout,
                    serde = options.serde,
                ),
        )

public suspend inline fun <reified T> createCallback(
    name: String? = null,
    options: CallbackOptions = CallbackOptions(),
): CallbackHandle<T> = createCallback(name, typeRef(), options)

public suspend fun <T> runInChildContext(
    name: String,
    type: TypeRef<T>,
    options: ChildOptions = ChildOptions(),
    block: suspend () -> T,
): T =
    currentExtensionContext()
        .reserve(name)
        .runInChildContext(
            subtype = "RunInChildContext",
            type = type,
            config =
                ExtensionContextConfig(
                    serde = options.serde,
                    virtual = options.virtual,
                ),
        ) {
            ExtensionContextResult.ReplayChildrenAboveSize(
                result = block(),
                replayState = null,
                thresholdBytes = LARGE_CONTEXT_RESULT_BYTES,
            )
        }

public suspend inline fun <reified T> runInChildContext(
    name: String,
    options: ChildOptions = ChildOptions(),
    noinline block: suspend () -> T,
): T = runInChildContext(name, typeRef(), options, block)

public suspend fun <I, O> map(
    name: String? = null,
    items: Collection<I>,
    outputType: TypeRef<O>,
    options: MapOptions<I> = MapOptions(),
    block: suspend (item: I, index: Int) -> O,
): MapResult<O> =
    currentRuntimeExtensionContext().runtime.map(
        name = name,
        items = items,
        outputType = outputType,
        options = options,
    ) { item, index ->
        withRuntime(this) {
            block(item, index)
        }
    }

public suspend inline fun <I, reified O> map(
    name: String? = null,
    items: Collection<I>,
    options: MapOptions<I> = MapOptions(),
    noinline block: suspend (item: I, index: Int) -> O,
): MapResult<O> = map(name, items, typeRef(), options, block)

public suspend fun parallel(
    name: String,
    options: ParallelOptions = ParallelOptions(),
    block: ParallelScope.() -> Unit,
): ParallelResult =
    currentRuntimeExtensionContext().runtime.parallel(name, options) {
        block(FacadeParallelScope(this))
    }

public suspend fun <T> waitForCallback(
    name: String? = null,
    type: TypeRef<T>,
    options: CallbackWaitOptions = CallbackWaitOptions(),
    submitter: suspend CallbackSubmitterScope.() -> Unit,
): T =
    currentRuntimeExtensionContext().runtime.waitForCallback(
        name = name,
        type = type,
        options = options,
        submitter = submitter,
    )

public suspend inline fun <reified T> waitForCallback(
    name: String? = null,
    options: CallbackWaitOptions = CallbackWaitOptions(),
    noinline submitter: suspend CallbackSubmitterScope.() -> Unit,
): T = waitForCallback(name, typeRef(), options, submitter)

public suspend fun <T> waitForCondition(
    name: String? = null,
    type: TypeRef<T>,
    options: ConditionOptions<T>,
    check: suspend ConditionScope<T>.() -> ConditionDecision<T>,
): T =
    currentRuntimeExtensionContext().runtime.waitForCondition(
        name = name,
        type = type,
        options = options,
        check = check,
    )

public suspend inline fun <reified T> waitForCondition(
    name: String? = null,
    options: ConditionOptions<T>,
    noinline check: suspend ConditionScope<T>.() -> ConditionDecision<T>,
): T = waitForCondition(name, typeRef(), options, check)

public inline fun <reified T> ParallelScope.branch(
    name: String,
    serde: Serde? = null,
    noinline block: suspend () -> T,
): DurableFuture<T> = branch(name, typeRef(), serde, block)

private class FacadeParallelScope(
    private val delegate: RuntimeParallelScope,
) : ParallelScope {
    override fun <T> branch(
        name: String,
        type: TypeRef<T>,
        serde: Serde?,
        block: suspend () -> T,
    ): DurableFuture<T> =
        delegate.branch(name, type, serde) {
            withRuntime(this) {
                block()
            }
        }
}

private const val LARGE_CONTEXT_RESULT_BYTES: Int = 256 * 1024
