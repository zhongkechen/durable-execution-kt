package io.github.zhongkechen.durable

import kotlin.time.Duration

public interface DurableFuture<out T> {
    public suspend fun await(): T
}

public interface CallbackHandle<out T> : DurableFuture<T> {
    public val id: String
}

public interface StepScope {
    public val attempt: Int
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
        block: suspend DurableContext.() -> T,
    ): DurableFuture<T>
}

public interface DurableContext {
    public val executionArn: String
    public val isReplaying: Boolean

    public suspend fun <T> step(
        name: String? = null,
        type: TypeRef<T>,
        options: StepOptions = StepOptions(),
        block: suspend StepScope.() -> T,
    ): T

    public suspend fun wait(
        duration: Duration,
        name: String? = null,
    )

    public suspend fun <I, O> invoke(
        name: String,
        functionName: String,
        input: I,
        outputType: TypeRef<O>,
        options: InvokeOptions = InvokeOptions(),
    ): O

    public suspend fun <T> callback(
        name: String? = null,
        type: TypeRef<T>,
        options: CallbackOptions = CallbackOptions(),
    ): CallbackHandle<T>

    public suspend fun <T> child(
        name: String,
        type: TypeRef<T>,
        options: ChildOptions = ChildOptions(),
        block: suspend DurableContext.() -> T,
    ): T

    public suspend fun <I, O> map(
        name: String? = null,
        items: Collection<I>,
        outputType: TypeRef<O>,
        options: MapOptions<I> = MapOptions(),
        block: suspend DurableContext.(item: I, index: Int) -> O,
    ): MapResult<O>

    public suspend fun parallel(
        name: String,
        options: ParallelOptions = ParallelOptions(),
        block: ParallelScope.() -> Unit,
    ): ParallelResult

    public suspend fun <T> waitForCallback(
        name: String? = null,
        type: TypeRef<T>,
        options: CallbackWaitOptions = CallbackWaitOptions(),
        submitter: suspend CallbackSubmitterScope.() -> Unit,
    ): T

    public suspend fun <T> waitForCondition(
        name: String? = null,
        type: TypeRef<T>,
        options: ConditionOptions<T>,
        check: suspend ConditionScope<T>.() -> ConditionDecision<T>,
    ): T
}

public suspend inline fun <reified T> DurableContext.step(
    name: String? = null,
    options: StepOptions = StepOptions(),
    noinline block: suspend StepScope.() -> T,
): T = step(name, typeRef(), options, block)

public suspend inline fun <reified T> DurableContext.child(
    name: String,
    options: ChildOptions = ChildOptions(),
    noinline block: suspend DurableContext.() -> T,
): T = child(name, typeRef(), options, block)
