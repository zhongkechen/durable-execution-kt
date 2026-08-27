// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package software.amazon.lambda.durable.kotlin

import java.time.Duration
import java.util.concurrent.CompletableFuture
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration as KotlinDuration
import kotlin.time.toJavaDuration
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.future.future
import kotlinx.coroutines.runBlocking
import software.amazon.lambda.durable.DurableCallbackFuture
import software.amazon.lambda.durable.DurableContext
import software.amazon.lambda.durable.DurableFuture
import software.amazon.lambda.durable.ParallelDurableFuture
import software.amazon.lambda.durable.StepContext
import software.amazon.lambda.durable.TypeToken
import software.amazon.lambda.durable.config.CallbackConfig
import software.amazon.lambda.durable.config.InvokeConfig
import software.amazon.lambda.durable.config.MapConfig
import software.amazon.lambda.durable.config.ParallelBranchConfig
import software.amazon.lambda.durable.config.ParallelConfig
import software.amazon.lambda.durable.config.RunInChildContextConfig
import software.amazon.lambda.durable.config.StepConfig
import software.amazon.lambda.durable.config.StepSemantics
import software.amazon.lambda.durable.config.WaitForCallbackConfig
import software.amazon.lambda.durable.config.WaitForConditionConfig
import software.amazon.lambda.durable.extension.DurableExecutionContextSnapshot
import software.amazon.lambda.durable.extension.ExtensionContext
import software.amazon.lambda.durable.extension.ExtensionContextConfig
import software.amazon.lambda.durable.extension.ExtensionContextResult
import software.amazon.lambda.durable.extension.ExtensionStepConfig
import software.amazon.lambda.durable.extension.ExtensionStepResult
import software.amazon.lambda.durable.model.MapResult
import software.amazon.lambda.durable.model.OperationSubType
import software.amazon.lambda.durable.model.ParallelResult
import software.amazon.lambda.durable.model.WaitForConditionResult
import software.amazon.lambda.durable.serde.SerDes

public class KotlinDurableContext internal constructor(
    public val javaContext: DurableContext,
) {
    @PublishedApi
    internal val extensionContext: ExtensionContext = javaContext as ExtensionContext

    public val isReplaying: Boolean
        get() = extensionContext.isReplaying

    public suspend inline fun <reified T> step(
        name: String? = null,
        retry: RetryPolicy = RetryPolicy.default,
        semantics: DeliverySemantics = DeliverySemantics.AT_LEAST_ONCE_PER_RETRY,
        serDes: SerDes? = null,
        noinline block: suspend KotlinStepContext.() -> T,
    ): T =
        step(
            name,
            typeToken(),
            StepConfig.builder()
                .retryStrategy(retry.javaStrategy)
                .semanticsPerRetry(
                    when (semantics) {
                        DeliverySemantics.AT_LEAST_ONCE_PER_RETRY -> StepSemantics.AT_LEAST_ONCE_PER_RETRY
                        DeliverySemantics.AT_MOST_ONCE_PER_RETRY -> StepSemantics.AT_MOST_ONCE_PER_RETRY
                    },
                ).serDes(serDes)
                .build(),
            block,
        )

    public suspend inline fun <reified T> step(
        name: String?,
        config: StepConfig,
        noinline block: suspend KotlinStepContext.() -> T,
    ): T = step(name, typeToken(), config, block)

    public suspend fun <T> step(
        name: String?,
        resultType: TypeToken<T>,
        config: StepConfig,
        block: suspend KotlinStepContext.() -> T,
    ): T {
        val parentCoroutineContext = currentCoroutineContext()
        return extensionContext
            .reserve(name)
            .stepAsync(
                OperationSubType.STEP.value,
                resultType,
                {
                    coroutineStage(parentCoroutineContext) {
                        ExtensionStepResult.succeed(block(KotlinStepContext(StepContext.requireCurrentContext())))
                    }
                },
                extensionStepConfig(config),
            ).await()
    }

    public suspend fun wait(
        duration: Duration,
        name: String? = null,
    ) {
        extensionContext
            .reserve(name)
            .waitAsync(OperationSubType.WAIT.value, duration)
            .await()
    }

    public suspend fun wait(
        duration: KotlinDuration,
        name: String? = null,
    ): Unit = wait(duration.toJavaDuration(), name)

    public suspend inline fun <reified T, U> invoke(
        name: String,
        functionName: String,
        payload: U,
        tenantId: String? = null,
        payloadSerDes: SerDes? = null,
        resultSerDes: SerDes? = null,
    ): T =
        invoke(
            name,
            functionName,
            payload,
            typeToken(),
            InvokeConfig.builder()
                .tenantId(tenantId)
                .payloadSerDes(payloadSerDes)
                .serDes(resultSerDes)
                .build(),
        )

    public suspend inline fun <reified T, U> invoke(
        name: String,
        functionName: String,
        payload: U,
        config: InvokeConfig,
    ): T = invoke(name, functionName, payload, typeToken(), config)

    public suspend fun <T, U> invoke(
        name: String,
        functionName: String,
        payload: U,
        resultType: TypeToken<T>,
        config: InvokeConfig,
    ): T = javaContext.invokeAsync(name, functionName, payload, resultType, config).await()

    public inline fun <reified T> callback(
        name: String? = null,
        timeout: KotlinDuration? = null,
        heartbeatTimeout: KotlinDuration? = null,
        serDes: SerDes? = null,
    ): KotlinCallback<T> =
        callback(
            name,
            typeToken(),
            CallbackConfig.builder()
                .timeout(timeout?.toJavaDuration())
                .heartbeatTimeout(heartbeatTimeout?.toJavaDuration())
                .serDes(serDes)
                .build(),
        )

    public inline fun <reified T> callback(
        name: String?,
        config: CallbackConfig,
    ): KotlinCallback<T> = callback(name, typeToken(), config)

    public fun <T> callback(
        name: String?,
        resultType: TypeToken<T>,
        config: CallbackConfig,
    ): KotlinCallback<T> =
        KotlinCallback(javaContext.createCallback(name, resultType, config))

    public suspend inline fun <reified T> childContext(
        name: String,
        serDes: SerDes? = null,
        virtual: Boolean = false,
        noinline block: suspend KotlinDurableContext.() -> T,
    ): T =
        childContext(
            name,
            typeToken(),
            RunInChildContextConfig.builder().serDes(serDes).isVirtual(virtual).build(),
            block,
        )

    public suspend inline fun <reified T> childContext(
        name: String,
        config: RunInChildContextConfig,
        noinline block: suspend KotlinDurableContext.() -> T,
    ): T = childContext(name, typeToken(), config, block)

    public suspend fun <T> childContext(
        name: String,
        resultType: TypeToken<T>,
        config: RunInChildContextConfig,
        block: suspend KotlinDurableContext.() -> T,
    ): T {
        val parentCoroutineContext = currentCoroutineContext()
        return extensionContext
            .reserve(name)
            .runInChildContextAsync(
                OperationSubType.RUN_IN_CHILD_CONTEXT.value,
                resultType,
                {
                    coroutineStage(parentCoroutineContext) {
                        val child = KotlinDurableContext(DurableContext.requireCurrentContext())
                        ExtensionContextResult.replayChildrenAboveSize(block(child), null, LARGE_RESULT_THRESHOLD)
                    }
                },
                ExtensionContextConfig.builder()
                    .serDes(config.serDes())
                    .isVirtual(config.isVirtual)
                    .build(),
            ).await()
    }

    public suspend inline fun <I, reified O> map(
        name: String? = null,
        items: Collection<I>,
        maxConcurrency: Int? = null,
        completion: CompletionPolicy = CompletionPolicy.allCompleted,
        nesting: Nesting = Nesting.NESTED,
        itemSerDes: SerDes? = null,
        resultSerDes: SerDes? = null,
        noinline itemName: ((item: I, index: Int) -> String?)? = null,
        noinline block: suspend KotlinDurableContext.(item: I, index: Int) -> O,
    ): MapResult<O> {
        val builder =
            MapConfig.builder()
                .completionConfig(completion.javaConfig)
                .nestingType(nesting.toJava())
                .itemSerDes(itemSerDes)
                .operationSerDes(resultSerDes)
        if (maxConcurrency != null) builder.maxConcurrency(maxConcurrency)
        if (itemName != null) {
            builder.itemNamer { item, index ->
                @Suppress("UNCHECKED_CAST")
                itemName(item as I, index)
            }
        }
        return map(name, items, typeToken(), builder.build(), block)
    }

    public suspend inline fun <I, reified O> map(
        name: String?,
        items: Collection<I>,
        config: MapConfig,
        noinline block: suspend KotlinDurableContext.(item: I, index: Int) -> O,
    ): MapResult<O> = map(name, items, typeToken(), config, block)

    public suspend fun <I, O> map(
        name: String?,
        items: Collection<I>,
        resultType: TypeToken<O>,
        config: MapConfig,
        block: suspend KotlinDurableContext.(item: I, index: Int) -> O,
    ): MapResult<O> {
        val parentCoroutineContext = currentCoroutineContext()
        return javaContext
            .mapAsync(
                name,
                items,
                resultType,
                { item, index, childContext ->
                    runSuspendBlocking(parentCoroutineContext) {
                        block(KotlinDurableContext(childContext), item, index)
                    }
                },
                config,
            ).await()
    }

    public suspend fun parallel(
        name: String,
        maxConcurrency: Int? = null,
        completion: CompletionPolicy = CompletionPolicy.allCompleted,
        nesting: Nesting = Nesting.NESTED,
        itemSerDes: SerDes? = null,
        block: KotlinParallel.() -> Unit,
    ): ParallelResult {
        val builder =
            ParallelConfig.builder()
                .completionConfig(completion.javaConfig)
                .nestingType(nesting.toJava())
        if (maxConcurrency != null) builder.maxConcurrency(maxConcurrency)
        return parallel(name, builder.build(), itemSerDes, block)
    }

    public suspend fun parallel(
        name: String,
        config: ParallelConfig,
        itemSerDes: SerDes? = null,
        block: KotlinParallel.() -> Unit,
    ): ParallelResult {
        val parentCoroutineContext = currentCoroutineContext()
        val future = javaContext.parallel(name, config)
        KotlinParallel(future, parentCoroutineContext, itemSerDes).block()
        return future.get()
    }

    public suspend inline fun <reified T> waitForCallback(
        name: String?,
        timeout: KotlinDuration? = null,
        heartbeatTimeout: KotlinDuration? = null,
        submitterRetry: RetryPolicy = RetryPolicy.default,
        submitterSemantics: DeliverySemantics = DeliverySemantics.AT_LEAST_ONCE_PER_RETRY,
        serDes: SerDes? = null,
        noinline submitter: suspend KotlinCallbackSubmitterContext.() -> Unit,
    ): T =
        waitForCallback(
            name,
            typeToken(),
            WaitForCallbackConfig.builder()
                .stepConfig(
                    StepConfig.builder()
                        .retryStrategy(submitterRetry.javaStrategy)
                        .semanticsPerRetry(
                            when (submitterSemantics) {
                                DeliverySemantics.AT_LEAST_ONCE_PER_RETRY ->
                                    StepSemantics.AT_LEAST_ONCE_PER_RETRY
                                DeliverySemantics.AT_MOST_ONCE_PER_RETRY ->
                                    StepSemantics.AT_MOST_ONCE_PER_RETRY
                            },
                        ).serDes(serDes)
                        .build(),
                ).callbackConfig(
                    CallbackConfig.builder()
                        .timeout(timeout?.toJavaDuration())
                        .heartbeatTimeout(heartbeatTimeout?.toJavaDuration())
                        .serDes(serDes)
                        .build(),
                ).build(),
            submitter,
        )

    public suspend inline fun <reified T> waitForCallback(
        name: String?,
        config: WaitForCallbackConfig,
        noinline submitter: suspend KotlinCallbackSubmitterContext.() -> Unit,
    ): T = waitForCallback(name, typeToken(), config, submitter)

    public suspend fun <T> waitForCallback(
        name: String?,
        resultType: TypeToken<T>,
        config: WaitForCallbackConfig,
        submitter: suspend KotlinCallbackSubmitterContext.() -> Unit,
    ): T {
        val parentCoroutineContext = currentCoroutineContext()
        return javaContext
            .waitForCallbackAsync(
                name,
                resultType,
                { callbackId, stepContext ->
                    runSuspendBlocking(parentCoroutineContext) {
                        submitter(KotlinCallbackSubmitterContext(callbackId, KotlinStepContext(stepContext)))
                    }
                },
                config,
            ).await()
    }

    public suspend inline fun <reified T> waitForCondition(
        name: String?,
        initialState: T? = null,
        wait: ConditionWaitPolicy<T>? = null,
        serDes: SerDes? = null,
        noinline check: suspend KotlinConditionContext<T>.() -> WaitForConditionResult<T>,
    ): T {
        val builder =
            WaitForConditionConfig.builder<T>()
                .initialState(initialState)
                .serDes(serDes)
        if (wait != null) builder.waitStrategy(wait.javaStrategy)
        return waitForCondition(name, typeToken(), builder.build(), check)
    }

    public suspend inline fun <reified T> waitForCondition(
        name: String?,
        config: WaitForConditionConfig<T>,
        noinline check: suspend KotlinConditionContext<T>.() -> WaitForConditionResult<T>,
    ): T = waitForCondition(name, typeToken(), config, check)

    public suspend fun <T> waitForCondition(
        name: String?,
        resultType: TypeToken<T>,
        config: WaitForConditionConfig<T>,
        check: suspend KotlinConditionContext<T>.() -> WaitForConditionResult<T>,
    ): T {
        val parentCoroutineContext = currentCoroutineContext()
        return javaContext
            .waitForConditionAsync(
                name,
                resultType,
                { state, stepContext ->
                    runSuspendBlocking(parentCoroutineContext) {
                        check(KotlinConditionContext(state, KotlinStepContext(stepContext)))
                    }
                },
                config,
            ).await()
    }

    private fun <T> coroutineStage(
        parentCoroutineContext: CoroutineContext,
        block: suspend () -> T,
    ): CompletableFuture<T> {
        val snapshot = DurableExecutionContextSnapshot.capture()
        val parentJob = parentCoroutineContext[Job]
        val supervisor = SupervisorJob()
        val cancellationHandle =
            parentJob?.invokeOnCompletion {
                supervisor.cancel()
            }
        val functionContext =
            parentCoroutineContext.minusKey(Job) +
                supervisor +
                DurableExecutionContextElement(snapshot)
        return CoroutineScope(functionContext)
            .future {
                block()
            }.whenComplete { _, _ ->
                cancellationHandle?.dispose()
                supervisor.complete()
            }
    }

    private fun <T> runSuspendBlocking(
        parentCoroutineContext: CoroutineContext,
        block: suspend () -> T,
    ): T {
        val snapshot = DurableExecutionContextSnapshot.capture()
        val supervisor = SupervisorJob()
        val cancellationHandle =
            parentCoroutineContext[Job]?.invokeOnCompletion {
                supervisor.cancel()
            }
        return try {
            runBlocking(
                parentCoroutineContext.minusKey(Job) +
                    supervisor +
                    DurableExecutionContextElement(snapshot),
            ) {
                block()
            }
        } finally {
            cancellationHandle?.dispose()
            supervisor.cancel()
        }
    }

    private fun <T> extensionStepConfig(config: StepConfig): ExtensionStepConfig<T> =
        ExtensionStepConfig.builder<T>()
            .serDes(config.serDes())
            .semanticsPerRetry(
                when (config.semanticsPerRetry()) {
                    StepSemantics.AT_LEAST_ONCE_PER_RETRY ->
                        ExtensionStepConfig.StepSemantics.AT_LEAST_ONCE_PER_RETRY
                    StepSemantics.AT_MOST_ONCE_PER_RETRY ->
                        ExtensionStepConfig.StepSemantics.AT_MOST_ONCE_PER_RETRY
                },
            ).retryStrategy { error, state, attempt ->
                val decision = config.retryStrategy().makeRetryDecision(error, attempt)
                if (decision.shouldRetry()) {
                    ExtensionStepResult.retry(state, decision.delay())
                } else {
                    ExtensionStepResult.doNotRetry()
                }
            }.build()

    private companion object {
        const val LARGE_RESULT_THRESHOLD: Int = 256 * 1024
    }
}

public class KotlinCallback<T> internal constructor(
    private val future: DurableCallbackFuture<T>,
) {
    public val id: String
        get() = future.callbackId()

    public suspend fun await(): T = future.await()
}

public class KotlinCallbackSubmitterContext internal constructor(
    public val callbackId: String,
    public val step: KotlinStepContext,
)

public class KotlinConditionContext<T> internal constructor(
    public val state: T,
    public val step: KotlinStepContext,
)

public class KotlinParallel internal constructor(
    private val future: ParallelDurableFuture,
    private val parentCoroutineContext: CoroutineContext,
    @PublishedApi internal val defaultSerDes: SerDes?,
) {
    public inline fun <reified T> branch(
        name: String,
        serDes: SerDes? = defaultSerDes,
        noinline block: suspend KotlinDurableContext.() -> T,
    ): DurableFuture<T> =
        branch(
            name,
            typeToken(),
            ParallelBranchConfig.builder().serDes(serDes).build(),
            block,
        )

    public inline fun <reified T> branch(
        name: String,
        config: ParallelBranchConfig,
        noinline block: suspend KotlinDurableContext.() -> T,
    ): DurableFuture<T> = branch(name, typeToken(), config, block)

    public fun <T> branch(
        name: String,
        resultType: TypeToken<T>,
        config: ParallelBranchConfig,
        block: suspend KotlinDurableContext.() -> T,
    ): DurableFuture<T> =
        future.branch(
            name,
            resultType,
            { childContext ->
                runBlockingWithSnapshot {
                    block(KotlinDurableContext(childContext))
                }
            },
            config,
        )

    private fun <T> runBlockingWithSnapshot(block: suspend () -> T): T {
        val snapshot = DurableExecutionContextSnapshot.capture()
        val supervisor = SupervisorJob()
        val cancellationHandle =
            parentCoroutineContext[Job]?.invokeOnCompletion {
                supervisor.cancel()
            }
        return try {
            runBlocking(
                parentCoroutineContext.minusKey(Job) +
                    supervisor +
                    DurableExecutionContextElement(snapshot),
            ) {
                block()
            }
        } finally {
            cancellationHandle?.dispose()
            supervisor.cancel()
        }
    }
}

public class KotlinStepContext internal constructor(
    public val javaContext: StepContext,
) {
    public val attempt: Int
        get() = javaContext.attempt
}
