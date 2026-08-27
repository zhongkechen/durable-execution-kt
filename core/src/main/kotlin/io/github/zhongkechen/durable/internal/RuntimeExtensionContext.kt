package io.github.zhongkechen.durable.internal

import io.github.zhongkechen.durable.CallbackHandle
import io.github.zhongkechen.durable.DurableLogger
import io.github.zhongkechen.durable.TypeRef
import io.github.zhongkechen.durable.extension.ExtensionCallbackConfig
import io.github.zhongkechen.durable.extension.ExtensionContext
import io.github.zhongkechen.durable.extension.ExtensionContextConfig
import io.github.zhongkechen.durable.extension.ExtensionContextReplay
import io.github.zhongkechen.durable.extension.ExtensionContextResult
import io.github.zhongkechen.durable.extension.ExtensionInvokeConfig
import io.github.zhongkechen.durable.extension.ExtensionOperation
import io.github.zhongkechen.durable.extension.ExtensionStepConfig
import io.github.zhongkechen.durable.extension.ExtensionStepResult
import java.util.concurrent.atomic.AtomicBoolean
import kotlin.coroutines.AbstractCoroutineContextElement
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.withContext

internal class RuntimeExtensionContext(
    internal val runtime: OperationRuntime,
) : ExtensionContext {
    override val executionArn: String
        get() = runtime.executionArn

    override val isReplaying: Boolean
        get() = runtime.isReplaying

    override val logger: DurableLogger
        get() = runtime.logger

    override fun reserve(name: String?): ExtensionOperation =
        RuntimeExtensionOperation(runtime, runtime.reserveOperation(name))

    override fun reserve(
        name: String?,
        localId: String,
    ): ExtensionOperation =
        RuntimeExtensionOperation(
            runtime,
            runtime.reserveOperation(name, localId),
        )
}

private class RuntimeExtensionOperation(
    private val runtime: OperationRuntime,
    private val reservation: ReservedOperation,
) : ExtensionOperation {
    private val consumed = AtomicBoolean()

    private fun consume(): ReservedOperation {
        check(consumed.compareAndSet(false, true)) {
            "A durable operation reservation can only be used once"
        }
        return reservation
    }

    override suspend fun <T> step(
        subtype: String,
        type: TypeRef<T>,
        config: ExtensionStepConfig<T>,
        function: suspend io.github.zhongkechen.durable.StepScope.(state: T?) -> ExtensionStepResult<T>,
    ): T =
        runtime.extensionStep(
            identity = consume().identity(OperationKind.STEP, subtype),
            type = type,
            config = config,
            function = function,
        )

    override suspend fun wait(
        subtype: String,
        duration: Duration,
    ) {
        runtime.wait(
            identity = consume().identity(OperationKind.WAIT, subtype),
            duration = duration,
        )
    }

    override suspend fun <I, O> invoke(
        subtype: String,
        functionName: String,
        input: I,
        outputType: TypeRef<O>,
        config: ExtensionInvokeConfig,
    ): O =
        runtime.invoke(
            identity = consume().identity(OperationKind.INVOKE, subtype),
            functionName = functionName,
            input = input,
            outputType = outputType,
            config = config,
        )

    override suspend fun <T> createCallback(
        subtype: String,
        type: TypeRef<T>,
        config: ExtensionCallbackConfig,
    ): CallbackHandle<T> =
        runtime.callback(
            identity = consume().identity(OperationKind.CALLBACK, subtype),
            type = type,
            config = config,
        )

    override suspend fun <T> runInChildContext(
        subtype: String,
        type: TypeRef<T>,
        config: ExtensionContextConfig,
        function: suspend ExtensionContextReplay<T>.() -> ExtensionContextResult<T>,
    ): T =
        runtime.extensionContext(
            identity = consume().identity(OperationKind.CONTEXT, subtype),
            type = type,
            config = config,
        ) { childRuntime, replay ->
            withRuntime(childRuntime) {
                function(replay)
            }
        }
}

private class ExtensionContextElement(
    val value: RuntimeExtensionContext,
) : AbstractCoroutineContextElement(Key) {
    companion object Key : CoroutineContext.Key<ExtensionContextElement>
}

internal suspend fun currentRuntimeExtensionContext(): RuntimeExtensionContext =
    currentCoroutineContext()[ExtensionContextElement]?.value
        ?: error("No durable extension context is active in the current coroutine")

internal suspend fun currentExtensionContext(): ExtensionContext =
    currentRuntimeExtensionContext()

internal suspend fun <T> withRuntime(
    runtime: OperationRuntime,
    block: suspend () -> T,
): T =
    withContext(ExtensionContextElement(RuntimeExtensionContext(runtime))) {
        block()
    }
