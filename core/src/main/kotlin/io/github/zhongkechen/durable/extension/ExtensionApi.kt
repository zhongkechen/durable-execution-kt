package io.github.zhongkechen.durable.extension

import io.github.zhongkechen.durable.CallbackHandle
import io.github.zhongkechen.durable.DeliverySemantics
import io.github.zhongkechen.durable.DurableLogger
import io.github.zhongkechen.durable.Serde
import io.github.zhongkechen.durable.StepScope
import io.github.zhongkechen.durable.TypeRef
import io.github.zhongkechen.durable.typeRef
import kotlin.time.Duration

/**
 * Active durable scope available to operation facades and extension libraries.
 *
 * Extensions reserve identities deterministically, then may launch those
 * reservations in any order.
 */
public interface ExtensionContext {
    public val executionArn: String
    public val isReplaying: Boolean
    public val logger: DurableLogger

    public fun reserve(name: String? = null): ExtensionOperation

    public fun reserve(
        name: String? = null,
        localId: String,
    ): ExtensionOperation
}

/** Opaque, one-shot reservation for one durable primitive. */
public interface ExtensionOperation {
    public suspend fun <T> step(
        subtype: String,
        type: TypeRef<T>,
        config: ExtensionStepConfig<T> = ExtensionStepConfig(),
        function: suspend StepScope.(state: T?) -> ExtensionStepResult<T>,
    ): T

    public suspend fun wait(
        subtype: String,
        duration: Duration,
    )

    public suspend fun <I, O> invoke(
        subtype: String,
        functionName: String,
        input: I,
        outputType: TypeRef<O>,
        config: ExtensionInvokeConfig = ExtensionInvokeConfig(),
    ): O

    public suspend fun <T> createCallback(
        subtype: String,
        type: TypeRef<T>,
        config: ExtensionCallbackConfig = ExtensionCallbackConfig(),
    ): CallbackHandle<T>

    public suspend fun <T> runInChildContext(
        subtype: String,
        type: TypeRef<T>,
        config: ExtensionContextConfig = ExtensionContextConfig(),
        function: suspend ExtensionContextReplay<T>.() -> ExtensionContextResult<T>,
    ): T
}

public suspend inline fun <reified T> ExtensionOperation.step(
    subtype: String,
    config: ExtensionStepConfig<T> = ExtensionStepConfig(),
    noinline block: suspend StepScope.(state: T?) -> ExtensionStepResult<T>,
): T = step(subtype, typeRef(), config, block)

public suspend inline fun <I, reified O> ExtensionOperation.invoke(
    subtype: String,
    functionName: String,
    input: I,
    config: ExtensionInvokeConfig = ExtensionInvokeConfig(),
): O = invoke(subtype, functionName, input, typeRef(), config)

public suspend inline fun <reified T> ExtensionOperation.createCallback(
    subtype: String,
    config: ExtensionCallbackConfig = ExtensionCallbackConfig(),
): CallbackHandle<T> = createCallback(subtype, typeRef(), config)

public suspend inline fun <reified T> ExtensionOperation.runInChildContext(
    subtype: String,
    config: ExtensionContextConfig = ExtensionContextConfig(),
    noinline block: suspend ExtensionContextReplay<T>.() -> ExtensionContextResult<T>,
): T = runInChildContext(subtype, typeRef(), config, block)

public data class ExtensionStepConfig<T>(
    val initialState: T? = null,
    val serde: Serde? = null,
    val retry: ExtensionRetryStrategy<T>? = null,
    val delivery: DeliverySemantics = DeliverySemantics.AT_LEAST_ONCE_PER_RETRY,
)

public fun interface ExtensionRetryStrategy<T> {
    public fun decide(
        error: Throwable,
        state: T?,
        attempt: Int,
    ): ExtensionRetryDecision<T>
}

public sealed interface ExtensionRetryDecision<out T> {
    public data class Retry<T>(
        val state: T?,
        val delay: Duration,
    ) : ExtensionRetryDecision<T> {
        init {
            require(delay.isPositive()) { "Retry delay must be positive" }
        }
    }

    public data object DoNotRetry : ExtensionRetryDecision<Nothing>
}

public sealed interface ExtensionStepResult<out T> {
    public data class Succeeded<T>(
        val value: T,
    ) : ExtensionStepResult<T>

    public data class Retry<T>(
        val state: T,
        val delay: Duration,
    ) : ExtensionStepResult<T> {
        init {
            require(delay.isPositive()) { "Retry delay must be positive" }
        }
    }

    public data class RetryAfterNormalization<T>(
        val state: T,
        val delay: (normalizedState: T) -> Duration,
    ) : ExtensionStepResult<T>
}

public data class ExtensionInvokeConfig(
    val payloadSerde: Serde? = null,
    val resultSerde: Serde? = null,
    val tenantId: String? = null,
)

public data class ExtensionCallbackConfig(
    val timeout: Duration? = null,
    val heartbeatTimeout: Duration? = null,
    val serde: Serde? = null,
) {
    init {
        require(timeout == null || timeout.isPositive()) { "timeout must be positive" }
        require(heartbeatTimeout == null || heartbeatTimeout.isPositive()) {
            "heartbeatTimeout must be positive"
        }
    }
}

public data class ExtensionContextConfig(
    val serde: Serde? = null,
    val virtual: Boolean = false,
    val emitFunctionEvents: Boolean = true,
    val errorHandler: ((ExtensionContextFailure) -> Throwable)? = null,
)

public data class ExtensionContextFailure(
    val contextName: String?,
    val subtype: String,
    val originalException: Throwable,
    val childOperations: List<ExtensionChildOperation>,
)

public data class ExtensionChildOperation(
    val id: String,
    val name: String?,
    val type: String,
    val subtype: String,
    val status: String,
)

public data class ExtensionContextReplay<T>(
    val replayingChildren: Boolean,
    val replayState: T?,
)

public sealed interface ExtensionContextResult<out T> {
    public val result: T

    public data class Completed<T>(
        override val result: T,
    ) : ExtensionContextResult<T>

    public data class ReplayChildren<T>(
        override val result: T,
        val replayState: T?,
    ) : ExtensionContextResult<T>

    public data class ReplayChildrenAboveSize<T>(
        override val result: T,
        val replayState: T?,
        val thresholdBytes: Int,
    ) : ExtensionContextResult<T> {
        init {
            require(thresholdBytes > 0) { "thresholdBytes must be positive" }
        }
    }
}

/** Returns the active extension scope for the current coroutine. */
public suspend fun currentExtensionContext(): ExtensionContext =
    io.github.zhongkechen.durable.internal.currentExtensionContext()
