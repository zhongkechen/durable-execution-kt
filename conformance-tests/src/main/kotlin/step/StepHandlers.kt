package step

import io.github.zhongkechen.durable.DeliverySemantics
import io.github.zhongkechen.durable.DurableContext
import io.github.zhongkechen.durable.DurableHandler
import io.github.zhongkechen.durable.DurableRuntimeConfig
import io.github.zhongkechen.durable.RetryDecision
import io.github.zhongkechen.durable.RetryJitter
import io.github.zhongkechen.durable.RetryPolicy
import io.github.zhongkechen.durable.Serde
import io.github.zhongkechen.durable.StepOptions
import io.github.zhongkechen.durable.TypeRef
import io.github.zhongkechen.durable.step
import io.github.zhongkechen.durable.typeRef
import kotlinx.coroutines.delay
import kotlin.time.Duration.Companion.seconds

public class StepBasic(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String, context: DurableContext): String =
        context.step("greet") { "Hello, $input!" }
}

public class StepWithName(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String, context: DurableContext): String =
        context.step("custom_step_name") { "Hello, $input!" }
}

public class StepNested(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): String {
        val first = context.step<String>("step-one") { "first" }
        return context.step("step-two") { "${first}_second" }
    }
}

public class StepComplexObject(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Map<String, Any?>, Map<String, Any?>>(typeRef(), typeRef(), config) {
    @Suppress("UNCHECKED_CAST")
    override suspend fun handle(
        input: Map<String, Any?>,
        context: DurableContext,
    ): Map<String, Any?> =
        context.step("build-response") {
            val tags = input["tags"] as List<String>
            mapOf(
                "user" to
                    mapOf(
                        "name" to input["name"],
                        "tags" to tags,
                    ),
                "count" to tags.size,
            )
        }
}

public class StepNullResult(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, Any?>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): Any? =
        context.step<Any?>("do-nothing") { null }
}

public class StepCustomSerdes(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String, context: DurableContext): String =
        context.step(
            name = "uppercase",
            options = StepOptions(serde = UppercaseSerde),
        ) {
            input
        }

    private object UppercaseSerde : Serde {
        override fun encode(value: Any?): String = value.toString().uppercase()

        @Suppress("UNCHECKED_CAST")
        override fun <T> decode(payload: String, type: TypeRef<T>): T = payload as T
    }
}

public class StepLogging(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String, context: DurableContext): String =
        context.step("greet") {
            logger.info("Greeting step started for: {}", input)
            val greeting = "Hello, $input!"
            logger.info("Greeting step completed with: {}", greeting)
            greeting
        }
}

public class StepAndWaitReplay(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): String {
        val result = context.step<String>("compute") { "computed" }
        context.wait(2.seconds)
        return result
    }
}

public class StepReplaySkipsSucceeded(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): String {
        val result =
            context.step<String>("cached-step") {
                logger.info("step executed")
                "cached_value"
            }
        context.wait(1.seconds)
        return result
    }
}

public class StepReplayRethrowsFailed(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): String {
        val message =
            runCatching {
                context.step<String>(
                    name = "failing-step",
                    options = StepOptions(retry = RetryPolicy.none),
                ) {
                    logger.info("step executed")
                    error("Something went wrong")
                }
            }.exceptionOrNull()
                ?.let { it.cause?.message ?: it.message }
                .orEmpty()
        context.wait(1.seconds)
        return "caught: $message"
    }
}

public class StepWithRetry(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): String =
        context.step(
            name = "retry-step",
            options = StepOptions(retry = RetryPolicy.fixed(maxAttempts = 3, delay = 1.seconds)),
        ) {
            if (attempt < 2) error("Attempt $attempt failed")
            "Operation succeeded"
        }
}

public class StepRetryExhaustion(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): String =
        context.step(
            name = "always-fails",
            options =
                StepOptions(
                    retry =
                        RetryPolicy.exponential(
                            maxAttempts = 4,
                            initialDelay = 1.seconds,
                            maximumDelay = 10.seconds,
                            multiplier = 1.0,
                            jitter = RetryJitter.NONE,
                        ),
                ),
        ) {
            error("Always fails")
        }
}

public class StepDefaultRetry(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): String =
        context.step("default-retry") {
            if (attempt < 3) error("Attempt $attempt failed")
            "recovered"
        }
}

public class StepRetryCustomConfig(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): String =
        context.step(
            name = "custom-retry",
            options =
                StepOptions(
                    retry =
                        RetryPolicy.exponential(
                            maxAttempts = 5,
                            initialDelay = 2.seconds,
                            maximumDelay = 60.seconds,
                            multiplier = 3.0,
                            jitter = RetryJitter.NONE,
                        ),
                ),
        ) {
            if (attempt < 3) error("Attempt $attempt failed")
            "finally succeeded"
        }
}

public class StepRetrySpecificException(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): String =
        context.step(
            name = "specific-retry",
            options =
                StepOptions(
                    retry =
                        RetryPolicy { error, attempt ->
                            if (error is TransientError && attempt < 3) {
                                RetryDecision.Retry(1.seconds)
                            } else {
                                RetryDecision.Fail
                            }
                        },
                ),
        ) {
            if (attempt < 2) throw TransientError("Temporary failure")
            "recovered from transient"
        }

    public class TransientError(message: String) : RuntimeException(message)
}

public class StepRetryNonRetryable(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): String =
        context.step(
            name = "non-retryable",
            options =
                StepOptions(
                    retry =
                        RetryPolicy { error, attempt ->
                            if (error::class.simpleName == "ValidationError" && attempt < 3) {
                                RetryDecision.Retry(1.seconds)
                            } else {
                                RetryDecision.Fail
                            }
                        },
                ),
        ) {
            throw TransientError("Temporary failure")
        }

    public class TransientError(message: String) : RuntimeException(message)
}

public class StepAtMostOnceNoRetry(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): String =
        context.step(
            name = "at_most_once_flaky_step",
            options =
                StepOptions(
                    retry = RetryPolicy.none,
                    delivery = DeliverySemantics.AT_MOST_ONCE_PER_RETRY,
                ),
        ) {
            logger.info("{}", input)
            delay(1_000)
            exitProcess()
        }
}

public class StepAtMostOnceWithRetry(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): String =
        context.step(
            name = "at-most-once-retry",
            options =
                StepOptions(
                    retry = RetryPolicy.fixed(maxAttempts = 3, delay = 1.seconds),
                    delivery = DeliverySemantics.AT_MOST_ONCE_PER_RETRY,
                ),
        ) {
            logger.info("{}", input)
            if (attempt < 2) {
                delay(1_000)
                exitProcess()
            }
            "succeeded on second attempt"
        }
}

public class StepWithError(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): String =
        context.step(
            name = "failing-step",
            options = StepOptions(retry = RetryPolicy.none),
        ) {
            error("Something went wrong")
        }
}

public class StepErrorCaught(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): String {
        runCatching {
            context.step<String>(
                name = "failing-step",
                options = StepOptions(retry = RetryPolicy.none),
            ) {
                error("Something went wrong")
            }
        }
        return context.step("fallback-step") { "fallback_result" }
    }
}

private fun exitProcess(): Nothing {
    System.exit(1)
    error("unreachable")
}
