// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package step

import java.time.Duration
import kotlinx.coroutines.delay
import software.amazon.lambda.durable.TypeToken
import software.amazon.lambda.durable.config.StepConfig
import software.amazon.lambda.durable.config.StepSemantics
import software.amazon.lambda.durable.kotlin.KotlinDurableContext
import software.amazon.lambda.durable.kotlin.KotlinDurableHandler
import software.amazon.lambda.durable.retry.JitterStrategy
import software.amazon.lambda.durable.retry.RetryDecision
import software.amazon.lambda.durable.retry.RetryStrategies
import software.amazon.lambda.durable.serde.SerDes

public class StepBasic : KotlinDurableHandler<String, String>() {
    override suspend fun handle(input: String, context: KotlinDurableContext): String =
        context.step("greet") { "Hello, $input!" }
}

public class StepWithName : KotlinDurableHandler<String, String>() {
    override suspend fun handle(input: String, context: KotlinDurableContext): String =
        context.step("custom_step_name") { "Hello, $input!" }
}

public class StepNested : KotlinDurableHandler<Any?, String>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String {
        val first = context.step<String>("step-one") { "first" }
        return context.step("step-two") { "${first}_second" }
    }
}

public class StepComplexObject : KotlinDurableHandler<Map<String, Any?>, Map<String, Any?>>() {
    @Suppress("UNCHECKED_CAST")
    override suspend fun handle(
        input: Map<String, Any?>,
        context: KotlinDurableContext,
    ): Map<String, Any?> =
        context.step("build-response") {
            val name = input["name"] as String
            val tags = input["tags"] as List<String>
            mapOf(
                "user" to mapOf("name" to name, "tags" to tags),
                "count" to tags.size,
            )
        }
}

public class StepNullResult : KotlinDurableHandler<Any?, Any?>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): Any? =
        context.step<Any?>("do-nothing") { null }
}

public class StepCustomSerdes : KotlinDurableHandler<String, String>() {
    override suspend fun handle(input: String, context: KotlinDurableContext): String =
        context.step(
            "uppercase",
            StepConfig.builder().serDes(UppercaseSerDes).build(),
        ) {
            input
        }

    private object UppercaseSerDes : SerDes {
        override fun serialize(value: Any?): String? = (value as String?)?.uppercase()

        @Suppress("UNCHECKED_CAST")
        override fun <T : Any?> deserialize(data: String?, typeToken: TypeToken<T>): T = data as T
    }
}

public class StepLogging : KotlinDurableHandler<String, String>() {
    override suspend fun handle(input: String, context: KotlinDurableContext): String =
        context.step("greet") {
            javaContext.logger.info("Greeting step started for: {}", input)
            val greeting = "Hello, $input!"
            javaContext.logger.info("Greeting step completed with: {}", greeting)
            greeting
        }
}

public class StepAndWaitReplay : KotlinDurableHandler<Any?, String>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String {
        val result = context.step<String>("compute") { "computed" }
        context.wait(Duration.ofSeconds(2))
        return result
    }
}

public class StepReplaySkipsSucceeded : KotlinDurableHandler<Any?, String>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String {
        val result =
            context.step<String>("cached-step") {
                javaContext.logger.info("step executed")
                "cached_value"
            }
        context.wait(Duration.ofSeconds(1))
        return result
    }
}

public class StepReplayRethrowsFailed : KotlinDurableHandler<Any?, String>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String {
        val errorMessage =
            try {
                context.step<String>(
                    "failing-step",
                    StepConfig.builder().retryStrategy(RetryStrategies.Presets.NO_RETRY).build(),
                ) {
                    javaContext.logger.info("step executed")
                    error("Something went wrong")
                }
                ""
            } catch (error: RuntimeException) {
                error.cause?.message ?: error.message.orEmpty()
            }
        context.wait(Duration.ofSeconds(1))
        return "caught: $errorMessage"
    }
}

public class StepWithRetry : KotlinDurableHandler<Any?, String>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String =
        context.step(
            "retry-step",
            StepConfig.builder()
                .retryStrategy { _, attempt ->
                    if (attempt >= 3) RetryDecision.fail() else RetryDecision.retry(Duration.ofSeconds(1))
                }.build(),
        ) {
            if (attempt < 2) error("Attempt $attempt failed")
            "Operation succeeded"
        }
}

public class StepRetryExhaustion : KotlinDurableHandler<Any?, String>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String =
        context.step(
            "always-fails",
            StepConfig.builder()
                .retryStrategy(
                    RetryStrategies.exponentialBackoff(
                        4,
                        Duration.ofSeconds(1),
                        Duration.ofSeconds(10),
                        1.0,
                        JitterStrategy.NONE,
                    ),
                ).build(),
        ) {
            error("Always fails")
        }
}

public class StepDefaultRetry : KotlinDurableHandler<Any?, String>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String =
        context.step("default-retry") {
            if (attempt < 3) error("Attempt $attempt failed")
            "recovered"
        }
}

public class StepRetryCustomConfig : KotlinDurableHandler<Any?, String>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String =
        context.step(
            "custom-retry",
            StepConfig.builder()
                .retryStrategy(
                    RetryStrategies.exponentialBackoff(
                        5,
                        Duration.ofSeconds(2),
                        Duration.ofSeconds(60),
                        3.0,
                        JitterStrategy.NONE,
                    ),
                ).build(),
        ) {
            if (attempt < 3) error("Attempt $attempt failed")
            "finally succeeded"
        }
}

public class StepRetrySpecificException : KotlinDurableHandler<Any?, String>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String =
        context.step(
            "specific-retry",
            StepConfig.builder()
                .retryStrategy { error, attempt ->
                    if (error is TransientError && attempt < 3) {
                        RetryDecision.retry(Duration.ofSeconds(1))
                    } else {
                        RetryDecision.fail()
                    }
                }.build(),
        ) {
            if (attempt < 2) throw TransientError("Temporary failure")
            "recovered from transient"
        }

    public class TransientError(message: String) : RuntimeException(message)
}

public class StepRetryNonRetryable : KotlinDurableHandler<Any?, String>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String =
        context.step(
            "non-retryable",
            StepConfig.builder()
                .retryStrategy { error, attempt ->
                    if (error.javaClass.simpleName == "ValidationError" && attempt < 3) {
                        RetryDecision.retry(Duration.ofSeconds(1))
                    } else {
                        RetryDecision.fail()
                    }
                }.build(),
        ) {
            throw TransientError("Temporary failure")
        }

    public class TransientError(message: String) : RuntimeException(message)
}

public class StepAtMostOnceNoRetry : KotlinDurableHandler<Any?, String>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String =
        context.step(
            "at_most_once_flaky_step",
            StepConfig.builder()
                .semanticsPerRetry(StepSemantics.AT_MOST_ONCE_PER_RETRY)
                .retryStrategy(RetryStrategies.Presets.NO_RETRY)
                .build(),
        ) {
            javaContext.logger.info("{}", input)
            delay(1_000)
            exitProcess()
        }
}

public class StepAtMostOnceWithRetry : KotlinDurableHandler<Any?, String>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String =
        context.step(
            "at-most-once-retry",
            StepConfig.builder()
                .semanticsPerRetry(StepSemantics.AT_MOST_ONCE_PER_RETRY)
                .retryStrategy { _, attempt ->
                    if (attempt >= 3) RetryDecision.fail() else RetryDecision.retry(Duration.ofSeconds(1))
                }.build(),
        ) {
            javaContext.logger.info("{}", input)
            if (attempt < 2) {
                delay(1_000)
                exitProcess()
            }
            "succeeded on second attempt"
        }
}

public class StepWithError : KotlinDurableHandler<Any?, String>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String =
        context.step(
            "failing-step",
            StepConfig.builder().retryStrategy(RetryStrategies.Presets.NO_RETRY).build(),
        ) {
            error("Something went wrong")
        }
}

public class StepErrorCaught : KotlinDurableHandler<Any?, String>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String {
        try {
            context.step<String>(
                "failing-step",
                StepConfig.builder().retryStrategy(RetryStrategies.Presets.NO_RETRY).build(),
            ) {
                error("Something went wrong")
            }
        } catch (_: RuntimeException) {
            // Continue with the checkpointed fallback.
        }
        return context.step("fallback-step") { "fallback_result" }
    }
}

private fun exitProcess(): Nothing {
    System.exit(1)
    error("unreachable")
}
