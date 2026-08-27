// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package child

import java.time.Duration
import kotlinx.coroutines.delay
import software.amazon.lambda.durable.TypeToken
import software.amazon.lambda.durable.config.RunInChildContextConfig
import software.amazon.lambda.durable.config.StepConfig
import software.amazon.lambda.durable.kotlin.KotlinDurableContext
import software.amazon.lambda.durable.kotlin.KotlinDurableHandler
import software.amazon.lambda.durable.kotlin.KotlinDurableRuntime
import software.amazon.lambda.durable.logging.LoggerConfig
import software.amazon.lambda.durable.retry.JitterStrategy
import software.amazon.lambda.durable.retry.RetryDecision
import software.amazon.lambda.durable.retry.RetryStrategies
import software.amazon.lambda.durable.serde.SerDes

public class ChildBasic : KotlinDurableHandler<String, String>() {
    override suspend fun handle(input: String, context: KotlinDurableContext): String =
        context.childContext("child-basic") {
            step<String>("inner-step") { input }
        }
}

public class ChildWithName : KotlinDurableHandler<Map<String, String>, String>() {
    override suspend fun handle(input: Map<String, String>, context: KotlinDurableContext): String =
        context.childContext(requireNotNull(input["name"])) {
            step<String>("step") { requireNotNull(input["value"]) }
        }
}

public class ChildMultipleSteps : KotlinDurableHandler<String, String>() {
    override suspend fun handle(input: String, context: KotlinDurableContext): String =
        context.childContext("multi-step") {
            val first = step<String>("step-one") { input }
            step("step-two") { first }
        }
}

public class ChildWithError : KotlinDurableHandler<Any?, String>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String =
        context.childContext("failing-child") {
            step(
                "failing-step",
                StepConfig.builder().retryStrategy(RetryStrategies.Presets.NO_RETRY).build(),
            ) {
                error("Something went wrong in child")
            }
        }
}

public class ChildErrorCaught : KotlinDurableHandler<String, String>() {
    override suspend fun handle(input: String, context: KotlinDurableContext): String {
        try {
            context.childContext<String>("failing-child") {
                step(
                    "failing-step",
                    StepConfig.builder().retryStrategy(RetryStrategies.Presets.NO_RETRY).build(),
                ) {
                    error("Something went wrong")
                }
            }
        } catch (_: RuntimeException) {
            // Continue with recovery.
        }
        return context.step("recovery-step") { input }
    }
}

public class ChildNested : KotlinDurableHandler<String, String>() {
    override suspend fun handle(input: String, context: KotlinDurableContext): String =
        context.childContext("outer") {
            step<String>("outer-step") { input }
            childContext("inner") {
                step<String>("inner-step") { input }
            }
        }
}

public class ChildWithRetry : KotlinDurableHandler<String, String>() {
    override suspend fun handle(input: String, context: KotlinDurableContext): String =
        context.childContext("retry-child") {
            step(
                "retry-step",
                StepConfig.builder()
                    .retryStrategy { _, attempt ->
                        if (attempt >= 3) RetryDecision.fail() else RetryDecision.retry(Duration.ofSeconds(1))
                    }.build(),
            ) {
                if (attempt < 2) error("Attempt $attempt failed")
                input
            }
        }
}

public class ChildRetryExhaustion : KotlinDurableHandler<Any?, String>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String =
        context.childContext("exhaust-child") {
            step(
                "always-fails",
                StepConfig.builder()
                    .retryStrategy(
                        RetryStrategies.exponentialBackoff(
                            2,
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
}

public class ChildReplay : KotlinDurableHandler<String, String>() {
    override suspend fun handle(input: String, context: KotlinDurableContext): String {
        val result =
            context.childContext<String>("cached-child") {
                step<String>("compute") { input }
            }
        context.wait(Duration.ofSeconds(2))
        return result
    }
}

public class ChildStepAndWait : KotlinDurableHandler<String, String>() {
    override suspend fun handle(input: String, context: KotlinDurableContext): String =
        context.childContext("mixed-ops") {
            step<String>("do-work") { input }
            wait(Duration.ofSeconds(2))
            input
        }
}

public class ChildLargePayload :
    KotlinDurableHandler<Any?, String>(
        KotlinDurableRuntime.config {
            withLoggerConfig(LoggerConfig.withReplayLogging())
        },
    ) {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String {
        val result =
            context.childContext<String>("large-data-processor") {
                javaContext.logger.info("{}", input)
                val seed = step<String>("fetch-seed") { "seed" }
                seed.repeat(300_000)
            }
        context.wait(Duration.ofSeconds(2))
        return result
    }
}

public class ChildInterrupted : KotlinDurableHandler<String, String>() {
    override suspend fun handle(input: String, context: KotlinDurableContext): String =
        context.childContext("interrupted-child") {
            step<String>("crashable-step") {
                if (attempt == 1) {
                    delay(1_000)
                    exitProcess()
                }
                input
            }
        }
}

public class ChildWaitReplay : KotlinDurableHandler<String, String>() {
    override suspend fun handle(input: String, context: KotlinDurableContext): String {
        context.childContext<String>("wait-child") {
            wait(Duration.ofSeconds(1))
            input
        }
        return context.step("after-child") { input }
    }
}

public class ChildCustomSerdes : KotlinDurableHandler<String, String>() {
    override suspend fun handle(input: String, context: KotlinDurableContext): String =
        context.childContext(
            "serdes-child",
            RunInChildContextConfig.builder().serDes(UppercaseSerDes).build(),
        ) {
            step<String>("return-input") { input }
        }

    private object UppercaseSerDes : SerDes {
        override fun serialize(value: Any?): String? = (value as String?)?.uppercase()

        @Suppress("UNCHECKED_CAST")
        override fun <T : Any?> deserialize(data: String?, typeToken: TypeToken<T>): T = data as T
    }
}

public class ChildErrorNoStep : KotlinDurableHandler<Any?, String>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String =
        context.childContext("direct-error") {
            error("direct error")
        }
}

public class ChildNullResult : KotlinDurableHandler<Any?, String?>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String? =
        context.childContext<String?>("null-child") { null }
}

public class ChildPrintOnly : KotlinDurableHandler<String, String>() {
    override suspend fun handle(input: String, context: KotlinDurableContext): String {
        val result =
            context.childContext<String>("print-child") {
                javaContext.logger.info("{}", input)
                input
            }
        context.wait(Duration.ofSeconds(1))
        return result
    }
}

public class ChildStepWaitAfter : KotlinDurableHandler<String, String>() {
    override suspend fun handle(input: String, context: KotlinDurableContext): String {
        context.childContext<String>("step-wait-child") {
            step<String>("inner-work") { input }
            wait(Duration.ofSeconds(2))
            input
        }
        val result = context.step<String>("outer-work") { input }
        context.wait(Duration.ofSeconds(2))
        return result
    }
}

private fun exitProcess(): Nothing {
    System.exit(1)
    error("unreachable")
}
