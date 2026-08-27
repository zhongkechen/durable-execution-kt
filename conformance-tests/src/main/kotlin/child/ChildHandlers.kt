package child

import io.github.zhongkechen.durable.ChildOptions
import io.github.zhongkechen.durable.DurableContext
import io.github.zhongkechen.durable.DurableHandler
import io.github.zhongkechen.durable.DurableRuntimeConfig
import io.github.zhongkechen.durable.RetryJitter
import io.github.zhongkechen.durable.RetryPolicy
import io.github.zhongkechen.durable.Serde
import io.github.zhongkechen.durable.StepOptions
import io.github.zhongkechen.durable.TypeRef
import io.github.zhongkechen.durable.child
import io.github.zhongkechen.durable.step
import io.github.zhongkechen.durable.typeRef
import kotlinx.coroutines.delay
import kotlin.time.Duration.Companion.seconds

public class ChildBasic(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String, context: DurableContext): String =
        context.child("child-basic") {
            step<String>("inner-step") { input }
        }
}

public class ChildWithName(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Map<String, String>, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(
        input: Map<String, String>,
        context: DurableContext,
    ): String =
        context.child(requireNotNull(input["name"])) {
            step<String>("step") { requireNotNull(input["value"]) }
        }
}

public class ChildMultipleSteps(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String, context: DurableContext): String =
        context.child("multi-step") {
            val first = step<String>("step-one") { input }
            step("step-two") { first }
        }
}

public class ChildWithError(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): String =
        context.child("failing-child") {
            step(
                name = "failing-step",
                options = StepOptions(retry = RetryPolicy.none),
            ) {
                error("Something went wrong in child")
            }
        }
}

public class ChildErrorCaught(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String, context: DurableContext): String {
        runCatching {
            context.child<String>("failing-child") {
                step(
                    name = "failing-step",
                    options = StepOptions(retry = RetryPolicy.none),
                ) {
                    error("Something went wrong")
                }
            }
        }
        return context.step("recovery-step") { input }
    }
}

public class ChildNested(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String, context: DurableContext): String =
        context.child("outer") {
            step<String>("outer-step") { input }
            child("inner") {
                step<String>("inner-step") { input }
            }
        }
}

public class ChildWithRetry(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String, context: DurableContext): String =
        context.child("retry-child") {
            step(
                name = "retry-step",
                options = StepOptions(retry = RetryPolicy.fixed(maxAttempts = 3, delay = 1.seconds)),
            ) {
                if (attempt < 2) error("Attempt $attempt failed")
                input
            }
        }
}

public class ChildRetryExhaustion(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): String =
        context.child("exhaust-child") {
            step(
                name = "always-fails",
                options =
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
                error("Always fails")
            }
        }
}

public class ChildReplay(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String, context: DurableContext): String {
        val result =
            context.child<String>("cached-child") {
                step<String>("compute") { input }
            }
        context.wait(2.seconds)
        return result
    }
}

public class ChildStepAndWait(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String, context: DurableContext): String =
        context.child("mixed-ops") {
            step<String>("do-work") { input }
            wait(2.seconds)
            input
        }
}

public class ChildLargePayload(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): String {
        val result =
            context.child<String>("large-data-processor") {
                logger.info("{}", input)
                val seed = step<String>("fetch-seed") { "seed" }
                seed.repeat(300_000)
            }
        context.wait(2.seconds)
        return result
    }
}

public class ChildInterrupted(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String, context: DurableContext): String =
        context.child("interrupted-child") {
            val shouldCrash = !isReplaying
            step<String>("crashable-step") {
                if (shouldCrash) {
                    delay(1_000)
                    exitProcess()
                }
                input
            }
        }
}

public class ChildWaitReplay(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String, context: DurableContext): String {
        context.child<String>("wait-child") {
            wait(1.seconds)
            input
        }
        return context.step("after-child") { input }
    }
}

public class ChildCustomSerdes(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String, context: DurableContext): String =
        context.child(
            name = "serdes-child",
            options = ChildOptions(serde = UppercaseSerde),
        ) {
            step<String>("return-input") { input }
        }

    private object UppercaseSerde : Serde {
        override fun encode(value: Any?): String = value.toString().uppercase()

        @Suppress("UNCHECKED_CAST")
        override fun <T> decode(payload: String, type: TypeRef<T>): T = payload as T
    }
}

public class ChildErrorNoStep(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): String =
        context.child("direct-error") {
            error("direct error")
        }
}

public class ChildNullResult(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String?>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): String? =
        context.child<String?>("null-child") { null }
}

public class ChildPrintOnly(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String, context: DurableContext): String {
        val result =
            context.child<String>("print-child") {
                logger.info("{}", input)
                input
            }
        context.wait(1.seconds)
        return result
    }
}

public class ChildStepWaitAfter(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String, context: DurableContext): String {
        context.child<String>("step-wait-child") {
            step<String>("inner-work") { input }
            wait(2.seconds)
            input
        }
        val result = context.step<String>("outer-work") { input }
        context.wait(2.seconds)
        return result
    }
}

private fun exitProcess(): Nothing {
    System.exit(1)
    error("unreachable")
}
