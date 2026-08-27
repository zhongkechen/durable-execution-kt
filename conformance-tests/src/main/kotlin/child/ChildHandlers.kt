package child

import io.github.zhongkechen.durable.*

import kotlinx.coroutines.delay
import kotlin.time.Duration.Companion.seconds

public class ChildBasic(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String): String =
        runInChildContext("child-basic") {
            step<String>("inner-step") { input }
        }
}

public class ChildWithName(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Map<String, String>, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(
        input: Map<String, String>,
    ): String =
        runInChildContext(requireNotNull(input["name"])) {
            step<String>("step") { requireNotNull(input["value"]) }
        }
}

public class ChildMultipleSteps(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String): String =
        runInChildContext("multi-step") {
            val first = step<String>("step-one") { input }
            step("step-two") { first }
        }
}

public class ChildWithError(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?): String =
        runInChildContext("failing-child") {
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
    override suspend fun handle(input: String): String {
        runCatching {
            runInChildContext<String>("failing-child") {
                step(
                    name = "failing-step",
                    options = StepOptions(retry = RetryPolicy.none),
                ) {
                    error("Something went wrong")
                }
            }
        }
        return step("recovery-step") { input }
    }
}

public class ChildNested(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String): String =
        runInChildContext("outer") {
            step<String>("outer-step") { input }
            runInChildContext("inner") {
                step<String>("inner-step") { input }
            }
        }
}

public class ChildWithRetry(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String): String =
        runInChildContext("retry-child") {
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
    override suspend fun handle(input: Any?): String =
        runInChildContext("exhaust-child") {
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
    override suspend fun handle(input: String): String {
        val result =
            runInChildContext<String>("cached-child") {
                step<String>("compute") { input }
            }
        wait(2.seconds)
        return result
    }
}

public class ChildStepAndWait(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String): String =
        runInChildContext("mixed-ops") {
            step<String>("do-work") { input }
            wait(2.seconds)
            input
        }
}

public class ChildLargePayload(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?): String {
        val result =
            runInChildContext<String>("large-data-processor") {
                durableLogger().info("{}", input)
                val seed = step<String>("fetch-seed") { "seed" }
                seed.repeat(300_000)
            }
        wait(2.seconds)
        return result
    }
}

public class ChildInterrupted(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String): String =
        runInChildContext("interrupted-child") {
            val shouldCrash = !isReplaying()
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
    override suspend fun handle(input: String): String {
        runInChildContext<String>("wait-child") {
            wait(1.seconds)
            input
        }
        return step("after-child") { input }
    }
}

public class ChildCustomSerdes(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String): String =
        runInChildContext(
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
    override suspend fun handle(input: Any?): String =
        runInChildContext("direct-error") {
            error("direct error")
        }
}

public class ChildNullResult(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String?>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?): String? =
        runInChildContext<String?>("null-child") { null }
}

public class ChildPrintOnly(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String): String {
        val result =
            runInChildContext<String>("print-child") {
                durableLogger().info("{}", input)
                input
            }
        wait(1.seconds)
        return result
    }
}

public class ChildStepWaitAfter(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String): String {
        runInChildContext<String>("step-wait-child") {
            step<String>("inner-work") { input }
            wait(2.seconds)
            input
        }
        val result = step<String>("outer-work") { input }
        wait(2.seconds)
        return result
    }
}

private fun exitProcess(): Nothing {
    System.exit(1)
    error("unreachable")
}
