package parallel

import io.github.zhongkechen.durable.BatchCompletion
import io.github.zhongkechen.durable.CompletionPolicy
import io.github.zhongkechen.durable.DurableContext
import io.github.zhongkechen.durable.DurableFuture
import io.github.zhongkechen.durable.DurableHandler
import io.github.zhongkechen.durable.DurableRuntimeConfig
import io.github.zhongkechen.durable.Nesting
import io.github.zhongkechen.durable.ParallelOptions
import io.github.zhongkechen.durable.ParallelResult
import io.github.zhongkechen.durable.Serde
import io.github.zhongkechen.durable.TypeRef
import io.github.zhongkechen.durable.step
import io.github.zhongkechen.durable.typeRef
import kotlin.time.Duration.Companion.seconds

public class ParallelBasic(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, List<String>>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): List<String> {
        val futures = mutableListOf<DurableFuture<String>>()
        context.parallel("parallel", ParallelOptions(maximumConcurrency = 1)) {
            futures += branch("branch-0", typeRef()) { step<String>("step-0") { "task-1" } }
            futures += branch("branch-1", typeRef()) { step<String>("step-1") { "task-2" } }
        }
        return futures.map { it.await() }
    }
}

public class ParallelBranchesOnly(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, List<String>>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): List<String> =
        twoStrings(context, "branches-only", "alpha", "beta")
}

public class ParallelNamedBranches(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, List<String>>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): List<String> {
        lateinit var first: DurableFuture<String>
        lateinit var second: DurableFuture<String>
        context.parallel("named", ParallelOptions(maximumConcurrency = 1)) {
            first = branch("first", typeRef()) { "one" }
            second = branch("second", typeRef()) { "two" }
        }
        return listOf(first.await(), second.await())
    }
}

public class ParallelHeterogeneous(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, List<Any>>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): List<Any> {
        lateinit var text: DurableFuture<String>
        lateinit var number: DurableFuture<Int>
        lateinit var data: DurableFuture<Map<String, String>>
        context.parallel("hetero", ParallelOptions(maximumConcurrency = 1)) {
            text = branch("branch-0", typeRef()) { "hello" }
            number = branch("branch-1", typeRef()) { 42 }
            data = branch("branch-2", typeRef()) { mapOf("k" to "v") }
        }
        return listOf(text.await(), number.await(), data.await())
    }
}

public class ParallelEmpty(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, List<Any>>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): List<Any> {
        context.parallel("empty") {}
        return emptyList()
    }
}

public class ParallelFailFast(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, Map<String, Any>>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): Map<String, Any> =
        summary(
            context.parallel(
                "failfast",
                ParallelOptions(
                    maximumConcurrency = 1,
                    completion = CompletionPolicy.TolerateFailures(count = 0),
                ),
            ) {
                branch("branch-0", typeRef<String>()) { "ok" }
                branch("branch-1", typeRef<String>()) { error("branch 1 failed") }
                branch("branch-2", typeRef<String>()) { "never" }
            },
            includeStatus = true,
        )
}

public class ParallelThrowIfError(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): String {
        lateinit var failed: DurableFuture<String>
        context.parallel(
            "throwing",
            ParallelOptions(
                maximumConcurrency = 1,
                completion = CompletionPolicy.TolerateFailures(count = 0),
            ),
        ) {
            failed = branch("branch-0", typeRef()) { error("branch 0 failed") }
            branch("branch-1", typeRef<String>()) { "never" }
        }
        return failed.await()
    }
}

public class ParallelMinSuccessful(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, Map<String, Any>>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): Map<String, Any> =
        summary(
            context.parallel(
                "min-successful",
                ParallelOptions(
                    maximumConcurrency = 1,
                    completion = CompletionPolicy.MinimumSuccessful(2),
                ),
            ) {
                repeat(4) { index ->
                    branch("branch-$index", typeRef<String>()) { "v$index" }
                }
            },
        ).filterKeys { it != "failureCount" }
}

public class ParallelToleratedWithin(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, Map<String, Any>>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): Map<String, Any> =
        summary(
            context.parallel(
                "tolerated",
                ParallelOptions(
                    maximumConcurrency = 1,
                    completion = CompletionPolicy.TolerateFailures(count = 1),
                ),
            ) {
                branch("branch-0", typeRef<String>()) { "s0" }
                branch("branch-1", typeRef<String>()) { error("branch 1 failed") }
                branch("branch-2", typeRef<String>()) { "s2" }
            },
            includeStatus = true,
        )
}

public class ParallelToleratedExceeded(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, Map<String, Any>>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): Map<String, Any> =
        summary(
            context.parallel(
                "tolerated-exceeded",
                ParallelOptions(
                    maximumConcurrency = 1,
                    completion = CompletionPolicy.TolerateFailures(count = 1),
                ),
            ) {
                branch("branch-0", typeRef<String>()) { error("branch 0 failed") }
                branch("branch-1", typeRef<String>()) { error("branch 1 failed") }
                branch("branch-2", typeRef<String>()) { "never" }
            },
        )
}

public class ParallelToleratedPct(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, Map<String, Any>>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): Map<String, Any> =
        percentage(context, "tolerated-pct", failOnlyFirst = false)
}

public class ParallelPctBoundary(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, Map<String, Any>>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): Map<String, Any> =
        percentage(context, "pct-boundary", failOnlyFirst = true)
}

public class ParallelItemSerdes(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, List<String>>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): List<String> {
        val futures = mutableListOf<DurableFuture<String>>()
        context.parallel(
            "serde",
            ParallelOptions(maximumConcurrency = 1, itemSerde = WrappedBranchSerde),
        ) {
            futures += branch("branch-0", typeRef()) { "x" }
            futures += branch("branch-1", typeRef()) { "y" }
        }
        return futures.map { it.await() }
    }
}

public class ParallelConcurrent(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, List<String>>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): List<String> {
        val futures = mutableListOf<DurableFuture<String>>()
        context.parallel("concurrent", ParallelOptions(maximumConcurrency = 2)) {
            repeat(3) { index ->
                futures += branch("branch-$index", typeRef()) { "r$index" }
            }
        }
        return futures.map { it.await() }
    }
}

public class ParallelFlat(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, List<String>>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): List<String> {
        val futures = mutableListOf<DurableFuture<String>>()
        context.parallel(
            "flat",
            ParallelOptions(maximumConcurrency = 1, nesting = Nesting.FLAT),
        ) {
            futures += branch("branch-0", typeRef()) { step<String>("step-0") { "fa" } }
            futures += branch("branch-1", typeRef()) { step<String>("step-1") { "fb" } }
        }
        return futures.map { it.await() }
    }
}

public class ParallelReplaySkipsSucceeded(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, List<String>>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): List<String> {
        val futures = mutableListOf<DurableFuture<String>>()
        context.parallel("replay", ParallelOptions(maximumConcurrency = 1)) {
            futures += branch("branch-0", typeRef()) { step<String>("step-0") { "b0" } }
            futures +=
                branch("branch-1", typeRef()) {
                    wait(2.seconds)
                    "b1"
                }
        }
        return futures.map { it.await() }
    }
}

public class ParallelAllFail(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, Map<String, Any>>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): Map<String, Any> =
        summary(
            context.parallel(
                "all-fail",
                ParallelOptions(
                    maximumConcurrency = 1,
                    completion = CompletionPolicy.TolerateFailures(count = 3),
                ),
            ) {
                repeat(3) { index ->
                    branch("branch-$index", typeRef<String>()) { error("branch $index failed") }
                }
            },
            includeStatus = true,
        )
}

public class ParallelMinNotReached(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, Map<String, Any>>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): Map<String, Any> =
        summary(
            context.parallel(
                "min-not-reached",
                ParallelOptions(
                    maximumConcurrency = 1,
                    completion = CompletionPolicy.MinimumSuccessful(3),
                ),
            ) {
                branch("branch-0", typeRef<String>()) { "ok0" }
                branch("branch-1", typeRef<String>()) { error("branch 1 failed") }
                branch("branch-2", typeRef<String>()) { "ok2" }
            },
            includeStatus = true,
        )
}

public class ParallelCombinedConfig(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, Map<String, Any>>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): Map<String, Any> =
        summary(
            context.parallel(
                "combined",
                ParallelOptions(
                    maximumConcurrency = 1,
                    completion =
                        CompletionPolicy.Combined(
                            minimumSuccessful = 3,
                            toleratedFailures = 1,
                        ),
                ),
            ) {
                branch("branch-0", typeRef<String>()) { error("branch 0 failed") }
                branch("branch-1", typeRef<String>()) { error("branch 1 failed") }
                branch("branch-2", typeRef<String>()) { "ok2" }
                branch("branch-3", typeRef<String>()) { "ok3" }
            },
        )
}

public class ParallelBadConcurrency(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, List<String>>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): List<String> {
        val options = ParallelOptions(maximumConcurrency = 0)
        context.parallel("bad-concurrency", options) {
            branch("branch-0", typeRef<String>()) { "a" }
            branch("branch-1", typeRef<String>()) { "b" }
        }
        return emptyList()
    }
}

public class ParallelAccessors(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, Map<String, Any>>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): Map<String, Any> {
        val result =
            context.parallel(
                "accessors",
                ParallelOptions(
                    maximumConcurrency = 1,
                    completion = CompletionPolicy.TolerateFailures(count = 1),
                ),
            ) {
                branch("branch-0", typeRef<String>()) { "ok0" }
                branch("branch-1", typeRef<String>()) { error("branch 1 failed") }
                branch("branch-2", typeRef<String>()) { "ok2" }
            }
        return mapOf(
            "hasFailure" to result.hasFailure,
            "successCount" to result.successes.size,
            "failureCount" to result.failures.size,
            "errorCount" to result.failures.size,
        )
    }
}

public class ParallelNested(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, Any>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): Any {
        lateinit var outer: DurableFuture<List<String>>
        context.parallel("outer", ParallelOptions(maximumConcurrency = 1)) {
            outer =
                branch("branchA", typeRef()) {
                    val inner = mutableListOf<DurableFuture<String>>()
                    parallel("inner", ParallelOptions(maximumConcurrency = 1)) {
                        inner += branch("inner-0", typeRef()) { step<String>("step-0") { "i1" } }
                        inner += branch("inner-1", typeRef()) { step<String>("step-1") { "i2" } }
                    }
                    inner.map { it.await() }
                }
        }
        return listOf(outer.await())
    }
}

private suspend fun twoStrings(
    context: DurableContext,
    name: String,
    firstValue: String,
    secondValue: String,
): List<String> {
    lateinit var first: DurableFuture<String>
    lateinit var second: DurableFuture<String>
    context.parallel(name, ParallelOptions(maximumConcurrency = 1)) {
        first = branch("branch-0", typeRef()) { firstValue }
        second = branch("branch-1", typeRef()) { secondValue }
    }
    return listOf(first.await(), second.await())
}

private suspend fun percentage(
    context: DurableContext,
    name: String,
    failOnlyFirst: Boolean,
): Map<String, Any> =
    summary(
        context.parallel(
            name,
            ParallelOptions(
                maximumConcurrency = 1,
                completion = CompletionPolicy.TolerateFailures(percentage = 25.0),
            ),
        ) {
            branch("branch-0", typeRef<String>()) { error("branch 0 failed") }
            branch("branch-1", typeRef<String>()) {
                if (!failOnlyFirst) error("branch 1 failed")
                "ok1"
            }
            branch("branch-2", typeRef<String>()) { "ok2" }
            branch("branch-3", typeRef<String>()) { "ok3" }
        },
        includeStatus = failOnlyFirst,
    )

private fun summary(
    result: ParallelResult,
    includeStatus: Boolean = false,
): Map<String, Any> =
    linkedMapOf<String, Any>().apply {
        put(
            "completionReason",
            when (result.completion) {
                BatchCompletion.ALL_COMPLETED -> "ALL_COMPLETED"
                BatchCompletion.MINIMUM_SUCCEEDED -> "MIN_SUCCESSFUL_REACHED"
                BatchCompletion.FAILURE_LIMIT_EXCEEDED -> "FAILURE_TOLERANCE_EXCEEDED"
            },
        )
        if (includeStatus) put("status", if (result.hasFailure) "FAILED" else "SUCCEEDED")
        put("successCount", result.successes.size)
        put("failureCount", result.failures.size)
        put("totalCount", result.successes.size + result.failures.size)
    }

private object WrappedBranchSerde : Serde {
    override fun encode(value: Any?): String = """{"wrapped":"$value"}"""

    @Suppress("UNCHECKED_CAST")
    override fun <T> decode(payload: String, type: TypeRef<T>): T =
        payload.substringAfter("\"wrapped\":\"").substringBefore('"') as T
}
