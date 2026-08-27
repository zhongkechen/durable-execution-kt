// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package parallel

import java.time.Duration
import software.amazon.lambda.durable.DurableFuture
import software.amazon.lambda.durable.TypeToken
import software.amazon.lambda.durable.config.CompletionConfig
import software.amazon.lambda.durable.config.NestingType
import software.amazon.lambda.durable.config.ParallelConfig
import software.amazon.lambda.durable.kotlin.CompletionPolicy
import software.amazon.lambda.durable.kotlin.KotlinDurableContext
import software.amazon.lambda.durable.kotlin.KotlinDurableHandler
import software.amazon.lambda.durable.kotlin.await
import software.amazon.lambda.durable.model.ParallelResult
import software.amazon.lambda.durable.serde.SerDes

public class ParallelBasic : KotlinDurableHandler<Any?, List<String>>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): List<String> {
        val futures = mutableListOf<DurableFuture<String>>()
        context.parallel("parallel", ParallelConfig.builder().maxConcurrency(1).build()) {
            futures += branch<String>("branch-0") { step<String>("step-0") { "task-1" } }
            futures += branch<String>("branch-1") { step<String>("step-1") { "task-2" } }
        }
        return futures.map { it.await() }
    }
}

public class ParallelBranchesOnly : KotlinDurableHandler<Any?, List<String>>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): List<String> {
        val futures = mutableListOf<DurableFuture<String>>()
        context.parallel("branches-only", ParallelConfig.builder().maxConcurrency(1).build()) {
            futures += branch<String>("branch-0") { "alpha" }
            futures += branch<String>("branch-1") { "beta" }
        }
        return futures.map { it.await() }
    }
}

public class ParallelNamedBranches : KotlinDurableHandler<Any?, List<String>>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): List<String> {
        val futures = mutableListOf<DurableFuture<String>>()
        context.parallel("named", ParallelConfig.builder().maxConcurrency(1).build()) {
            futures += branch<String>("first") { "one" }
            futures += branch<String>("second") { "two" }
        }
        return futures.map { it.await() }
    }
}

public class ParallelHeterogeneous : KotlinDurableHandler<Any?, List<Any>>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): List<Any> {
        lateinit var stringFuture: DurableFuture<String>
        lateinit var intFuture: DurableFuture<Int>
        lateinit var mapFuture: DurableFuture<Map<String, String>>
        context.parallel("hetero", ParallelConfig.builder().maxConcurrency(1).build()) {
            stringFuture = branch("branch-0") { "hello" }
            intFuture = branch("branch-1") { 42 }
            mapFuture = branch("branch-2") { mapOf("k" to "v") }
        }
        return listOf(stringFuture.await(), intFuture.await(), mapFuture.await())
    }
}

public class ParallelEmpty : KotlinDurableHandler<Any?, List<Any>>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): List<Any> {
        context.parallel("empty") {}
        return emptyList()
    }
}

public class ParallelFailFast : KotlinDurableHandler<Any?, Map<String, Any>>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): Map<String, Any> =
        summary(
            context.parallel(
                "failfast",
                ParallelConfig.builder()
                    .maxConcurrency(1)
                    .completionConfig(CompletionConfig.allSuccessful())
                    .build(),
            ) {
                branch<String>("branch-0") { "ok" }
                branch<String>("branch-1") { error("branch 1 failed") }
                branch<String>("branch-2") { "never" }
            },
            includeStatus = true,
        )
}

public class ParallelThrowIfError : KotlinDurableHandler<Any?, String>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String {
        lateinit var failed: DurableFuture<String>
        context.parallel(
            "throwing",
            ParallelConfig.builder()
                .maxConcurrency(1)
                .completionConfig(CompletionConfig.allSuccessful())
                .build(),
        ) {
            failed = branch("branch-0") { error("branch 0 failed") }
            branch<String>("branch-1") { "never" }
        }
        return failed.await()
    }
}

public class ParallelMinSuccessful : KotlinDurableHandler<Any?, Map<String, Any>>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): Map<String, Any> =
        summary(
            context.parallel(
                "min-successful",
                ParallelConfig.builder()
                    .maxConcurrency(1)
                    .completionConfig(CompletionConfig.minSuccessful(2))
                    .build(),
            ) {
                branch<String>("branch-0") { "v0" }
                branch<String>("branch-1") { "v1" }
                branch<String>("branch-2") { "v2" }
                branch<String>("branch-3") { "v3" }
            },
        ).filterKeys { it != "failureCount" }
}

public class ParallelToleratedWithin : KotlinDurableHandler<Any?, Map<String, Any>>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): Map<String, Any> =
        summary(
            context.parallel(
                "tolerated",
                ParallelConfig.builder()
                    .maxConcurrency(1)
                    .completionConfig(CompletionConfig.toleratedFailureCount(1))
                    .build(),
            ) {
                branch<String>("branch-0") { "s0" }
                branch<String>("branch-1") { error("branch 1 failed") }
                branch<String>("branch-2") { "s2" }
            },
            includeStatus = true,
        )
}

public class ParallelToleratedExceeded : KotlinDurableHandler<Any?, Map<String, Any>>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): Map<String, Any> =
        summary(
            context.parallel(
                "tolerated-exceeded",
                ParallelConfig.builder()
                    .maxConcurrency(1)
                    .completionConfig(CompletionConfig.toleratedFailureCount(1))
                    .build(),
            ) {
                branch<String>("branch-0") { error("branch 0 failed") }
                branch<String>("branch-1") { error("branch 1 failed") }
                branch<String>("branch-2") { "never" }
            },
        )
}

public class ParallelToleratedPct : KotlinDurableHandler<Any?, Map<String, Any>>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): Map<String, Any> =
        summary(
            context.parallel(
                name = "tolerated-pct",
                maxConcurrency = 1,
                completion = CompletionPolicy.toleratedFailurePercentage(0.25),
            ) {
                branch<String>("branch-0") { error("branch 0 failed") }
                branch<String>("branch-1") { error("branch 1 failed") }
                branch<String>("branch-2") { "never" }
                branch<String>("branch-3") { "never" }
            },
        )
}

public class ParallelPctBoundary : KotlinDurableHandler<Any?, Map<String, Any>>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): Map<String, Any> =
        summary(
            context.parallel(
                name = "pct-boundary",
                maxConcurrency = 1,
                completion = CompletionPolicy.toleratedFailurePercentage(0.25),
            ) {
                branch<String>("branch-0") { error("branch 0 failed") }
                branch<String>("branch-1") { "ok1" }
                branch<String>("branch-2") { "ok2" }
                branch<String>("branch-3") { "ok3" }
            },
            includeStatus = true,
        )
}

public class ParallelItemSerdes : KotlinDurableHandler<Any?, List<String>>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): List<String> {
        val futures = mutableListOf<DurableFuture<String>>()
        context.parallel(
            name = "serde",
            maxConcurrency = 1,
            itemSerDes = WrappedBranchSerDes,
        ) {
            futures += branch<String>("branch-0") { "x" }
            futures += branch<String>("branch-1") { "y" }
        }
        return futures.map { it.await() }
    }
}

public class ParallelConcurrent : KotlinDurableHandler<Any?, List<String>>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): List<String> {
        val futures = mutableListOf<DurableFuture<String>>()
        context.parallel("concurrent", ParallelConfig.builder().maxConcurrency(2).build()) {
            futures += branch<String>("branch-0") { "r0" }
            futures += branch<String>("branch-1") { "r1" }
            futures += branch<String>("branch-2") { "r2" }
        }
        return futures.map { it.await() }
    }
}

public class ParallelFlat : KotlinDurableHandler<Any?, List<String>>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): List<String> {
        val futures = mutableListOf<DurableFuture<String>>()
        context.parallel(
            "flat",
            ParallelConfig.builder()
                .maxConcurrency(1)
                .nestingType(NestingType.FLAT)
                .build(),
        ) {
            futures += branch<String>("branch-0") { step<String>("step-0") { "fa" } }
            futures += branch<String>("branch-1") { step<String>("step-1") { "fb" } }
        }
        return futures.map { it.await() }
    }
}

public class ParallelReplaySkipsSucceeded : KotlinDurableHandler<Any?, List<String>>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): List<String> {
        val futures = mutableListOf<DurableFuture<String>>()
        context.parallel("replay", ParallelConfig.builder().maxConcurrency(1).build()) {
            futures += branch<String>("branch-0") { step<String>("step-0") { "b0" } }
            futures +=
                branch<String>("branch-1") {
                    wait(Duration.ofSeconds(2))
                    "b1"
                }
        }
        return futures.map { it.await() }
    }
}

public class ParallelAllFail : KotlinDurableHandler<Any?, Map<String, Any>>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): Map<String, Any> =
        summary(
            context.parallel(
                "all-fail",
                ParallelConfig.builder()
                    .maxConcurrency(1)
                    .completionConfig(CompletionConfig.toleratedFailureCount(3))
                    .build(),
            ) {
                branch<String>("branch-0") { error("branch 0 failed") }
                branch<String>("branch-1") { error("branch 1 failed") }
                branch<String>("branch-2") { error("branch 2 failed") }
            },
            includeStatus = true,
        )
}

public class ParallelMinNotReached : KotlinDurableHandler<Any?, Map<String, Any>>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): Map<String, Any> =
        summary(
            context.parallel(
                "min-not-reached",
                ParallelConfig.builder()
                    .maxConcurrency(1)
                    .completionConfig(CompletionConfig.minSuccessful(3))
                    .build(),
            ) {
                branch<String>("branch-0") { "ok0" }
                branch<String>("branch-1") { error("branch 1 failed") }
                branch<String>("branch-2") { "ok2" }
            },
            includeStatus = true,
        )
}

public class ParallelCombinedConfig : KotlinDurableHandler<Any?, Map<String, Any>>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): Map<String, Any> =
        summary(
            context.parallel(
                "combined",
                ParallelConfig.builder()
                    .maxConcurrency(1)
                    .completionConfig(CompletionConfig(3, 1, null))
                    .build(),
            ) {
                branch<String>("branch-0") { error("branch 0 failed") }
                branch<String>("branch-1") { error("branch 1 failed") }
                branch<String>("branch-2") { "ok2" }
                branch<String>("branch-3") { "ok3" }
            },
        )
}

public class ParallelBadConcurrency : KotlinDurableHandler<Any?, List<String>>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): List<String> {
        val config = ParallelConfig.builder().maxConcurrency(0).build()
        val futures = mutableListOf<DurableFuture<String>>()
        context.parallel("bad-concurrency", config) {
            futures += branch<String>("branch-0") { "a" }
            futures += branch<String>("branch-1") { "b" }
        }
        return futures.map { it.await() }
    }
}

public class ParallelAccessors : KotlinDurableHandler<Any?, Map<String, Any>>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): Map<String, Any> {
        val result =
            context.parallel(
                "accessors",
                ParallelConfig.builder()
                    .maxConcurrency(1)
                    .completionConfig(CompletionConfig.toleratedFailureCount(1))
                    .build(),
            ) {
                branch<String>("branch-0") { "ok0" }
                branch<String>("branch-1") { error("branch 1 failed") }
                branch<String>("branch-2") { "ok2" }
            }
        return mapOf(
            "hasFailure" to (result.failed() > 0),
            "successCount" to result.succeeded(),
            "failureCount" to result.failed(),
            "errorCount" to result.failed(),
        )
    }
}

public class ParallelNested : KotlinDurableHandler<Any?, Any>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): Any {
        lateinit var outerBranch: DurableFuture<List<String>>
        context.parallel("outer", ParallelConfig.builder().maxConcurrency(1).build()) {
            outerBranch =
                branch<List<String>>("branchA") {
                    val inner = mutableListOf<DurableFuture<String>>()
                    parallel("inner", ParallelConfig.builder().maxConcurrency(1).build()) {
                        inner += branch<String>("inner-0") { step<String>("step-0") { "i1" } }
                        inner += branch<String>("inner-1") { step<String>("step-1") { "i2" } }
                    }
                    inner.map { it.await() }
                }
        }
        return listOf(outerBranch.await())
    }
}

private fun summary(
    result: ParallelResult,
    includeStatus: Boolean = false,
): Map<String, Any> =
    linkedMapOf<String, Any>().apply {
        put("completionReason", result.completionStatus().name)
        if (includeStatus) put("status", if (result.failed() > 0) "FAILED" else "SUCCEEDED")
        put("successCount", result.succeeded())
        put("failureCount", result.failed())
        put("totalCount", result.succeeded() + result.failed())
    }

private object WrappedBranchSerDes : SerDes {
    override fun serialize(value: Any?): String? = value?.let { """{"wrapped":"$it"}""" }

    @Suppress("UNCHECKED_CAST")
    override fun <T : Any?> deserialize(data: String?, typeToken: TypeToken<T>): T {
        val value = data?.substringAfter("\"wrapped\":\"")?.substringBefore('"')
        return value as T
    }
}
