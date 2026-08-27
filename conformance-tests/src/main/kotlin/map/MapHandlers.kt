// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package map

import kotlin.time.Duration.Companion.seconds
import software.amazon.lambda.durable.kotlin.CompletionPolicy
import software.amazon.lambda.durable.kotlin.KotlinDurableContext
import software.amazon.lambda.durable.kotlin.KotlinDurableHandler
import software.amazon.lambda.durable.kotlin.KotlinDurableRuntime
import software.amazon.lambda.durable.kotlin.Nesting
import software.amazon.lambda.durable.kotlin.throwIfFailed
import software.amazon.lambda.durable.TypeToken
import software.amazon.lambda.durable.model.ConcurrencyCompletionStatus
import software.amazon.lambda.durable.model.MapResult
import software.amazon.lambda.durable.serde.SerDes

public class MapBasic : KotlinDurableHandler<Any?, List<String>>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): List<String> =
        context.map(
            name = "map",
            items = listOf("World", "Kiro"),
            maxConcurrency = 1,
        ) { item, index ->
            step<String>("step-$index") { "Hello, $item!" }
        }.results()
}

public class MapItemIndex : KotlinDurableHandler<Any?, List<Int>>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): List<Int> =
        context.map(
            name = "indexed",
            items = listOf(10, 20, 30),
            maxConcurrency = 1,
        ) { item, index ->
            item + index
        }.results()
}

public class MapItemsOnly : KotlinDurableHandler<List<Int>, List<Int>>() {
    override suspend fun handle(input: List<Int>, context: KotlinDurableContext): List<Int> =
        context.map(
            items = input.ifEmpty { listOf(1, 2) },
            maxConcurrency = 1,
        ) { item, _ ->
            item * 2
        }.results()
}

public class MapEmpty :
    KotlinDurableHandler<Any?, List<String>>(
        KotlinDurableRuntime.config {
            withCheckpointEmptyMap(true)
        },
    ) {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): List<String> =
        context.map<String, String>(name = "empty", items = emptyList()) { item, _ -> item }.results()
}

public class MapFailFast : KotlinDurableHandler<Any?, Map<String, Any>>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): Map<String, Any> =
        summary(
            context.map(
                name = "failfast",
                items = listOf("ok", "fail", "never"),
                maxConcurrency = 1,
                completion = CompletionPolicy.allSuccessful,
            ) { item, _ ->
                if (item == "fail") error("item failed")
                item
            },
            includeStatus = true,
        )
}

public class MapMinSuccessful : KotlinDurableHandler<Any?, Map<String, Any>>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): Map<String, Any> =
        summary(
            context.map(
                name = "min-successful",
                items = listOf("s0", "s1", "s2", "s3"),
                maxConcurrency = 1,
                completion = CompletionPolicy.minSuccessful(2),
            ) { item, _ ->
                item
            },
        ).filterKeys { it != "failureCount" }
}

public class MapThrowIfError : KotlinDurableHandler<Any?, List<String>>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): List<String> =
        context.map(
            name = "throwing",
            items = listOf("fail", "never"),
            maxConcurrency = 1,
            completion = CompletionPolicy.allSuccessful,
        ) { item, _ ->
            if (item == "fail") error("item failed")
            item
        }.throwIfFailed()
            .results()
}

public class MapToleratedWithin : KotlinDurableHandler<Any?, Map<String, Any>>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): Map<String, Any> =
        summary(
            context.map(
                name = "tolerated",
                items = listOf("s0", "fail", "s2"),
                maxConcurrency = 1,
                completion = CompletionPolicy.toleratedFailures(1),
            ) { item, _ ->
                if (item == "fail") error("item failed")
                item
            },
            includeStatus = true,
        )
}

public class MapToleratedExceeded : KotlinDurableHandler<Any?, Map<String, Any>>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): Map<String, Any> =
        summary(
            context.map(
                name = "tolerated-exceeded",
                items = listOf("f0", "f1", "never"),
                maxConcurrency = 1,
                completion = CompletionPolicy.toleratedFailures(1),
            ) { item, _ ->
                if (item != "never") error("item failed")
                item
            },
        )
}

public class MapToleratedPct : KotlinDurableHandler<Any?, Map<String, Any>>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): Map<String, Any> =
        summary(
            context.map(
                name = "tolerated-pct",
                items = listOf("f0", "f1", "never", "never"),
                maxConcurrency = 1,
                completion = CompletionPolicy.toleratedFailurePercentage(0.25),
            ) { item, _ ->
                if (item != "never") error("item failed")
                item
            },
        )
}

public class MapConcurrent : KotlinDurableHandler<Any?, List<String>>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): List<String> =
        context.map(
            name = "concurrent",
            items = listOf("r0", "r1", "r2"),
            maxConcurrency = 2,
        ) { item, _ ->
            item
        }.results()
}

public class MapFlat : KotlinDurableHandler<Any?, List<String>>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): List<String> =
        context.map(
            name = "flat",
            items = listOf("fa", "fb"),
            maxConcurrency = 1,
            nesting = Nesting.FLAT,
        ) { item, index ->
            step<String>("step-$index") { item }
        }.results()
}

public class MapItemNamer : KotlinDurableHandler<List<Int>, List<Int>>() {
    override suspend fun handle(input: List<Int>, context: KotlinDurableContext): List<Int> =
        context.map(
            name = "named-items",
            items = input,
            maxConcurrency = 1,
            itemName = { item, _ -> "item-$item" },
        ) { item, _ ->
            item * 10
        }.results()
}

public class MapItemSerdes : KotlinDurableHandler<Any?, List<String>>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): List<String> =
        context.map(
            name = "serdes",
            items = listOf("x", "y"),
            maxConcurrency = 1,
            itemSerDes = WrappedItemSerDes,
        ) { item, _ ->
            item.uppercase()
        }.results()
}

public class MapSuspendIteration : KotlinDurableHandler<Any?, List<String>>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): List<String> =
        context.map(
            name = "suspend",
            items = listOf("r0", "r1"),
            maxConcurrency = 1,
        ) { item, index ->
            if (index == 1) wait(1.seconds)
            step<String>("step-$index") { item }
        }.results()
}

public class MapLargeResult : KotlinDurableHandler<Any?, Map<String, Any>>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): Map<String, Any> {
        val big = "x".repeat(70_000)
        val result =
            context.map(
                name = "large",
                items = listOf(0, 1, 2, 3),
                maxConcurrency = 1,
            ) { _, _ ->
                big
            }
        return mapOf(
            "successCount" to result.succeeded().size,
            "totalCount" to result.succeeded().size + result.failed().size,
        )
    }
}

public class MapThenWait : KotlinDurableHandler<Any?, List<String>>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): List<String> {
        val result =
            context.map(
                name = "then-wait",
                items = listOf("a", "b"),
                maxConcurrency = 1,
            ) { item, _ ->
                item.uppercase()
            }
        context.wait(1.seconds)
        return result.results()
    }
}

public class MapFailThenWait : KotlinDurableHandler<Any?, Map<String, Any>>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): Map<String, Any> {
        val result =
            context.map(
                name = "fail-then-wait",
                items = listOf("ok", "fail"),
                maxConcurrency = 1,
                completion = CompletionPolicy.toleratedFailures(1),
            ) { item, _ ->
                if (item == "fail") error("item failed")
                item
            }
        context.wait(1.seconds)
        return summary(result, includeStatus = true)
    }
}

public class MapOpSerde : KotlinDurableHandler<Any?, List<String>>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): List<String> =
        context.map(
            name = "op-serde",
            items = listOf("x", "y"),
            maxConcurrency = 1,
            resultSerDes = WholeMapSerDes,
        ) { item, _ ->
            item.uppercase()
        }.results()
}

public class MapOpSerdeReplay : KotlinDurableHandler<Any?, List<String>>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): List<String> {
        val result =
            context.map(
                name = "op-serde-replay",
                items = listOf("x", "y"),
                maxConcurrency = 1,
                resultSerDes = WholeMapSerDes,
            ) { item, _ ->
                item.uppercase()
            }
        context.wait(1.seconds)
        return result.results()
    }
}

private object WrappedItemSerDes : SerDes {
    override fun serialize(value: Any?): String? = value?.let { "wrapped:$it" }

    @Suppress("UNCHECKED_CAST")
    override fun <T : Any?> deserialize(data: String?, typeToken: TypeToken<T>): T =
        data?.removePrefix("wrapped:") as T
}

private object WholeMapSerDes : SerDes {
    override fun serialize(value: Any?): String {
        val result = value as MapResult<*>
        return "OPSERDE:" + result.results().joinToString(",")
    }

    @Suppress("UNCHECKED_CAST")
    override fun <T : Any?> deserialize(data: String?, typeToken: TypeToken<T>): T {
        val values = data.orEmpty().removePrefix("OPSERDE:").split(",")
        val items = values.map { MapResult.MapResultItem.succeeded(it) }
        return MapResult(items, ConcurrencyCompletionStatus.ALL_COMPLETED) as T
    }
}

private fun <T> summary(
    result: MapResult<T>,
    includeStatus: Boolean = false,
): Map<String, Any> =
    linkedMapOf<String, Any>().apply {
        put("completionReason", result.completionReason().name)
        if (includeStatus) put("status", if (result.allSucceeded()) "SUCCEEDED" else "FAILED")
        put("successCount", result.succeeded().size)
        put("failureCount", result.failed().size)
        put("totalCount", result.succeeded().size + result.failed().size)
    }
