package map

import io.github.zhongkechen.durable.*

import kotlin.time.Duration.Companion.seconds

public class MapBasic(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, List<String>>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?): List<String> =
        map(
            name = "map",
            items = listOf("World", "Kiro"),
            outputType = typeRef(),
            options = MapOptions(maximumConcurrency = 1),
        ) { item, index ->
            step<String>("step-$index") { "Hello, $item!" }
        }.values()
}

public class MapItemIndex(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, List<Int>>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?): List<Int> =
        map(
            name = "indexed",
            items = listOf(10, 20, 30),
            outputType = typeRef(),
            options = MapOptions(maximumConcurrency = 1),
        ) { item, index ->
            item + index
        }.values()
}

public class MapItemsOnly(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<List<Int>, List<Int>>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: List<Int>): List<Int> =
        map(
            items = input.ifEmpty { listOf(1, 2) },
            outputType = typeRef(),
            options = MapOptions(maximumConcurrency = 1),
        ) { item, _ ->
            item * 2
        }.values()
}

public class MapEmpty(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, List<String>>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?): List<String> =
        map(
            name = "empty",
            items = emptyList<String>(),
            outputType = typeRef(),
        ) { item, _ -> item }
            .values()
}

public class MapFailFast(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, Map<String, Any>>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?): Map<String, Any> =
        summary(
            map(
                name = "failfast",
                items = listOf("ok", "fail", "never"),
                outputType = typeRef(),
                options =
                    MapOptions(
                        maximumConcurrency = 1,
                        completion = CompletionPolicy.TolerateFailures(count = 0),
                    ),
            ) { item, _ ->
                if (item == "fail") error("item failed")
                item
            },
            includeStatus = true,
        )
}

public class MapMinSuccessful(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, Map<String, Any>>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?): Map<String, Any> =
        summary(
            map(
                name = "min-successful",
                items = listOf("s0", "s1", "s2", "s3"),
                outputType = typeRef(),
                options =
                    MapOptions(
                        maximumConcurrency = 1,
                        completion = CompletionPolicy.MinimumSuccessful(2),
                    ),
            ) { item, _ -> item },
        ).filterKeys { it != "failureCount" }
}

public class MapThrowIfError(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, List<String>>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?): List<String> =
        map(
            name = "throwing",
            items = listOf("fail", "never"),
            outputType = typeRef(),
            options =
                MapOptions(
                    maximumConcurrency = 1,
                    completion = CompletionPolicy.TolerateFailures(count = 0),
                ),
        ) { item, _ ->
            if (item == "fail") error("item failed")
            item
        }.throwIfFailed()
            .values()
}

public class MapToleratedWithin(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, Map<String, Any>>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?): Map<String, Any> =
        summary(
            map(
                name = "tolerated",
                items = listOf("s0", "fail", "s2"),
                outputType = typeRef(),
                options =
                    MapOptions(
                        maximumConcurrency = 1,
                        completion = CompletionPolicy.TolerateFailures(count = 1),
                    ),
            ) { item, _ ->
                if (item == "fail") error("item failed")
                item
            },
            includeStatus = true,
        )
}

public class MapToleratedExceeded(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, Map<String, Any>>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?): Map<String, Any> =
        summary(
            map(
                name = "tolerated-exceeded",
                items = listOf("f0", "f1", "never"),
                outputType = typeRef(),
                options =
                    MapOptions(
                        maximumConcurrency = 1,
                        completion = CompletionPolicy.TolerateFailures(count = 1),
                    ),
            ) { item, _ ->
                if (item != "never") error("item failed")
                item
            },
        )
}

public class MapToleratedPct(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, Map<String, Any>>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?): Map<String, Any> =
        summary(
            map(
                name = "tolerated-pct",
                items = listOf("f0", "f1", "never", "never"),
                outputType = typeRef(),
                options =
                    MapOptions(
                        maximumConcurrency = 1,
                        completion = CompletionPolicy.TolerateFailures(percentage = 25.0),
                    ),
            ) { item, _ ->
                if (item != "never") error("item failed")
                item
            },
        )
}

public class MapConcurrent(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, List<String>>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?): List<String> =
        map(
            name = "concurrent",
            items = listOf("r0", "r1", "r2"),
            outputType = typeRef(),
            options = MapOptions(maximumConcurrency = 2),
        ) { item, _ -> item }
            .values()
}

public class MapFlat(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, List<String>>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?): List<String> =
        map(
            name = "flat",
            items = listOf("fa", "fb"),
            outputType = typeRef(),
            options =
                MapOptions(
                    maximumConcurrency = 1,
                    nesting = Nesting.FLAT,
                ),
        ) { item, index ->
            step<String>("step-$index") { item }
        }.values()
}

public class MapItemNamer(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<List<Int>, List<Int>>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: List<Int>): List<Int> =
        map(
            name = "named-items",
            items = input,
            outputType = typeRef(),
            options =
                MapOptions(
                    maximumConcurrency = 1,
                    itemName = { item, _ -> "item-$item" },
                ),
        ) { item, _ -> item * 10 }
            .values()
}

public class MapItemSerdes(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, List<String>>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?): List<String> =
        map(
            name = "serdes",
            items = listOf("x", "y"),
            outputType = typeRef(),
            options =
                MapOptions(
                    maximumConcurrency = 1,
                    itemSerde = WrappedItemSerde,
                ),
        ) { item, _ -> item.uppercase() }
            .values()
}

public class MapSuspendIteration(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, List<String>>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?): List<String> =
        map(
            name = "suspend",
            items = listOf("r0", "r1"),
            outputType = typeRef(),
            options = MapOptions(maximumConcurrency = 1),
        ) { item, index ->
            if (index == 1) wait(1.seconds)
            step<String>("step-$index") { item }
        }.values()
}

public class MapLargeResult(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, Map<String, Any>>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?): Map<String, Any> {
        val big = "x".repeat(70_000)
        val result =
            map(
                name = "large",
                items = listOf(0, 1, 2, 3),
                outputType = typeRef<String>(),
                options = MapOptions(maximumConcurrency = 1),
            ) { _, _ -> big }
        return mapOf(
            "successCount" to result.successes.size,
            "totalCount" to result.successes.size + result.failures.size,
        )
    }
}

public class MapThenWait(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, List<String>>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?): List<String> {
        val result =
            map(
                name = "then-wait",
                items = listOf("a", "b"),
                outputType = typeRef<String>(),
                options = MapOptions(maximumConcurrency = 1),
            ) { item, _ -> item.uppercase() }
        wait(1.seconds)
        return result.values()
    }
}

public class MapFailThenWait(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, Map<String, Any>>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?): Map<String, Any> {
        val result =
            map(
                name = "fail-then-wait",
                items = listOf("ok", "fail"),
                outputType = typeRef<String>(),
                options =
                    MapOptions(
                        maximumConcurrency = 1,
                        completion = CompletionPolicy.TolerateFailures(count = 1),
                    ),
            ) { item, _ ->
                if (item == "fail") error("item failed")
                item
            }
        wait(1.seconds)
        return summary(result, includeStatus = true)
    }
}

public class MapOpSerde(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, List<String>>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?): List<String> =
        map(
            name = "op-serde",
            items = listOf("x", "y"),
            outputType = typeRef(),
            options =
                MapOptions(
                    maximumConcurrency = 1,
                    resultSerde = WholeMapSerde,
                ),
        ) { item, _ -> item.uppercase() }
            .values()
}

public class MapOpSerdeReplay(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, List<String>>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?): List<String> {
        val result =
            map(
                name = "op-serde-replay",
                items = listOf("x", "y"),
                outputType = typeRef<String>(),
                options =
                    MapOptions(
                        maximumConcurrency = 1,
                        resultSerde = WholeMapSerde,
                    ),
            ) { item, _ -> item.uppercase() }
        wait(1.seconds)
        return result.values()
    }
}

private object WrappedItemSerde : Serde {
    override fun encode(value: Any?): String = "wrapped:$value"

    @Suppress("UNCHECKED_CAST")
    override fun <T> decode(payload: String, type: TypeRef<T>): T =
        payload.removePrefix("wrapped:") as T
}

private object WholeMapSerde : Serde {
    override fun encode(value: Any?): String {
        val result = value as MapResult<*>
        return "OPSERDE:" + result.values().joinToString(",")
    }

    @Suppress("UNCHECKED_CAST")
    override fun <T> decode(payload: String, type: TypeRef<T>): T {
        val values = payload.removePrefix("OPSERDE:").split(",").filter(String::isNotEmpty)
        return MapResult(
            BatchCompletion.ALL_COMPLETED,
            values.mapIndexed { index, value ->
                ItemResult.Success(index, null, value)
            },
        ) as T
    }
}

private fun <T> summary(
    result: MapResult<T>,
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
        if (includeStatus) put("status", if (result.failures.isEmpty()) "SUCCEEDED" else "FAILED")
        put("successCount", result.successes.size)
        put("failureCount", result.failures.size)
        put("totalCount", result.successes.size + result.failures.size)
    }
