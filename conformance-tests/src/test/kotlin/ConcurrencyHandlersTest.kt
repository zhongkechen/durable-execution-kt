// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

import com.amazonaws.services.lambda.runtime.RequestStreamHandler
import io.github.zhongkechen.durable.DurableRuntimeConfig
import io.github.zhongkechen.durable.testing.LocalDurableRunner
import io.github.zhongkechen.durable.testing.LocalExecutionStatus
import kotlin.test.Test
import kotlin.test.assertEquals
import map.MapBasic
import map.MapFailFast
import map.MapItemSerdes
import map.MapItemsOnly
import map.MapOpSerde
import map.MapOpSerdeReplay
import map.MapSuspendIteration
import map.MapThrowIfError
import parallel.ParallelBasic
import parallel.ParallelFailFast
import parallel.ParallelNested
import software.amazon.lambda.durable.AsyncDurableHandler
import software.amazon.lambda.durable.TypeToken
import software.amazon.lambda.durable.model.ExecutionStatus
import software.amazon.lambda.durable.testing.LocalDurableTestRunner

class ConcurrencyHandlersTest {
    @Test
    fun mapHandlersComplete() {
        assertEquals(
            listOf("Hello, World!", "Hello, Kiro!"),
            runClean<Any?, List<String>>(null) { MapBasic(it) },
        )
        assertEquals(
            "FAILURE_TOLERANCE_EXCEEDED",
            runClean<Any?, Map<String, Any>>(null) { MapFailFast(it) }["completionReason"],
        )
        assertEquals(listOf("r0", "r1"), runClean<Any?, List<String>>(null) { MapSuspendIteration(it) })
        assertEquals(listOf("X", "Y"), runClean<Any?, List<String>>(null) { MapItemSerdes(it) })
        assertEquals(listOf("X", "Y"), runClean<Any?, List<String>>(null) { MapOpSerde(it) })
        assertEquals(listOf("X", "Y"), runClean<Any?, List<String>>(null) { MapOpSerdeReplay(it) })
    }

    @Test
    fun mapKotlinSpecificCapabilitiesComplete() {
        assertEquals(
            listOf(2, 4),
            runClean(listOf(1, 2)) { MapItemsOnly(it) },
        )
        val runner =
            LocalDurableRunner.create<Any?, List<String>> { config ->
                MapThrowIfError(config)
            }
        assertEquals(LocalExecutionStatus.FAILED, runner.runUntilComplete(null).status)
    }

    @Test
    fun parallelHandlersComplete() {
        assertEquals(listOf("task-1", "task-2"), runLegacy(ParallelBasic(), typeToken<List<String>>()))
        assertEquals(
            "FAILURE_TOLERANCE_EXCEEDED",
            runLegacy(ParallelFailFast(), typeToken<Map<String, Any>>())["completionReason"],
        )
        assertEquals(listOf(listOf("i1", "i2")), runLegacy(ParallelNested(), TypeToken.get(Any::class.java)))
    }

    @Suppress("UNCHECKED_CAST")
    private fun <O> runLegacy(handler: AsyncDurableHandler<Any?, O>, outputType: TypeToken<O>): O {
        val inputType = TypeToken.get(Any::class.java) as TypeToken<Any?>
        val runner = LocalDurableTestRunner.create(inputType, handler).withOutputType(outputType)
        val result = runner.runUntilComplete(null)
        val error = result.error.orElse(null)
        val operations =
            result.operations.joinToString("\n") {
                "${it.id} ${it.name} ${it.type} ${it.subtype} ${it.status}"
            }
        assertEquals(
            ExecutionStatus.SUCCEEDED,
            result.status,
            error?.let { "${it.errorType()}: ${it.errorMessage()}\n$operations" },
        )
        return result.getResult(outputType)
    }

    private inline fun <reified I, reified O> runClean(
        input: I,
        noinline handler: (DurableRuntimeConfig) -> RequestStreamHandler,
    ): O {
        val result = LocalDurableRunner.create<I, O>(handlerFactory = handler).runUntilComplete(input)
        assertEquals(LocalExecutionStatus.SUCCEEDED, result.status, result.error?.message)
        return requireNotNull(result.result)
    }

    private inline fun <reified T> typeToken(): TypeToken<T> = object : TypeToken<T>() {}
}
