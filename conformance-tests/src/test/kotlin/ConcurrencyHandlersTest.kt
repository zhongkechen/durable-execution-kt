// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

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
        assertEquals(listOf("Hello, World!", "Hello, Kiro!"), run(MapBasic(), typeToken<List<String>>()))
        assertEquals(
            "FAILURE_TOLERANCE_EXCEEDED",
            run(MapFailFast(), typeToken<Map<String, Any>>())["completionReason"],
        )
        assertEquals(listOf("r0", "r1"), run(MapSuspendIteration(), typeToken<List<String>>()))
        assertEquals(listOf("X", "Y"), run(MapItemSerdes(), typeToken<List<String>>()))
        assertEquals(listOf("X", "Y"), run(MapOpSerde(), typeToken<List<String>>()))
        assertEquals(listOf("X", "Y"), run(MapOpSerdeReplay(), typeToken<List<String>>()))
    }

    @Test
    fun mapKotlinSpecificCapabilitiesComplete() {
        assertEquals(
            listOf(2, 4),
            run(
                MapItemsOnly(),
                listOf(1, 2),
                typeToken<List<Int>>(),
                typeToken<List<Int>>(),
            ),
        )
        val handler = MapThrowIfError()
        @Suppress("UNCHECKED_CAST")
        val inputType = TypeToken.get(Any::class.java) as TypeToken<Any?>
        val runner = LocalDurableTestRunner.create(inputType, handler)
        assertEquals(ExecutionStatus.FAILED, runner.runUntilComplete(null).status)
    }

    @Test
    fun parallelHandlersComplete() {
        assertEquals(listOf("task-1", "task-2"), run(ParallelBasic(), typeToken<List<String>>()))
        assertEquals(
            "FAILURE_TOLERANCE_EXCEEDED",
            run(ParallelFailFast(), typeToken<Map<String, Any>>())["completionReason"],
        )
        assertEquals(listOf(listOf("i1", "i2")), run(ParallelNested(), TypeToken.get(Any::class.java)))
    }

    @Suppress("UNCHECKED_CAST")
    private fun <O> run(handler: AsyncDurableHandler<Any?, O>, outputType: TypeToken<O>): O {
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

    private fun <I, O> run(
        handler: AsyncDurableHandler<I, O>,
        input: I,
        inputType: TypeToken<I>,
        outputType: TypeToken<O>,
    ): O {
        val runner = LocalDurableTestRunner.create(inputType, handler).withOutputType(outputType)
        val result = runner.runUntilComplete(input)
        assertEquals(ExecutionStatus.SUCCEEDED, result.status)
        return result.getResult(outputType)
    }

    private inline fun <reified T> typeToken(): TypeToken<T> = object : TypeToken<T>() {}
}
