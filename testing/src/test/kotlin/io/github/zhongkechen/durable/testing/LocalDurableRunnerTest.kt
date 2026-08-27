package io.github.zhongkechen.durable.testing

import io.github.zhongkechen.durable.DurableContext
import io.github.zhongkechen.durable.DurableHandler
import io.github.zhongkechen.durable.DurableRuntimeConfig
import io.github.zhongkechen.durable.step
import io.github.zhongkechen.durable.typeRef
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.time.Duration.Companion.seconds

class LocalDurableRunnerTest {
    @Test
    fun `step and replaying wait complete locally`() {
        val runner =
            LocalDurableRunner.create<Int, Int> { config ->
                SampleHandler(config)
            }

        val result = runner.runUntilComplete(21)

        assertEquals(LocalExecutionStatus.SUCCEEDED, result.status)
        assertEquals(42, result.result)
        assertEquals(2, result.invocations)
        assertNotNull(result.operations.firstOrNull { it.name == "double" })
        assertNotNull(result.operations.firstOrNull { it.name == "pause" })
    }

    private class SampleHandler(
        config: DurableRuntimeConfig,
    ) : DurableHandler<Int, Int>(typeRef(), typeRef(), config) {
        override suspend fun handle(
            input: Int,
            context: DurableContext,
        ): Int {
            val result = context.step("double") { input * 2 }
            context.wait(1.seconds, "pause")
            return result
        }
    }
}
