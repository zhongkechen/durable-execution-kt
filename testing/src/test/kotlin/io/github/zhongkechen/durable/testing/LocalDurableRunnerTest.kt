package io.github.zhongkechen.durable.testing

import io.github.zhongkechen.durable.DurableHandler
import io.github.zhongkechen.durable.DurableRuntimeConfig
import io.github.zhongkechen.durable.step
import io.github.zhongkechen.durable.typeRef
import io.github.zhongkechen.durable.wait
import io.github.zhongkechen.durable.extension.ExtensionContextResult
import io.github.zhongkechen.durable.extension.ExtensionStepConfig
import io.github.zhongkechen.durable.extension.ExtensionStepResult
import io.github.zhongkechen.durable.extension.currentExtensionContext
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
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

    @Test
    fun `stateful extension step checkpoints custom state and subtype`() {
        val runner =
            LocalDurableRunner.create<Int, Int> { config ->
                StatefulExtensionHandler(config)
            }

        val result = runner.runUntilComplete(3)
        val operation = result.operations.single { it.name == "counter" }

        assertEquals(LocalExecutionStatus.SUCCEEDED, result.status)
        assertEquals(3, result.result)
        assertEquals("CounterStep", operation.subtype)
        assertEquals(3, result.invocations)
    }

    @Test
    fun `extension reservations are one shot and child scope is coroutine local`() {
        val runner =
            LocalDurableRunner.create<String, String> { config ->
                ExtensionCompositionHandler(config)
            }

        val result = runner.runUntilComplete("value")

        assertEquals(LocalExecutionStatus.SUCCEEDED, result.status)
        assertEquals("value:true", result.result)
        assertEquals("AcmeContext", result.operations.single { it.name == "child" }.subtype)
        assertEquals("AcmeStep", result.operations.single { it.name == "inner" }.subtype)
    }

    private class SampleHandler(
        config: DurableRuntimeConfig,
    ) : DurableHandler<Int, Int>(typeRef(), typeRef(), config) {
        override suspend fun handle(input: Int): Int {
            val result = step("double") { input * 2 }
            wait(1.seconds, "pause")
            return result
        }
    }

    private class StatefulExtensionHandler(
        config: DurableRuntimeConfig,
    ) : DurableHandler<Int, Int>(typeRef(), typeRef(), config) {
        override suspend fun handle(input: Int): Int =
            currentExtensionContext()
                .reserve(name = "counter", localId = "stable-counter")
                .step(
                    subtype = "CounterStep",
                    type = typeRef(),
                    config = ExtensionStepConfig(initialState = 0),
                ) { state ->
                    val next = (state ?: 0) + 1
                    if (next >= input) {
                        ExtensionStepResult.Succeeded(next)
                    } else {
                        ExtensionStepResult.Retry(next, 1.seconds)
                    }
                }
    }

    private class ExtensionCompositionHandler(
        config: DurableRuntimeConfig,
    ) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
        override suspend fun handle(input: String): String {
            val outer = currentExtensionContext()
            val value =
                outer.reserve("child").runInChildContext(
                    subtype = "AcmeContext",
                    type = typeRef<String>(),
                ) {
                    val child = currentExtensionContext()
                    check(child !== outer)
                    val reservation = child.reserve("inner")
                    val result =
                        reservation.step("AcmeStep", typeRef<String>()) {
                            ExtensionStepResult.Succeeded(input)
                        }
                    val reuseRejected =
                        runCatching {
                            reservation.wait("InvalidReuse", 1.seconds)
                        }.isFailure
                    ExtensionContextResult.Completed("$result:$reuseRejected")
                }
            assertTrue(value.endsWith(":true"))
            return value
        }
    }
}
