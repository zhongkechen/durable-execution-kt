// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package software.amazon.lambda.durable.kotlin

import java.time.Duration
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.withContext
import software.amazon.lambda.durable.model.ExecutionStatus
import software.amazon.lambda.durable.testing.LocalDurableTestRunner

class KotlinDurableHandlerTest {
    @Test
    fun suspendHandlerAndStepResumeAcrossDurableWait() {
        val handler =
            object : KotlinDurableHandler<String, String>() {
                override suspend fun handle(input: String, context: KotlinDurableContext): String {
                    delay(1)
                    val first =
                        context.step<String>("suspend-step") {
                            delay(1)
                            "$input-$attempt"
                        }
                    context.wait(Duration.ofSeconds(1), "durable-wait")
                    return withContext(Dispatchers.Default) {
                        context.step<String>("after-thread-hop") {
                            "$first-resumed"
                        }
                    }
                }
            }
        val runner =
            LocalDurableTestRunner.create(
                String::class.java,
                { input, context -> handler.handleRequest(input, context) },
                handler.configuration,
            )

        val result = runner.runUntilComplete("value")

        assertEquals(ExecutionStatus.SUCCEEDED, result.status, errorMessage(result))
        assertEquals("value-1-resumed", result.getResult(String::class.java))
    }

    @Test
    fun suspendChildContextCanRunNestedDurableOperations() {
        val handler =
            object : KotlinDurableHandler<String, String>() {
                override suspend fun handle(input: String, context: KotlinDurableContext): String =
                    context.childContext("child") {
                        delay(1)
                        step<String>("nested") { input.uppercase() }
                    }
            }
        val runner =
            LocalDurableTestRunner.create(
                String::class.java,
                { input, context -> handler.handleRequest(input, context) },
                handler.configuration,
            )

        val result = runner.runUntilComplete("value")

        assertEquals(ExecutionStatus.SUCCEEDED, result.status, errorMessage(result))
        assertEquals("VALUE", result.getResult(String::class.java))
    }

    private fun errorMessage(result: software.amazon.lambda.durable.testing.TestResult<String>): String? =
        result.error.orElse(null)?.let { error ->
            "${error.errorType()}: ${error.errorMessage()}\n${error.stackTrace()?.joinToString("\n")}"
        }
}
