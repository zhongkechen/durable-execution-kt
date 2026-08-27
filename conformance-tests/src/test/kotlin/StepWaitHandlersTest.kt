// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0

import kotlin.test.Test
import kotlin.test.assertEquals
import software.amazon.lambda.durable.AsyncDurableHandler
import software.amazon.lambda.durable.model.ExecutionStatus
import software.amazon.lambda.durable.testing.LocalDurableTestRunner
import step.StepAndWaitReplay
import step.StepBasic
import step.StepCustomSerdes
import step.StepDefaultRetry
import step.StepErrorCaught
import step.StepNested
import step.StepWithRetry
import wait.WaitMultipleSequential

class StepWaitHandlersTest {
    @Suppress("UNCHECKED_CAST")
    private val anyClass: Class<Any?> = Any::class.java as Class<Any?>

    @Suppress("UNCHECKED_CAST")
    private val mapClass: Class<Map<String, Any>> = Map::class.java as Class<Map<String, Any>>

    @Test
    fun basicAndNestedStepsComplete() {
        assertEquals("Hello, Kotlin!", run(StepBasic(), "Kotlin", String::class.java, String::class.java))
        assertEquals("first_second", run(StepNested(), null, anyClass, String::class.java))
    }

    @Test
    fun replayAndErrorHandlingStepsComplete() {
        assertEquals("computed", run(StepAndWaitReplay(), null, anyClass, String::class.java))
        assertEquals("fallback_result", run(StepErrorCaught(), null, anyClass, String::class.java))
    }

    @Test
    fun customStepSerializationCompletes() {
        assertEquals("VALUE", run(StepCustomSerdes(), "value", String::class.java, String::class.java))
    }

    @Test
    fun retryHandlersComplete() {
        assertEquals("Operation succeeded", run(StepWithRetry(), null, anyClass, String::class.java))
        assertEquals("recovered", run(StepDefaultRetry(), null, anyClass, String::class.java))
    }

    @Test
    fun sequentialWaitsComplete() {
        val result = run(WaitMultipleSequential(), null, anyClass, mapClass)
        assertEquals(2, result["completedWaits"])
    }

    private fun <I, O> run(
        handler: AsyncDurableHandler<I, O>,
        input: I,
        inputType: Class<I>,
        outputType: Class<O>,
    ): O {
        val runner = LocalDurableTestRunner.create(inputType, handler)
        val result = runner.runUntilComplete(input)
        val error = result.error.orElse(null)
        assertEquals(
            ExecutionStatus.SUCCEEDED,
            result.status,
            error?.let { "${it.errorType()}: ${it.errorMessage()}" },
        )
        return result.getResult(outputType)
    }
}
