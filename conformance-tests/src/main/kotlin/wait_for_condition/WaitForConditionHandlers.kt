// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package wait_for_condition

import java.time.Duration
import kotlin.time.Duration.Companion.seconds
import software.amazon.lambda.durable.kotlin.ConditionWaitPolicy
import software.amazon.lambda.durable.kotlin.KotlinDurableContext
import software.amazon.lambda.durable.kotlin.KotlinDurableHandler
import software.amazon.lambda.durable.model.WaitForConditionResult
import software.amazon.lambda.durable.serde.SerDes
import software.amazon.lambda.durable.TypeToken

public class WaitForConditionBasic : KotlinDurableHandler<Int, Int>() {
    override suspend fun handle(input: Int, context: KotlinDurableContext): Int =
        context.waitForCondition(
            name = null,
            initialState = 0,
        ) {
            val next = state + 1
            if (next >= input) WaitForConditionResult.stopPolling(next)
            else WaitForConditionResult.continuePolling(next)
        }
}

public class WaitForConditionImmediate : KotlinDurableHandler<Int, Int>() {
    override suspend fun handle(input: Int, context: KotlinDurableContext): Int =
        context.waitForCondition(
            name = null,
            initialState = input,
        ) {
            if (state >= 5) WaitForConditionResult.stopPolling(state)
            else WaitForConditionResult.continuePolling(state)
        }
}

public class WaitForConditionNamed : KotlinDurableHandler<Int, Int>() {
    override suspend fun handle(input: Int, context: KotlinDurableContext): Int =
        context.waitForCondition(
            name = "poll-status",
            initialState = 0,
        ) {
            val next = state + 1
            if (next >= input) WaitForConditionResult.stopPolling(next)
            else WaitForConditionResult.continuePolling(next)
        }
}

public class WaitForConditionInitialState : KotlinDurableHandler<Int, Int>() {
    override suspend fun handle(input: Int, context: KotlinDurableContext): Int =
        context.waitForCondition(
            name = null,
            initialState = 5,
        ) {
            val next = state + 1
            if (next >= input) WaitForConditionResult.stopPolling(next)
            else WaitForConditionResult.continuePolling(next)
        }
}

public class WaitForConditionFixedDelay : KotlinDurableHandler<Int, Int>() {
    override suspend fun handle(input: Int, context: KotlinDurableContext): Int =
        context.waitForCondition(
            name = null,
            initialState = 0,
            wait = ConditionWaitPolicy.fixed(maxAttempts = 60, delay = 2.seconds),
        ) {
            val next = state + 1
            if (next >= input) WaitForConditionResult.stopPolling(next)
            else WaitForConditionResult.continuePolling(next)
        }
}

public class WaitForConditionMaxAttempts : KotlinDurableHandler<Any?, Int>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): Int =
        context.waitForCondition(
            name = null,
            initialState = 0,
            wait = ConditionWaitPolicy.fixed(maxAttempts = 3, delay = 1.seconds),
        ) {
            WaitForConditionResult.continuePolling(state + 1)
        }
}

public class WaitForConditionCheckThrows : KotlinDurableHandler<Any?, Any?>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): Any? =
        context.waitForCondition<Any?>(null) {
            error("Check function error")
        }
}

public class WaitForConditionCheckThrowsCaught : KotlinDurableHandler<Any?, String>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String =
        try {
            context.waitForCondition<Any?>(null) {
                error("Check function error")
            }
            "should_not_reach_here"
        } catch (_: RuntimeException) {
            "recovered"
        }
}

public class WaitForConditionComplexObject : KotlinDurableHandler<Any?, Map<String, Any>>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): Map<String, Any> {
        val initial = mapOf<String, Any>("status" to "PENDING", "attempts" to 0)
        return context.waitForCondition<Map<String, Any>>(
            name = null,
            initialState = initial,
        ) {
            val attempts = (state["attempts"] as Number).toInt() + 1
            val next =
                mapOf<String, Any>(
                    "status" to if (attempts >= 2) "DONE" else "PENDING",
                    "attempts" to attempts,
                )
            if (attempts >= 2) WaitForConditionResult.stopPolling(next)
            else WaitForConditionResult.continuePolling(next)
        }
    }
}

public class WaitForConditionNullResult : KotlinDurableHandler<Any?, Any?>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): Any? =
        context.waitForCondition<Any?>(null) {
            WaitForConditionResult.stopPolling(null)
        }
}

public class WaitForConditionCustomSerdes : KotlinDurableHandler<Any?, String>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String =
        context.waitForCondition(
            name = null,
            initialState = "",
            serDes = CustomSerDes,
        ) {
            val next = state + "x"
            if (next.length >= 2) WaitForConditionResult.stopPolling(next)
            else WaitForConditionResult.continuePolling(next)
        }

    private object CustomSerDes : SerDes {
        override fun serialize(value: Any?): String? = value?.let { "ENC:$it" }

        @Suppress("UNCHECKED_CAST")
        override fun <T : Any?> deserialize(data: String?, typeToken: TypeToken<T>): T =
            (data?.removePrefix("ENC:")) as T
    }
}

public class WaitForConditionThenStep : KotlinDurableHandler<Int, Int>() {
    override suspend fun handle(input: Int, context: KotlinDurableContext): Int {
        val result =
            context.waitForCondition(
                name = null,
                initialState = 0,
            ) {
                val next = state + 1
                if (next >= input) WaitForConditionResult.stopPolling(next)
                else WaitForConditionResult.continuePolling(next)
            }
        return context.step(null) { result * 10 }
    }
}

public class WaitForConditionMultipleSequential : KotlinDurableHandler<Any?, Int>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): Int {
        val first =
            context.waitForCondition(
                name = null,
                initialState = 0,
            ) {
                val next = state + 1
                if (next >= 2) WaitForConditionResult.stopPolling(next)
                else WaitForConditionResult.continuePolling(next)
            }
        return context.waitForCondition(
            name = null,
            initialState = first,
        ) {
            val next = state + 1
            if (next >= 4) WaitForConditionResult.stopPolling(next)
            else WaitForConditionResult.continuePolling(next)
        }
    }
}
