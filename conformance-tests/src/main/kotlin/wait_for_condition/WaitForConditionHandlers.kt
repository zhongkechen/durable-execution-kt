package wait_for_condition

import io.github.zhongkechen.durable.ConditionDecision
import io.github.zhongkechen.durable.ConditionFailureException
import io.github.zhongkechen.durable.ConditionOptions
import io.github.zhongkechen.durable.DurableContext
import io.github.zhongkechen.durable.DurableHandler
import io.github.zhongkechen.durable.DurableRuntimeConfig
import io.github.zhongkechen.durable.Serde
import io.github.zhongkechen.durable.TypeRef
import io.github.zhongkechen.durable.step
import io.github.zhongkechen.durable.typeRef
import kotlin.time.Duration.Companion.seconds

public class WaitForConditionBasic(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Int, Int>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Int, context: DurableContext): Int =
        poll(context, input, initial = 0)
}

public class WaitForConditionImmediate(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Int, Int>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Int, context: DurableContext): Int =
        context.waitForCondition(
            name = null,
            type = typeRef(),
            options = integerOptions(input),
        ) {
            if (state >= 5) ConditionDecision.Complete(state)
            else ConditionDecision.Continue(state)
        }
}

public class WaitForConditionNamed(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Int, Int>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Int, context: DurableContext): Int =
        poll(context, input, initial = 0, name = "poll-status")
}

public class WaitForConditionInitialState(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Int, Int>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Int, context: DurableContext): Int =
        poll(context, input, initial = 5)
}

public class WaitForConditionFixedDelay(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Int, Int>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Int, context: DurableContext): Int =
        context.waitForCondition(
            name = null,
            type = typeRef(),
            options =
                ConditionOptions(
                    initialState = 0,
                    maximumAttempts = 60,
                    delay = { _, _ -> 2.seconds },
                ),
        ) {
            val next = state + 1
            if (next >= input) ConditionDecision.Complete(next)
            else ConditionDecision.Continue(next)
        }
}

public class WaitForConditionMaxAttempts(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, Int>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): Int =
        context.waitForCondition(
            name = null,
            type = typeRef(),
            options =
                ConditionOptions(
                    initialState = 0,
                    maximumAttempts = 3,
                    delay = { _, _ -> 1.seconds },
                ),
        ) {
            ConditionDecision.Continue(state + 1)
        }
}

public class WaitForConditionCheckThrows(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, Any?>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): Any? =
        context.waitForCondition(
            name = null,
            type = typeRef(),
            options = ConditionOptions<Any?>(delay = { _, _ -> 1.seconds }),
        ) {
            error("Check function error")
        }
}

public class WaitForConditionCheckThrowsCaught(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): String =
        try {
            context.waitForCondition(
                name = null,
                type = typeRef(),
                options = ConditionOptions<Any?>(delay = { _, _ -> 1.seconds }),
            ) {
                error("Check function error")
            }
            "should_not_reach_here"
        } catch (_: ConditionFailureException) {
            "recovered"
        }
}

public class WaitForConditionComplexObject(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, Map<String, Any>>(typeRef(), typeRef(), config) {
    override suspend fun handle(
        input: Any?,
        context: DurableContext,
    ): Map<String, Any> =
        context.waitForCondition(
            name = null,
            type = typeRef(),
            options =
                ConditionOptions(
                    initialState = mapOf<String, Any>("status" to "PENDING", "attempts" to 0),
                    delay = { _, _ -> 1.seconds },
                ),
        ) {
            val attempts = (state["attempts"] as Number).toInt() + 1
            val next =
                mapOf<String, Any>(
                    "status" to if (attempts >= 2) "DONE" else "PENDING",
                    "attempts" to attempts,
                )
            if (attempts >= 2) ConditionDecision.Complete(next)
            else ConditionDecision.Continue(next)
        }
}

public class WaitForConditionNullResult(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, Any?>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): Any? =
        context.waitForCondition(
            name = null,
            type = typeRef(),
            options = ConditionOptions<Any?>(delay = { _, _ -> 1.seconds }),
        ) {
            ConditionDecision.Complete(null)
        }
}

public class WaitForConditionCustomSerdes(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): String =
        context.waitForCondition(
            name = null,
            type = typeRef(),
            options =
                ConditionOptions(
                    initialState = "",
                    delay = { _, _ -> 1.seconds },
                    serde = CustomSerde,
                ),
        ) {
            val next = state + "x"
            if (next.length >= 2) ConditionDecision.Complete(next)
            else ConditionDecision.Continue(next)
        }

    private object CustomSerde : Serde {
        override fun encode(value: Any?): String = "ENC:$value"

        @Suppress("UNCHECKED_CAST")
        override fun <T> decode(payload: String, type: TypeRef<T>): T =
            payload.removePrefix("ENC:") as T
    }
}

public class WaitForConditionThenStep(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Int, Int>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Int, context: DurableContext): Int {
        val result = poll(context, input, initial = 0)
        return context.step(null) { result * 10 }
    }
}

public class WaitForConditionMultipleSequential(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, Int>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): Int {
        val first = poll(context, target = 2, initial = 0)
        return poll(context, target = 4, initial = first)
    }
}

private suspend fun poll(
    context: DurableContext,
    target: Int,
    initial: Int,
    name: String? = null,
): Int =
    context.waitForCondition(
        name = name,
        type = typeRef(),
        options = integerOptions(initial),
    ) {
        val next = state + 1
        if (next >= target) ConditionDecision.Complete(next)
        else ConditionDecision.Continue(next)
    }

private fun integerOptions(initial: Int): ConditionOptions<Int> =
    ConditionOptions(
        initialState = initial,
        delay = { _, _ -> 1.seconds },
    )
