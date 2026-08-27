package callback

import io.github.zhongkechen.durable.CallbackFailureException
import io.github.zhongkechen.durable.CallbackOptions
import io.github.zhongkechen.durable.DurableContext
import io.github.zhongkechen.durable.DurableHandler
import io.github.zhongkechen.durable.DurableRuntimeConfig
import io.github.zhongkechen.durable.Serde
import io.github.zhongkechen.durable.TypeRef
import io.github.zhongkechen.durable.step
import io.github.zhongkechen.durable.typeRef
import java.time.Instant
import kotlin.time.Duration.Companion.seconds

public class CallbackBasic(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String, context: DurableContext): String =
        context.callback(input, typeRef<String>()).await()
}

public class CallbackWithName(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): String =
        context.callback("approval", typeRef<String>()).await()
}

public class CallbackTimeout(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String, context: DurableContext): String =
        context.callback(
            name = input,
            type = typeRef<String>(),
            options = CallbackOptions(timeout = 5.seconds),
        ).await()
}

public class CallbackHeartbeatTimeout(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String, context: DurableContext): String =
        context.callback(
            name = input,
            type = typeRef<String>(),
            options = CallbackOptions(heartbeatTimeout = 5.seconds),
        ).await()
}

public class CallbackHeartbeatSuccess(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String, context: DurableContext): String =
        context.callback(
            name = input,
            type = typeRef<String>(),
            options = CallbackOptions(heartbeatTimeout = 10.seconds),
        ).await()
}

public class CallbackFailure(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String, context: DurableContext): String =
        context.callback(input, typeRef<String>()).await()
}

public class CallbackStepFailure(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String, context: DurableContext): String {
        val callback = context.callback(input, typeRef<String>())
        context.step<String>("notify-external") { "notified" }
        return callback.await()
    }
}

public class CallbackStepTimeout(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String, context: DurableContext): String {
        val callback =
            context.callback(
                name = input,
                type = typeRef<String>(),
                options = CallbackOptions(timeout = 5.seconds),
            )
        context.step<String>("notify-external") { "notified" }
        return callback.await()
    }
}

public class CallbackWaitSuccess(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String, context: DurableContext): String {
        val callback = context.callback(input, typeRef<String>())
        context.wait(5.seconds, "delay")
        return callback.await()
    }
}

public class CallbackWaitFailure(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String, context: DurableContext): String {
        val callback = context.callback(input, typeRef<String>())
        context.wait(5.seconds, "delay")
        return callback.await()
    }
}

public class CallbackWaitTimeout(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String, context: DurableContext): String {
        val callback =
            context.callback(
                name = input,
                type = typeRef<String>(),
                options = CallbackOptions(timeout = 3.seconds),
            )
        context.wait(6.seconds, "delay")
        return callback.await()
    }
}

public class CallbackReplayWithWait(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String, context: DurableContext): String {
        val result = context.callback(input, typeRef<String>()).await()
        context.wait(2.seconds, "after-cb")
        return result
    }
}

public class CallbackReplayFailure(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String, context: DurableContext): String {
        val outcome =
            try {
                context.callback(input, typeRef<String>()).await()
            } catch (error: CallbackFailureException) {
                "caught_failure:${error.message}"
            } catch (error: RuntimeException) {
                "caught_other:${error::class.simpleName}:${error.message}"
            }
        context.wait(2.seconds, "after-cb")
        return outcome
    }
}

public class CallbackReplayTimeout(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String, context: DurableContext): String {
        val outcome =
            try {
                context.callback(
                    name = input,
                    type = typeRef<String>(),
                    options = CallbackOptions(timeout = 3.seconds),
                ).await()
            } catch (error: CallbackFailureException) {
                "caught_timeout:${error.message}"
            } catch (error: RuntimeException) {
                "caught_other:${error::class.simpleName}:${error.message}"
            }
        context.wait(2.seconds, "after-cb")
        return outcome
    }
}

public class CallbackSerdesHappy(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, Map<String, Any?>>(typeRef(), typeRef(), config) {
    override suspend fun handle(
        input: String,
        context: DurableContext,
    ): Map<String, Any?> {
        val result =
            context.callback(
                name = input,
                type = typeRef<CustomData>(),
                options = CallbackOptions(serde = CustomDataSerde),
            ).await()
        return mapOf(
            "received" to
                mapOf(
                    "id" to result.id,
                    "message" to result.message,
                    "timestamp" to result.timestamp.epochSecond,
                ),
        )
    }

    public data class CustomData(
        val id: String = "",
        val message: String = "",
        val timestamp: Instant = Instant.EPOCH,
    )

    private object CustomDataSerde : Serde {
        override fun encode(value: Any?): String {
            val data = value as CustomData
            return """{"id":"${data.id}","message":"${data.message}","timestamp":"${data.timestamp}"}"""
        }

        @Suppress("UNCHECKED_CAST")
        override fun <T> decode(payload: String, type: TypeRef<T>): T =
            CustomData(
                id = extract(payload, "id"),
                message = extract(payload, "message"),
                timestamp = Instant.parse(extract(payload, "timestamp")),
            ) as T

        private fun extract(
            value: String,
            field: String,
        ): String {
            val key = "\"$field\":"
            var start = value.indexOf(key) + key.length
            while (start < value.length && value[start] == ' ') start += 1
            if (start < value.length && value[start] == '"') start += 1
            val end = value.indexOf('"', start)
            return if (end < 0) value.substring(start) else value.substring(start, end)
        }
    }
}

public class CallbackSerdesNumeric(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, Map<String, Int>>(typeRef(), typeRef(), config) {
    override suspend fun handle(
        input: String,
        context: DurableContext,
    ): Map<String, Int> {
        val value =
            context.callback(
                name = input,
                type = typeRef<Int>(),
                options = CallbackOptions(serde = NumericSerde),
            ).await()
        return mapOf("count" to value, "doubled" to value * 2)
    }

    private object NumericSerde : Serde {
        override fun encode(value: Any?): String = value.toString()

        @Suppress("UNCHECKED_CAST")
        override fun <T> decode(payload: String, type: TypeRef<T>): T = payload.trim().toInt() as T
    }
}

public class CallbackTwoSequential(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<List<String>, Map<String, String>>(typeRef(), typeRef(), config) {
    override suspend fun handle(
        input: List<String>,
        context: DurableContext,
    ): Map<String, String> {
        val first = context.callback(input[0], typeRef<String>()).await()
        val second = context.callback(input[1], typeRef<String>()).await()
        return mapOf("a" to first, "b" to second)
    }
}

public class CallbackTwoParallel(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<List<String>, Map<String, String>>(typeRef(), typeRef(), config) {
    override suspend fun handle(
        input: List<String>,
        context: DurableContext,
    ): Map<String, String> {
        val first = context.callback(input[0], typeRef<String>())
        val second = context.callback(input[1], typeRef<String>())
        return mapOf("a" to first.await(), "b" to second.await())
    }
}

public class CallbackTwoParallelReverse(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<List<String>, Map<String, String>>(typeRef(), typeRef(), config) {
    override suspend fun handle(
        input: List<String>,
        context: DurableContext,
    ): Map<String, String> {
        val first = context.callback(input[0], typeRef<String>())
        val second = context.callback(input[1], typeRef<String>())
        val secondResult = second.await()
        val firstResult = first.await()
        return mapOf("a" to firstResult, "b" to secondResult)
    }
}
