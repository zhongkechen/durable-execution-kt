// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package callback

import java.time.Duration
import java.time.Instant
import software.amazon.lambda.durable.TypeToken
import software.amazon.lambda.durable.config.CallbackConfig
import software.amazon.lambda.durable.exception.CallbackException
import software.amazon.lambda.durable.kotlin.KotlinDurableContext
import software.amazon.lambda.durable.kotlin.KotlinDurableHandler
import software.amazon.lambda.durable.serde.SerDes

public class CallbackBasic : KotlinDurableHandler<String, String>() {
    override suspend fun handle(input: String, context: KotlinDurableContext): String =
        context.callback<String>(input).await()
}

public class CallbackWithName : KotlinDurableHandler<Any?, String>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String =
        context.callback<String>("approval").await()
}

public class CallbackTimeout : KotlinDurableHandler<String, String>() {
    override suspend fun handle(input: String, context: KotlinDurableContext): String =
        context.callback<String>(
            input,
            CallbackConfig.builder().timeout(Duration.ofSeconds(5)).build(),
        ).await()
}

public class CallbackHeartbeatTimeout : KotlinDurableHandler<String, String>() {
    override suspend fun handle(input: String, context: KotlinDurableContext): String =
        context.callback<String>(
            input,
            CallbackConfig.builder().heartbeatTimeout(Duration.ofSeconds(5)).build(),
        ).await()
}

public class CallbackHeartbeatSuccess : KotlinDurableHandler<String, String>() {
    override suspend fun handle(input: String, context: KotlinDurableContext): String =
        context.callback<String>(
            input,
            CallbackConfig.builder().heartbeatTimeout(Duration.ofSeconds(10)).build(),
        ).await()
}

public class CallbackFailure : KotlinDurableHandler<String, String>() {
    override suspend fun handle(input: String, context: KotlinDurableContext): String =
        context.callback<String>(input).await()
}

public class CallbackStepFailure : KotlinDurableHandler<String, String>() {
    override suspend fun handle(input: String, context: KotlinDurableContext): String {
        val callback = context.callback<String>(input)
        context.step<String>("notify-external") { "notified" }
        return callback.await()
    }
}

public class CallbackStepTimeout : KotlinDurableHandler<String, String>() {
    override suspend fun handle(input: String, context: KotlinDurableContext): String {
        val callback =
            context.callback<String>(
                input,
                CallbackConfig.builder().timeout(Duration.ofSeconds(5)).build(),
            )
        context.step<String>("notify-external") { "notified" }
        return callback.await()
    }
}

public class CallbackWaitSuccess : KotlinDurableHandler<String, String>() {
    override suspend fun handle(input: String, context: KotlinDurableContext): String {
        val callback = context.callback<String>(input)
        context.wait(Duration.ofSeconds(5), "delay")
        return callback.await()
    }
}

public class CallbackWaitFailure : KotlinDurableHandler<String, String>() {
    override suspend fun handle(input: String, context: KotlinDurableContext): String {
        val callback = context.callback<String>(input)
        context.wait(Duration.ofSeconds(5), "delay")
        return callback.await()
    }
}

public class CallbackWaitTimeout : KotlinDurableHandler<String, String>() {
    override suspend fun handle(input: String, context: KotlinDurableContext): String {
        val callback =
            context.callback<String>(
                input,
                CallbackConfig.builder().timeout(Duration.ofSeconds(3)).build(),
            )
        context.wait(Duration.ofSeconds(6), "delay")
        return callback.await()
    }
}

public class CallbackReplayWithWait : KotlinDurableHandler<String, String>() {
    override suspend fun handle(input: String, context: KotlinDurableContext): String {
        val result = context.callback<String>(input).await()
        context.wait(Duration.ofSeconds(2), "after-cb")
        return result
    }
}

public class CallbackReplayFailure : KotlinDurableHandler<String, String>() {
    override suspend fun handle(input: String, context: KotlinDurableContext): String {
        val outcome =
            try {
                context.callback<String>(input).await()
            } catch (error: CallbackException) {
                "caught_failure:${error.message}"
            } catch (error: RuntimeException) {
                "caught_other:${error.javaClass.simpleName}:${error.message}"
            }
        context.wait(Duration.ofSeconds(2), "after-cb")
        return outcome
    }
}

public class CallbackReplayTimeout : KotlinDurableHandler<String, String>() {
    override suspend fun handle(input: String, context: KotlinDurableContext): String {
        val outcome =
            try {
                context.callback<String>(
                    input,
                    CallbackConfig.builder().timeout(Duration.ofSeconds(3)).build(),
                ).await()
            } catch (error: CallbackException) {
                "caught_timeout:${error.message}"
            } catch (error: RuntimeException) {
                "caught_other:${error.javaClass.simpleName}:${error.message}"
            }
        context.wait(Duration.ofSeconds(2), "after-cb")
        return outcome
    }
}

public class CallbackSerdesHappy : KotlinDurableHandler<String, Map<String, Any?>>() {
    override suspend fun handle(input: String, context: KotlinDurableContext): Map<String, Any?> {
        val result =
            context.callback(
                input,
                TypeToken.get(CustomData::class.java),
                CallbackConfig.builder().serDes(CustomDataSerDes).build(),
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

    public class CustomData {
        public var id: String = ""
        public var message: String = ""
        public var timestamp: Instant = Instant.EPOCH
    }

    private object CustomDataSerDes : SerDes {
        override fun serialize(value: Any?): String? {
            val data = value as CustomData? ?: return null
            return """{"id":"${data.id}","message":"${data.message}","timestamp":"${data.timestamp}"}"""
        }

        @Suppress("UNCHECKED_CAST")
        override fun <T : Any?> deserialize(data: String?, typeToken: TypeToken<T>): T {
            if (data == null) return null as T
            return CustomData()
                .apply {
                    id = extract(data, "id")
                    message = extract(data, "message")
                    timestamp = Instant.parse(extract(data, "timestamp"))
                } as T
        }

        private fun extract(value: String, field: String): String {
            val key = "\"$field\":"
            var start = value.indexOf(key) + key.length
            while (start < value.length && value[start] == ' ') start++
            if (start < value.length && value[start] == '"') start++
            val end = value.indexOf('"', start)
            return if (end < 0) value.substring(start) else value.substring(start, end)
        }
    }
}

public class CallbackSerdesNumeric : KotlinDurableHandler<String, Map<String, Any>>() {
    override suspend fun handle(input: String, context: KotlinDurableContext): Map<String, Any> {
        val value =
            context.callback(
                input,
                TypeToken.get(Int::class.javaObjectType),
                CallbackConfig.builder().serDes(NumericSerDes).build(),
            ).await()
        return mapOf("count" to value, "doubled" to value * 2)
    }

    private object NumericSerDes : SerDes {
        override fun serialize(value: Any?): String? = (value as Int?)?.toString()

        @Suppress("UNCHECKED_CAST")
        override fun <T : Any?> deserialize(data: String?, typeToken: TypeToken<T>): T =
            data?.trim()?.toInt() as T
    }
}

public class CallbackTwoSequential : KotlinDurableHandler<List<String>, Map<String, String>>() {
    override suspend fun handle(input: List<String>, context: KotlinDurableContext): Map<String, String> {
        val first = context.callback<String>(input[0]).await()
        val second = context.callback<String>(input[1]).await()
        return mapOf("a" to first, "b" to second)
    }
}

public class CallbackTwoParallel : KotlinDurableHandler<List<String>, Map<String, String>>() {
    override suspend fun handle(input: List<String>, context: KotlinDurableContext): Map<String, String> {
        val first = context.callback<String>(input[0])
        val second = context.callback<String>(input[1])
        return mapOf("a" to first.await(), "b" to second.await())
    }
}

public class CallbackTwoParallelReverse : KotlinDurableHandler<List<String>, Map<String, String>>() {
    override suspend fun handle(input: List<String>, context: KotlinDurableContext): Map<String, String> {
        val first = context.callback<String>(input[0])
        val second = context.callback<String>(input[1])
        val secondResult = second.await()
        val firstResult = first.await()
        return mapOf("a" to firstResult, "b" to secondResult)
    }
}
