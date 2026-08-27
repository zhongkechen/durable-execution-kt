// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package invoke

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import java.time.Duration
import kotlinx.coroutines.delay
import software.amazon.lambda.durable.TypeToken
import software.amazon.lambda.durable.config.InvokeConfig
import software.amazon.lambda.durable.exception.InvokeException
import software.amazon.lambda.durable.kotlin.KotlinDurableContext
import software.amazon.lambda.durable.kotlin.KotlinDurableHandler
import software.amazon.lambda.durable.serde.SerDes

public class InvokeBasic : KotlinDurableHandler<Any?, Any?>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): Any? =
        context.invoke("invoke-basic", target(), input)
}

public class InvokeWithName : KotlinDurableHandler<Map<String, Any?>, Any?>() {
    override suspend fun handle(input: Map<String, Any?>, context: KotlinDurableContext): Any? =
        context.invoke(requireNotNull(input["name"] as String?), target(), input["payload"])
}

public class InvokeComplexObject : KotlinDurableHandler<Any?, Any?>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): Any? =
        context.invoke("invoke-complex", target(), input)
}

public class InvokeNullResult : KotlinDurableHandler<Any?, Any?>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): Any? =
        context.invoke("invoke-null", target(), null)
}

public class InvokeTargetFails : KotlinDurableHandler<Any?, String>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String =
        context.invoke("invoke-failing", target(), null)
}

public class InvokeTargetFailsCaught : KotlinDurableHandler<Any?, String>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String {
        try {
            context.invoke<String, Any?>("invoke-failing", target(), null)
        } catch (_: InvokeException) {
            // Continue with the fallback.
        }
        return "fallback"
    }
}

public class InvokeLargePayload : KotlinDurableHandler<Any?, String>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String =
        context.invoke("invoke-large", target(), "x".repeat(200_000))
}

public class InvokeTimeout : KotlinDurableHandler<Any?, String>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String =
        context.invoke("invoke-slow", target(), null)
}

public class InvokeTimeoutCaught : KotlinDurableHandler<Any?, String>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String {
        try {
            context.invoke<String, Any?>("invoke-slow", target(), null)
        } catch (_: InvokeException) {
            // Continue with the fallback.
        }
        return "fallback"
    }
}

public class InvokeWithTenantId : KotlinDurableHandler<Map<String, String>, String>() {
    override suspend fun handle(input: Map<String, String>, context: KotlinDurableContext): String =
        context.invoke(
            "invoke-tenant",
            target(),
            input["payload"],
            InvokeConfig.builder().tenantId(input["tenantId"]).build(),
        )
}

public class InvokeReplaySkips : KotlinDurableHandler<Any?, String>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String {
        val result = context.invoke<String, Any?>("invoke-target", target(), null)
        context.wait(Duration.ofSeconds(1))
        return result
    }
}

public class InvokeReplayRethrows : KotlinDurableHandler<Any?, String>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String {
        try {
            context.invoke<String, Any?>("invoke-failing", target(), null)
        } catch (_: InvokeException) {
            // The checkpointed error is expected on initial execution and replay.
        }
        context.wait(Duration.ofSeconds(1))
        return "done"
    }
}

public class StepThenInvoke : KotlinDurableHandler<Any?, String>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String {
        val stepResult = context.step<String>("compute") { "step-data" }
        return context.invoke("invoke-with-step", target(), stepResult)
    }
}

public class InvokeThenStep : KotlinDurableHandler<Any?, String>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String {
        val invokeResult = context.invoke<String, Any?>("invoke-target", target(), null)
        return context.step("process") { "processed: $invokeResult" }
    }
}

public class InvokeInChildContext : KotlinDurableHandler<Any?, String>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String =
        context.childContext("child-invoke") {
            invoke<String, Any?>("invoke-in-child", target(), null)
        }
}

public class InvokeMultipleSequential : KotlinDurableHandler<Map<String, Any?>, String>() {
    override suspend fun handle(input: Map<String, Any?>, context: KotlinDurableContext): String {
        val first = context.invoke<String, Any?>("invoke-first", requireEnv("TARGET_FUNCTION_NAME_1"), null)
        val second = context.invoke<String, Any?>("invoke-second", requireEnv("TARGET_FUNCTION_NAME_2"), null)
        return "$first,$second"
    }
}

public class InvokeCustomPayloadSerdes : KotlinDurableHandler<Any?, String>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String =
        context.invoke(
            "invoke-custom-payload",
            target(),
            "hello",
            InvokeConfig.builder().payloadSerDes(UppercasePayloadSerDes).build(),
        )

    private object UppercasePayloadSerDes : SerDes {
        override fun serialize(value: Any?): String? = value?.toString()?.uppercase()?.let { "\"$it\"" }

        @Suppress("UNCHECKED_CAST")
        override fun <T : Any?> deserialize(data: String?, typeToken: TypeToken<T>): T = data as T
    }
}

public class InvokeCustomResultSerdes : KotlinDurableHandler<Any?, String>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String =
        context.invoke(
            "invoke-custom-result",
            target(),
            input,
            InvokeConfig.builder().serDes(UppercaseResultSerDes).build(),
        )

    private object UppercaseResultSerDes : SerDes {
        override fun serialize(value: Any?): String? = value?.toString()?.let { "\"$it\"" }

        @Suppress("UNCHECKED_CAST")
        override fun <T : Any?> deserialize(data: String?, typeToken: TypeToken<T>): T =
            data?.uppercase() as T
    }
}

public class TargetEcho : KotlinDurableHandler<Any?, Any?>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): Any? {
        context.wait(Duration.ofSeconds(1))
        return input
    }
}

public class TargetError : KotlinDurableHandler<Any?, String>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String {
        context.wait(Duration.ofSeconds(1))
        error("Target function error")
    }
}

public class TargetSlow : KotlinDurableHandler<Any?, String>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String {
        delay(60_000)
        return "should-not-reach"
    }
}

public class TargetNonDurable : RequestHandler<Any?, String> {
    override fun handleRequest(input: Any?, context: Context): String = "non-durable-result"
}

private fun target(): String = requireEnv("TARGET_FUNCTION_NAME")

private fun requireEnv(name: String): String =
    requireNotNull(System.getenv(name)) { "Missing environment variable $name" }
