package invoke

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import io.github.zhongkechen.durable.DurableContext
import io.github.zhongkechen.durable.DurableHandler
import io.github.zhongkechen.durable.DurableRuntimeConfig
import io.github.zhongkechen.durable.InvokeFailureException
import io.github.zhongkechen.durable.InvokeOptions
import io.github.zhongkechen.durable.Serde
import io.github.zhongkechen.durable.TypeRef
import io.github.zhongkechen.durable.child
import io.github.zhongkechen.durable.step
import io.github.zhongkechen.durable.typeRef
import kotlinx.coroutines.delay
import kotlin.time.Duration.Companion.seconds

public class InvokeBasic(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, Any?>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): Any? =
        context.invoke("invoke-basic", target(), input, typeRef())
}

public class InvokeWithName(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Map<String, Any?>, Any?>(typeRef(), typeRef(), config) {
    override suspend fun handle(
        input: Map<String, Any?>,
        context: DurableContext,
    ): Any? =
        context.invoke(
            requireNotNull(input["name"] as String?),
            target(),
            input["payload"],
            typeRef(),
        )
}

public class InvokeComplexObject(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, Any?>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): Any? =
        context.invoke("invoke-complex", target(), input, typeRef())
}

public class InvokeNullResult(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, Any?>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): Any? =
        context.invoke("invoke-null", target(), null, typeRef())
}

public class InvokeTargetFails(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): String =
        context.invoke("invoke-failing", target(), null, typeRef())
}

public class InvokeTargetFailsCaught(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): String {
        try {
            context.invoke("invoke-failing", target(), null, typeRef<String>())
        } catch (_: InvokeFailureException) {
            // The fallback is the durable result for this handler.
        }
        return "fallback"
    }
}

public class InvokeLargePayload(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): String =
        context.invoke("invoke-large", target(), "x".repeat(200_000), typeRef())
}

public class InvokeTimeout(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): String =
        context.invoke("invoke-slow", target(), null, typeRef())
}

public class InvokeTimeoutCaught(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): String {
        try {
            context.invoke("invoke-slow", target(), null, typeRef<String>())
        } catch (_: InvokeFailureException) {
            // Continue after the checkpointed timeout.
        }
        return "fallback"
    }
}

public class InvokeWithTenantId(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Map<String, String>, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(
        input: Map<String, String>,
        context: DurableContext,
    ): String =
        context.invoke(
            name = "invoke-tenant",
            functionName = target(),
            input = input["payload"],
            outputType = typeRef(),
            options = InvokeOptions(tenantId = input["tenantId"]),
        )
}

public class InvokeReplaySkips(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): String {
        val result = context.invoke("invoke-target", target(), null, typeRef<String>())
        context.wait(1.seconds)
        return result
    }
}

public class InvokeReplayRethrows(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): String {
        try {
            context.invoke("invoke-failing", target(), null, typeRef<String>())
        } catch (_: InvokeFailureException) {
            // Initial execution and replay both observe the same checkpointed error.
        }
        context.wait(1.seconds)
        return "done"
    }
}

public class StepThenInvoke(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): String {
        val stepResult = context.step<String>("compute") { "step-data" }
        return context.invoke("invoke-with-step", target(), stepResult, typeRef())
    }
}

public class InvokeThenStep(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): String {
        val invokeResult = context.invoke("invoke-target", target(), null, typeRef<String>())
        return context.step("process") { "processed: $invokeResult" }
    }
}

public class InvokeInChildContext(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): String =
        context.child("child-invoke") {
            invoke("invoke-in-child", target(), null, typeRef<String>())
        }
}

public class InvokeMultipleSequential(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Map<String, Any?>, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(
        input: Map<String, Any?>,
        context: DurableContext,
    ): String {
        val first =
            context.invoke(
                "invoke-first",
                requireEnv("TARGET_FUNCTION_NAME_1"),
                null,
                typeRef<String>(),
            )
        val second =
            context.invoke(
                "invoke-second",
                requireEnv("TARGET_FUNCTION_NAME_2"),
                null,
                typeRef<String>(),
            )
        return "$first,$second"
    }
}

public class InvokeCustomPayloadSerdes(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): String =
        context.invoke(
            name = "invoke-custom-payload",
            functionName = target(),
            input = "hello",
            outputType = typeRef(),
            options = InvokeOptions(payloadSerde = UppercasePayloadSerde),
        )

    private object UppercasePayloadSerde : Serde {
        override fun encode(value: Any?): String = "\"${value.toString().uppercase()}\""

        @Suppress("UNCHECKED_CAST")
        override fun <T> decode(payload: String, type: TypeRef<T>): T = payload as T
    }
}

public class InvokeCustomResultSerdes(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): String =
        context.invoke(
            name = "invoke-custom-result",
            functionName = target(),
            input = input,
            outputType = typeRef(),
            options = InvokeOptions(resultSerde = UppercaseResultSerde),
        )

    private object UppercaseResultSerde : Serde {
        override fun encode(value: Any?): String = "\"${value.toString()}\""

        @Suppress("UNCHECKED_CAST")
        override fun <T> decode(payload: String, type: TypeRef<T>): T =
            payload.trim('"').uppercase() as T
    }
}

public class TargetEcho(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, Any?>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): Any? {
        context.wait(1.seconds)
        return input
    }
}

public class TargetError(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): String {
        context.wait(1.seconds)
        error("Target function error")
    }
}

public class TargetSlow(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): String {
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
