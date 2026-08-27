package invoke

import io.github.zhongkechen.durable.*

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import kotlinx.coroutines.delay
import kotlin.time.Duration.Companion.seconds

public class InvokeBasic(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, Any?>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?): Any? =
        invoke("invoke-basic", target(), input, typeRef())
}

public class InvokeWithName(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Map<String, Any?>, Any?>(typeRef(), typeRef(), config) {
    override suspend fun handle(
        input: Map<String, Any?>,
    ): Any? =
        invoke(
            requireNotNull(input["name"] as String?),
            target(),
            input["payload"],
            typeRef(),
        )
}

public class InvokeComplexObject(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, Any?>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?): Any? =
        invoke("invoke-complex", target(), input, typeRef())
}

public class InvokeNullResult(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, Any?>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?): Any? =
        invoke("invoke-null", target(), null, typeRef())
}

public class InvokeTargetFails(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?): String =
        invoke("invoke-failing", target(), null, typeRef())
}

public class InvokeTargetFailsCaught(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?): String {
        try {
            invoke("invoke-failing", target(), null, typeRef<String>())
        } catch (_: InvokeFailureException) {
            // The fallback is the durable result for this handler.
        }
        return "fallback"
    }
}

public class InvokeLargePayload(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?): String =
        invoke("invoke-large", target(), "x".repeat(200_000), typeRef())
}

public class InvokeTimeout(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?): String =
        invoke("invoke-slow", target(), null, typeRef())
}

public class InvokeTimeoutCaught(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?): String {
        try {
            invoke("invoke-slow", target(), null, typeRef<String>())
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
    ): String =
        invoke(
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
    override suspend fun handle(input: Any?): String {
        val result = invoke("invoke-target", target(), null, typeRef<String>())
        wait(1.seconds)
        return result
    }
}

public class InvokeReplayRethrows(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?): String {
        try {
            invoke("invoke-failing", target(), null, typeRef<String>())
        } catch (_: InvokeFailureException) {
            // Initial execution and replay both observe the same checkpointed error.
        }
        wait(1.seconds)
        return "done"
    }
}

public class StepThenInvoke(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?): String {
        val stepResult = step<String>("compute") { "step-data" }
        return invoke("invoke-with-step", target(), stepResult, typeRef())
    }
}

public class InvokeThenStep(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?): String {
        val invokeResult = invoke("invoke-target", target(), null, typeRef<String>())
        return step("process") { "processed: $invokeResult" }
    }
}

public class InvokeInChildContext(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?): String =
        runInChildContext("child-invoke") {
            invoke("invoke-in-child", target(), null, typeRef<String>())
        }
}

public class InvokeMultipleSequential(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Map<String, Any?>, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(
        input: Map<String, Any?>,
    ): String {
        val first =
            invoke(
                "invoke-first",
                requireEnv("TARGET_FUNCTION_NAME_1"),
                null,
                typeRef<String>(),
            )
        val second =
            invoke(
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
    override suspend fun handle(input: Any?): String =
        invoke(
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
    override suspend fun handle(input: Any?): String =
        invoke(
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
            payload.uppercase() as T
    }
}

public class TargetEcho(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, Any?>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?): Any? {
        wait(1.seconds)
        return input
    }
}

public class TargetError(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?): String {
        wait(1.seconds)
        error("Target function error")
    }
}

public class TargetSlow(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?): String {
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
