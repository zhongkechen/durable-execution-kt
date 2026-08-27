package wait_for_callback

import io.github.zhongkechen.durable.CallbackFailureException
import io.github.zhongkechen.durable.CallbackOptions
import io.github.zhongkechen.durable.CallbackWaitOptions
import io.github.zhongkechen.durable.DurableContext
import io.github.zhongkechen.durable.DurableHandler
import io.github.zhongkechen.durable.DurableRuntimeConfig
import io.github.zhongkechen.durable.RetryPolicy
import io.github.zhongkechen.durable.StepOptions
import io.github.zhongkechen.durable.child
import io.github.zhongkechen.durable.step
import io.github.zhongkechen.durable.typeRef
import kotlin.time.Duration.Companion.seconds

public class WaitForCallbackBasic(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String, context: DurableContext): String =
        context.waitForCallback(input, typeRef()) {}
}

public class WaitForCallbackExplicitName(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): String =
        context.waitForCallback("approval", typeRef()) {}
}

public class WaitForCallbackAnonymous(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): String =
        context.waitForCallback(null, typeRef()) {}
}

public class WaitForCallbackExternalFailure(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String, context: DurableContext): String =
        context.waitForCallback(input, typeRef()) {}
}

public class WaitForCallbackTimeout(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String, context: DurableContext): String =
        context.waitForCallback(
            name = input,
            type = typeRef(),
            options =
                CallbackWaitOptions(
                    callback = CallbackOptions(timeout = 3.seconds),
                ),
        ) {}
}

public class WaitForCallbackFailureCaught(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String, context: DurableContext): String =
        try {
            context.waitForCallback(input, typeRef()) {}
        } catch (_: CallbackFailureException) {
            "recovered"
        }
}

public class WaitForCallbackSubmitterRetryExhaustion(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String, context: DurableContext): String =
        context.waitForCallback(
            name = input,
            type = typeRef(),
            options =
                CallbackWaitOptions(
                    submitter =
                        StepOptions(
                            retry = RetryPolicy.fixed(maxAttempts = 2, delay = 1.seconds),
                        ),
                ),
        ) {
            error("submitter always fails")
        }
}

public class WaitForCallbackInChildContext(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String, context: DurableContext): String =
        context.child("wrapper") {
            waitForCallback(input, typeRef()) {}
        }
}

public class WaitForCallbackTwoSequential(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?, context: DurableContext): String {
        context.waitForCallback("first", typeRef<String>()) {}
        return context.waitForCallback("second", typeRef()) {}
    }
}

public class WaitForCallbackMixedOps(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String, context: DurableContext): String {
        context.wait(1.seconds)
        context.step<String>(null) { "fixed-data" }
        return context.waitForCallback(input, typeRef()) {}
    }
}

public class WaitForCallbackJsonResult(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String, context: DurableContext): String {
        val result =
            context.waitForCallback(
                name = input,
                type = typeRef<Map<String, String>>(),
            ) {}
        return requireNotNull(result["status"])
    }
}

public class WaitForCallbackHeartbeatTimeout(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String, context: DurableContext): String =
        context.waitForCallback(
            name = input,
            type = typeRef(),
            options =
                CallbackWaitOptions(
                    callback = CallbackOptions(heartbeatTimeout = 5.seconds),
                ),
        ) {}
}

public class WaitForCallbackHeartbeatThenSuccess(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String, context: DurableContext): String =
        context.waitForCallback(
            name = input,
            type = typeRef(),
            options =
                CallbackWaitOptions(
                    callback = CallbackOptions(heartbeatTimeout = 10.seconds),
                ),
        ) {}
}

public class WaitForCallbackTimeoutCaught(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String, context: DurableContext): String =
        try {
            context.waitForCallback(
                name = input,
                type = typeRef<String>(),
                options =
                    CallbackWaitOptions(
                        callback = CallbackOptions(timeout = 3.seconds),
                    ),
            ) {}
        } catch (_: CallbackFailureException) {
            "timed-out-handled"
        }
}

public class WaitForCallbackNullResult(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, Any?>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String, context: DurableContext): Any? =
        context.waitForCallback(input, typeRef()) {}
}
