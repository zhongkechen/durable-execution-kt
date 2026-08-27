package wait_for_callback

import io.github.zhongkechen.durable.*

import kotlin.time.Duration.Companion.seconds

public class WaitForCallbackBasic(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String): String =
        waitForCallback(input, typeRef()) {}
}

public class WaitForCallbackExplicitName(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?): String =
        waitForCallback("approval", typeRef()) {}
}

public class WaitForCallbackAnonymous(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?): String =
        waitForCallback(null, typeRef()) {}
}

public class WaitForCallbackExternalFailure(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String): String =
        waitForCallback(input, typeRef()) {}
}

public class WaitForCallbackTimeout(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String): String =
        waitForCallback(
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
    override suspend fun handle(input: String): String =
        try {
            waitForCallback(input, typeRef()) {}
        } catch (_: CallbackFailureException) {
            "recovered"
        }
}

public class WaitForCallbackSubmitterRetryExhaustion(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String): String =
        waitForCallback(
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
    override suspend fun handle(input: String): String =
        runInChildContext("wrapper") {
            waitForCallback(input, typeRef()) {}
        }
}

public class WaitForCallbackTwoSequential(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: Any?): String {
        waitForCallback("first", typeRef<String>()) {}
        return waitForCallback("second", typeRef()) {}
    }
}

public class WaitForCallbackMixedOps(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String): String {
        wait(1.seconds)
        step<String>(null) { "fixed-data" }
        return waitForCallback(input, typeRef()) {}
    }
}

public class WaitForCallbackJsonResult(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String): String {
        val result =
            waitForCallback(
                name = input,
                type = typeRef<Map<String, String>>(),
            ) {}
        return requireNotNull(result["status"])
    }
}

public class WaitForCallbackHeartbeatTimeout(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<String, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(input: String): String =
        waitForCallback(
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
    override suspend fun handle(input: String): String =
        waitForCallback(
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
    override suspend fun handle(input: String): String =
        try {
            waitForCallback(
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
    override suspend fun handle(input: String): Any? =
        waitForCallback(input, typeRef()) {}
}
