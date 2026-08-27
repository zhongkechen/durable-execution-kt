// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package wait_for_callback

import java.time.Duration
import kotlin.time.Duration.Companion.seconds
import software.amazon.lambda.durable.execution.SuspendExecutionException
import software.amazon.lambda.durable.kotlin.KotlinDurableContext
import software.amazon.lambda.durable.kotlin.KotlinDurableHandler
import software.amazon.lambda.durable.kotlin.RetryPolicy

public class WaitForCallbackBasic : KotlinDurableHandler<String, String>() {
    override suspend fun handle(input: String, context: KotlinDurableContext): String =
        context.waitForCallback(input) {}
}

public class WaitForCallbackExplicitName : KotlinDurableHandler<Any?, String>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String =
        context.waitForCallback("approval") {}
}

public class WaitForCallbackAnonymous : KotlinDurableHandler<Any?, String>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String =
        context.waitForCallback(null) {}
}

public class WaitForCallbackExternalFailure : KotlinDurableHandler<String, String>() {
    override suspend fun handle(input: String, context: KotlinDurableContext): String =
        context.waitForCallback(input) {}
}

public class WaitForCallbackTimeout : KotlinDurableHandler<String, String>() {
    override suspend fun handle(input: String, context: KotlinDurableContext): String =
        context.waitForCallback(
            name = input,
            timeout = 3.seconds,
        ) {}
}

public class WaitForCallbackFailureCaught : KotlinDurableHandler<String, String>() {
    override suspend fun handle(input: String, context: KotlinDurableContext): String =
        try {
            context.waitForCallback(input) {}
        } catch (error: SuspendExecutionException) {
            throw error
        } catch (_: Exception) {
            "recovered"
        }
}

public class WaitForCallbackSubmitterRetryExhaustion : KotlinDurableHandler<String, String>() {
    override suspend fun handle(input: String, context: KotlinDurableContext): String =
        context.waitForCallback(
            name = input,
            submitterRetry = RetryPolicy.fixed(maxAttempts = 2, delay = 1.seconds),
        ) {
            error("submitter always fails")
        }
}

public class WaitForCallbackInChildContext : KotlinDurableHandler<String, String>() {
    override suspend fun handle(input: String, context: KotlinDurableContext): String =
        context.childContext("wrapper") {
            waitForCallback(input) {}
        }
}

public class WaitForCallbackTwoSequential : KotlinDurableHandler<Any?, String>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String {
        context.waitForCallback<String>("first") {}
        return context.waitForCallback("second") {}
    }
}

public class WaitForCallbackMixedOps : KotlinDurableHandler<String, String>() {
    override suspend fun handle(input: String, context: KotlinDurableContext): String {
        context.wait(Duration.ofSeconds(1))
        context.step<String>(null) { "fixed-data" }
        return context.waitForCallback(input) {}
    }
}

public class WaitForCallbackJsonResult : KotlinDurableHandler<String, String>() {
    override suspend fun handle(input: String, context: KotlinDurableContext): String {
        val result =
            context.waitForCallback<Map<String, String>>(name = input) {}
        return requireNotNull(result["status"])
    }
}

public class WaitForCallbackHeartbeatTimeout : KotlinDurableHandler<String, String>() {
    override suspend fun handle(input: String, context: KotlinDurableContext): String =
        context.waitForCallback(
            name = input,
            heartbeatTimeout = 5.seconds,
        ) {}
}

public class WaitForCallbackHeartbeatThenSuccess : KotlinDurableHandler<String, String>() {
    override suspend fun handle(input: String, context: KotlinDurableContext): String =
        context.waitForCallback(
            name = input,
            heartbeatTimeout = 10.seconds,
        ) {}
}

public class WaitForCallbackTimeoutCaught : KotlinDurableHandler<String, String>() {
    override suspend fun handle(input: String, context: KotlinDurableContext): String =
        try {
            context.waitForCallback<String>(
                name = input,
                timeout = 3.seconds,
            ) {}
        } catch (error: SuspendExecutionException) {
            throw error
        } catch (_: Exception) {
            "timed-out-handled"
        }
}

public class WaitForCallbackNullResult : KotlinDurableHandler<String, Any?>() {
    override suspend fun handle(input: String, context: KotlinDurableContext): Any? =
        context.waitForCallback<Any?>(input) {}
}
