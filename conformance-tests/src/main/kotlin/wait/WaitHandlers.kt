// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package wait

import java.time.Duration
import software.amazon.lambda.durable.kotlin.KotlinDurableContext
import software.amazon.lambda.durable.kotlin.KotlinDurableHandler

public class WaitBasic : KotlinDurableHandler<Any?, String>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String {
        context.wait(Duration.ofSeconds(2))
        return "Wait completed"
    }
}

public class WaitWithName : KotlinDurableHandler<Any?, String>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String {
        context.wait(Duration.ofSeconds(2), "custom_wait_name")
        return "Wait with name completed"
    }
}

public class WaitMultipleSequential : KotlinDurableHandler<Any?, Map<String, Any>>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): Map<String, Any> {
        context.wait(Duration.ofSeconds(2), "wait-1")
        context.wait(Duration.ofSeconds(2), "wait-2")
        return mapOf("completedWaits" to 2)
    }
}

public class WaitDurationUnits : KotlinDurableHandler<Any?, String>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String {
        context.wait(Duration.ofMinutes(1))
        return "Wait with minutes completed"
    }
}

public class WaitLongDuration : KotlinDurableHandler<Any?, String>() {
    override suspend fun handle(input: Any?, context: KotlinDurableContext): String {
        context.wait(Duration.ofHours(1))
        return "Wait with hours completed"
    }
}
