// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package software.amazon.lambda.durable.kotlin

import java.util.concurrent.CompletionStage
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.future.future
import software.amazon.lambda.durable.AsyncDurableHandler
import software.amazon.lambda.durable.DurableConfig
import software.amazon.lambda.durable.DurableContext
import software.amazon.lambda.durable.extension.DurableExecutionContextSnapshot

public abstract class KotlinDurableHandler<I, O>(
    config: DurableConfig = KotlinDurableRuntime.defaultConfig(),
) : AsyncDurableHandler<I, O>(config) {
    final override fun handleRequestAsync(input: I, context: DurableContext): CompletionStage<O> {
        val snapshot = DurableExecutionContextSnapshot.capture()
        val coroutineContext =
            SupervisorJob() + KotlinDurableRuntime.dispatcher + DurableExecutionContextElement(snapshot)
        return CoroutineScope(coroutineContext).future {
            handle(input, KotlinDurableContext(context))
        }
    }

    protected abstract suspend fun handle(input: I, context: KotlinDurableContext): O
}
