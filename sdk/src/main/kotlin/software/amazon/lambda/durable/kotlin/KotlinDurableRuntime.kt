// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package software.amazon.lambda.durable.kotlin

import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.asCoroutineDispatcher
import software.amazon.lambda.durable.DurableConfig

public object KotlinDurableRuntime {
    public val executor: ExecutorService = Executors.newVirtualThreadPerTaskExecutor()

    public val dispatcher: CoroutineDispatcher = executor.asCoroutineDispatcher()

    public fun defaultConfig(): DurableConfig =
        config()

    public fun config(configure: DurableConfig.Builder.() -> Unit = {}): DurableConfig =
        DurableConfig.builder()
            .withExecutorService(executor)
            .apply(configure)
            .build()
}
