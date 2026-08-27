// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package software.amazon.lambda.durable.kotlin

import kotlinx.coroutines.future.await
import software.amazon.lambda.durable.DurableFuture

public suspend fun <T> DurableFuture<T>.await(): T = awaitAsync().await()
