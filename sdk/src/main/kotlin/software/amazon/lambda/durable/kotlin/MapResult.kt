// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package software.amazon.lambda.durable.kotlin

import software.amazon.lambda.durable.model.MapResult

/**
 * Rethrows the first map iteration failure using its checkpointed type and message.
 *
 * @return this result when no iteration failed
 */
public fun <T> MapResult<T>.throwIfFailed(): MapResult<T> {
    val failure = failed().firstOrNull() ?: return this
    throw IllegalStateException("${failure.errorType()}: ${failure.errorMessage()}")
}
