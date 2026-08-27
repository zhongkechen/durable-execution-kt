// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package software.amazon.lambda.durable.kotlin

import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.ThreadContextElement
import software.amazon.lambda.durable.extension.DurableExecutionContextSnapshot
import software.amazon.lambda.durable.model.SafeCloseable

internal class DurableExecutionContextElement(
    private val snapshot: DurableExecutionContextSnapshot,
) : ThreadContextElement<SafeCloseable?> {
    companion object Key : CoroutineContext.Key<DurableExecutionContextElement>

    override val key: CoroutineContext.Key<DurableExecutionContextElement>
        get() = Key

    override fun updateThreadContext(context: CoroutineContext): SafeCloseable = snapshot.attach()

    override fun restoreThreadContext(context: CoroutineContext, oldState: SafeCloseable?) {
        oldState?.close()
    }
}
