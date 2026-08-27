// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package software.amazon.lambda.durable.extension;

import software.amazon.lambda.durable.context.BaseContext;
import software.amazon.lambda.durable.context.BaseContextImpl;
import software.amazon.lambda.durable.execution.ExecutionManager;
import software.amazon.lambda.durable.execution.ThreadContext;
import software.amazon.lambda.durable.logging.DurableLogger;
import software.amazon.lambda.durable.model.SafeCloseable;

/**
 * Captures the current durable execution context so language adapters can propagate it across asynchronous continuations.
 */
public final class DurableExecutionContextSnapshot {
    private final BaseContext baseContext;
    private final ExecutionManager executionManager;
    private final ThreadContext threadContext;

    private DurableExecutionContextSnapshot(
            BaseContext baseContext, ExecutionManager executionManager, ThreadContext threadContext) {
        this.baseContext = baseContext;
        this.executionManager = executionManager;
        this.threadContext = threadContext;
    }

    /**
     * Captures the durable context attached to the current SDK-managed thread.
     *
     * @return a reusable context snapshot
     * @throws IllegalStateException if no durable context is active
     */
    public static DurableExecutionContextSnapshot capture() {
        var context = BaseContext.getCurrentContext();
        if (!(context instanceof BaseContextImpl contextImpl)) {
            throw new IllegalStateException("No durable execution context is active on the current thread");
        }
        var executionManager = contextImpl.getExecutionManager();
        return new DurableExecutionContextSnapshot(
                context, executionManager, executionManager.getCurrentThreadContext());
    }

    /**
     * Attaches this snapshot to the current thread until the returned scope is closed.
     *
     * @return a scope that restores the preceding context
     */
    public SafeCloseable attach() {
        var previousContext = BaseContext.getCurrentContext();
        var previousThreadContext = executionManager.getCurrentThreadContext();
        BaseContextImpl.setCurrentContext(baseContext);
        executionManager.setCurrentThreadContext(threadContext);
        var loggerScope = DurableLogger.attachContext();
        return () -> {
            loggerScope.close();
            BaseContextImpl.setCurrentContext(previousContext);
            executionManager.setCurrentThreadContext(previousThreadContext);
        };
    }
}
