// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package software.amazon.lambda.durable;

import java.util.Objects;
import java.util.concurrent.CompletionStage;

/**
 * Asynchronous durable handler entry point for language adapters and Java applications using {@link CompletionStage}.
 *
 * <p>The existing synchronous {@link DurableHandler} lifecycle remains the Lambda runtime boundary. The asynchronous
 * handler stage is awaited on the configured user executor, which can be a Java 21 virtual-thread executor.
 *
 * @param <I> the input type
 * @param <O> the output type
 */
public abstract class AsyncDurableHandler<I, O> extends DurableHandler<I, O> {
    protected AsyncDurableHandler() {}

    protected AsyncDurableHandler(TypeToken<I> inputType) {
        super(inputType);
    }

    protected AsyncDurableHandler(DurableConfig config) {
        super(config);
    }

    protected AsyncDurableHandler(TypeToken<I> inputType, DurableConfig config) {
        super(inputType, config);
    }

    /**
     * Executes the asynchronous handler and bridges its result to the existing Java handler contract.
     *
     * @param input user input
     * @param context durable context
     * @return the asynchronous handler result
     */
    @Override
    public final O handleRequest(I input, DurableContext context) {
        var stage = Objects.requireNonNull(
                handleRequestAsync(input, context), "Async durable handler result stage cannot be null");
        return stage.toCompletableFuture().join();
    }

    /**
     * Handles the durable execution asynchronously.
     *
     * @param input user input
     * @param context durable context
     * @return a stage producing the handler result
     */
    public abstract CompletionStage<O> handleRequestAsync(I input, DurableContext context);
}
