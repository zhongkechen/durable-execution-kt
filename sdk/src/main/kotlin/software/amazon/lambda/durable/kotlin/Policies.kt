// Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package software.amazon.lambda.durable.kotlin

import kotlin.time.Duration
import kotlin.time.toJavaDuration
import software.amazon.lambda.durable.config.CompletionConfig
import software.amazon.lambda.durable.config.NestingType
import software.amazon.lambda.durable.retry.JitterStrategy
import software.amazon.lambda.durable.retry.RetryDecision as JavaRetryDecision
import software.amazon.lambda.durable.retry.RetryStrategies
import software.amazon.lambda.durable.retry.RetryStrategy
import software.amazon.lambda.durable.retry.WaitForConditionWaitStrategy
import software.amazon.lambda.durable.retry.WaitStrategies

public enum class DeliverySemantics {
    AT_LEAST_ONCE_PER_RETRY,
    AT_MOST_ONCE_PER_RETRY,
}

public enum class RetryJitter {
    NONE,
    FULL,
}

public sealed interface RetryDirective {
    public data class Retry(public val delay: Duration) : RetryDirective

    public data object Fail : RetryDirective
}

public class RetryPolicy internal constructor(
    @PublishedApi
    internal val javaStrategy: RetryStrategy,
) {
    public companion object {
        public val default: RetryPolicy = RetryPolicy(RetryStrategies.Presets.DEFAULT)

        public val none: RetryPolicy = RetryPolicy(RetryStrategies.Presets.NO_RETRY)

        public fun fixed(
            maxAttempts: Int,
            delay: Duration,
        ): RetryPolicy =
            RetryPolicy(RetryStrategies.fixedDelay(maxAttempts, delay.toJavaDuration()))

        public fun exponential(
            maxAttempts: Int,
            initialDelay: Duration,
            maxDelay: Duration,
            backoffRate: Double = 2.0,
            jitter: RetryJitter = RetryJitter.FULL,
        ): RetryPolicy =
            RetryPolicy(
                RetryStrategies.exponentialBackoff(
                    maxAttempts,
                    initialDelay.toJavaDuration(),
                    maxDelay.toJavaDuration(),
                    backoffRate,
                    when (jitter) {
                        RetryJitter.NONE -> JitterStrategy.NONE
                        RetryJitter.FULL -> JitterStrategy.FULL
                    },
                ),
            )

        public fun custom(decide: (error: Throwable, attempt: Int) -> RetryDirective): RetryPolicy =
            RetryPolicy { error, attempt ->
                when (val directive = decide(error, attempt)) {
                    RetryDirective.Fail -> JavaRetryDecision.fail()
                    is RetryDirective.Retry -> JavaRetryDecision.retry(directive.delay.toJavaDuration())
                }
            }
    }
}

public class CompletionPolicy internal constructor(
    @PublishedApi
    internal val javaConfig: CompletionConfig,
) {
    public companion object {
        public val allCompleted: CompletionPolicy = CompletionPolicy(CompletionConfig.allCompleted())

        public val allSuccessful: CompletionPolicy = CompletionPolicy(CompletionConfig.allSuccessful())

        public val firstSuccessful: CompletionPolicy = CompletionPolicy(CompletionConfig.firstSuccessful())

        public fun minSuccessful(count: Int): CompletionPolicy =
            CompletionPolicy(CompletionConfig.minSuccessful(count))

        public fun toleratedFailures(count: Int): CompletionPolicy =
            CompletionPolicy(CompletionConfig.toleratedFailureCount(count))

        public fun toleratedFailurePercentage(fraction: Double): CompletionPolicy =
            CompletionPolicy(CompletionConfig.toleratedFailurePercentage(fraction))

        public fun thresholds(
            minSuccessful: Int? = null,
            toleratedFailures: Int? = null,
            toleratedFailurePercentage: Double? = null,
        ): CompletionPolicy =
            CompletionPolicy(CompletionConfig(minSuccessful, toleratedFailures, toleratedFailurePercentage))
    }
}

public enum class Nesting {
    NESTED,
    FLAT,
}

@PublishedApi
internal fun Nesting.toJava(): NestingType =
    when (this) {
        Nesting.NESTED -> NestingType.NESTED
        Nesting.FLAT -> NestingType.FLAT
    }

public class ConditionWaitPolicy<T> internal constructor(
    @PublishedApi
    internal val javaStrategy: WaitForConditionWaitStrategy<T>,
) {
    public companion object {
        public fun <T> fixed(
            maxAttempts: Int,
            delay: Duration,
        ): ConditionWaitPolicy<T> =
            ConditionWaitPolicy(WaitStrategies.fixedDelay(maxAttempts, delay.toJavaDuration()))
    }
}
