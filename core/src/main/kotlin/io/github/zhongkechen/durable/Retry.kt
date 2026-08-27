package io.github.zhongkechen.durable

import kotlin.math.pow
import kotlin.random.Random
import kotlin.time.Duration
import kotlin.time.Duration.Companion.seconds

public enum class DeliverySemantics {
    AT_LEAST_ONCE_PER_RETRY,
    AT_MOST_ONCE_PER_RETRY,
}

public enum class RetryJitter {
    NONE,
    FULL,
    HALF,
}

public sealed interface RetryDecision {
    public data object Fail : RetryDecision

    public data class Retry(
        val delay: Duration,
    ) : RetryDecision {
        init {
            require(!delay.isNegative()) { "Retry delay cannot be negative" }
        }
    }
}

public fun interface RetryPolicy {
    public fun decide(
        error: Throwable,
        attempt: Int,
    ): RetryDecision

    public companion object {
        public val none: RetryPolicy = RetryPolicy { _, _ -> RetryDecision.Fail }

        public val default: RetryPolicy =
            exponential(
                maxAttempts = 6,
                initialDelay = 5.seconds,
                maximumDelay = 60.seconds,
                multiplier = 2.0,
                jitter = RetryJitter.FULL,
            )

        public fun fixed(
            maxAttempts: Int = 3,
            delay: Duration = 1.seconds,
            retryIf: (Throwable) -> Boolean = { true },
        ): RetryPolicy {
            require(maxAttempts >= 1) { "maxAttempts must be at least 1" }
            require(!delay.isNegative()) { "delay cannot be negative" }
            return RetryPolicy { error, attempt ->
                if (attempt < maxAttempts && retryIf(error)) {
                    RetryDecision.Retry(delay)
                } else {
                    RetryDecision.Fail
                }
            }
        }

        public fun exponential(
            maxAttempts: Int = 3,
            initialDelay: Duration = 1.seconds,
            maximumDelay: Duration = 60.seconds,
            multiplier: Double = 2.0,
            jitter: RetryJitter = RetryJitter.NONE,
            random: Random = Random.Default,
            retryIf: (Throwable) -> Boolean = { true },
        ): RetryPolicy {
            require(maxAttempts >= 1) { "maxAttempts must be at least 1" }
            require(!initialDelay.isNegative()) { "initialDelay cannot be negative" }
            require(maximumDelay >= initialDelay) {
                "maximumDelay must be greater than or equal to initialDelay"
            }
            require(multiplier >= 1.0) { "multiplier must be at least 1.0" }
            return RetryPolicy { error, attempt ->
                if (attempt >= maxAttempts || !retryIf(error)) {
                    RetryDecision.Fail
                } else {
                    val scaled =
                        initialDelay * multiplier.pow((attempt - 1).coerceAtLeast(0))
                    val capped = minOf(scaled, maximumDelay)
                    val jittered =
                        when (jitter) {
                            RetryJitter.NONE -> capped
                            RetryJitter.FULL -> capped * random.nextDouble()
                            RetryJitter.HALF -> capped * (0.5 + random.nextDouble() / 2.0)
                        }
                    RetryDecision.Retry(jittered)
                }
            }
        }
    }
}
