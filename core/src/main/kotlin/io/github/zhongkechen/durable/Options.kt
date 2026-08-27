package io.github.zhongkechen.durable

import kotlin.time.Duration

public enum class Nesting {
    NESTED,
    FLAT,
}

public sealed interface CompletionPolicy {
    public data object AllCompleted : CompletionPolicy

    public data class MinimumSuccessful(
        val count: Int,
    ) : CompletionPolicy {
        init {
            require(count >= 1) { "Minimum successful count must be positive" }
        }
    }

    public data class TolerateFailures(
        val count: Int? = null,
        val percentage: Double? = null,
    ) : CompletionPolicy {
        init {
            require(count != null || percentage != null) {
                "A tolerated failure count or percentage is required"
            }
            require(count == null || count >= 0) {
                "Tolerated failure count cannot be negative"
            }
            require(percentage == null || percentage in 0.0..100.0) {
                "Tolerated failure percentage must be between 0 and 100"
            }
        }
    }

    public data class Combined(
        val minimumSuccessful: Int? = null,
        val toleratedFailures: Int? = null,
        val toleratedFailurePercentage: Double? = null,
    ) : CompletionPolicy {
        init {
            require(
                minimumSuccessful != null ||
                    toleratedFailures != null ||
                    toleratedFailurePercentage != null,
            ) { "At least one completion limit is required" }
            require(minimumSuccessful == null || minimumSuccessful >= 1)
            require(toleratedFailures == null || toleratedFailures >= 0)
            require(
                toleratedFailurePercentage == null ||
                    toleratedFailurePercentage in 0.0..100.0,
            )
        }
    }
}

public data class StepOptions(
    val retry: RetryPolicy = RetryPolicy.default,
    val delivery: DeliverySemantics = DeliverySemantics.AT_LEAST_ONCE_PER_RETRY,
    val serde: Serde? = null,
)

public data class InvokeOptions(
    val tenantId: String? = null,
    val payloadSerde: Serde? = null,
    val resultSerde: Serde? = null,
)

public data class CallbackOptions(
    val timeout: Duration? = null,
    val heartbeatTimeout: Duration? = null,
    val serde: Serde? = null,
) {
    init {
        require(timeout == null || timeout.isPositive()) { "timeout must be positive" }
        require(heartbeatTimeout == null || heartbeatTimeout.isPositive()) {
            "heartbeatTimeout must be positive"
        }
    }
}

public data class ChildOptions(
    val virtual: Boolean = false,
    val serde: Serde? = null,
)

public data class MapOptions<I>(
    val maximumConcurrency: Int? = null,
    val completion: CompletionPolicy = CompletionPolicy.AllCompleted,
    val nesting: Nesting = Nesting.NESTED,
    val itemSerde: Serde? = null,
    val resultSerde: Serde? = null,
    val itemName: ((item: I, index: Int) -> String?)? = null,
) {
    init {
        require(maximumConcurrency == null || maximumConcurrency >= 1) {
            "maximumConcurrency must be positive"
        }
    }
}

public data class ParallelOptions(
    val maximumConcurrency: Int? = null,
    val completion: CompletionPolicy = CompletionPolicy.AllCompleted,
    val nesting: Nesting = Nesting.NESTED,
    val itemSerde: Serde? = null,
) {
    init {
        require(maximumConcurrency == null || maximumConcurrency >= 1) {
            "maximumConcurrency must be positive"
        }
    }
}

public data class CallbackWaitOptions(
    val callback: CallbackOptions = CallbackOptions(),
    val submitter: StepOptions = StepOptions(),
)

public data class ConditionOptions<T>(
    val initialState: T? = null,
    val maximumAttempts: Int? = null,
    val delay: (result: T, attemptsMade: Int) -> Duration,
    val serde: Serde? = null,
) {
    init {
        require(maximumAttempts == null || maximumAttempts >= 1) {
            "maximumAttempts must be positive"
        }
    }
}
