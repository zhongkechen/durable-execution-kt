package io.github.zhongkechen.durable

public enum class ExecutionStatus {
    PENDING,
    SUCCEEDED,
    FAILED,
    TIMED_OUT,
    STOPPED,
}

public enum class BatchCompletion {
    ALL_COMPLETED,
    MINIMUM_SUCCEEDED,
    FAILURE_LIMIT_EXCEEDED,
}

public sealed interface ItemResult<out T> {
    public val index: Int
    public val name: String?

    public data class Success<T>(
        override val index: Int,
        override val name: String?,
        val value: T,
    ) : ItemResult<T>

    public data class Failure(
        override val index: Int,
        override val name: String?,
        val error: Throwable,
    ) : ItemResult<Nothing>

    public data class Skipped(
        override val index: Int,
        override val name: String?,
    ) : ItemResult<Nothing>
}

public open class BatchResult<T>(
    public val completion: BatchCompletion,
    items: List<ItemResult<T>>,
) {
    public val items: List<ItemResult<T>> = items.toList()

    public val successes: List<ItemResult.Success<T>>
        get() = items.filterIsInstance<ItemResult.Success<T>>()

    public val failures: List<ItemResult.Failure>
        get() = items.filterIsInstance<ItemResult.Failure>()

    public val skipped: List<ItemResult.Skipped>
        get() = items.filterIsInstance<ItemResult.Skipped>()

    public val hasFailure: Boolean
        get() = failures.isNotEmpty()

    public fun values(): List<T> = successes.sortedBy { it.index }.map { it.value }

    public fun throwIfFailed(): BatchResult<T> {
        failures.firstOrNull()?.let { throw BatchFailureException(it.error, items) }
        return this
    }
}

public class MapResult<T>(
    completion: BatchCompletion,
    items: List<ItemResult<T>>,
) : BatchResult<T>(completion, items)

public class ParallelResult(
    completion: BatchCompletion,
    items: List<ItemResult<Any?>>,
) : BatchResult<Any?>(completion, items)

public sealed interface ConditionDecision<out T> {
    public data class Continue<T>(
        val state: T,
    ) : ConditionDecision<T>

    public data class Complete<T>(
        val result: T,
    ) : ConditionDecision<T>
}
