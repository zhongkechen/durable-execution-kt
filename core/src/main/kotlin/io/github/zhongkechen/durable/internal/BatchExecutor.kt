package io.github.zhongkechen.durable.internal

import io.github.zhongkechen.durable.BatchCompletion
import io.github.zhongkechen.durable.CompletionPolicy
import io.github.zhongkechen.durable.ItemResult
import java.util.concurrent.CancellationException
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.supervisorScope
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit

internal data class BatchWork<T>(
    val index: Int,
    val name: String?,
    val execute: suspend () -> T,
)

internal data class BatchOutcome<T>(
    val completion: BatchCompletion,
    val items: List<ItemResult<T>>,
)

private sealed interface BatchSignal<out T> {
    data class Completed<T>(
        val result: ItemResult<T>,
    ) : BatchSignal<T>

    data class Suspended(
        val error: ExecutionSuspended,
    ) : BatchSignal<Nothing>
}

internal suspend fun <T> executeBatch(
    work: List<BatchWork<T>>,
    maximumConcurrency: Int,
    completionPolicy: CompletionPolicy,
): BatchOutcome<T> {
    if (work.isEmpty()) return BatchOutcome(BatchCompletion.ALL_COMPLETED, emptyList())
    require(maximumConcurrency >= 1) { "maximumConcurrency must be positive" }

    return supervisorScope {
        val semaphore = Semaphore(maximumConcurrency)
        val completed = Channel<BatchSignal<T>>(Channel.UNLIMITED)
        val results = arrayOfNulls<ItemResult<T>>(work.size)
        val jobs =
            work.map { item ->
                launch {
                    val signal =
                        try {
                            BatchSignal.Completed(
                                semaphore.withPermit {
                                    try {
                                        ItemResult.Success(item.index, item.name, item.execute())
                                    } catch (suspension: ExecutionSuspended) {
                                        throw suspension
                                    } catch (cancelled: CancellationException) {
                                        throw cancelled
                                    } catch (error: Throwable) {
                                        ItemResult.Failure(item.index, item.name, error)
                                    }
                                },
                            )
                        } catch (suspension: ExecutionSuspended) {
                            BatchSignal.Suspended(suspension)
                        }
                    completed.send(signal)
                }
            }

        var completion = BatchCompletion.ALL_COMPLETED
        var received = 0
        var stop = false
        var suspended = false
        try {
            while (received < work.size && !stop) {
                when (val signal = completed.receive()) {
                    is BatchSignal.Suspended -> {
                        suspended = true
                        throw signal.error
                    }
                    is BatchSignal.Completed -> {
                        val result = signal.result
                        results[result.index] = result
                        received += 1
                        val successes = results.count { it is ItemResult.Success<*> }
                        val failures = results.count { it is ItemResult.Failure }
                        stop =
                            when (completionPolicy) {
                                CompletionPolicy.AllCompleted -> false
                                is CompletionPolicy.MinimumSuccessful -> {
                                    if (successes >= completionPolicy.count) {
                                        completion = BatchCompletion.MINIMUM_SUCCEEDED
                                        true
                                    } else {
                                        false
                                    }
                                }
                                is CompletionPolicy.TolerateFailures -> {
                                    val countExceeded =
                                        completionPolicy.count?.let { failures > it } ?: false
                                    val percentageExceeded =
                                        completionPolicy.percentage?.let {
                                            received > 0 && failures * 100.0 / received > it
                                        } ?: false
                                    if (countExceeded || percentageExceeded) {
                                        completion = BatchCompletion.FAILURE_LIMIT_EXCEEDED
                                        true
                                    } else {
                                        false
                                    }
                                }
                            }
                        }
                    }
                }
            }
        finally {
            if (stop || suspended) jobs.forEach { if (it.isActive) it.cancel() }
            jobs.joinAll()
            completed.close()
        }

        BatchOutcome(
            completion = completion,
            items =
                results.mapIndexed { index, result ->
                    result ?: ItemResult.Skipped(work[index].index, work[index].name)
                },
        )
    }
}
