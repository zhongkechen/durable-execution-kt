package io.github.zhongkechen.durable.internal

import io.github.zhongkechen.durable.BatchCompletion
import io.github.zhongkechen.durable.CompletionPolicy
import io.github.zhongkechen.durable.ItemResult
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlinx.coroutines.test.runTest

class BatchExecutorTest {
    @Test
    fun `minimum successful stops remaining work`() =
        runTest {
            val started = mutableListOf<Int>()
            val outcome =
                executeBatch(
                    work =
                        (0 until 5).map { index ->
                            BatchWork(index, "item-$index") {
                                started += index
                                index
                            }
                        },
                    maximumConcurrency = 1,
                    completionPolicy = CompletionPolicy.MinimumSuccessful(2),
                )

            assertEquals(BatchCompletion.MINIMUM_SUCCEEDED, outcome.completion)
            assertEquals(listOf(0, 1), started)
            assertEquals(2, outcome.items.count { it is ItemResult.Success })
            assertEquals(3, outcome.items.count { it is ItemResult.Skipped })
        }

    @Test
    fun `failure count exceeding tolerance stops batch`() =
        runTest {
            val started = mutableListOf<Int>()
            val outcome =
                executeBatch(
                    work =
                        (0 until 4).map { index ->
                            BatchWork(index, null) {
                                started += index
                                if (index < 2) error("bad-$index")
                                index
                            }
                        },
                    maximumConcurrency = 1,
                    completionPolicy = CompletionPolicy.TolerateFailures(count = 1),
                )

            assertEquals(BatchCompletion.FAILURE_LIMIT_EXCEEDED, outcome.completion)
            assertEquals(listOf(0, 1), started)
            assertEquals(2, outcome.items.count { it is ItemResult.Failure })
            assertTrue(outcome.items.drop(2).all { it is ItemResult.Skipped })
        }
}
