package io.github.zhongkechen.durable

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds

class CoreContractsTest {
    @Test
    fun `generic types survive a serde round trip`() {
        val serde = JsonSerde()
        val value = mapOf("numbers" to listOf(1, 2, 3))

        val payload = serde.encode(value)
        val decoded = serde.decode(payload, typeRef<Map<String, List<Int>>>())

        assertEquals(value, decoded)
    }

    @Test
    fun `fixed retry stops at the configured attempt`() {
        val retry = RetryPolicy.fixed(maxAttempts = 2, delay = 3.seconds)

        assertEquals(RetryDecision.Retry(3.seconds), retry.decide(IllegalStateException(), 1))
        assertEquals(RetryDecision.Fail, retry.decide(IllegalStateException(), 2))
    }

    @Test
    fun `batch results preserve item order and surface failure`() {
        val result =
            MapResult(
                BatchCompletion.ALL_COMPLETED,
                listOf(
                    ItemResult.Success(1, "second", "b"),
                    ItemResult.Success(0, "first", "a"),
                    ItemResult.Failure(2, "third", IllegalArgumentException("bad")),
                ),
            )

        assertEquals(listOf("a", "b"), result.values())
        assertTrue(result.hasFailure)
        assertFailsWith<BatchFailureException> { result.throwIfFailed() }
    }
}
