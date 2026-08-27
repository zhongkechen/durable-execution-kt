package io.github.zhongkechen.durable.internal

import io.github.zhongkechen.durable.NonDeterministicExecutionException
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue

class ReplayLedgerTest {
    @Test
    fun `root and nested operation ids are stable`() {
        val root = OperationIdSequence()
        val childId = root.next()
        val secondRootId = root.next()
        val child = OperationIdSequence(childId)

        assertEquals(
            "6b86b273ff34fce19d6b804eff5a3f5747ada4eaa22f1d49c01e52ddb7875b4b",
            childId,
        )
        assertEquals(
            "d4735e3a265e16eee03f59718b9b5d03019c07d8b6c51f90da3a666eec13ab35",
            secondRootId,
        )
        assertEquals(
            "2ac06c59dbc2f95f867ebb0f4e986076465c3dfd08e9353610dcf46b8b030df6",
            child.next(),
        )
    }

    @Test
    fun `custom local ids remain scoped and unique`() {
        val firstContext = OperationIdSequence("parent-a")
        val secondContext = OperationIdSequence("parent-b")

        val first = firstContext.next("charge")
        val second = secondContext.next("charge")

        assertFalse(first == second)
        assertFailsWith<IllegalStateException> { firstContext.next("charge") }
    }

    @Test
    fun `replay accepts unchanged identity and exposes invocation metadata`() {
        val identity =
            OperationIdentity(
                id = "op-1",
                name = "reserve",
                kind = OperationKind.STEP,
                subtype = "Step",
                parentId = null,
            )
        val record = OperationRecord(identity, CheckpointStatus.SUCCEEDED, resultPayload = "\"ok\"")
        val ledger = ReplayLedger(listOf(record), setOf(identity.id))

        assertEquals(record, ledger.find(identity))
        assertTrue(ledger.wasPresentAtInvocationStart(identity.id))
        assertTrue(ledger.wasExternallyUpdated(identity.id))
        assertEquals(record, ledger.updatedSnapshot()[identity.id])
    }

    @Test
    fun `replay rejects identity drift`() {
        val checkpointed =
            OperationIdentity(
                id = "op-1",
                name = "reserve",
                kind = OperationKind.STEP,
                subtype = "Step",
                parentId = null,
            )
        val requested = checkpointed.copy(name = "charge")
        val ledger = ReplayLedger(listOf(OperationRecord(checkpointed, CheckpointStatus.STARTED)))

        assertFailsWith<NonDeterministicExecutionException> {
            ledger.find(requested)
        }
    }
}
