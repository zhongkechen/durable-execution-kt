package io.github.zhongkechen.durable.internal

import io.github.zhongkechen.durable.NonDeterministicExecutionException
import java.util.concurrent.ConcurrentHashMap

/**
 * Invocation-local view of checkpointed operations.
 *
 * The ledger validates identity before an operation may consume a replayed
 * checkpoint. It never advances sequence state by reading a record.
 */
internal class ReplayLedger(
    initialOperations: Collection<OperationRecord>,
    updatedOperationIds: Set<String> = emptySet(),
) {
    private val records =
        ConcurrentHashMap<String, OperationRecord>().apply {
            initialOperations.forEach { put(it.identity.id, it) }
        }
    private val initialIds = initialOperations.mapTo(mutableSetOf()) { it.identity.id }.toSet()
    private val externallyUpdatedIds = updatedOperationIds.toSet()

    fun find(identity: OperationIdentity): OperationRecord? {
        val checkpointed = records[identity.id] ?: return null
        validate(identity, checkpointed.identity)
        return checkpointed
    }

    fun put(record: OperationRecord) {
        records[record.identity.id] = record
    }

    fun wasPresentAtInvocationStart(operationId: String): Boolean = operationId in initialIds

    fun wasExternallyUpdated(operationId: String): Boolean = operationId in externallyUpdatedIds

    fun snapshot(): Map<String, OperationRecord> = records.toMap()

    fun updatedSnapshot(): Map<String, OperationRecord> =
        externallyUpdatedIds.mapNotNull { id -> records[id]?.let { id to it } }.toMap()

    private fun validate(
        requested: OperationIdentity,
        checkpointed: OperationIdentity,
    ) {
        val differences =
            buildList {
                if (requested.kind != checkpointed.kind) {
                    add("kind expected ${checkpointed.kind} but requested ${requested.kind}")
                }
                if (requested.subtype != checkpointed.subtype) {
                    add("subtype expected ${checkpointed.subtype} but requested ${requested.subtype}")
                }
                if (requested.name != checkpointed.name) {
                    add("name expected ${checkpointed.name} but requested ${requested.name}")
                }
                if (requested.parentId != checkpointed.parentId) {
                    add("parent expected ${checkpointed.parentId} but requested ${requested.parentId}")
                }
            }
        if (differences.isNotEmpty()) {
            throw NonDeterministicExecutionException(
                "Operation ${requested.id} changed during replay: ${differences.joinToString()}",
            )
        }
    }
}
