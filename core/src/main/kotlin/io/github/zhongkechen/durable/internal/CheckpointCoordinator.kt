package io.github.zhongkechen.durable.internal

import java.util.concurrent.atomic.AtomicReference
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.channels.Channel
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.withContext
import kotlinx.coroutines.withTimeoutOrNull

/**
 * Serializes checkpoint traffic, rotates service tokens, and feeds completed
 * operation records back into the replay ledger.
 */
internal class CheckpointCoordinator(
    private val service: DurableService,
    private val executionArn: String,
    checkpointToken: String,
    private val ledger: ReplayLedger,
    coroutineContext: CoroutineContext = Dispatchers.IO,
    private val batchWindow: Duration = 5.milliseconds,
    private val maximumBatchItems: Int = 200,
    private val maximumBatchBytes: Int = 750 * 1024,
) : AutoCloseable {
    private data class Submission(
        val command: CheckpointCommand,
        val completion: CompletableDeferred<Unit>,
    )

    private val token = AtomicReference(checkpointToken)
    private val serviceLock = Mutex()
    private val queue = Channel<Submission>(Channel.UNLIMITED)
    private val serviceContext = coroutineContext
    private val scope = CoroutineScope(SupervisorJob() + serviceContext)
    private val worker: Job = scope.launch { processQueue() }

    suspend fun checkpoint(command: CheckpointCommand) {
        val completion = CompletableDeferred<Unit>()
        queue.send(Submission(command, completion))
        completion.await()
    }

    suspend fun refresh(): Map<String, OperationRecord> =
        serviceLock.withLock {
            val reply =
                withContext(serviceContext) {
                    service.checkpoint(executionArn, token.get(), emptyList())
                }
            token.set(reply.checkpointToken)
            applyPages(reply.state)
            ledger.snapshot()
        }

    suspend fun pollUntil(
        operationId: String,
        interval: (attempt: Int) -> Duration,
        predicate: (OperationRecord) -> Boolean,
    ): OperationRecord {
        var attempt = 1
        while (scope.isActive) {
            ledger.snapshot()[operationId]?.takeIf(predicate)?.let { return it }
            delay(interval(attempt))
            refresh()
            attempt += 1
        }
        error("Checkpoint coordinator closed while polling $operationId")
    }

    override fun close() {
        queue.close()
    }

    suspend fun join() {
        worker.join()
    }

    private suspend fun processQueue() {
        var carry: Submission? = null
        try {
            while (scope.isActive) {
                val first = carry ?: queue.receiveCatching().getOrNull() ?: break
                carry = null
                val batch = mutableListOf(first)
                var estimatedBytes = estimate(first.command)

                while (batch.size < maximumBatchItems) {
                    val next =
                        withTimeoutOrNull(batchWindow) {
                            queue.receiveCatching().getOrNull()
                        } ?: break
                    val nextBytes = estimate(next.command)
                    if (estimatedBytes + nextBytes > maximumBatchBytes) {
                        carry = next
                        break
                    }
                    batch += next
                    estimatedBytes += nextBytes
                }

                try {
                    send(batch.map(Submission::command))
                    batch.forEach { it.completion.complete(Unit) }
                } catch (error: Throwable) {
                    batch.forEach { it.completion.completeExceptionally(error) }
                }
            }
        } finally {
            carry?.completion?.completeExceptionally(
                IllegalStateException("Checkpoint coordinator closed"),
            )
            while (true) {
                val pending = queue.tryReceive().getOrNull() ?: break
                pending.completion.completeExceptionally(
                    IllegalStateException("Checkpoint coordinator closed"),
                )
            }
        }
    }

    private suspend fun send(commands: List<CheckpointCommand>) {
        serviceLock.withLock {
            val reply =
                withContext(serviceContext) {
                    service.checkpoint(executionArn, token.get(), commands)
                }
            token.set(reply.checkpointToken)
            applyPages(reply.state)
        }
    }

    private suspend fun applyPages(firstPage: ServicePage?) {
        var page = firstPage
        while (page != null) {
            page.operations.forEach(ledger::put)
            val marker = page.nextMarker
            page =
                if (marker.isNullOrEmpty()) {
                    null
                } else {
                    withContext(serviceContext) {
                        service.getState(executionArn, token.get(), marker)
                    }
                }
        }
    }

    private fun estimate(command: CheckpointCommand): Int =
        command.identity.id.length +
            command.identity.name.orEmpty().length +
            command.identity.subtype.length +
            command.payload.orEmpty().length +
            command.error?.message.orEmpty().length +
            128
}
