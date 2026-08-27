package io.github.zhongkechen.durable.internal

import java.util.concurrent.atomic.AtomicReference
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
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
    private val stateChanged: (
        changed: Map<String, OperationRecord>,
        all: Map<String, OperationRecord>,
    ) -> Unit = { _, _ -> },
) : AutoCloseable {
    private data class Submission(
        val commands: List<CheckpointCommand>,
        val completion: CompletableDeferred<Unit>,
    )

    private val token = AtomicReference(checkpointToken)
    private val serviceLock = Mutex()
    private val queue = Channel<Submission>(Channel.UNLIMITED)
    private val serviceContext = coroutineContext
    private val scope = CoroutineScope(SupervisorJob() + serviceContext)
    private val worker: Job = scope.launch { processQueue() }

    suspend fun checkpoint(command: CheckpointCommand) {
        checkpointAsync(command).await()
    }

    suspend fun checkpoint(commands: List<CheckpointCommand>) {
        require(commands.isNotEmpty()) { "A checkpoint batch must contain at least one command" }
        submit(commands).await()
    }

    suspend fun checkpointAsync(command: CheckpointCommand): Deferred<Unit> {
        return submit(listOf(command))
    }

    private suspend fun submit(commands: List<CheckpointCommand>): Deferred<Unit> {
        val completion = CompletableDeferred<Unit>()
        queue.send(Submission(commands, completion))
        return completion
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
                var commandCount = first.commands.size
                var estimatedBytes = estimate(first.commands)

                while (commandCount < maximumBatchItems) {
                    val next =
                        withTimeoutOrNull(batchWindow) {
                            queue.receiveCatching().getOrNull()
                        } ?: break
                    val nextBytes = estimate(next.commands)
                    if (commandCount + next.commands.size > maximumBatchItems ||
                        estimatedBytes + nextBytes > maximumBatchBytes
                    ) {
                        carry = next
                        break
                    }
                    batch += next
                    commandCount += next.commands.size
                    estimatedBytes += nextBytes
                }

                try {
                    send(batch.flatMap(Submission::commands))
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
            val before = ledger.snapshot()
            page.operations.forEach(ledger::put)
            val changed =
                page.operations
                    .filter { before[it.identity.id] != it }
                    .associateBy { it.identity.id }
            if (changed.isNotEmpty()) stateChanged(changed, ledger.snapshot())
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

    private fun estimate(commands: List<CheckpointCommand>): Int =
        commands.sumOf { command ->
            command.identity.id.length +
                command.identity.name.orEmpty().length +
                command.identity.subtype.length +
                command.payload.orEmpty().length +
                command.error?.message.orEmpty().length +
                128
        }
}
