package io.github.zhongkechen.durable.internal

import io.github.zhongkechen.durable.DurableContext
import io.github.zhongkechen.durable.DurablePlugin
import io.github.zhongkechen.durable.ExecutionStatus
import io.github.zhongkechen.durable.InvocationEnded
import io.github.zhongkechen.durable.InvocationStarted
import io.github.zhongkechen.durable.JsonSerde
import io.github.zhongkechen.durable.Serde
import io.github.zhongkechen.durable.TypeRef
import java.time.Instant
import kotlin.coroutines.CoroutineContext
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlinx.coroutines.Dispatchers

internal data class InvocationRequest(
    val executionArn: String,
    val checkpointToken: String,
    val inputPayload: String,
    val initialOperations: List<OperationRecord>,
    val updatedOperationIds: Set<String> = emptySet(),
    val requestId: String? = null,
    val executionStartedAt: Instant? = null,
)

internal sealed interface EngineResult {
    data class Success(
        val payload: String,
    ) : EngineResult

    data class Pending(
        val operationId: String,
        val resumeAt: Instant?,
    ) : EngineResult

    data class Failure(
        val error: CheckpointError,
    ) : EngineResult
}

/**
 * Invocation-level coordinator for suspend durable handlers.
 */
internal class ExecutionEngine(
    private val service: DurableService,
    private val serde: Serde = JsonSerde(),
    private val serviceContext: CoroutineContext = Dispatchers.IO,
    private val checkpointBatchWindow: Duration = 5.milliseconds,
    plugins: List<DurablePlugin> = emptyList(),
) {
    private val plugins = PluginDispatcher(plugins)

    suspend fun <I, O> execute(
        request: InvocationRequest,
        inputType: TypeRef<I>,
        outputType: TypeRef<O>,
        handler: suspend (I, DurableContext) -> O,
    ): EngineResult {
        val ledger = ReplayLedger(request.initialOperations, request.updatedOperationIds)
        val firstInvocation =
            request.initialOperations.none { it.identity.kind != OperationKind.EXECUTION }
        val startedAt = request.executionStartedAt ?: Instant.now()
        val input = serde.decode(request.inputPayload, inputType)
        val checkpoints =
            CheckpointCoordinator(
                service = service,
                executionArn = request.executionArn,
                checkpointToken = request.checkpointToken,
                ledger = ledger,
                coroutineContext = serviceContext,
                batchWindow = checkpointBatchWindow,
                stateChanged = { changed, all ->
                    changed.values
                        .filter { it.status.terminal }
                        .forEach { operation ->
                            plugins.operationEnded(
                                operation.toSnapshot(
                                    ledger.wasPresentAtInvocationStart(operation.identity.id),
                                ),
                            )
                        }
                    plugins.operationsChanged(
                        request.executionArn,
                        changed.mapValues { (_, operation) ->
                            operation.toSnapshot(
                                ledger.wasPresentAtInvocationStart(operation.identity.id),
                            )
                        },
                        all.mapValues { (_, operation) ->
                            operation.toSnapshot(
                                ledger.wasPresentAtInvocationStart(operation.identity.id),
                            )
                        },
                    )
                },
            )
        plugins.invocationStarted(
            InvocationStarted(
                requestId = request.requestId,
                executionArn = request.executionArn,
                firstInvocation = firstInvocation,
                executionStartedAt = startedAt,
                operations =
                    ledger.snapshot().mapValues { (_, operation) ->
                        operation.toSnapshot(true)
                    },
                updatedOperations =
                    ledger.updatedSnapshot().mapValues { (_, operation) ->
                        operation.toSnapshot(true)
                    },
                input = input,
            ),
        )
        ledger.updatedSnapshot().values
            .filter { it.status.terminal }
            .forEach { plugins.operationEnded(it.toSnapshot(true)) }
        val runtime =
            OperationRuntime(
                executionArn = request.executionArn,
                isReplaying = request.initialOperations.any { it.identity.kind != OperationKind.EXECUTION },
                ledger = ledger,
                checkpoints = checkpoints,
                defaultSerde = serde,
                plugins = plugins,
            )
        return try {
            val result = handler(input, RuntimeDurableContext(runtime))
            val payload = serde.encode(result)
            serde.decode(payload, outputType)
            EngineResult.Success(payload).also {
                plugins.invocationEnded(
                    invocationEnd(
                        request,
                        firstInvocation,
                        startedAt,
                        ledger,
                        ExecutionStatus.SUCCEEDED,
                        input,
                        result,
                        null,
                    ),
                )
            }
        } catch (suspension: ExecutionSuspended) {
            EngineResult.Pending(suspension.operationId, suspension.resumeAt).also {
                plugins.invocationEnded(
                    invocationEnd(
                        request,
                        firstInvocation,
                        startedAt,
                        ledger,
                        ExecutionStatus.PENDING,
                        input,
                        null,
                        null,
                    ),
                )
            }
        } catch (error: Throwable) {
            EngineResult.Failure(error.toEngineError()).also {
                plugins.invocationEnded(
                    invocationEnd(
                        request,
                        firstInvocation,
                        startedAt,
                        ledger,
                        ExecutionStatus.FAILED,
                        input,
                        null,
                        error,
                    ),
                )
            }
        } finally {
            checkpoints.close()
            checkpoints.join()
        }
    }
}

private fun invocationEnd(
    request: InvocationRequest,
    firstInvocation: Boolean,
    startedAt: Instant,
    ledger: ReplayLedger,
    status: ExecutionStatus,
    input: Any?,
    result: Any?,
    error: Throwable?,
): InvocationEnded =
    InvocationEnded(
        requestId = request.requestId,
        executionArn = request.executionArn,
        firstInvocation = firstInvocation,
        executionStartedAt = startedAt,
        operations =
            ledger.snapshot().mapValues { (_, operation) ->
                operation.toSnapshot(
                    ledger.wasPresentAtInvocationStart(operation.identity.id),
                )
            },
        status = status,
        error = error,
        input = input,
        result = result,
    )

private fun Throwable.toEngineError(): CheckpointError =
    CheckpointError(
        type = this::class.qualifiedName,
        message = message,
        stack =
            stackTrace.map {
                "${it.className}|${it.methodName}|${it.fileName.orEmpty()}|${it.lineNumber}"
            },
    )
