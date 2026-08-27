package io.github.zhongkechen.durable.internal

import io.github.zhongkechen.durable.DurableContext
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
) {
    suspend fun <I, O> execute(
        request: InvocationRequest,
        inputType: TypeRef<I>,
        outputType: TypeRef<O>,
        handler: suspend (I, DurableContext) -> O,
    ): EngineResult {
        val ledger = ReplayLedger(request.initialOperations, request.updatedOperationIds)
        val checkpoints =
            CheckpointCoordinator(
                service = service,
                executionArn = request.executionArn,
                checkpointToken = request.checkpointToken,
                ledger = ledger,
                coroutineContext = serviceContext,
                batchWindow = checkpointBatchWindow,
            )
        val runtime =
            OperationRuntime(
                executionArn = request.executionArn,
                isReplaying = request.initialOperations.any { it.identity.kind != OperationKind.EXECUTION },
                ledger = ledger,
                checkpoints = checkpoints,
                defaultSerde = serde,
            )
        return try {
            val input = serde.decode(request.inputPayload, inputType)
            val result = handler(input, RuntimeDurableContext(runtime))
            val payload = serde.encode(result)
            serde.decode(payload, outputType)
            EngineResult.Success(payload)
        } catch (suspension: ExecutionSuspended) {
            EngineResult.Pending(suspension.operationId, suspension.resumeAt)
        } catch (error: Throwable) {
            EngineResult.Failure(error.toEngineError())
        } finally {
            checkpoints.close()
            checkpoints.join()
        }
    }
}

private fun Throwable.toEngineError(): CheckpointError =
    CheckpointError(
        type = this::class.qualifiedName,
        message = message,
        stack =
            stackTrace.map {
                "${it.className}|${it.methodName}|${it.fileName.orEmpty()}|${it.lineNumber}"
            },
    )
