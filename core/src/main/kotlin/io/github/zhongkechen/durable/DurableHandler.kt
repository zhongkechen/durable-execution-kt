package io.github.zhongkechen.durable

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestStreamHandler
import io.github.zhongkechen.durable.internal.DurableService
import io.github.zhongkechen.durable.internal.ExecutionEngine
import io.github.zhongkechen.durable.internal.InvocationRequest
import io.github.zhongkechen.durable.internal.BackendDurableService
import io.github.zhongkechen.durable.internal.WireProtocol
import java.io.InputStream
import java.io.OutputStream
import java.util.concurrent.ExecutorService
import java.util.concurrent.Executors
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.asCoroutineDispatcher
import kotlinx.coroutines.runBlocking

public data class DurableRuntimeConfig(
    val serde: Serde = JsonSerde(),
    val backend: DurableBackend = LambdaBackend(),
    val dispatcher: CoroutineDispatcher = DurableDispatchers.virtualThreads,
    val checkpointBatchWindow: Duration = 5.milliseconds,
)

public object DurableDispatchers {
    public val executor: ExecutorService = Executors.newVirtualThreadPerTaskExecutor()
    public val virtualThreads: CoroutineDispatcher = executor.asCoroutineDispatcher()
}

/**
 * Stream handler for coroutine-based durable functions.
 */
public abstract class DurableHandler<I, O>(
    private val inputType: TypeRef<I>,
    private val outputType: TypeRef<O>,
    public val config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : RequestStreamHandler {
    private val wire = WireProtocol()
    private val service: DurableService = BackendDurableService(config.backend)
    private val engine =
        ExecutionEngine(
            service = service,
            serde = config.serde,
            serviceContext = config.dispatcher,
            checkpointBatchWindow = config.checkpointBatchWindow,
        )

    final override fun handleRequest(
        input: InputStream,
        output: OutputStream,
        context: Context,
    ) {
        val invocation = wire.decodeInvocation(input.readAllBytes().decodeToString())
        val initialOperations = invocation.operations.toMutableList()
        var marker = invocation.nextMarker
        while (!marker.isNullOrEmpty()) {
            val page =
                service.getState(
                    executionArn = invocation.executionArn,
                    checkpointToken = invocation.checkpointToken,
                    marker = marker,
                )
            initialOperations += page.operations
            marker = page.nextMarker
        }
        val result =
            runBlocking(config.dispatcher) {
                engine.execute(
                    request =
                        InvocationRequest(
                            executionArn = invocation.executionArn,
                            checkpointToken = invocation.checkpointToken,
                            inputPayload = invocation.inputPayload,
                            initialOperations = initialOperations,
                            updatedOperationIds = invocation.updatedOperationIds,
                        ),
                    inputType = inputType,
                    outputType = outputType,
                    handler = ::handle,
                )
            }
        output.write(wire.encodeResult(result).encodeToByteArray())
    }

    protected abstract suspend fun handle(
        input: I,
        context: DurableContext,
    ): O
}

public inline fun <reified I, reified O> durableHandler(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
    crossinline block: suspend (I, DurableContext) -> O,
): RequestStreamHandler =
    object : DurableHandler<I, O>(typeRef(), typeRef(), config) {
        override suspend fun handle(
            input: I,
            context: DurableContext,
        ): O = block(input, context)
    }
