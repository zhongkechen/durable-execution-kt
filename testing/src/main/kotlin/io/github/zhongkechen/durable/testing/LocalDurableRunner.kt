package io.github.zhongkechen.durable.testing

import com.amazonaws.services.lambda.runtime.ClientContext
import com.amazonaws.services.lambda.runtime.CognitoIdentity
import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.LambdaLogger
import com.amazonaws.services.lambda.runtime.RequestStreamHandler
import com.fasterxml.jackson.databind.ObjectMapper
import io.github.zhongkechen.durable.BackendError
import io.github.zhongkechen.durable.BackendOperation
import io.github.zhongkechen.durable.BackendOperationType
import io.github.zhongkechen.durable.DurableRuntimeConfig
import io.github.zhongkechen.durable.JsonSerde
import io.github.zhongkechen.durable.Serde
import io.github.zhongkechen.durable.TypeRef
import io.github.zhongkechen.durable.typeRef
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.nio.charset.Charset
import java.util.UUID

public enum class LocalExecutionStatus {
    SUCCEEDED,
    FAILED,
    PENDING,
}

public data class LocalExecutionResult<O>(
    val status: LocalExecutionStatus,
    val result: O?,
    val error: BackendError?,
    val invocations: Int,
    val operations: List<BackendOperation>,
)

public class LocalDurableRunner<I, O>(
    handlerFactory: (DurableRuntimeConfig) -> RequestStreamHandler,
    private val inputType: TypeRef<I>,
    private val outputType: TypeRef<O>,
    private val serde: Serde = JsonSerde(),
) {
    public val backend: InMemoryDurableBackend = InMemoryDurableBackend()
    private val mapper: ObjectMapper = JsonSerde.defaultMapper()
    private val handler =
        handlerFactory(
            DurableRuntimeConfig(
                serde = serde,
                backend = backend,
            ),
        )

    public fun runUntilComplete(
        input: I,
        maximumInvocations: Int = 100,
    ): LocalExecutionResult<O> {
        require(maximumInvocations >= 1) { "maximumInvocations must be positive" }
        val executionId = UUID.randomUUID().toString()
        val executionArn = "local/$executionId"
        val inputPayload = serde.encode(input)
        backend.initializeExecution(executionId, inputPayload)

        repeat(maximumInvocations) { index ->
            val output = invoke(executionArn, inputPayload)
            val status = output.path("Status").asText()
            when (status) {
                "SUCCEEDED" -> {
                    val payload = output.path("Result").asText()
                    return LocalExecutionResult(
                        status = LocalExecutionStatus.SUCCEEDED,
                        result = serde.decode(payload, outputType),
                        error = null,
                        invocations = index + 1,
                        operations = backend.snapshot(),
                    )
                }
                "FAILED" ->
                    return LocalExecutionResult(
                        status = LocalExecutionStatus.FAILED,
                        result = null,
                        error =
                            BackendError(
                                type = output.path("Error").path("ErrorType").nullableText(),
                                message = output.path("Error").path("ErrorMessage").nullableText(),
                                data = output.path("Error").path("ErrorData").nullableText(),
                                stack =
                                    output.path("Error").path("StackTrace")
                                        .takeIf { it.isArray }
                                        ?.map { it.asText() }
                                        .orEmpty(),
                            ),
                        invocations = index + 1,
                        operations = backend.snapshot(),
                    )
                "PENDING" -> backend.advanceExternalOperations()
                else -> error("Unknown local execution status: $status")
            }
        }
        return LocalExecutionResult(
            status = LocalExecutionStatus.PENDING,
            result = null,
            error = null,
            invocations = maximumInvocations,
            operations = backend.snapshot(),
        )
    }

    private fun invoke(
        executionArn: String,
        inputPayload: String,
    ) = ByteArrayOutputStream().use { output ->
        handler.handleRequest(
            ByteArrayInputStream(envelope(executionArn, inputPayload).encodeToByteArray()),
            output,
            LocalLambdaContext,
        )
        mapper.readTree(output.toString(Charset.defaultCharset()))
    }

    private fun envelope(
        executionArn: String,
        inputPayload: String,
    ): String {
        val root = mapper.createObjectNode()
        root.put("DurableExecutionArn", executionArn)
        root.put("CheckpointToken", "local-token")
        val state = root.putObject("InitialExecutionState")
        val operations = state.putArray("Operations")
        backend.snapshot().forEach { operation ->
            val node = operations.addObject()
            node.put("Id", operation.id)
            operation.name?.let { node.put("Name", it) }
            node.put(
                "Type",
                when (operation.type) {
                    BackendOperationType.INVOKE -> "CHAINED_INVOKE"
                    else -> operation.type.name
                },
            )
            node.put("SubType", operation.subtype)
            operation.parentId?.let { node.put("ParentId", it) }
            node.put("Status", operation.status.name)
            operation.startedAt?.let { node.put("StartTimestamp", it.toString()) }
            operation.endedAt?.let { node.put("EndTimestamp", it.toString()) }
            when (operation.type) {
                BackendOperationType.EXECUTION ->
                    node.putObject("ExecutionDetails").put("InputPayload", inputPayload)
                BackendOperationType.STEP -> {
                    val details = node.putObject("StepDetails")
                    operation.attempt?.let { details.put("Attempt", it) }
                    operation.resultPayload?.let { details.put("Result", it) }
                    operation.nextAttemptAt?.let { details.put("NextAttemptTimestamp", it.toString()) }
                    operation.error?.let { details.set<com.fasterxml.jackson.databind.JsonNode>("Error", errorNode(it)) }
                }
                BackendOperationType.CALLBACK -> {
                    val details = node.putObject("CallbackDetails")
                    operation.callbackId?.let { details.put("CallbackId", it) }
                    operation.resultPayload?.let { details.put("Result", it) }
                    operation.error?.let { details.set<com.fasterxml.jackson.databind.JsonNode>("Error", errorNode(it)) }
                }
                BackendOperationType.INVOKE -> {
                    val details = node.putObject("ChainedInvokeDetails")
                    operation.resultPayload?.let { details.put("Result", it) }
                    operation.error?.let { details.set<com.fasterxml.jackson.databind.JsonNode>("Error", errorNode(it)) }
                }
                BackendOperationType.CONTEXT -> {
                    val details = node.putObject("ContextDetails")
                    operation.resultPayload?.let { details.put("Result", it) }
                    details.put("ReplayChildren", operation.replayChildren)
                    operation.error?.let { details.set<com.fasterxml.jackson.databind.JsonNode>("Error", errorNode(it)) }
                }
                BackendOperationType.WAIT -> Unit
            }
        }
        state.putNull("NextMarker")
        return mapper.writeValueAsString(root)
    }

    private fun errorNode(error: BackendError) =
        mapper.createObjectNode().apply {
            error.type?.let { put("ErrorType", it) }
            error.message?.let { put("ErrorMessage", it) }
            error.data?.let { put("ErrorData", it) }
            val frames = putArray("StackTrace")
            error.stack.forEach(frames::add)
        }

    public companion object {
        public inline fun <reified I, reified O> create(
            serde: Serde = JsonSerde(),
            noinline handlerFactory: (DurableRuntimeConfig) -> RequestStreamHandler,
        ): LocalDurableRunner<I, O> =
            LocalDurableRunner(handlerFactory, typeRef(), typeRef(), serde)
    }
}

private fun com.fasterxml.jackson.databind.JsonNode.nullableText(): String? =
    takeUnless { isMissingNode || isNull }?.asText()

private object LocalLambdaContext : Context {
    override fun getAwsRequestId(): String = UUID.randomUUID().toString()
    override fun getLogGroupName(): String = "local"
    override fun getLogStreamName(): String = "local"
    override fun getFunctionName(): String = "local"
    override fun getFunctionVersion(): String = "1"
    override fun getInvokedFunctionArn(): String = "local"
    override fun getIdentity(): CognitoIdentity? = null
    override fun getClientContext(): ClientContext? = null
    override fun getRemainingTimeInMillis(): Int = Int.MAX_VALUE
    override fun getMemoryLimitInMB(): Int = 1024
    override fun getLogger(): LambdaLogger =
        object : LambdaLogger {
            override fun log(message: String) {
                print(message)
            }

            override fun log(message: ByteArray) {
                print(message.decodeToString())
            }
        }
}
