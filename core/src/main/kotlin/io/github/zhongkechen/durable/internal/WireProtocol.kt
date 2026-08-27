package io.github.zhongkechen.durable.internal

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.ObjectMapper
import io.github.zhongkechen.durable.JsonSerde
import java.time.Instant

internal data class WireInvocation(
    val executionArn: String,
    val checkpointToken: String,
    val inputPayload: String,
    val operations: List<OperationRecord>,
    val updatedOperationIds: Set<String>,
    val nextMarker: String?,
)

internal class WireProtocol(
    private val mapper: ObjectMapper = JsonSerde.defaultMapper(),
) {
    fun decodeInvocation(json: String): WireInvocation {
        val root = mapper.readTree(json)
        val executionArn = root.requiredText("DurableExecutionArn")
        val checkpointToken = root.requiredText("CheckpointToken")
        val state = root.path("InitialExecutionState")
        val operations =
            state.path("Operations")
                .takeIf(JsonNode::isArray)
                ?.map(::decodeOperation)
                .orEmpty()
        val execution =
            operations.firstOrNull { it.identity.kind == OperationKind.EXECUTION }
                ?: error("Initial execution state does not contain an EXECUTION operation")
        val inputPayload =
            state.path("Operations")
                .firstOrNull {
                    it.path("Type").asText() == "EXECUTION"
                }?.path("ExecutionDetails")
                ?.path("InputPayload")
                ?.takeUnless(JsonNode::isMissingNode)
                ?.asText()
                ?: execution.resultPayload
                ?: "null"
        return WireInvocation(
            executionArn = executionArn,
            checkpointToken = checkpointToken,
            inputPayload = inputPayload,
            operations = operations,
            updatedOperationIds =
                root.path("UpdatedOperationIds")
                    .takeIf(JsonNode::isArray)
                    ?.mapTo(mutableSetOf(), JsonNode::asText)
                    .orEmpty(),
            nextMarker = state.path("NextMarker").nullableText(),
        )
    }

    fun encodeResult(result: EngineResult): String {
        val root = mapper.createObjectNode()
        when (result) {
            is EngineResult.Success -> {
                root.put("Status", "SUCCEEDED")
                root.put("Result", result.payload)
                root.putNull("Error")
            }
            is EngineResult.Pending -> {
                root.put("Status", "PENDING")
                root.putNull("Result")
                root.putNull("Error")
            }
            is EngineResult.Failure -> {
                root.put("Status", "FAILED")
                root.putNull("Result")
                val error = root.putObject("Error")
                result.error.type?.let { error.put("ErrorType", it) } ?: error.putNull("ErrorType")
                result.error.message?.let { error.put("ErrorMessage", it) } ?: error.putNull("ErrorMessage")
                result.error.data?.let { error.put("ErrorData", it) } ?: error.putNull("ErrorData")
                val stack = error.putArray("StackTrace")
                result.error.stack.forEach(stack::add)
            }
        }
        return mapper.writeValueAsString(root)
    }

    private fun decodeOperation(node: JsonNode): OperationRecord {
        val kind = node.requiredText("Type").toOperationKind()
        val step = node.path("StepDetails")
        val context = node.path("ContextDetails")
        val callback = node.path("CallbackDetails")
        val invoke = node.path("ChainedInvokeDetails")
        val error =
            sequenceOf(step, context, callback, invoke)
                .map { it.path("Error") }
                .firstOrNull { !it.isMissingNode && !it.isNull }
                ?.let(::decodeError)
        val result =
            when (kind) {
                OperationKind.EXECUTION ->
                    node.path("ExecutionDetails").path("InputPayload").nullableText()
                OperationKind.STEP -> step.path("Result").nullableText()
                OperationKind.INVOKE -> invoke.path("Result").nullableText()
                OperationKind.CALLBACK -> callback.path("Result").nullableText()
                OperationKind.CONTEXT -> context.path("Result").nullableText()
                OperationKind.WAIT -> null
            }
        return OperationRecord(
            identity =
                OperationIdentity(
                    id = node.requiredText("Id"),
                    name = node.path("Name").nullableText(),
                    kind = kind,
                    subtype = node.path("SubType").nullableText() ?: kind.defaultSubtype(),
                    parentId = node.path("ParentId").nullableText(),
                ),
            status = node.requiredText("Status").toCheckpointStatus(),
            startedAt = node.path("StartTimestamp").nullableInstant(),
            endedAt = node.path("EndTimestamp").nullableInstant(),
            attempt = step.path("Attempt").takeIf(JsonNode::isNumber)?.asInt(),
            resultPayload = result,
            error = error,
            nextAttemptAt = step.path("NextAttemptTimestamp").nullableInstant(),
            replayChildren = context.path("ReplayChildren").asBoolean(false),
            callbackId = callback.path("CallbackId").nullableText(),
        )
    }

    private fun decodeError(node: JsonNode): CheckpointError =
        CheckpointError(
            type = node.path("ErrorType").nullableText(),
            message = node.path("ErrorMessage").nullableText(),
            data = node.path("ErrorData").nullableText(),
            stack =
                node.path("StackTrace")
                    .takeIf(JsonNode::isArray)
                    ?.map(JsonNode::asText)
                    .orEmpty(),
        )
}

private fun JsonNode.requiredText(name: String): String =
    path(name).nullableText() ?: error("Required field $name is missing")

private fun JsonNode.nullableText(): String? =
    takeUnless { isMissingNode || isNull }?.asText()?.takeUnless(String::isEmpty)

private fun JsonNode.nullableInstant(): Instant? {
    if (isMissingNode || isNull) return null
    if (isNumber) return Instant.ofEpochMilli(asLong())
    return Instant.parse(asText().replace(' ', 'T'))
}

private fun String.toOperationKind(): OperationKind =
    when (this) {
        "EXECUTION" -> OperationKind.EXECUTION
        "STEP" -> OperationKind.STEP
        "WAIT" -> OperationKind.WAIT
        "CHAINED_INVOKE" -> OperationKind.INVOKE
        "CALLBACK" -> OperationKind.CALLBACK
        "CONTEXT" -> OperationKind.CONTEXT
        else -> error("Unknown operation type: $this")
    }

private fun String.toCheckpointStatus(): CheckpointStatus =
    CheckpointStatus.entries.firstOrNull { it.name == this } ?: CheckpointStatus.UNKNOWN

private fun OperationKind.defaultSubtype(): String =
    when (this) {
        OperationKind.EXECUTION -> "Execution"
        OperationKind.STEP -> "Step"
        OperationKind.WAIT -> "Wait"
        OperationKind.INVOKE -> "ChainedInvoke"
        OperationKind.CALLBACK -> "Callback"
        OperationKind.CONTEXT -> "Context"
    }
