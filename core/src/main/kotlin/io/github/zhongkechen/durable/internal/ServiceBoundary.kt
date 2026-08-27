package io.github.zhongkechen.durable.internal

import java.time.Instant
import kotlin.time.Duration
import software.amazon.awssdk.services.lambda.LambdaClient
import software.amazon.awssdk.services.lambda.model.CallbackOptions
import software.amazon.awssdk.services.lambda.model.ChainedInvokeOptions
import software.amazon.awssdk.services.lambda.model.CheckpointDurableExecutionRequest
import software.amazon.awssdk.services.lambda.model.ContextOptions
import software.amazon.awssdk.services.lambda.model.ErrorObject
import software.amazon.awssdk.services.lambda.model.GetDurableExecutionStateRequest
import software.amazon.awssdk.services.lambda.model.Operation
import software.amazon.awssdk.services.lambda.model.OperationAction
import software.amazon.awssdk.services.lambda.model.OperationType
import software.amazon.awssdk.services.lambda.model.OperationUpdate
import software.amazon.awssdk.services.lambda.model.StepOptions
import software.amazon.awssdk.services.lambda.model.WaitOptions

internal enum class CheckpointAction {
    START,
    SUCCEED,
    FAIL,
    RETRY,
}

internal data class CheckpointCommand(
    val identity: OperationIdentity,
    val action: CheckpointAction,
    val payload: String? = null,
    val error: CheckpointError? = null,
    val retryDelay: Duration? = null,
    val waitDuration: Duration? = null,
    val targetFunction: String? = null,
    val tenantId: String? = null,
    val callbackTimeout: Duration? = null,
    val heartbeatTimeout: Duration? = null,
    val replayChildren: Boolean = false,
) {
    fun toSdkUpdate(): OperationUpdate {
        val builder =
            OperationUpdate
                .builder()
                .id(identity.id)
                .name(identity.name)
                .type(identity.kind.toSdkType())
                .subType(identity.subtype)
                .parentId(identity.parentId)
                .action(action.toSdkAction())
                .payload(payload)

        error?.let { builder.error(it.toSdkError()) }
        retryDelay?.let {
            builder.stepOptions(
                StepOptions
                    .builder()
                    .nextAttemptDelaySeconds(it.inWholeSeconds.toInt())
                    .build(),
            )
        }
        waitDuration?.let {
            builder.waitOptions(
                WaitOptions
                    .builder()
                    .waitSeconds(it.inWholeSeconds.toInt())
                    .build(),
            )
        }
        targetFunction?.let {
            builder.chainedInvokeOptions(
                ChainedInvokeOptions
                    .builder()
                    .functionName(it)
                    .tenantId(tenantId)
                    .build(),
            )
        }
        if (callbackTimeout != null || heartbeatTimeout != null) {
            builder.callbackOptions(
                CallbackOptions
                    .builder()
                    .timeoutSeconds(callbackTimeout?.inWholeSeconds?.toInt())
                    .heartbeatTimeoutSeconds(heartbeatTimeout?.inWholeSeconds?.toInt())
                    .build(),
            )
        }
        if (replayChildren) {
            builder.contextOptions(ContextOptions.builder().replayChildren(true).build())
        }
        return builder.build()
    }
}

internal data class ServicePage(
    val operations: List<OperationRecord>,
    val nextMarker: String?,
)

internal data class CheckpointReply(
    val checkpointToken: String,
    val state: ServicePage?,
)

internal interface DurableService {
    fun checkpoint(
        executionArn: String,
        checkpointToken: String,
        commands: List<CheckpointCommand>,
    ): CheckpointReply

    fun getState(
        executionArn: String,
        checkpointToken: String,
        marker: String?,
    ): ServicePage
}

internal class LambdaDurableService(
    private val client: LambdaClient,
) : DurableService {
    override fun checkpoint(
        executionArn: String,
        checkpointToken: String,
        commands: List<CheckpointCommand>,
    ): CheckpointReply {
        val response =
            client.checkpointDurableExecution(
                CheckpointDurableExecutionRequest
                    .builder()
                    .durableExecutionArn(executionArn)
                    .checkpointToken(checkpointToken)
                    .updates(commands.map(CheckpointCommand::toSdkUpdate))
                    .build(),
            )
        val state =
            response.newExecutionState()?.let {
                ServicePage(
                    operations = it.operations().map(Operation::toRecord),
                    nextMarker = it.nextMarker(),
                )
            }
        return CheckpointReply(response.checkpointToken(), state)
    }

    override fun getState(
        executionArn: String,
        checkpointToken: String,
        marker: String?,
    ): ServicePage {
        val response =
            client.getDurableExecutionState(
                GetDurableExecutionStateRequest
                    .builder()
                    .durableExecutionArn(executionArn)
                    .checkpointToken(checkpointToken)
                    .marker(marker)
                    .build(),
            )
        return ServicePage(
            operations = response.operations().map(Operation::toRecord),
            nextMarker = response.nextMarker(),
        )
    }
}

internal fun Operation.toRecord(): OperationRecord =
    OperationRecord(
        identity =
            OperationIdentity(
                id = id(),
                name = name(),
                kind = type().toKind(),
                subtype = subType() ?: typeAsString(),
                parentId = parentId(),
            ),
        status = statusAsString().toCheckpointStatus(),
        startedAt = startTimestamp(),
        endedAt = endTimestamp(),
        attempt = stepDetails()?.attempt(),
        resultPayload = resultPayload(),
        error = errorObject()?.toCheckpointError(),
        nextAttemptAt = stepDetails()?.nextAttemptTimestamp(),
        replayChildren = contextDetails()?.replayChildren() == true,
        callbackId = callbackDetails()?.callbackId(),
    )

private fun Operation.resultPayload(): String? =
    when (type()) {
        OperationType.STEP -> stepDetails()?.result()
        OperationType.CHAINED_INVOKE -> chainedInvokeDetails()?.result()
        OperationType.CALLBACK -> callbackDetails()?.result()
        OperationType.CONTEXT -> contextDetails()?.result()
        else -> null
    }

private fun Operation.errorObject(): ErrorObject? =
    when (type()) {
        OperationType.STEP -> stepDetails()?.error()
        OperationType.CHAINED_INVOKE -> chainedInvokeDetails()?.error()
        OperationType.CALLBACK -> callbackDetails()?.error()
        OperationType.CONTEXT -> contextDetails()?.error()
        else -> null
    }

private fun CheckpointError.toSdkError(): ErrorObject =
    ErrorObject
        .builder()
        .errorType(type)
        .errorMessage(message)
        .errorData(data)
        .stackTrace(stack)
        .build()

private fun ErrorObject.toCheckpointError(): CheckpointError =
    CheckpointError(
        type = errorType(),
        message = errorMessage(),
        data = errorData(),
        stack = stackTrace().orEmpty(),
    )

private fun OperationKind.toSdkType(): OperationType =
    when (this) {
        OperationKind.EXECUTION -> OperationType.EXECUTION
        OperationKind.STEP -> OperationType.STEP
        OperationKind.WAIT -> OperationType.WAIT
        OperationKind.INVOKE -> OperationType.CHAINED_INVOKE
        OperationKind.CALLBACK -> OperationType.CALLBACK
        OperationKind.CONTEXT -> OperationType.CONTEXT
    }

private fun OperationType.toKind(): OperationKind =
    when (this) {
        OperationType.EXECUTION -> OperationKind.EXECUTION
        OperationType.STEP -> OperationKind.STEP
        OperationType.WAIT -> OperationKind.WAIT
        OperationType.CHAINED_INVOKE -> OperationKind.INVOKE
        OperationType.CALLBACK -> OperationKind.CALLBACK
        OperationType.CONTEXT -> OperationKind.CONTEXT
        else -> OperationKind.CONTEXT
    }

private fun CheckpointAction.toSdkAction(): OperationAction =
    when (this) {
        CheckpointAction.START -> OperationAction.START
        CheckpointAction.SUCCEED -> OperationAction.SUCCEED
        CheckpointAction.FAIL -> OperationAction.FAIL
        CheckpointAction.RETRY -> OperationAction.RETRY
    }

private fun String.toCheckpointStatus(): CheckpointStatus =
    CheckpointStatus.entries.firstOrNull { it.name == this } ?: CheckpointStatus.UNKNOWN
