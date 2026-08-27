package io.github.zhongkechen.durable

public open class DurableExecutionException(
    message: String,
    cause: Throwable? = null,
) : RuntimeException(message, cause)

public open class DurableOperationException(
    public val operationId: String,
    message: String,
    cause: Throwable? = null,
) : DurableExecutionException(message, cause)

public class NonDeterministicExecutionException(
    message: String,
) : DurableExecutionException(message)

public class SerializationException(
    message: String,
    cause: Throwable? = null,
) : DurableExecutionException(message, cause)

public class StepFailureException(
    operationId: String,
    cause: Throwable,
) : DurableOperationException(operationId, "Step $operationId failed", cause)

public class StepInterruptedException(
    operationId: String,
) : DurableOperationException(operationId, "Step $operationId was interrupted")

public class ChildFailureException(
    operationId: String,
    cause: Throwable,
) : DurableOperationException(operationId, "Child context $operationId failed", cause)

public class InvokeFailureException(
    operationId: String,
    cause: Throwable,
) : DurableOperationException(operationId, "Invocation $operationId failed", cause)

public class CallbackFailureException(
    operationId: String,
    cause: Throwable,
) : DurableOperationException(operationId, "Callback $operationId failed", cause)

public class ConditionFailureException(
    operationId: String,
    cause: Throwable,
) : DurableOperationException(operationId, "Condition $operationId failed", cause)

public class BatchFailureException(
    cause: Throwable,
    public val items: List<ItemResult<*>>,
) : DurableExecutionException("A batch item failed", cause)
