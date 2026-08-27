import com.amazonaws.services.lambda.runtime.RequestStreamHandler
import io.github.zhongkechen.durable.DurableRuntimeConfig
import io.github.zhongkechen.durable.testing.LocalDurableRunner
import io.github.zhongkechen.durable.testing.LocalExecutionStatus
import kotlin.test.Test
import kotlin.test.assertEquals
import step.StepAndWaitReplay
import step.StepBasic
import step.StepCustomSerdes
import step.StepDefaultRetry
import step.StepErrorCaught
import step.StepNested
import step.StepWithRetry
import wait.WaitMultipleSequential

class StepWaitHandlersTest {
    @Test
    fun basicAndNestedStepsComplete() {
        assertEquals(
            "Hello, Kotlin!",
            run("Kotlin") { StepBasic(it) },
        )
        assertEquals(
            "first_second",
            run<Any?, String>(null) { StepNested(it) },
        )
    }

    @Test
    fun replayAndErrorHandlingStepsComplete() {
        assertEquals(
            "computed",
            run<Any?, String>(null) { StepAndWaitReplay(it) },
        )
        assertEquals(
            "fallback_result",
            run<Any?, String>(null) { StepErrorCaught(it) },
        )
    }

    @Test
    fun customStepSerializationCompletes() {
        assertEquals(
            "VALUE",
            run("value") { StepCustomSerdes(it) },
        )
    }

    @Test
    fun retryHandlersComplete() {
        assertEquals(
            "Operation succeeded",
            run<Any?, String>(null) { StepWithRetry(it) },
        )
        assertEquals(
            "recovered",
            run<Any?, String>(null) { StepDefaultRetry(it) },
        )
    }

    @Test
    fun sequentialWaitsComplete() {
        val result =
            run<Any?, Map<String, Int>>(null) {
                WaitMultipleSequential(it)
            }
        assertEquals(2, result["completedWaits"])
    }

    private inline fun <reified I, reified O> run(
        input: I,
        noinline handler: (DurableRuntimeConfig) -> RequestStreamHandler,
    ): O {
        val runner = LocalDurableRunner.create<I, O>(handlerFactory = handler)
        val result = runner.runUntilComplete(input)
        assertEquals(LocalExecutionStatus.SUCCEEDED, result.status, result.error?.message)
        return requireNotNull(result.result)
    }
}
