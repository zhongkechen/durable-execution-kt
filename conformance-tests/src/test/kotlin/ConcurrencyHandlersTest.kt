import com.amazonaws.services.lambda.runtime.RequestStreamHandler
import io.github.zhongkechen.durable.DurableRuntimeConfig
import io.github.zhongkechen.durable.testing.LocalDurableRunner
import io.github.zhongkechen.durable.testing.LocalExecutionStatus
import kotlin.test.Test
import kotlin.test.assertEquals
import map.MapBasic
import map.MapFailFast
import map.MapItemSerdes
import map.MapItemsOnly
import map.MapOpSerde
import map.MapOpSerdeReplay
import map.MapSuspendIteration
import map.MapThrowIfError
import parallel.ParallelBasic
import parallel.ParallelFailFast
import parallel.ParallelNested

class ConcurrencyHandlersTest {
    @Test
    fun mapHandlersComplete() {
        assertEquals(
            listOf("Hello, World!", "Hello, Kiro!"),
            run<Any?, List<String>>(null) { MapBasic(it) },
        )
        assertEquals(
            "FAILURE_TOLERANCE_EXCEEDED",
            run<Any?, Map<String, Any>>(null) { MapFailFast(it) }["completionReason"],
        )
        assertEquals(listOf("r0", "r1"), run<Any?, List<String>>(null) { MapSuspendIteration(it) })
        assertEquals(listOf("X", "Y"), run<Any?, List<String>>(null) { MapItemSerdes(it) })
        assertEquals(listOf("X", "Y"), run<Any?, List<String>>(null) { MapOpSerde(it) })
        assertEquals(listOf("X", "Y"), run<Any?, List<String>>(null) { MapOpSerdeReplay(it) })
    }

    @Test
    fun mapKotlinSpecificCapabilitiesComplete() {
        assertEquals(
            listOf(2, 4),
            run(listOf(1, 2)) { MapItemsOnly(it) },
        )
        val failed =
            LocalDurableRunner
                .create<Any?, List<String>> { MapThrowIfError(it) }
                .runUntilComplete(null)
        assertEquals(LocalExecutionStatus.FAILED, failed.status)
    }

    @Test
    fun parallelHandlersComplete() {
        assertEquals(
            listOf("task-1", "task-2"),
            run<Any?, List<String>>(null) { ParallelBasic(it) },
        )
        assertEquals(
            "FAILURE_TOLERANCE_EXCEEDED",
            run<Any?, Map<String, Any>>(null) { ParallelFailFast(it) }["completionReason"],
        )
        assertEquals(
            listOf(listOf("i1", "i2")),
            run<Any?, Any>(null) { ParallelNested(it) },
        )
    }

    private inline fun <reified I, reified O> run(
        input: I,
        noinline handler: (DurableRuntimeConfig) -> RequestStreamHandler,
    ): O {
        val result = LocalDurableRunner.create<I, O>(handlerFactory = handler).runUntilComplete(input)
        assertEquals(LocalExecutionStatus.SUCCEEDED, result.status, result.error?.message)
        return requireNotNull(result.result)
    }
}
