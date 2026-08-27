import child.ChildBasic
import child.ChildCustomSerdes
import child.ChildLargePayload
import child.ChildNested
import child.ChildReplay
import child.ChildWithRetry
import com.amazonaws.services.lambda.runtime.RequestStreamHandler
import io.github.zhongkechen.durable.DurableRuntimeConfig
import io.github.zhongkechen.durable.testing.LocalDurableRunner
import io.github.zhongkechen.durable.testing.LocalExecutionStatus
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue

class ChildHandlersTest {
    @Test
    fun basicNestedAndRetryChildrenComplete() {
        assertEquals("value", run("value") { ChildBasic(it) })
        assertEquals("nested", run("nested") { ChildNested(it) })
        assertEquals("retry", run("retry") { ChildWithRetry(it) })
    }

    @Test
    fun replayAndCustomSerializationComplete() {
        assertEquals("cached", run("cached") { ChildReplay(it) })
        assertEquals("VALUE", run("value") { ChildCustomSerdes(it) })
    }

    @Test
    fun largeChildResultReconstructsByReplayingChildren() {
        val result = run<Any?, String>(null) { ChildLargePayload(it) }
        assertTrue(result.length >= 1_000_000)
        assertTrue(result.startsWith("seed"))
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
