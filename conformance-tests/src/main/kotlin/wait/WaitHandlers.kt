package wait

import io.github.zhongkechen.durable.*

import kotlin.time.Duration.Companion.hours
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

public class WaitBasic(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(
        input: Any?,
    ): String {
        wait(2.seconds)
        return "Wait completed"
    }
}

public class WaitWithName(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(
        input: Any?,
    ): String {
        wait(2.seconds, "custom_wait_name")
        return "Wait with name completed"
    }
}

public class WaitMultipleSequential(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, Map<String, Int>>(typeRef(), typeRef(), config) {
    override suspend fun handle(
        input: Any?,
    ): Map<String, Int> {
        wait(2.seconds, "wait-1")
        wait(2.seconds, "wait-2")
        return mapOf("completedWaits" to 2)
    }
}

public class WaitDurationUnits(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(
        input: Any?,
    ): String {
        wait(1.minutes)
        return "Wait with minutes completed"
    }
}

public class WaitLongDuration(
    config: DurableRuntimeConfig = DurableRuntimeConfig(),
) : DurableHandler<Any?, String>(typeRef(), typeRef(), config) {
    override suspend fun handle(
        input: Any?,
    ): String {
        wait(1.hours)
        return "Wait with hours completed"
    }
}
