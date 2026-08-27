package io.github.zhongkechen.durable

import io.github.zhongkechen.durable.internal.CheckpointAction
import io.github.zhongkechen.durable.internal.CheckpointCommand
import io.github.zhongkechen.durable.internal.OperationIdentity
import io.github.zhongkechen.durable.internal.OperationKind
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.time.Duration.Companion.seconds

class BackendTest {
    @Test
    fun `backend updates round trip through the runtime adapter`() {
        val command =
            CheckpointCommand(
                identity =
                    OperationIdentity(
                        id = "op-1",
                        name = "pause",
                        kind = OperationKind.WAIT,
                        subtype = "Wait",
                        parentId = null,
                    ),
                action = CheckpointAction.START,
                waitDuration = 3.seconds,
            )

        val roundTripped = command.toPublic().toInternal()

        assertEquals(command, roundTripped)
    }
}
