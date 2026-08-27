package io.github.zhongkechen.durable.internal

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.seconds
import software.amazon.awssdk.services.lambda.model.Operation
import software.amazon.awssdk.services.lambda.model.OperationStatus
import software.amazon.awssdk.services.lambda.model.OperationType
import software.amazon.awssdk.services.lambda.model.StepDetails

class ServiceBoundaryTest {
    @Test
    fun `checkpoint commands preserve identity and operation options`() {
        val identity =
            OperationIdentity(
                id = "op-1",
                name = "pause",
                kind = OperationKind.WAIT,
                subtype = "Wait",
                parentId = "parent",
            )

        val update =
            CheckpointCommand(
                identity = identity,
                action = CheckpointAction.START,
                waitDuration = 7.seconds,
            ).toSdkUpdate()

        assertEquals(identity.id, update.id())
        assertEquals(identity.name, update.name())
        assertEquals(OperationType.WAIT, update.type())
        assertEquals("Wait", update.subType())
        assertEquals("parent", update.parentId())
        assertEquals(7, update.waitOptions().waitSeconds())
    }

    @Test
    fun `service operations become clean-room replay records`() {
        val operation =
            Operation
                .builder()
                .id("step-1")
                .name("reserve")
                .type(OperationType.STEP)
                .subType("Step")
                .status(OperationStatus.SUCCEEDED)
                .stepDetails(
                    StepDetails
                        .builder()
                        .attempt(2)
                        .result("\"ok\"")
                        .build(),
                ).build()

        val record = operation.toRecord()

        assertEquals(OperationKind.STEP, record.identity.kind)
        assertEquals(CheckpointStatus.SUCCEEDED, record.status)
        assertEquals(2, record.attempt)
        assertEquals("\"ok\"", record.resultPayload)
        assertTrue(record.status.terminal)
    }
}
