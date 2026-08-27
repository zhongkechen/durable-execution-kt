package io.github.zhongkechen.durable.internal

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertNull
import kotlin.test.assertTrue

class WireProtocolTest {
    private val wire = WireProtocol()

    @Test
    fun `invocation payload decodes without SDK model reflection`() {
        val invocation =
            wire.decodeInvocation(
                """
                {
                  "DurableExecutionArn": "arn:test",
                  "CheckpointToken": "token-1",
                  "UpdatedOperationIds": ["step-1"],
                  "InitialExecutionState": {
                    "Operations": [
                      {
                        "Id": "execution-id",
                        "Type": "EXECUTION",
                        "Status": "STARTED",
                        "StartTimestamp": 1767356934930,
                        "ExecutionDetails": {"InputPayload": "{\"name\":\"Alice\"}"}
                      },
                      {
                        "Id": "step-1",
                        "Name": "greet",
                        "Type": "STEP",
                        "SubType": "Step",
                        "Status": "SUCCEEDED",
                        "StartTimestamp": "2025-12-18 10:53:55.057877+00:00",
                        "EndTimestamp": "2025-12-18 10:53:57.413501+00:00",
                        "StepDetails": {"Attempt": 1, "Result": "\"hello\""}
                      }
                    ],
                    "NextMarker": ""
                  }
                }
                """.trimIndent(),
            )

        assertEquals("arn:test", invocation.executionArn)
        assertEquals("{\"name\":\"Alice\"}", invocation.inputPayload)
        assertEquals(setOf("step-1"), invocation.updatedOperationIds)
        assertNull(invocation.nextMarker)
        assertEquals(CheckpointStatus.SUCCEEDED, invocation.operations[1].status)
        assertEquals("\"hello\"", invocation.operations[1].resultPayload)
    }

    @Test
    fun `engine outputs use service field casing`() {
        val success = wire.encodeResult(EngineResult.Success("\"done\""))
        val pending = wire.encodeResult(EngineResult.Pending("wait", null))
        val failure =
            wire.encodeResult(
                EngineResult.Failure(
                    CheckpointError(
                        type = "Example",
                        message = "broken",
                        stack = listOf("A|b|A.kt|1"),
                    ),
                ),
            )

        assertTrue(success.contains("\"Status\":\"SUCCEEDED\""))
        assertTrue(success.contains("\"Result\":\"\\\"done\\\"\""))
        assertTrue(pending.contains("\"Status\":\"PENDING\""))
        assertTrue(failure.contains("\"ErrorType\":\"Example\""))
        assertTrue(failure.contains("\"ErrorMessage\":\"broken\""))
    }
}
