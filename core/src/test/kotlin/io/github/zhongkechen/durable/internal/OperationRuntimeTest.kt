package io.github.zhongkechen.durable.internal

import io.github.zhongkechen.durable.ChildOptions
import io.github.zhongkechen.durable.CallbackOptions
import io.github.zhongkechen.durable.InvokeOptions
import io.github.zhongkechen.durable.RetryPolicy
import io.github.zhongkechen.durable.StepOptions
import io.github.zhongkechen.durable.StepScope
import io.github.zhongkechen.durable.typeRef
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.TestDispatcher
import kotlinx.coroutines.test.runTest

class OperationRuntimeTest {
    @Test
    fun `step checkpoints start and normalized success`() =
        runtimeTest { runtime, service, _ ->
            val result =
                runtime.step("answer", typeRef<Int>()) {
                    assertEquals(1, attempt)
                    42
                }

            assertEquals(42, result)
            assertEquals(
                listOf(CheckpointAction.START, CheckpointAction.SUCCEED),
                service.commands.map { it.action },
            )
        }

    @Test
    fun `completed step replays without calling user code`() =
        runtimeTest(
            initial =
                listOf(
                    record(
                        rootIdentity("1", "answer", OperationKind.STEP, "Step"),
                        CheckpointStatus.SUCCEEDED,
                        result = "42",
                    ),
                ),
        ) { runtime, service, _ ->
            var called = false
            val result =
                runtime.step("answer", typeRef<Int>()) {
                    called = true
                    0
                }

            assertEquals(42, result)
            assertFalse(called)
            assertTrue(service.commands.isEmpty())
        }

    @Test
    fun `retry checkpoints pending state and suspends`() =
        runtimeTest { runtime, service, _ ->
            assertFailsWith<ExecutionSuspended> {
                runtime.step(
                    name = "flaky",
                    type = typeRef<String>(),
                    options = StepOptions(retry = RetryPolicy.fixed(maxAttempts = 2, delay = 1.seconds)),
                ) {
                    error("temporary")
                }
            }

            assertEquals(CheckpointAction.RETRY, service.commands.last().action)
            assertEquals(1.seconds, service.commands.last().retryDelay)
        }

    @Test
    fun `wait starts once and completed replay returns`() =
        runtimeTest { runtime, service, dispatcher ->
            assertFailsWith<ExecutionSuspended> {
                runtime.wait(5.seconds, "pause")
            }
            assertEquals(CheckpointAction.START, service.commands.single().action)

            val identity = rootIdentity("1", "pause", OperationKind.WAIT, "Wait")
            service.force(OperationRecord(identity, CheckpointStatus.SUCCEEDED))
            val replayHarness =
                runtime(
                    initial = listOf(OperationRecord(identity, CheckpointStatus.SUCCEEDED)),
                    service = service,
                    dispatcher = dispatcher,
                )
            replayHarness.runtime.wait(5.seconds, "pause")
            replayHarness.close()
        }

    @Test
    fun `started child replays its child step with replay state`() =
        runTest {
            val childIdentity = rootIdentity("1", "child", OperationKind.CONTEXT, "RunInChildContext")
            val stepIdentity =
                OperationIdentity(
                    id = OperationIdSequence(childIdentity.id).next(),
                    name = "inside",
                    kind = OperationKind.STEP,
                    subtype = "Step",
                    parentId = childIdentity.id,
                )
            val initial =
                listOf(
                    OperationRecord(childIdentity, CheckpointStatus.STARTED),
                    OperationRecord(stepIdentity, CheckpointStatus.STARTED, attempt = 0),
                )
            val service = RuntimeService(initial)
            val harness = runtime(initial, service, StandardTestDispatcher(testScheduler))

            val result =
                harness.runtime.child("child", typeRef<String>(), ChildOptions()) {
                    assertTrue(isReplaying)
                    step("inside", typeRef<String>()) { "done" }
                }

            assertEquals("done", result)
            assertEquals(
                listOf(CheckpointAction.SUCCEED, CheckpointAction.SUCCEED),
                service.commands.map { it.action },
            )
            harness.close()
        }

    @Test
    fun `invoke starts once and replays a checkpointed result`() =
        runtimeTest { runtime, service, dispatcher ->
            assertFailsWith<ExecutionSuspended> {
                runtime.invoke(
                    name = "target",
                    functionName = "other-function",
                    input = mapOf("value" to 7),
                    outputType = typeRef<String>(),
                    options = InvokeOptions(tenantId = "tenant"),
                )
            }
            val start = service.commands.single()
            assertEquals("other-function", start.targetFunction)
            assertEquals("tenant", start.tenantId)

            val identity = rootIdentity("1", "target", OperationKind.INVOKE, "ChainedInvoke")
            val completed =
                OperationRecord(
                    identity = identity,
                    status = CheckpointStatus.SUCCEEDED,
                    resultPayload = "\"done\"",
                )
            service.force(completed)
            val replayHarness = runtime(listOf(completed), service, dispatcher)
            assertEquals(
                "done",
                replayHarness.runtime.invoke(
                    "target",
                    "other-function",
                    emptyMap<String, String>(),
                    typeRef<String>(),
                ),
            )
            replayHarness.close()
        }

    @Test
    fun `callback exposes service id and suspends until completed`() =
        runtimeTest { runtime, service, dispatcher ->
            val callback = runtime.callback("approval", typeRef<String>(), CallbackOptions())

            assertEquals("callback-1", callback.id)
            assertFailsWith<ExecutionSuspended> { callback.await() }

            val identity = rootIdentity("1", "approval", OperationKind.CALLBACK, "Callback")
            val completed =
                OperationRecord(
                    identity = identity,
                    status = CheckpointStatus.SUCCEEDED,
                    resultPayload = "\"approved\"",
                    callbackId = "callback-1",
                )
            service.force(completed)
            val replayHarness = runtime(listOf(completed), service, dispatcher)
            val replayed = replayHarness.runtime.callback("approval", typeRef<String>())
            assertEquals("approved", replayed.await())
            replayHarness.close()
        }

    private fun runtimeTest(
        initial: List<OperationRecord> = emptyList(),
        block: suspend (OperationRuntime, RuntimeService, TestDispatcher) -> Unit,
    ) = runTest {
        val service = RuntimeService(initial)
        val dispatcher = StandardTestDispatcher(testScheduler)
        val harness = runtime(initial, service, dispatcher)
        try {
            block(harness.runtime, service, dispatcher)
        } finally {
            harness.close()
        }
    }

    private fun runtime(
        initial: List<OperationRecord>,
        service: RuntimeService,
        dispatcher: TestDispatcher,
    ): RuntimeHarness {
        val ledger = ReplayLedger(initial)
        val coordinator =
            CheckpointCoordinator(
                service = service,
                executionArn = "arn:test",
                checkpointToken = "token-0",
                ledger = ledger,
                coroutineContext = dispatcher,
                batchWindow = 1.milliseconds,
            )
        return RuntimeHarness(
            runtime =
                OperationRuntime(
                    executionArn = "arn:test",
                    isReplaying = initial.isNotEmpty(),
                    ledger = ledger,
                    checkpoints = coordinator,
                ),
            coordinator = coordinator,
        )
    }

    private data class RuntimeHarness(
        val runtime: OperationRuntime,
        val coordinator: CheckpointCoordinator,
    ) {
        suspend fun close() {
            coordinator.close()
            coordinator.join()
        }
    }

    private fun rootIdentity(
        localId: String,
        name: String,
        kind: OperationKind,
        subtype: String,
    ): OperationIdentity =
        OperationIdentity(
            id = OperationIdSequence.digest(localId),
            name = name,
            kind = kind,
            subtype = subtype,
            parentId = null,
        )

    private fun record(
        identity: OperationIdentity,
        status: CheckpointStatus,
        result: String? = null,
    ): OperationRecord =
        OperationRecord(
            identity = identity,
            status = status,
            resultPayload = result,
        )

    private class RuntimeService(
        initial: List<OperationRecord>,
    ) : DurableService {
        val commands = mutableListOf<CheckpointCommand>()
        private val records = initial.associateByTo(linkedMapOf()) { it.identity.id }

        override fun checkpoint(
            executionArn: String,
            checkpointToken: String,
            commands: List<CheckpointCommand>,
        ): CheckpointReply {
            this.commands += commands
            for (command in commands) {
                val previous = records[command.identity.id]
                val next =
                    when (command.action) {
                        CheckpointAction.START ->
                            OperationRecord(
                                command.identity,
                                CheckpointStatus.STARTED,
                                attempt = previous?.attempt,
                                callbackId =
                                    if (command.identity.kind == OperationKind.CALLBACK) {
                                        "callback-${records.size + 1}"
                                    } else {
                                        null
                                    },
                            )
                        CheckpointAction.SUCCEED ->
                            OperationRecord(
                                command.identity,
                                CheckpointStatus.SUCCEEDED,
                                resultPayload = command.payload,
                            )
                        CheckpointAction.FAIL ->
                            OperationRecord(
                                command.identity,
                                CheckpointStatus.FAILED,
                                error = command.error,
                            )
                        CheckpointAction.RETRY ->
                            OperationRecord(
                                command.identity,
                                CheckpointStatus.PENDING,
                                attempt = (previous?.attempt ?: 0) + 1,
                                error = command.error,
                            )
                    }
                records[command.identity.id] = next
            }
            return CheckpointReply("next-token", ServicePage(records.values.toList(), null))
        }

        override fun getState(
            executionArn: String,
            checkpointToken: String,
            marker: String?,
        ): ServicePage = ServicePage(records.values.toList(), null)

        fun force(record: OperationRecord) {
            records[record.identity.id] = record
        }
    }
}
