package io.github.zhongkechen.durable.internal

import io.github.zhongkechen.durable.ChildOptions
import io.github.zhongkechen.durable.CallbackOptions
import io.github.zhongkechen.durable.CallbackWaitOptions
import io.github.zhongkechen.durable.ConditionDecision
import io.github.zhongkechen.durable.ConditionOptions
import io.github.zhongkechen.durable.InvokeOptions
import io.github.zhongkechen.durable.CompletionPolicy
import io.github.zhongkechen.durable.ItemResult
import io.github.zhongkechen.durable.MapOptions
import io.github.zhongkechen.durable.DurableFuture
import io.github.zhongkechen.durable.ParallelOptions
import io.github.zhongkechen.durable.RetryPolicy
import io.github.zhongkechen.durable.StepOptions
import io.github.zhongkechen.durable.StepScope
import io.github.zhongkechen.durable.extension.ExtensionRetryDecision
import io.github.zhongkechen.durable.extension.ExtensionStepConfig
import io.github.zhongkechen.durable.typeRef
import kotlin.time.Duration
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
    fun `stateless extension retry does not combine error and payload`() =
        runtimeTest { runtime, service, _ ->
            val identity =
                rootIdentity("1", "flaky", OperationKind.STEP, "Step")

            assertFailsWith<ExecutionSuspended> {
                runtime.extensionStep(
                    identity = identity,
                    type = typeRef<String>(),
                    config =
                        ExtensionStepConfig(
                            retry = {
                                    _,
                                    state,
                                    _,
                                ->
                                ExtensionRetryDecision.Retry(state, 1.milliseconds)
                            },
                        ),
                ) {
                    error("temporary")
                }
            }

            val retry = service.commands.last()
            assertEquals(CheckpointAction.RETRY, retry.action)
            assertEquals(null, retry.payload)
            assertEquals("temporary", retry.error?.message)
            assertEquals(1.seconds, retry.retryDelay)
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

    @Test
    fun `wait for callback suspends when callback completes during submitter checkpoint`() =
        runtimeTest(batchWindow = Duration.ZERO) { runtime, service, dispatcher ->
            val contextIdentity =
                rootIdentity("1", "approval", OperationKind.CONTEXT, "WaitForCallback")
            val callbackIdentity =
                OperationIdentity(
                    id = OperationIdSequence(contextIdentity.id).next(),
                    name = null,
                    kind = OperationKind.CALLBACK,
                    subtype = "Callback",
                    parentId = contextIdentity.id,
                )

            assertFailsWith<ExecutionSuspended> {
                runtime.waitForCallback(
                    name = "approval",
                    type = typeRef<String>(),
                    options = CallbackWaitOptions(),
                ) {
                    service.force(
                        OperationRecord(
                            identity = callbackIdentity,
                            status = CheckpointStatus.SUCCEEDED,
                            resultPayload = "\"approved\"",
                            callbackId = callbackId,
                        ),
                    )
                }
            }

            assertEquals(
                listOf(
                    CheckpointAction.START,
                    CheckpointAction.START,
                    CheckpointAction.START,
                    CheckpointAction.SUCCEED,
                ),
                service.commands.map { it.action },
            )
            assertEquals(
                listOf(
                    listOf(
                        CheckpointAction.START,
                        CheckpointAction.START,
                        CheckpointAction.START,
                    ),
                    listOf(CheckpointAction.SUCCEED),
                ),
                service.requests.map { request -> request.map { it.action } },
            )

            val replayHarness = runtime(service.snapshot(), service, dispatcher)
            assertEquals(
                "approved",
                replayHarness.runtime.waitForCallback(
                    name = "approval",
                    type = typeRef<String>(),
                    options = CallbackWaitOptions(),
                ) {
                    error("completed submitter must replay without executing")
                },
            )
            replayHarness.close()
        }

    @Test
    fun `map preserves input order and replays its aggregate checkpoint`() =
        runtimeTest { runtime, service, dispatcher ->
            val result =
                runtime.map(
                    name = "double",
                    items = listOf(3, 1, 2),
                    outputType = typeRef<Int>(),
                    options = MapOptions(maximumConcurrency = 2),
                ) { item, index ->
                    step("item-$index", typeRef<Int>()) { item * 2 }
                }

            assertEquals(listOf(6, 2, 4), result.values())
            val checkpointed = service.snapshot()
            var replayCalled = false
            val replayHarness = runtime(checkpointed, service, dispatcher)
            val replayed =
                replayHarness.runtime.map(
                    name = "double",
                    items = listOf(3, 1, 2),
                    outputType = typeRef<Int>(),
                    options = MapOptions(maximumConcurrency = 2),
                ) { _, _ ->
                    replayCalled = true
                    0
                }

            assertEquals(listOf(6, 2, 4), replayed.values())
            assertFalse(replayCalled)
            replayHarness.close()
        }

    @Test
    fun `map failure policy marks unstarted items skipped`() =
        runtimeTest { runtime, _, _ ->
            val result =
                runtime.map(
                    name = "fail-fast",
                    items = listOf(1, 2, 3),
                    outputType = typeRef<Int>(),
                    options =
                        MapOptions(
                            maximumConcurrency = 1,
                            completion = CompletionPolicy.TolerateFailures(count = 0),
                        ),
                ) { item, _ ->
                    if (item == 1) error("bad")
                    item
                }

            assertTrue(result.items.first() is ItemResult.Failure)
            assertTrue(result.items.drop(1).all { it is ItemResult.Skipped })
        }

    @Test
    fun `parallel supports heterogeneous branches and replayed futures`() =
        runtimeTest { runtime, service, dispatcher ->
            lateinit var text: DurableFuture<String>
            lateinit var number: DurableFuture<Int>
            val result =
                runtime.parallel("load", ParallelOptions(maximumConcurrency = 2)) {
                    text = branch("text", typeRef<String>()) { "ready" }
                    number = branch("number", typeRef<Int>()) { 7 }
                }

            assertEquals(listOf("ready", 7), result.values())
            assertEquals("ready", text.await())
            assertEquals(7, number.await())

            val checkpointed = service.snapshot()
            val replayHarness = runtime(checkpointed, service, dispatcher)
            var replayCalled = false
            lateinit var replayedText: DurableFuture<String>
            val replayed =
                replayHarness.runtime.parallel("load", ParallelOptions(maximumConcurrency = 2)) {
                    replayedText =
                        branch("text", typeRef<String>()) {
                            replayCalled = true
                            "wrong"
                        }
                    branch("number", typeRef<Int>()) {
                        replayCalled = true
                        0
                    }
                }

            assertEquals(listOf("ready", 7), replayed.values())
            assertEquals("ready", replayedText.await())
            assertFalse(replayCalled)
            replayHarness.close()
        }

    @Test
    fun `parallel suspension does not checkpoint cancelled sibling as failed`() =
        runtimeTest { runtime, service, _ ->
            assertFailsWith<ExecutionSuspended> {
                runtime.parallel(
                    name = "waits",
                    options = ParallelOptions(maximumConcurrency = 2),
                ) {
                    branch("short", typeRef<String>()) {
                        wait(1.seconds, "short")
                        "short"
                    }
                    branch("long", typeRef<String>()) {
                        wait(2.seconds, "long")
                        "long"
                    }
                }
            }

            assertTrue(service.commands.none { it.action == CheckpointAction.FAIL })
        }

    @Test
    fun `condition checkpoints state and completes from ready replay`() =
        runtimeTest { runtime, service, dispatcher ->
            assertFailsWith<ExecutionSuspended> {
                runtime.waitForCondition(
                    name = "poll",
                    type = typeRef<Int>(),
                    options =
                        ConditionOptions(
                            initialState = 0,
                            delay = { _, _ -> 1.seconds },
                        ),
                ) {
                    ConditionDecision.Continue(state + 1)
                }
            }
            assertEquals(CheckpointAction.RETRY, service.commands.last().action)
            assertEquals("1", service.commands.last().payload)

            val identity = rootIdentity("1", "poll", OperationKind.STEP, "WaitForCondition")
            val ready =
                OperationRecord(
                    identity = identity,
                    status = CheckpointStatus.READY,
                    attempt = 1,
                    resultPayload = "1",
                )
            service.force(ready)
            val replayHarness = runtime(listOf(ready), service, dispatcher)
            val result =
                replayHarness.runtime.waitForCondition(
                    name = "poll",
                    type = typeRef<Int>(),
                    options = ConditionOptions(delay = { _, _ -> 1.seconds }),
                ) {
                    assertEquals(2, attempt)
                    ConditionDecision.Complete(state + 1)
                }
            assertEquals(2, result)
            replayHarness.close()
        }

    @Test
    fun `wait for callback checkpoints submitter once and resumes from callback result`() =
        runtimeTest { runtime, service, dispatcher ->
            var submittedId: String? = null
            assertFailsWith<ExecutionSuspended> {
                runtime.waitForCallback(
                    name = "approval",
                    type = typeRef<String>(),
                    options = CallbackWaitOptions(),
                ) {
                    submittedId = callbackId
                }
            }
            assertEquals("callback-2", submittedId)

            val parentIdentity =
                rootIdentity("1", "approval", OperationKind.CONTEXT, "WaitForCallback")
            val childIds = OperationIdSequence(parentIdentity.id)
            val callbackIdentity =
                OperationIdentity(
                    id = childIds.next(),
                    name = null,
                    kind = OperationKind.CALLBACK,
                    subtype = "Callback",
                    parentId = parentIdentity.id,
                )
            val submitterIdentity =
                OperationIdentity(
                    id = childIds.next(),
                    name = null,
                    kind = OperationKind.STEP,
                    subtype = "Step",
                    parentId = parentIdentity.id,
                )
            val completed =
                listOf(
                    OperationRecord(
                        identity = parentIdentity,
                        status = CheckpointStatus.STARTED,
                    ),
                    OperationRecord(
                        identity = callbackIdentity,
                        status = CheckpointStatus.SUCCEEDED,
                        resultPayload = "\"yes\"",
                        callbackId = "callback-2",
                    ),
                    OperationRecord(
                        identity = submitterIdentity,
                        status = CheckpointStatus.SUCCEEDED,
                        resultPayload = "{}",
                    ),
                )
            completed.forEach(service::force)
            val replayHarness = runtime(completed, service, dispatcher)
            var replaySubmitted = false
            val result =
                replayHarness.runtime.waitForCallback(
                    name = "approval",
                    type = typeRef<String>(),
                ) {
                    replaySubmitted = true
                }
            assertEquals("yes", result)
            assertFalse(replaySubmitted)
            replayHarness.close()
        }

    private fun runtimeTest(
        initial: List<OperationRecord> = emptyList(),
        batchWindow: Duration = 1.milliseconds,
        block: suspend (OperationRuntime, RuntimeService, TestDispatcher) -> Unit,
    ) = runTest {
        val service = RuntimeService(initial)
        val dispatcher = StandardTestDispatcher(testScheduler)
        val harness = runtime(initial, service, dispatcher, batchWindow)
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
        batchWindow: Duration = 1.milliseconds,
    ): RuntimeHarness {
        val ledger = ReplayLedger(initial)
        val coordinator =
            CheckpointCoordinator(
                service = service,
                executionArn = "arn:test",
                checkpointToken = "token-0",
                ledger = ledger,
                coroutineContext = dispatcher,
                batchWindow = batchWindow,
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
        val requests = mutableListOf<List<CheckpointCommand>>()
        private val records = initial.associateByTo(linkedMapOf()) { it.identity.id }

        override fun checkpoint(
            executionArn: String,
            checkpointToken: String,
            commands: List<CheckpointCommand>,
        ): CheckpointReply {
            requests += commands
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

        fun snapshot(): List<OperationRecord> = records.values.toList()
    }
}
