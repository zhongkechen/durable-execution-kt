package io.github.zhongkechen.durable.internal

import io.github.zhongkechen.durable.step
import io.github.zhongkechen.durable.typeRef
import io.github.zhongkechen.durable.DurablePlugin
import io.github.zhongkechen.durable.FunctionAttemptEnded
import io.github.zhongkechen.durable.FunctionAttemptStarted
import io.github.zhongkechen.durable.InvocationEnded
import io.github.zhongkechen.durable.InvocationStarted
import io.github.zhongkechen.durable.OperationSnapshot
import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertIs
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.seconds
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.runTest

class ExecutionEngineTest {
    @Test
    fun `handler success is serialized after durable step completion`() =
        runTest {
            val service = EngineService()
            val engine =
                ExecutionEngine(
                    service = service,
                    serviceContext = StandardTestDispatcher(testScheduler),
                    checkpointBatchWindow = 1.milliseconds,
                )

            val result =
                engine.execute(
                    request(),
                    inputType = typeRef<Int>(),
                    outputType = typeRef<Int>(),
                ) { input, context ->
                    context.step("double") { input * 2 }
                }

            assertEquals("42", assertIs<EngineResult.Success>(result).payload)
            assertEquals(
                listOf(CheckpointAction.START, CheckpointAction.SUCCEED),
                service.commands.map { it.action },
            )
        }

    @Test
    fun `durable wait returns pending without failing the handler`() =
        runTest {
            val service = EngineService()
            val engine =
                ExecutionEngine(
                    service = service,
                    serviceContext = StandardTestDispatcher(testScheduler),
                    checkpointBatchWindow = 1.milliseconds,
                )

            val result =
                engine.execute(
                    request(),
                    inputType = typeRef<Int>(),
                    outputType = typeRef<Int>(),
                ) { input, context ->
                    context.wait(3.seconds, "pause")
                    input
                }

            assertIs<EngineResult.Pending>(result)
            assertEquals(CheckpointAction.START, service.commands.single().action)
        }

    @Test
    fun `uncaught handler error becomes failure output`() =
        runTest {
            val engine =
                ExecutionEngine(
                    service = EngineService(),
                    serviceContext = StandardTestDispatcher(testScheduler),
                )

            val result =
                engine.execute(
                    request(),
                    inputType = typeRef<Int>(),
                    outputType = typeRef<Int>(),
                ) { _, _ ->
                    error("broken")
                }

            assertEquals("broken", assertIs<EngineResult.Failure>(result).error.message)
        }

    @Test
    fun `plugins observe invocation operation and function lifecycle with isolation`() =
        runTest {
            val events = mutableListOf<String>()
            val recorder =
                object : DurablePlugin {
                    override fun invocationStarted(info: InvocationStarted) {
                        events += "invocation-start"
                    }

                    override fun operationStarted(operation: OperationSnapshot) {
                        events += "operation-start"
                    }

                    override fun functionStarted(info: FunctionAttemptStarted) {
                        events += "function-start"
                    }

                    override fun functionEnded(info: FunctionAttemptEnded) {
                        events += "function-end"
                    }

                    override fun operationEnded(operation: OperationSnapshot) {
                        events += "operation-end"
                    }

                    override fun invocationEnded(info: InvocationEnded) {
                        events += "invocation-end"
                    }
                }
            val faulty =
                object : DurablePlugin {
                    override fun operationStarted(operation: OperationSnapshot) {
                        error("plugin failure")
                    }
                }
            val engine =
                ExecutionEngine(
                    service = EngineService(),
                    serviceContext = StandardTestDispatcher(testScheduler),
                    checkpointBatchWindow = 1.milliseconds,
                    plugins = listOf(faulty, recorder),
                )

            val result =
                engine.execute(
                    request(),
                    inputType = typeRef<Int>(),
                    outputType = typeRef<Int>(),
                ) { input, context ->
                    context.step("double") { input * 2 }
                }

            assertIs<EngineResult.Success>(result)
            assertEquals(
                listOf(
                    "invocation-start",
                    "operation-start",
                    "function-start",
                    "function-end",
                    "operation-end",
                    "invocation-end",
                ),
                events,
            )
        }

    private fun request(): InvocationRequest =
        InvocationRequest(
            executionArn = "arn:test",
            checkpointToken = "token-0",
            inputPayload = "21",
            initialOperations = emptyList(),
        )

    private class EngineService : DurableService {
        val commands = mutableListOf<CheckpointCommand>()
        private val records = linkedMapOf<String, OperationRecord>()

        override fun checkpoint(
            executionArn: String,
            checkpointToken: String,
            commands: List<CheckpointCommand>,
        ): CheckpointReply {
            this.commands += commands
            commands.forEach { command ->
                records[command.identity.id] =
                    when (command.action) {
                        CheckpointAction.START ->
                            OperationRecord(command.identity, CheckpointStatus.STARTED)
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
                                error = command.error,
                            )
                    }
            }
            return CheckpointReply("next-token", ServicePage(records.values.toList(), null))
        }

        override fun getState(
            executionArn: String,
            checkpointToken: String,
            marker: String?,
        ): ServicePage = ServicePage(records.values.toList(), null)
    }
}
