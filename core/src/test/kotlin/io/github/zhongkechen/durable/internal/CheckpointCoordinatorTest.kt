package io.github.zhongkechen.durable.internal

import kotlin.test.Test
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import kotlin.time.Duration.Companion.milliseconds
import kotlinx.coroutines.async
import kotlinx.coroutines.test.StandardTestDispatcher
import kotlinx.coroutines.test.runTest

class CheckpointCoordinatorTest {
    @Test
    fun `commands are batched and checkpoint state updates the ledger`() =
        runTest {
            val service = RecordingService()
            val ledger = ReplayLedger(emptyList())
            val dispatcher = StandardTestDispatcher(testScheduler)
            val coordinator =
                CheckpointCoordinator(
                    service = service,
                    executionArn = "arn:test",
                    checkpointToken = "token-0",
                    ledger = ledger,
                    coroutineContext = dispatcher,
                    batchWindow = 1.milliseconds,
                )
            val first = identity("1")
            val second = identity("2")

            val one =
                async {
                    coordinator.checkpoint(
                        CheckpointCommand(first, CheckpointAction.START),
                    )
                }
            val two =
                async {
                    coordinator.checkpoint(
                        CheckpointCommand(second, CheckpointAction.START),
                    )
                }
            testScheduler.advanceUntilIdle()
            one.await()
            two.await()

            assertEquals(1, service.checkpoints.size)
            assertEquals(listOf("1", "2"), service.checkpoints.single().map { it.identity.id })
            assertEquals(CheckpointStatus.STARTED, ledger.snapshot()["1"]?.status)
            assertEquals(CheckpointStatus.STARTED, ledger.snapshot()["2"]?.status)

            coordinator.close()
            testScheduler.advanceUntilIdle()
            coordinator.join()
        }

    @Test
    fun `pagination and polling consume later state`() =
        runTest {
            val target = identity("target")
            val service =
                RecordingService(
                    pages =
                        ArrayDeque(
                            listOf(
                                ServicePage(
                                    listOf(OperationRecord(target, CheckpointStatus.STARTED)),
                                    "next",
                                ),
                                ServicePage(
                                    listOf(OperationRecord(target, CheckpointStatus.SUCCEEDED)),
                                    null,
                                ),
                            ),
                        ),
                )
            val ledger = ReplayLedger(emptyList())
            val dispatcher = StandardTestDispatcher(testScheduler)
            val coordinator =
                CheckpointCoordinator(
                    service = service,
                    executionArn = "arn:test",
                    checkpointToken = "token-0",
                    ledger = ledger,
                    coroutineContext = dispatcher,
                )

            val result =
                async {
                    coordinator.pollUntil(target.id, { 1.milliseconds }) { it.status.terminal }
                }
            testScheduler.advanceUntilIdle()

            assertEquals(CheckpointStatus.SUCCEEDED, result.await().status)
            assertTrue(service.stateRequests.isNotEmpty())

            coordinator.close()
            testScheduler.advanceUntilIdle()
            coordinator.join()
        }

    private fun identity(id: String): OperationIdentity =
        OperationIdentity(
            id = id,
            name = id,
            kind = OperationKind.STEP,
            subtype = "Step",
            parentId = null,
        )

    private class RecordingService(
        private val pages: ArrayDeque<ServicePage> = ArrayDeque(),
    ) : DurableService {
        val checkpoints = mutableListOf<List<CheckpointCommand>>()
        val stateRequests = mutableListOf<String?>()
        private var tokenNumber = 0

        override fun checkpoint(
            executionArn: String,
            checkpointToken: String,
            commands: List<CheckpointCommand>,
        ): CheckpointReply {
            checkpoints += commands
            tokenNumber += 1
            val page =
                pages.removeFirstOrNull()
                    ?: ServicePage(
                        operations =
                            commands.map {
                                OperationRecord(it.identity, CheckpointStatus.STARTED)
                            },
                        nextMarker = null,
                    )
            return CheckpointReply("token-$tokenNumber", page)
        }

        override fun getState(
            executionArn: String,
            checkpointToken: String,
            marker: String?,
        ): ServicePage {
            stateRequests += marker
            return pages.removeFirst()
        }
    }
}
