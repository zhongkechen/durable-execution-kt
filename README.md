# Durable Execution for Kotlin

A coroutine-native durable workflow runtime for AWS Lambda on Java 21 or later.

The public API is context-free and Kotlin-first:

- handlers receive only their typed input;
- durable operations are top-level suspend functions;
- configuration uses immutable data classes with named/default arguments;
- extension libraries compose suspend-native primitives through deterministic,
  one-shot reservations;
- active durable scope follows the coroutine context rather than a thread local;
- map, parallel, callbacks, invokes, retries, child contexts, and polling share
  the same replay engine.

```kotlin
import io.github.zhongkechen.durable.*
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

class OrderHandler :
    DurableHandler<Order, OrderResult>(
        inputType = typeRef(),
        outputType = typeRef(),
    ) {
    override suspend fun handle(input: Order): OrderResult {
        val reservation =
            step<Reservation>(
                name = "reserve-inventory",
                options =
                    StepOptions(
                        retry = RetryPolicy.fixed(
                            maxAttempts = 3,
                            delay = 1.seconds,
                        ),
                    ),
            ) {
                inventory.reserve(input.items)
            }

        wait(5.minutes, name = "packing-window")

        val shipments =
            map(
                name = "ship-items",
                items = input.items,
                outputType = typeRef<Shipment>(),
                options = MapOptions(maximumConcurrency = 4),
            ) { item, index ->
                step("ship-$index") {
                    shipping.ship(item, reservation)
                }
            }

        return OrderResult(shipments.values())
    }
}
```

Child scopes and concurrent branches automatically install their own durable
scope in the coroutine context, so the same top-level functions remain valid
when nested.

## Extension SPI

Extensions reserve identities before launching work. Reservation order defines
durable identity; launch order may differ.

```kotlin
import io.github.zhongkechen.durable.extension.*
import io.github.zhongkechen.durable.typeRef
import kotlinx.coroutines.async
import kotlinx.coroutines.coroutineScope

suspend fun paired(name: String): String = coroutineScope {
    val extension = currentExtensionContext()
    val left = extension.reserve("$name-left")
    val right = extension.reserve("$name-right")

    val rightResult = async {
        right.step("PairStep", typeRef<String>()) {
            ExtensionStepResult.Succeeded("R")
        }
    }
    val leftResult = async {
        left.step("PairStep", typeRef<String>()) {
            ExtensionStepResult.Succeeded("L")
        }
    }

    leftResult.await() + rightResult.await()
}
```

The SPI uses suspend functions, sealed outcomes, Kotlin durations, and data
classes. It does not expose builders, `CompletionStage`, raw checkpoint APIs,
or backend operation IDs.

## Modules

- `core` — public operation facades, extension SPI, replay engine, checkpoint
  coordination, Lambda wire protocol, and backend adapter.
- `testing` — in-memory backend and typed local invocation runner.
- `conformance-tests` — executable handlers and infrastructure templates for
  the shared durable-execution behavior suite.

## Build

```bash
./gradlew build
```

## Local testing

```kotlin
val runner =
    LocalDurableRunner.create<Input, Output> { config ->
        MyHandler(config)
    }

val result = runner.runUntilComplete(input)
```

## Cloud conformance

Cloud conformance runs automatically for same-repository pull requests and
pushes to `main`. Each suite reuses one persistent stack across branches and
runs, with deployment concurrency controlled globally.

## Namespace

```text
io.github.zhongkechen.durable
```

## License

Apache License 2.0.
