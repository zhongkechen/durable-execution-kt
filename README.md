# Durable Execution for Kotlin

A coroutine-native durable workflow runtime for AWS Lambda on Java 21 or later.

The implementation is written in Kotlin and exposes Kotlin-first APIs:

- suspend handlers and operation bodies;
- named/default arguments and Kotlin durations;
- reified generic type helpers;
- deterministic replay validation;
- Java 21 virtual-thread dispatch;
- concurrent map and parallel operations;
- callback, invoke, retry, child-context, and condition primitives;
- lifecycle instrumentation plugins;
- an in-memory replay backend for tests.

```kotlin
import io.github.zhongkechen.durable.DurableContext
import io.github.zhongkechen.durable.DurableHandler
import io.github.zhongkechen.durable.MapOptions
import io.github.zhongkechen.durable.RetryPolicy
import io.github.zhongkechen.durable.StepOptions
import io.github.zhongkechen.durable.step
import io.github.zhongkechen.durable.typeRef
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

class OrderHandler :
    DurableHandler<Order, OrderResult>(
        inputType = typeRef(),
        outputType = typeRef(),
    ) {
    override suspend fun handle(
        input: Order,
        context: DurableContext,
    ): OrderResult {
        val reservation =
            context.step<Reservation>(
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

        context.wait(5.minutes, name = "packing-window")

        val shipments =
            context.map(
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

## Modules

- `core` — public API, replay engine, checkpoint coordination, Lambda wire
  protocol, and real backend adapter.
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

The cloud workflow runs automatically for same-repository pull requests and
pushes to `main`. Each suite reuses one persistent stack across every branch
and run. Deployments are globally serialized and limited to two suites at a
time.

Manual execution remains available for either one suite or the complete
matrix.

## Namespace

All runtime and testing APIs use:

```text
io.github.zhongkechen.durable
```

## License

Apache License 2.0.
