# Durable Execution SDK for Kotlin

Kotlin-first AWS Lambda Durable Execution SDK for Java 21 and later.

The public API is coroutine-native:

- handlers and durable user functions are `suspend`;
- operation result types use reified generics;
- configuration uses named/default arguments and Kotlin durations;
- map and parallel operations use receiver lambdas;
- Java 21 virtual threads bridge the synchronous Lambda runtime boundary without pinning carrier threads.

```kotlin
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds
import software.amazon.lambda.durable.kotlin.*

class OrderHandler : KotlinDurableHandler<Order, OrderResult>() {
    override suspend fun handle(
        input: Order,
        context: KotlinDurableContext,
    ): OrderResult {
        val reservation =
            context.step<Reservation>(
                name = "reserve-inventory",
                retry = RetryPolicy.fixed(maxAttempts = 3, delay = 1.seconds),
            ) {
                inventory.reserve(input.items)
            }

        context.wait(5.minutes)

        val shipments =
            context.map(
                name = "ship-items",
                items = input.items,
                maxConcurrency = 4,
            ) { item, index ->
                step<Shipment>("ship-$index") {
                    shipping.ship(item, reservation)
                }
            }

        return OrderResult(shipments.results())
    }
}
```

## Modules

- `java-core` — self-contained Java Durable Execution core based on AWS Java SDK PR #607, including the asynchronous extension SPI.
- `java-testing` — local/cloud testing utilities from the Java SDK core.
- `sdk` — Kotlin coroutine facade.
- `conformance-tests` — Kotlin handlers for the shared AWS Durable Execution conformance requirements.

The repository does not depend on PR #607 being merged or on an unpublished Maven artifact. The audited Java core is
compiled from source as part of this build.

## Build

```bash
./gradlew build
```

The build targets Java 21. `KotlinDurableRuntime` configures a shared virtual-thread-per-task executor for the Java
core and a coroutine dispatcher backed by that executor.

## Kotlin API

```kotlin
context.step<Result>(
    name = "call-service",
    retry = RetryPolicy.exponential(
        maxAttempts = 5,
        initialDelay = 1.seconds,
        maxDelay = 30.seconds,
    ),
) {
    service.call() // suspend function
}

context.wait(10.minutes, name = "cooldown")

context.parallel(
    name = "load",
    maxConcurrency = 2,
    completion = CompletionPolicy.allSuccessful,
) {
    branch<User>("user") { users.get() }
    branch<List<Order>>("orders") { orders.list() }
}
```

Java builder configurations remain available through explicit overloads for interoperability, but they are not the
primary Kotlin API.

## Conformance

The conformance package contains Kotlin handlers and SAM templates for:

- step
- wait
- child context
- callback
- invoke
- wait-for-condition
- wait-for-callback
- parallel
- map
- plugin lifecycle

The current cloud suite passes all 171 applicable requirements with no failures, uncovered cases, optional failures, or
`NOT_IMPLEMENTED` declarations.

Build the Lambda artifact with:

```bash
./gradlew :conformance-tests:shadowJar
```

## Java core provenance

The initial core was imported from `aws/aws-durable-execution-sdk-java` PR #607 at commit
`e416145586413569dc52e041245deb58e16f4606` and then extended in this repository with:

- asynchronous extension step/context functions;
- suspension-aware `DurableFuture.awaitAsync()`;
- durable execution context snapshots for coroutine propagation;
- asynchronous handler support;
- separate map item/result serializers.
- complete plugin invocation, operation, attempt, replay, result, and context-child replay information.

The upstream Apache-2.0 license and notice are preserved.
