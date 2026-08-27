# Clean-room Kotlin runtime

The runtime is being rebuilt as an independent Kotlin implementation.

## Boundary

- Public packages live under `io.github.zhongkechen.durable`.
- Runtime and testing modules contain Kotlin sources only.
- Behavior is derived from the language-neutral conformance requirements and
  observed service contracts.
- Existing vendored sources are temporary regression oracles. New code does not
  import their package or expose their types.

## Modules

- `core`: public contracts and the durable execution engine.
- `testing`: local and cloud test utilities.
- `conformance-tests`: executable behavior specifications.

The previous vendored implementation has been removed. Runtime, testing, and
conformance code are Kotlin-only.
