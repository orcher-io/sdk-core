# orcher-sdk-core

The shared Rust core of the [Orcher](https://github.com/orcher-io) SDKs.

Orcher runs durable workflows: a workflow is replayed from its execution
journal, so it survives worker restarts, and its side effects run as tasks
that are retried under their retry policy. This crate is the part every language
SDK has in common. It talks gRPC to the Orcher engine, long-polls for
workflow, task and actor work, replays workflow history deterministically,
sends completions back with safe retries, and keeps a cache of workflow state.
The language SDKs own the user-facing API and run the handler code.

## Status

Pre-1.0. The API may change in any minor release, and breaking changes are
listed in [CHANGELOG.md](CHANGELOG.md).

## Who should use this crate

Most applications should not depend on it directly. Use a language SDK:

- [sdk-rust](https://github.com/orcher-io/sdk-rust): Rust
- [sdk-py](https://github.com/orcher-io/sdk-py): Python
- [sdk-ts](https://github.com/orcher-io/sdk-ts): TypeScript

Depend on `orcher-sdk-core` when you are building a language SDK, or when
you only need a client that starts and inspects workflows.

## Install

```bash
cargo add orcher-sdk-core
```

## Example

Start a workflow and wait for its result:

```rust,no_run
use orcher_sdk_core::client::WorkflowClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = WorkflowClient::connect("http://localhost:50051")
        .await?
        .with_api_key("my-api-key")
        .with_namespace("default");

    let handle = client
        .start_workflow(
            "order-123",
            "OrderProcessingWorkflow",
            "orders",
            serde_json::json!({ "order_id": 123 }),
        )
        .await?;

    let result: serde_json::Value = handle.result().await?;
    println!("workflow result: {result}");
    Ok(())
}
```

## How it fits together

```
┌─────────────────────────────────────────────┐
│        Language SDK (Rust/Python/TS)        │
│  • User-facing API (Worker, WorkerBuilder)  │
│  • Handler storage and execution            │
└─────────────────┬───────────────────────────┘
                  │ channels
                  ▼
┌─────────────────────────────────────────────┐
│           SDK core (this crate)             │
│  • WorkflowDriver / TaskDriver / ActorDriver│
│  • gRPC client (tonic)                      │
│  • Deterministic replay                     │
│  • Workflow cache (LRU)                     │
└─────────────────┬───────────────────────────┘
                  │ gRPC
                  ▼
┌─────────────────────────────────────────────┐
│               Orcher engine                 │
└─────────────────────────────────────────────┘
```

The core polls and the language SDK executes: drivers hand work to the SDK
over channels and send its results back to the engine.

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).

## License

Licensed under the [Apache License, Version 2.0](LICENSE).
