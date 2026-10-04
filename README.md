<p>
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="https://raw.githubusercontent.com/orcher-io/sdk-core/main/assets/banner.svg">
    <source media="(prefers-color-scheme: light)" srcset="https://raw.githubusercontent.com/orcher-io/sdk-core/main/assets/banner-light.svg">
    <img alt="ORCHER SDK Core" src="https://raw.githubusercontent.com/orcher-io/sdk-core/main/assets/banner.svg" width="100%">
  </picture>
</p>

<p align="center"><sub>The shared engine room of every ORCHER SDK: gRPC, workers and deterministic replay, in Rust.</sub></p>

<br />

<div>
  <a href="https://crates.io/crates/orcher-sdk-core"><img src="https://img.shields.io/crates/v/orcher-sdk-core?style=flat-square&labelColor=0a0a0a&color=04B385&logo=rust&logoColor=white" alt="crates.io"></a>
  <a href="https://docs.rs/orcher-sdk-core"><img src="https://img.shields.io/docsrs/orcher-sdk-core?style=flat-square&labelColor=0a0a0a&color=38BDF0&logo=docsdotrs&logoColor=white" alt="docs.rs"></a>
  <a href="https://github.com/orcher-io/sdk-core/actions/workflows/ci.yml"><img src="https://img.shields.io/github/actions/workflow/status/orcher-io/sdk-core/ci.yml?branch=main&style=flat-square&labelColor=0a0a0a&color=04B385&logo=github&logoColor=white&label=CI" alt="CI"></a>
  <a href="./LICENSE"><img src="https://img.shields.io/badge/license-Apache_2.0-38BDF0?style=flat-square&labelColor=0a0a0a" alt="Apache 2.0"></a>
</div>

<br />

The part every ORCHER language SDK has in common: it talks to the engine, and the SDK runs your code.

- <img height="14" src="https://octicons-col.vercel.app/plug/38BDF0"> **gRPC client**: start, query, update, cancel and list workflows, namespaces and actors
- <img height="14" src="https://octicons-col.vercel.app/sync/38BDF0"> **Drivers**: long-poll workflow, task and actor work and hand it to the SDK over channels
- <img height="14" src="https://octicons-col.vercel.app/history/38BDF0"> **Deterministic replay**: rebuilds workflow state from its journal and flags code that drifted
- <img height="14" src="https://octicons-col.vercel.app/check-circle/38BDF0"> **Safe completions**: every report carries a token, so a retried one is applied at most once
- <img height="14" src="https://octicons-col.vercel.app/cache/38BDF0"> **Workflow cache**: an LRU of active workflows, so warm runs skip reloading their journal
- <img height="14" src="https://octicons-col.vercel.app/lock/38BDF0"> **Payload codecs**: gzip compression and AES-GCM encryption
- <img height="14" src="https://octicons-col.vercel.app/shield-lock/38BDF0"> **Multi-tenant**: API keys, organizations and namespaces on every call

<br />

### <img height="16" src="https://octicons-col.vercel.app/download/38BDF0"> Install

```toml
[dependencies]
orcher-sdk-core = "0.8"
tokio = { version = "1", features = ["full"] }
serde_json = "1"
```

> [!NOTE]
> To write workflows, use a language SDK instead: [`orcher-sdk`](https://crates.io/crates/orcher-sdk) for Rust, [`@orcher/sdk`](https://www.npmjs.com/package/@orcher/sdk) for TypeScript, or the Python SDK. Depend on this crate when you are building a language SDK, or when all you need is a client that starts and inspects workflows. It is pre-1.0: the API may change between minor releases, and every breaking change is listed in [CHANGELOG.md](CHANGELOG.md).

<br />

### <img height="16" src="https://octicons-col.vercel.app/play/38BDF0"> Quick start

Start a workflow and wait for its result:

```rust
use orcher_sdk_core::WorkflowClient;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let client = WorkflowClient::connect("http://localhost:50051")
        .await?
        .with_api_key("my-api-key")
        .with_namespace("default");

    let handle = client
        .start_workflow(
            "order-1001",
            "confirm_order",
            "orders",
            serde_json::json!({ "id": "order-1001" }),
        )
        .await?;

    let receipt: serde_json::Value = handle.result().await?;
    println!("{} finished: {receipt}", handle.workflow_id());
    Ok(())
}
```

A language SDK builds on the drivers instead: `WorkflowDriver`, `TaskDriver` and `ActorDriver` poll the engine and pass each piece of work to the SDK as a request, and the SDK answers with a result that the driver reports back. The request and result types live in the `bridge` module; see [docs.rs](https://docs.rs/orcher-sdk-core) for the full API.

<br />

### <img height="16" src="https://octicons-col.vercel.app/stack/38BDF0"> How it fits

| Layer | Package | Repository |
|-------|---------|------------|
| API definitions | [`orcher-proto`](https://crates.io/crates/orcher-proto) | [orcher-io/protos](https://github.com/orcher-io/protos) |
| SDK core | [`orcher-sdk-core`](https://crates.io/crates/orcher-sdk-core) | [orcher-io/sdk-core](https://github.com/orcher-io/sdk-core) |
| Rust SDK | [`orcher-sdk`](https://crates.io/crates/orcher-sdk) | [orcher-io/sdk-rust](https://github.com/orcher-io/sdk-rust) |
| TypeScript SDK | [`@orcher/sdk`](https://www.npmjs.com/package/@orcher/sdk) | [orcher-io/sdk-ts](https://github.com/orcher-io/sdk-ts) |

The core polls and the language SDK executes: the engine never sees your handlers, and your handlers never see gRPC. The generated protocol types are re-exported as `orcher_sdk_core::proto`.

<br />

### <img height="16" src="https://octicons-col.vercel.app/heart/38BDF0"> Contributing

Issues and pull requests are welcome. See [CONTRIBUTING.md](CONTRIBUTING.md) for how to build, test and propose a change.

### <img height="16" src="https://octicons-col.vercel.app/law/38BDF0"> License

Licensed under the [Apache License, Version 2.0](LICENSE).

<sub>The Rust logo is a trademark of the Rust Foundation, shown here to indicate the language this crate is written in.</sub>
