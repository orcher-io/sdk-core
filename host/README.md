# orcher-sdk-host

The worker host for ORCHER SDKs: one worker's plumbing, written once.

It builds the engine drivers from one configuration, runs the worker's lifecycle, and registers and deregisters the worker. The Rust SDK uses it directly, and the Python and TypeScript native modules are bindings over it. Each kind of work is handed to the language by pull: the language asks for the next piece of work when it has room to run it.

It is released together with [`orcher-sdk-core`](https://crates.io/crates/orcher-sdk-core), at the same version. The design is in [`docs/worker-host.md`](https://github.com/orcher-io/sdk-core/blob/main/docs/worker-host.md).
