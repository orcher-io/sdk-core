#![allow(clippy::result_large_err)]
//! # ORCHER SDK Core
//!
//! The kernel behind every ORCHER language SDK. It owns, once for every
//! language:
//! - the worker: the [`Host`] every SDK runs its worker on, pulling
//!   activations, tasks and actor operations and handing back their outcomes;
//! - the kernel: what a run's history means, and the determinism checks
//!   every answer goes through, with the same replay tools for every
//!   language ([`kernel::encoded`]);
//! - the connection to the engine ([`Connection`]): gRPC, TLS, credentials,
//!   retries, limits;
//! - the clients for workflows, namespaces and actors.
//!
//! A language SDK keeps only its language's idioms: how handlers are
//! declared, the workflow context's API, and running the user's code.
//!
//! ## Terminology
//!
//! - **Worker** - A process that pulls work from the engine and runs it.
//! - **Task** - A unit of work that performs side effects, executed by a worker.
//! - **Event** - An external message delivered to a running workflow.
//! - **Execution Journal** - The event-sourced history of a workflow execution.
//! - **Activation** - One time the engine hands a run to a worker: its history,
//!   resolved by the kernel into the outcomes the run's code observes.
//!
//! ## Architecture
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────┐
//! │        Language SDK (Rust, Python, TypeScript, …)       │
//! │  handlers · workflow context · running the user's code  │
//! └────────────────────────────┬────────────────────────────┘
//!                              │ pull: next activation / task / actor op,
//!                              │ then its outcome (kernel and host messages)
//!                              ▼
//! ┌─────────────────────────────────────────────────────────┐
//! │              ORCHER SDK Core (this crate)               │
//! │   Host ─ worker machinery (drivers, polls, governor)    │
//! │   kernel (history → activation, answers → commands)     │
//! │   clients · transport                                   │
//! └────────────────────────────┬────────────────────────────┘
//!                              │ gRPC
//!                              ▼
//!                       ORCHER engine
//! ```
//!
//! ## Layout
//!
//! ```text
//! src/
//!   kernel/      what a run's history means: resolution, answers, step ids, random
//!   worker/
//!     host/      the Host: the one way a language SDK runs a worker
//!     record     recorded runs, for replay tools
//!     …          drivers, polling, the governor, heartbeats (private)
//!   client/      the connection to the engine, and the clients that start,
//!                query and manage workflows, namespaces and actors on it
//!   protocol/    what workflow code produces: commands, in an ExecutionResult
//!   state/       the journal checks every activation goes through (private)
//!   transport/   channels, credentials and message limits (private)
//!   payload/     the Payload type, converters and codecs
//!   error, types, interceptor, observability
//! ```
//!
//! ## Usage
//!
//! Most applications should not depend on this crate directly. Use a language
//! SDK instead; it builds on this crate and provides a more ergonomic API.
//!
//! ### Client Usage (Starting Workflows)
//!
//! ```rust,no_run
//! use orcher_sdk_core::{ConnectOptions, Connection};
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Connect to an ORCHER server.
//!     let connection = Connection::connect(ConnectOptions::new("http://localhost:50051")).await?;
//!     let client = connection.workflow_client();
//!
//!     // Start a workflow.
//!     let handle = client.start_workflow(
//!         "my-workflow-id",
//!         "OrderProcessingWorkflow",
//!         "order-queue",
//!         serde_json::json!({"order_id": 123}),
//!     ).await?;
//!
//!     // Wait for its result.
//!     let result: serde_json::Value = handle.result().await?;
//!     println!("Workflow result: {:?}", result);
//!
//!     Ok(())
//! }
//! ```

// The generated protocol code, for this crate's own use. None of it is part
// of the public API: a language SDK exchanges the kernel, host and client
// protocols with this crate as encoded messages ([`EncodedHost`],
// [`kernel::encoded`], [`ProtocolClient`]) and decodes them with generated
// code of its own, at the protocol version it implements. That is what lets
// this crate adopt a new orcher-proto without a breaking release, and lets an
// SDK move to a new protocol version when it wants something in it, not
// because this crate did. `scripts/check-public-surface.sh` keeps it so.
pub(crate) mod proto {
    pub use orcher_proto::{host, kernel, prost, prost_types};
}

pub mod client;
pub mod error;
pub mod error_code;
pub mod interceptor;
pub mod kernel;
pub mod observability;
pub mod payload;
pub mod protocol;
pub(crate) mod state;
pub(crate) mod transport;
pub mod types;
mod wire;
pub mod worker;

// The worker host: the one way a language SDK runs a worker.
pub use worker::host::{
    ActorHandler, EncodedHost, Host, HostConfig, Registrations, StateScope, StateValue,
};

// The connection to the engine, and the clients for workflows, namespaces
// and actors that share it.
pub use client::{
    ActorClient, ConnectOptions, Connection, NamespaceClient, ProtocolClient, StartWorkflowOpts,
    WorkflowClient, WorkflowHandle,
};
pub use transport::TlsConfig;

// Errors.
pub use error::{Error, Result, TaskFailure};
pub use error_code::ErrorCode;

// Payloads.
pub use payload::{Payload, Payloads};

// Shared vocabulary.
pub use types::{
    ListWorkflowsOptions, ListWorkflowsSortOrder, Namespace, SearchWorkflowsOptions, StepIdScheme,
    WorkflowExecution, WorkflowExecutionDescription, WorkflowExecutionInfo, WorkflowIdReusePolicy,
    WorkflowListPage, WorkflowRetryPolicy, WorkflowStatus,
};

// What workflow code produces, as the kernel hands it to the worker.
pub use protocol::{Command, ExecutionResult};

// Determinism: what the kernel reports when an activation does not replay
// its history.
pub use state::replayer::{
    DeterminismViolation, Difference, ViolationKind, ViolationSeverity,
    NON_DETERMINISM_FAILURE_TYPE,
};

/// The version of this crate.
pub const VERSION: &str = env!("CARGO_PKG_VERSION");

/// The namespace used when none is configured.
pub const DEFAULT_NAMESPACE: &str = "default";

/// Worker protocol versions, numbered as in the protocol's
/// `WorkerCapabilities.protocol_version`.
///
/// A worker reports its version when it registers and on every workflow
/// completion and failure, so the engine only asks of it what it can handle.
/// What a version promises covers the whole worker, the language SDK on top
/// of this crate included, so the language SDK chooses it:
/// [`HostConfig::with_protocol_version`].
pub mod worker_protocol {
    /// What every worker from before protocol versions handles.
    pub const BASELINE: u32 = 0;

    /// The worker hands a workflow cancellation request (a `cancel`
    /// resolution in the activation) to workflow code, letting the workflow
    /// clean up before it ends.
    ///
    /// Report it only once workflow code really sees the request. The engine
    /// then waits for the workflow to end itself, so a worker that reports it
    /// and drops the request leaves a cancelled run running until it is
    /// terminated or its cleanup limit passes. Below it, the engine ends the
    /// run as cancelled at once, as it did before cancellation requests.
    pub const CANCEL_REQUEST: u32 = 1;

    /// A workflow failure's `non_retryable` is what the workflow code raised:
    /// set only for a failure the code raised as non-retryable, with the same
    /// error the language gives task code for it. The engine then never
    /// retries such a failure, whatever the workflow's retry policy allows.
    ///
    /// Report it only once the language SDK answers a failed activation that
    /// way. Below it, the engine retries every workflow failure by its retry
    /// policy alone, whatever its `non_retryable` says.
    pub const WORKFLOW_FAILURE_RETRYABILITY: u32 = 2;
}

/// The worker protocol version reported unless the language SDK sets one:
/// [`worker_protocol::BASELINE`], which is what this crate guarantees on its
/// own.
pub const WORKER_PROTOCOL_VERSION: u32 = worker_protocol::BASELINE;

/// Default timeout for a long-poll request, in seconds.
///
/// Kept short so that workers stay responsive and pick up work with low latency.
pub const DEFAULT_POLL_TIMEOUT_SECONDS: i64 = 5;

/// Default timeout for a whole workflow execution, in seconds.
pub const DEFAULT_WORKFLOW_EXECUTION_TIMEOUT_SECONDS: i64 = 3600;

/// Default timeout for a single task execution, in seconds.
pub const DEFAULT_TASK_EXECUTION_TIMEOUT_SECONDS: i64 = 300;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_version() {
        assert!(!VERSION.is_empty());
    }

    #[test]
    fn test_constants() {
        assert_eq!(DEFAULT_NAMESPACE, "default");
        const {
            assert!(DEFAULT_POLL_TIMEOUT_SECONDS > 0);
            assert!(DEFAULT_WORKFLOW_EXECUTION_TIMEOUT_SECONDS > 0);
            assert!(DEFAULT_TASK_EXECUTION_TIMEOUT_SECONDS > 0);
        }
    }
}
