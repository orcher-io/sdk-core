#![allow(clippy::result_large_err)]
// Protocol messages are built with `..Default::default()` even when every
// field is set today: a field added to the protocol then leaves an older
// release of this crate building, sending the field unset.
#![allow(clippy::needless_update)]
//! # ORCHER SDK Core
//!
//! The shared engine behind the ORCHER language SDKs. It handles:
//! - gRPC communication with the ORCHER server
//! - State machines for workflows and tasks
//! - Deterministic replay
//! - Workflow state caching
//! - Drivers that hand polled work to a language SDK
//!
//! ## Terminology
//!
//! - **Worker** - A process that polls for and executes workflows and tasks.
//! - **Task** - A unit of work that performs side effects, executed by a worker.
//! - **Event** - An external signal sent to a running workflow.
//! - **Execution Journal** - The event-sourced history of a workflow execution.
//! - **Execution Step** - A single decision point in a workflow execution.
//!
//! ## Architecture
//!
//! The work is split between this crate and a language SDK:
//! - This crate polls the server and owns all gRPC communication, through
//!   `WorkflowDriver`, `TaskDriver` and `ActorDriver`.
//! - The language SDK (Rust `orcher`, Python, TypeScript) stores the user's
//!   handlers and runs them.
//! - In short: core polls, the SDK executes.
//!
//! ```text
//! ┌─────────────────────────────────────────────────────────┐
//! │        Language SDK (Rust, Python, TypeScript)          │
//! │  • Handler storage                                      │
//! │  • Workflow/Task execution                              │
//! │  • User-facing API                                      │
//! └────────────────────────────┬────────────────────────────┘
//!                              │ Channels
//!                              ▼
//! ┌─────────────────────────────────────────────────────────┐
//! │              ORCHER SDK Core (this crate)               │
//! │                                                         │
//! │  ┌────────────────┐  ┌──────────────┐  ┌─────────────┐  │
//! │  │ Workflow Client│  │   Poller     │  │  Replayer   │  │
//! │  │  (gRPC calls)  │  │  (Drivers)   │  │  (State     │  │
//! │  │                │  │              │  │  Machine)   │  │
//! │  └────────────────┘  └──────────────┘  └─────────────┘  │
//! │                                                         │
//! │  ┌───────────────────────────────────────────────────┐  │
//! │  │              Workflow Cache (LRU)                 │  │
//! │  └───────────────────────────────────────────────────┘  │
//! └────────────────────────────┬────────────────────────────┘
//!                              │ gRPC
//!                              ▼
//! ┌─────────────────────────────────────────────────────────┐
//! │                     ORCHER Server                       │
//! │  • Orchestration engine                                 │
//! │  • State management                                     │
//! │  • Task distribution                                    │
//! └─────────────────────────────────────────────────────────┘
//! ```
//!
//! ## Modules
//!
//! - **client** - Start, query and manage workflows, namespaces and actors.
//! - **poller** - Polling and drivers.
//!   - **driver** - Workflow, task and actor drivers; the main API for language SDKs.
//!   - **polling** - Long-polling.
//!   - **channel** - gRPC channel setup, including TLS.
//! - **state** - State management and deterministic execution.
//!   - **machine** - Workflow and task state machines.
//!   - **replayer** - Deterministic replay.
//!   - **cache** - Workflow state cache.
//! - **bridge** - The request/result types exchanged with language SDKs.
//! - **interceptor** - Shared types for interceptors (logging, metrics, tracing).
//! - **codec** - Payload encoding and decoding (compression, encryption).
//! - **converter** - Conversion between values and payloads.
//! - **error** - Error types.
//! - **types** - Common type definitions.
//!
//! ## Usage
//!
//! Most applications should not depend on this crate directly. Use a language
//! SDK instead; it builds on this crate and provides a more ergonomic API.
//!
//! ### Client Usage (Starting Workflows)
//!
//! ```rust,no_run
//! use orcher_sdk_core::client::WorkflowClient;
//!
//! #[tokio::main]
//! async fn main() -> Result<(), Box<dyn std::error::Error>> {
//!     // Connect to an ORCHER server.
//!     let client = WorkflowClient::connect("http://localhost:50051").await?;
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

// The generated protocol types, re-exported for convenience.
pub use orcher_proto as proto;

pub mod bridge;
pub mod client;
pub mod codec;
pub mod converter;
pub mod error;
pub mod interceptor;
pub mod limits;
pub mod payload;
pub mod poller;
pub mod state;
pub mod types;

// Commonly used types, re-exported at the crate root.

// Client types
pub use client::{NamespaceClient, WorkflowClient, WorkflowHandle};

// Poller/Driver types (for language SDK integration)
pub use poller::{
    ActorDriver, ActorDriverConfig, ActorDriverEvent, ActorOperationPoller, ActorPollerConfig,
    ActorWork, ActorWorkResult, PollerConfig, TaskDriver, TaskDriverConfig, TaskExecutionPoller,
    TaskExecutionTask, TaskWork, TaskWorkResult, WorkerRegistrationConfig,
    WorkerRegistrationDriver, WorkflowDriver, WorkflowDriverConfig, WorkflowExecutionPoller,
    WorkflowExecutionTask, WorkflowWork, WorkflowWorkResult,
};

// State types
pub use state::{
    cache::{CachedWorkflowState, WorkflowCache},
    machine::{
        ActiveTimer, PendingTask, TaskState, TaskStateMachine, WorkflowState, WorkflowStateMachine,
    },
    replayer::{
        CommandCoverage, DeterminismViolation, ReplayConfig, ReplayResult, Replayer, ViolationKind,
        ViolationSeverity, NON_DETERMINISM_FAILURE_TYPE,
    },
};

// Error and Result types
pub use error::{Error, Result, TaskFailure};

// Payload types
pub use payload::{Payload, Payloads};

// Common types
pub use types::{
    ListWorkflowsOptions, ListWorkflowsSortOrder, OrganizationContext, SearchWorkflowsOptions,
    WorkflowExecution, WorkflowExecutionDescription, WorkflowExecutionInfo, WorkflowListPage,
    WorkflowStatus,
};

// Bridge types - for language SDK integration
pub use bridge::{
    task_to_execution_request, validate_execution_result, Command, CompleteTaskJob,
    ExecutionRequest, ExecutionResult, FireTimerJob, HandleEventJob, RequestJob, StartWorkflowJob,
    TaskExecutionResult as BridgeTaskResult,
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
/// [`WorkflowDriverConfig::protocol_version`] and
/// [`WorkerRegistrationConfig::protocol_version`].
pub mod worker_protocol {
    /// What every worker from before protocol versions handles.
    pub const BASELINE: u32 = 0;

    /// The worker hands a workflow cancellation request in the journal
    /// ([`crate::bridge::RequestJob::CancelWorkflow`]) to workflow code,
    /// letting the workflow clean up before it ends.
    ///
    /// Report it only once workflow code really sees the request. The engine
    /// then waits for the workflow to end itself, so a worker that reports it
    /// and drops the request leaves a cancelled run running until it is
    /// terminated or its cleanup limit passes. Below it, the engine ends the
    /// run as cancelled at once, as it did before cancellation requests.
    pub const CANCEL_REQUEST: u32 = 1;
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
