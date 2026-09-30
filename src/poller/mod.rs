//! Polling and driver infrastructure for language SDK integration.
//!
//! - [`WorkflowDriver`]: polls for workflow executions and exposes the work over channels.
//! - [`TaskDriver`]: polls for task executions and exposes the work over channels.
//! - [`ActorDriver`]: polls for actor operations.
//! - [`WorkerRegistrationDriver`]: registers the worker with the server and heartbeats.
//! - [`ChannelManager`]: manages the gRPC channel, including TLS.
//!
//! A language SDK receives [`WorkflowWork`] and [`TaskWork`] from the drivers, runs its
//! handlers, and sends the results back. Core handles all gRPC communication.

pub mod channel;
mod completion;
pub mod credentials;
pub mod driver;
pub mod heartbeat;
mod lifecycle;
pub mod metrics;
pub mod polling;
pub mod worker_registration;

// Driver types: the main API for language SDKs.
pub use driver::{
    ActorDriver, ActorDriverConfig, ActorDriverEvent, ActorWork, ActorWorkResult,
    EagerTaskInjector, SessionQueueChange, ShutdownHandle, TaskDriver, TaskDriverConfig, TaskWork,
    TaskWorkResult, WorkflowDriver, WorkflowDriverConfig, WorkflowWork, WorkflowWorkResult,
};

// A running task's heartbeat, as the language SDK sees it.
pub use heartbeat::{CancellationToken, TaskHeartbeat};

// The id this process sends on its polls and with its shutdown notice.
pub use lifecycle::worker_instance_id;

// How hard the drivers try to deliver a completion.
pub use completion::CompletionRetryConfig;

// Polling types.
pub use polling::{
    ActorOperationPoller, ActorPollerConfig, PollerConfig, TaskExecutionPoller, TaskExecutionTask,
    WorkflowExecutionPoller, WorkflowExecutionTask,
};

// Worker metrics and registration.
pub use metrics::{WorkerMetrics, WorkerMetricsSnapshot};
pub use worker_registration::{WorkerRegistrationConfig, WorkerRegistrationDriver};

// Channel manager and TLS config.
pub use channel::{ChannelManager, TlsConfig};

// The credential helper every worker-originated call uses.
pub use credentials::credentialed_request;
