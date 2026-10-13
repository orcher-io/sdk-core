//! Everything a worker runs: the drivers a language SDK builds on, the pollers
//! and completion reporting under them, task heartbeats, shutdown notices and
//! worker metrics.
//!
//! - [`WorkflowDriver`]: polls for workflow executions and exposes the work over channels.
//! - [`TaskDriver`]: polls for task executions and exposes the work over channels.
//! - [`ActorDriver`]: polls for actor operations.
//! - [`WorkerRegistrationDriver`]: registers the worker with the server and heartbeats.
//!
//! A language SDK receives [`WorkflowWork`] and [`TaskWork`] from the drivers, runs its
//! handlers, and sends the results back. Core handles all gRPC communication.

pub mod drivers;
pub(crate) mod governor;
pub mod heartbeat;
mod lifecycle;
pub mod metrics;
pub mod poll;
pub mod record;

// Driver types: the main API for language SDKs.
pub use drivers::{
    ActorDriver, ActorDriverConfig, ActorDriverEvent, ActorWork, ActorWorkResult,
    EagerTaskInjector, RegistrationHandle, SessionQueueChange, ShutdownHandle, TaskDriver,
    TaskDriverConfig, TaskWork, TaskWorkResult, WorkflowDriver, WorkflowDriverConfig, WorkflowWork,
    WorkflowWorkResult,
};

// Which of the work drivers a worker starts, and the built-in task types that
// do not count towards it.
pub use drivers::{NeededDrivers, BUILT_IN_TASK_TYPES, SESSION_COMPLETE_TASK, SESSION_CREATE_TASK};

// A running task's heartbeat, as the language SDK sees it.
pub use heartbeat::{CancellationToken, TaskHeartbeat};

// The id this process sends on its polls and with its shutdown notice.
pub use lifecycle::worker_instance_id;

// How hard the drivers try to deliver a completion.
pub use poll::CompletionRetryConfig;

// Polling types.
pub use poll::{
    ActorOperationPoller, ActorPollerConfig, PollerConfig, TaskExecutionPoller, TaskExecutionTask,
    WorkflowExecutionPoller, WorkflowExecutionTask,
};

// Worker metrics and registration.
pub use drivers::{WorkerRegistrationConfig, WorkerRegistrationDriver};
pub use metrics::{WorkerMetrics, WorkerMetricsSnapshot};
