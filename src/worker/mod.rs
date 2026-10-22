//! Everything a worker runs. Its face is the [`Host`](host::Host): one worker's plumbing,
//! which every language SDK runs its worker on, pulling work and handing back
//! outcomes. Under it, private to this crate, are the drivers, the pollers and
//! completion reporting, the governor, task heartbeats, shutdown notices and
//! worker metrics.

pub(crate) mod drivers;
pub(crate) mod governor;
pub(crate) mod heartbeat;
pub mod host;
mod lifecycle;
pub(crate) mod metrics;
pub(crate) mod poll;
pub mod record;

// The work a poll brings, as the kernel takes it to resolve an activation.
pub use poll::{TaskExecutionTask, WorkflowExecutionTask};

// The built-in task types the host runs itself: a session's creation and end.
pub use drivers::{SESSION_COMPLETE_TASK, SESSION_CREATE_TASK};

pub(crate) use drivers::WorkerRegistrationDriver;
pub(crate) use drivers::{
    ActorDriver, ActorDriverEvent, ActorWork, ActorWorkResult, RegistrationHandle,
    SessionQueueChange, ShutdownHandle, TaskDriver, TaskWork, TaskWorkResult, WorkflowDriver,
    WorkflowWork, WorkflowWorkResult,
};
pub(crate) use drivers::{NeededDrivers, BUILT_IN_TASK_TYPES};
pub(crate) use heartbeat::{CancellationToken, TaskHeartbeat};
pub(crate) use metrics::WorkerMetrics;
pub(crate) use poll::CompletionRetryConfig;
