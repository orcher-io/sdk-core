//! Long-polling the engine for work, and reporting how the work went.
//!
//! - [`polling`]: one poller for each kind of work.
//! - `completion`: sends completions and failure reports, retries them, and
//!   hands the tasks the engine returns eagerly to the task driver.

pub(crate) mod completion;
pub mod polling;
pub mod scaler;

pub use completion::CompletionRetryConfig;
pub use polling::{
    ActorOperationPoller, ActorPollerConfig, PollerConfig, TaskExecutionPoller, TaskExecutionTask,
    WorkflowExecutionPoller, WorkflowExecutionTask,
};
