//! Deterministic replay: the workflow and task state machines, the replayer
//! that checks workflow code against its journal step by step, and the cache
//! of live workflow state. Formerly the `state` module, which still
//! re-exports it.
//!
//! ## Terminology
//!
//! - **Execution Journal**: the event-sourced history of a workflow run.
//! - **Execution Step**: one point at which workflow code runs and decides what to do next.
//! - **State Machine**: tracks workflow and task state transitions.

pub mod cache;
pub mod machine;
pub mod replayer;

pub use cache::{CachedWorkflowState, WorkflowCache};
pub use machine::{
    ActiveTimer, PendingTask, TaskState, TaskStateMachine, WorkflowState, WorkflowStateMachine,
};
pub use replayer::{
    CommandCoverage, DeterminismViolation, ReplayConfig, ReplayResult, Replayer, ViolationKind,
    ViolationSeverity, NON_DETERMINISM_FAILURE_TYPE,
};
