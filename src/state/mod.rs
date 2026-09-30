//! State tracking and replay for deterministic workflow execution.
//!
//! This module holds the workflow and task state machines, the replayer that
//! checks workflow code against its recorded history, and the cache of live
//! workflow state.
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
