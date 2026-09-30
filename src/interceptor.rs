//! Hooks for cross-cutting concerns around workflow, task, and actor
//! executions.
//!
//! This module defines the shared `InterceptorHook` trait and
//! `InterceptorContext` type. Language SDKs build logging, metrics, tracing,
//! and custom interceptors on them.
//!
//! ## Division of work
//!
//! This crate defines only the minimal shared types. Each language SDK
//! provides:
//! - `InterceptorChain`, which runs several interceptors in order
//! - built-in interceptors (logging, metrics, tracing)
//! - the integration with its own execution runtime
//!
//! ## Example
//!
//! ```rust
//! use orcher_sdk_core::interceptor::{InterceptorHook, InterceptorContext, OperationType};
//!
//! struct MyInterceptor;
//!
//! impl InterceptorHook for MyInterceptor {
//!     fn name(&self) -> &str { "my-interceptor" }
//!
//!     fn before_execution(&self, ctx: &InterceptorContext) {
//!         println!("Starting {:?}: {}", ctx.operation_type, ctx.operation_id);
//!     }
//!
//!     fn after_execution(&self, ctx: &InterceptorContext, duration_ms: u64) {
//!         println!("Completed {} in {}ms", ctx.operation_id, duration_ms);
//!     }
//! }
//! ```

use std::collections::HashMap;
use std::time::Instant;

/// Type of operation being intercepted.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OperationType {
    /// Workflow execution.
    Workflow,
    /// Task execution.
    Task,
    /// Actor operation.
    Actor,
}

impl std::fmt::Display for OperationType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::Workflow => write!(f, "workflow"),
            Self::Task => write!(f, "task"),
            Self::Actor => write!(f, "actor"),
        }
    }
}

/// Context passed to interceptor hooks.
///
/// Describes the current operation for logging, metrics, and tracing.
#[derive(Debug, Clone)]
pub struct InterceptorContext {
    /// Kind of operation: workflow, task, or actor
    pub operation_type: OperationType,

    /// Identifier of this operation, such as the workflow ID or task ID
    pub operation_id: String,

    /// Human-readable type name (workflow type, task type, actor name)
    pub operation_name: String,

    /// Attempt number (1-based)
    pub attempt: u32,

    /// Namespace the operation runs in
    pub namespace: String,

    /// Task queue the operation was dispatched from
    pub task_queue: String,

    /// When this operation started
    pub start_time: Instant,

    /// Free-form key-value data interceptors can use to pass information to
    /// each other
    pub metadata: HashMap<String, String>,
}

impl InterceptorContext {
    /// Creates a context for a workflow execution, timed from this call.
    pub fn workflow(
        workflow_id: impl Into<String>,
        workflow_type: impl Into<String>,
        attempt: u32,
        namespace: impl Into<String>,
        task_queue: impl Into<String>,
    ) -> Self {
        Self {
            operation_type: OperationType::Workflow,
            operation_id: workflow_id.into(),
            operation_name: workflow_type.into(),
            attempt,
            namespace: namespace.into(),
            task_queue: task_queue.into(),
            start_time: Instant::now(),
            metadata: HashMap::new(),
        }
    }

    /// Creates a context for a task execution, timed from this call.
    pub fn task(
        task_id: impl Into<String>,
        task_type: impl Into<String>,
        attempt: u32,
        namespace: impl Into<String>,
        task_queue: impl Into<String>,
    ) -> Self {
        Self {
            operation_type: OperationType::Task,
            operation_id: task_id.into(),
            operation_name: task_type.into(),
            attempt,
            namespace: namespace.into(),
            task_queue: task_queue.into(),
            start_time: Instant::now(),
            metadata: HashMap::new(),
        }
    }

    /// Returns the milliseconds elapsed since the operation started.
    pub fn elapsed_ms(&self) -> u64 {
        self.start_time.elapsed().as_millis() as u64
    }
}

/// Trait for intercepting workflow and task executions.
///
/// Implement this trait to add cross-cutting concerns like logging, metrics,
/// or tracing. Every hook except `name` has a no-op default, so override only
/// the ones you need.
///
/// Hooks are synchronous to keep overhead minimal. They must be fast and
/// non-blocking (logging, counter increments, span creation); for async work,
/// spawn a task inside the hook.
///
/// The language SDK's `InterceptorChain` calls interceptors in order.
pub trait InterceptorHook: Send + Sync {
    /// Returns a human-readable name for this interceptor, used in debugging.
    fn name(&self) -> &str;

    /// Called before an execution starts.
    ///
    /// Use this to log the start, open timers or spans, or validate inputs.
    fn before_execution(&self, _ctx: &InterceptorContext) {}

    /// Called after a successful execution.
    ///
    /// `duration_ms` is the wall-clock time of the execution.
    fn after_execution(&self, _ctx: &InterceptorContext, _duration_ms: u64) {}

    /// Called when an execution fails.
    ///
    /// `error` is the error message. `duration_ms` is the wall-clock time.
    fn on_error(&self, _ctx: &InterceptorContext, _error: &str, _duration_ms: u64) {}
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_operation_type_display() {
        assert_eq!(OperationType::Workflow.to_string(), "workflow");
        assert_eq!(OperationType::Task.to_string(), "task");
        assert_eq!(OperationType::Actor.to_string(), "actor");
    }

    #[test]
    fn test_interceptor_context_workflow() {
        let ctx = InterceptorContext::workflow("wf-1", "OrderWorkflow", 1, "default", "orders");
        assert_eq!(ctx.operation_type, OperationType::Workflow);
        assert_eq!(ctx.operation_id, "wf-1");
        assert_eq!(ctx.operation_name, "OrderWorkflow");
        assert_eq!(ctx.attempt, 1);
    }

    #[test]
    fn test_interceptor_context_task() {
        let ctx = InterceptorContext::task("task-1", "ProcessPayment", 2, "default", "payments");
        assert_eq!(ctx.operation_type, OperationType::Task);
        assert_eq!(ctx.operation_id, "task-1");
        assert_eq!(ctx.attempt, 2);
    }

    #[test]
    fn test_elapsed_ms() {
        let ctx = InterceptorContext::workflow("wf-1", "Test", 1, "default", "q");
        std::thread::sleep(std::time::Duration::from_millis(10));
        assert!(ctx.elapsed_ms() >= 10);
    }

    struct NoopInterceptor;

    impl InterceptorHook for NoopInterceptor {
        fn name(&self) -> &str {
            "noop"
        }
    }

    #[test]
    fn test_noop_interceptor() {
        let interceptor = NoopInterceptor;
        let ctx = InterceptorContext::workflow("wf-1", "Test", 1, "default", "q");

        // The default hooks are no-ops and must not panic.
        interceptor.before_execution(&ctx);
        interceptor.after_execution(&ctx, 100);
        interceptor.on_error(&ctx, "test error", 50);
    }
}
