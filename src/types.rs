//! The shared vocabulary of workflows, tasks and events: ids, statuses,
//! retry policies, the options a workflow starts with, and what the clients
//! list and describe.
//! Their conversions to and from the wire are in `crate::wire`.
//!
//! ## Terminology
//!
//! - **Worker**: a process that polls for and executes workflows and tasks.
//! - **Task**: a unit of work a workflow schedules.
//! - **Event**: an external message delivered to a running workflow.
//! - **Execution journal**: the durable record of everything a workflow
//!   execution has done, replayed to rebuild its state.
//! - **Execution step**: a point at which the workflow makes its next
//!   decisions.

use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::fmt;
use std::time::{Duration, SystemTime};

// `crate::payload::Payload` is the one payload type; it is re-exported here for
// convenience.
pub use crate::payload::Payload;

/// Identifies one workflow execution by workflow id and run id.
///
/// On the wire the run id is the `execution_id` field. Displays as
/// `workflow_id:run_id`.
#[derive(Debug, Clone, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct WorkflowExecution {
    /// Workflow identifier, chosen by the caller or assigned by the server.
    pub workflow_id: String,
    /// Identifier of this particular run of the workflow.
    pub run_id: String,
}

impl WorkflowExecution {
    /// Creates an execution identifier from a workflow id and a run id.
    pub fn new(workflow_id: impl Into<String>, run_id: impl Into<String>) -> Self {
        Self {
            workflow_id: workflow_id.into(),
            run_id: run_id.into(),
        }
    }
}

impl fmt::Display for WorkflowExecution {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.workflow_id, self.run_id)
    }
}

// There is no `From` conversion between `Payload` and the wire's payload.
// Convert at the call site: both have the same fields, `data` and `metadata`.

/// Status of a workflow execution.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub enum WorkflowStatus {
    /// Workflow is running
    Running,
    /// Workflow completed successfully
    Completed,
    /// Workflow failed
    Failed,
    /// Workflow was cancelled
    Cancelled,
    /// Workflow was terminated
    Terminated,
    /// Workflow timed out
    TimedOut,
    /// Workflow was restarted as a fresh run
    RestartedFresh,
}

impl WorkflowStatus {
    /// Returns whether the workflow has reached a terminal state.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Self::Completed
                | Self::Failed
                | Self::Cancelled
                | Self::Terminated
                | Self::TimedOut
                | Self::RestartedFresh
        )
    }

    /// Returns whether the workflow is still running.
    pub fn is_running(&self) -> bool {
        matches!(self, Self::Running)
    }
}

impl fmt::Display for WorkflowStatus {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Running => write!(f, "RUNNING"),
            Self::Completed => write!(f, "COMPLETED"),
            Self::Failed => write!(f, "FAILED"),
            Self::Cancelled => write!(f, "CANCELLED"),
            Self::Terminated => write!(f, "TERMINATED"),
            Self::TimedOut => write!(f, "TIMED_OUT"),
            Self::RestartedFresh => write!(f, "RESTARTED_FRESH"),
        }
    }
}

/// The retry policy a described workflow runs under, as the engine reports
/// it, in whole seconds.
///
/// The default starts at 1 second, doubles each attempt up to 60 seconds, and
/// retries without limit.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct RetryPolicy {
    /// Delay before the first retry, in seconds
    pub initial_interval_seconds: i64,
    /// Multiplier applied to the delay after each retry
    pub backoff_coefficient: f64,
    /// Upper bound on the delay between retries, in seconds
    pub max_interval_seconds: i64,
    /// Maximum number of attempts; 0 means unlimited
    pub max_attempts: i32,
    /// Error types that fail immediately instead of being retried
    pub non_retryable_error_types: Vec<String>,
}

impl Default for RetryPolicy {
    fn default() -> Self {
        Self {
            initial_interval_seconds: 1,
            backoff_coefficient: 2.0,
            max_interval_seconds: 60,
            max_attempts: 0, // Unlimited
            non_retryable_error_types: Vec::new(),
        }
    }
}

impl RetryPolicy {
    /// Sets the delay before the first retry, in seconds.
    pub fn with_initial_interval_seconds(mut self, seconds: i64) -> Self {
        self.initial_interval_seconds = seconds;
        self
    }

    /// Sets the backoff coefficient.
    pub fn with_backoff_coefficient(mut self, coefficient: f64) -> Self {
        self.backoff_coefficient = coefficient;
        self
    }

    /// Sets the maximum delay between retries, in seconds.
    pub fn with_max_interval_seconds(mut self, seconds: i64) -> Self {
        self.max_interval_seconds = seconds;
        self
    }

    /// Sets the maximum number of attempts; 0 means unlimited.
    pub fn with_max_attempts(mut self, max_attempts: i32) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    /// Sets the error types that are never retried.
    pub fn with_non_retryable_error_types<I, S>(mut self, error_types: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.non_retryable_error_types = error_types.into_iter().map(Into::into).collect();
        self
    }
}

/// How a workflow run is retried: a child, or the fresh run a workflow
/// restarts as. A delay or multiplier left unset takes the engine's default
/// for workflow retries, which is not the task default.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct WorkflowRetryPolicy {
    /// Maximum number of attempts, the first included; 0 is unlimited.
    pub max_attempts: u32,

    /// Delay before the first retry; `None`: the engine's default.
    pub initial_interval: Option<Duration>,

    /// Upper bound on the delay between retries; `None`: the engine's
    /// default.
    pub max_interval: Option<Duration>,

    /// Multiplier applied to the delay after each retry; 0: the engine's
    /// default.
    pub backoff_coefficient: f64,

    /// Error types that are never retried.
    pub non_retryable_errors: Vec<String>,
}

/// What a start does when its workflow id is already in use in the
/// namespace.
///
/// An id whose execution is still open is never started a second time:
/// every policy but [`WorkflowIdReusePolicy::TerminateIfRunning`] refuses it
/// with [`crate::Error::WorkflowAlreadyExists`]. The policies differ in what
/// they allow once the previous execution has closed. An execution that is
/// being retried, or that restarted fresh, is still open.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum WorkflowIdReusePolicy {
    /// Start once the previous execution has closed, however it closed: the
    /// engine's default.
    AllowDuplicate,
    /// Start once the previous execution has closed without completing:
    /// failed, cancelled, terminated or timed out.
    AllowDuplicateFailedOnly,
    /// Never start an id an execution has carried before.
    RejectDuplicate,
    /// Terminate the open execution, if there is one, and start.
    TerminateIfRunning,
}

/// A rule a run's step ids are made by: the key that matches each durable
/// call to its recorded outcome (`docs/kernel-protocol-v1.md` §8.3).
///
/// The engine binds a run to one at its first activation, from the schemes
/// the worker reports, and hands the run only to workers reporting it after.
/// Schemes are frozen: a different rule is a new scheme.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[non_exhaustive]
pub enum StepIdScheme {
    /// The kernel's: the kernel assigns every id, the same in every SDK.
    Kernel,
    /// The Rust SDK's ids from before the kernel assigned them.
    RustV0,
    /// The Python SDK's ids from before the kernel assigned them.
    PythonV0,
    /// The TypeScript SDK's ids from before the kernel assigned them.
    TypescriptV0,
}

/// A namespace, as the engine reports it.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct Namespace {
    /// The namespace's unique name.
    pub name: String,
    /// `active`, `deprecated` or `deleted`.
    pub status: String,
    /// What the namespace is for.
    pub description: String,
    /// Who owns it.
    pub owner_email: String,
    /// How long a closed workflow's data is kept, in days.
    pub retention_period_days: i32,
    /// Any other data recorded with it.
    pub data: HashMap<String, String>,
    /// When it was created.
    pub created_at: Option<SystemTime>,
    /// When it last changed.
    pub updated_at: Option<SystemTime>,
}

/// Detailed description of a workflow execution.
///
/// Returned by `WorkflowHandle::describe()`.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct WorkflowExecutionDescription {
    /// Workflow id and run id
    pub execution: WorkflowExecution,
    /// Workflow type name
    pub workflow_type: String,
    /// Task queue the workflow is running on
    pub task_queue: String,
    /// Current status
    pub status: WorkflowStatus,
    /// When the workflow started
    pub start_time: Option<SystemTime>,
    /// When the workflow closed (if finished)
    pub close_time: Option<SystemTime>,
    /// Number of journal entries
    pub journal_length: i64,
    /// Current retry attempt number
    pub attempt: i32,
    /// Labels (indexed custom fields)
    pub labels: HashMap<String, String>,
    /// Annotations (non-indexed metadata)
    pub annotations: HashMap<String, String>,
    /// Tags
    pub tags: Vec<String>,
    /// Who started the workflow
    pub started_by: String,
    /// Cron schedule (if periodic)
    pub cron_schedule: String,
    /// State transition count
    pub state_transition_count: i64,
    /// Parent workflow, if this is a child workflow
    pub parent_execution: Option<ParentExecutionInfo>,
    /// Number of pending tasks
    pub pending_tasks: i32,
    /// Number of pending timers
    pub pending_timers: i32,
    /// Number of pending events
    pub pending_events: i32,
    /// Timeouts, retry policy, and schedule the execution runs under
    pub execution_config: Option<WorkflowExecutionConfig>,
}

/// Identifies the parent of a child workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct ParentExecutionInfo {
    /// Parent's namespace
    pub namespace: String,
    /// Parent workflow ID
    pub workflow_id: String,
    /// Parent execution ID
    pub execution_id: String,
}

/// Configuration a workflow execution runs under.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct WorkflowExecutionConfig {
    /// Task queue
    pub task_queue: String,
    /// Execution timeout in seconds
    pub execution_timeout_seconds: Option<i64>,
    /// Run timeout in seconds
    pub run_timeout_seconds: Option<i64>,
    /// Default task timeout in seconds
    pub default_task_timeout_seconds: Option<i64>,
    /// Retry policy
    pub retry_policy: Option<RetryPolicy>,
    /// Cron schedule
    pub cron_schedule: String,
}

/// Summary of a workflow execution, as returned in list and search results.
///
/// It lacks the pending counts and execution configuration that
/// [`WorkflowExecutionDescription`] carries.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct WorkflowExecutionInfo {
    /// User-facing workflow ID
    pub workflow_id: String,
    /// Server-assigned execution ID (a UUID); the run id
    pub execution_id: String,
    /// Workflow type name
    pub workflow_type: String,
    /// Task queue the workflow runs on
    pub task_queue: String,
    /// Namespace
    pub namespace: String,
    /// Current status
    pub status: WorkflowStatus,
    /// When the workflow started
    pub start_time: Option<SystemTime>,
    /// When the workflow closed (if finished)
    pub close_time: Option<SystemTime>,
    /// Execution duration in seconds (if finished)
    pub execution_duration_seconds: Option<i64>,
    /// Number of journal entries
    pub journal_length: i64,
    /// Parent workflow, if this is a child workflow
    pub parent_execution: Option<ParentExecutionInfo>,
    /// Labels (indexed custom fields)
    pub labels: HashMap<String, String>,
    /// Annotations (non-indexed metadata)
    pub annotations: HashMap<String, String>,
    /// Tags
    pub tags: Vec<String>,
    /// Who started the workflow
    pub started_by: String,
    /// Retry attempt number
    pub attempt: i32,
    /// Cron schedule (if periodic)
    pub cron_schedule: String,
    /// State transition count
    pub state_transition_count: i64,
}

/// One page of workflow execution summaries.
///
/// Returned by `list_workflows()` and `search_workflows()`, together with the
/// token for fetching the following page.
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
#[non_exhaustive]
pub struct WorkflowListPage {
    /// Workflow execution summaries
    pub executions: Vec<WorkflowExecutionInfo>,
    /// Token for the following page; empty on the last page
    pub next_page_token: Vec<u8>,
}

impl WorkflowListPage {
    /// Returns whether another page follows this one.
    pub fn has_more(&self) -> bool {
        !self.next_page_token.is_empty()
    }
}

/// Sort order for listing workflows.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub enum ListWorkflowsSortOrder {
    /// Sort by start time, ascending
    StartTimeAsc,
    /// Sort by start time, descending (newest first)
    StartTimeDesc,
    /// Sort by close time, ascending
    CloseTimeAsc,
    /// Sort by close time, descending
    CloseTimeDesc,
}

/// Filter, sort, and pagination options for listing workflows.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct ListWorkflowsOptions {
    /// Maximum number of results per page; 0 requests 100
    pub page_size: i32,
    /// Pagination token from a previous response
    pub next_page_token: Vec<u8>,
    /// Filter by workflow type
    pub workflow_type: Option<String>,
    /// Filter by task queue
    pub task_queue: Option<String>,
    /// Filter by one or more statuses
    pub status_filter: Vec<WorkflowStatus>,
    /// Sort order
    pub sort_order: Option<ListWorkflowsSortOrder>,
}

/// Pagination options for searching workflows.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct SearchWorkflowsOptions {
    /// Maximum number of results per page; 0 requests 100
    pub page_size: i32,
    /// Pagination token from a previous response
    pub next_page_token: Vec<u8>,
}

impl ListWorkflowsOptions {
    /// Sets the maximum number of results per page.
    pub fn with_page_size(mut self, page_size: i32) -> Self {
        self.page_size = page_size;
        self
    }

    /// Continues from the pagination token of a previous page.
    pub fn with_next_page_token(mut self, token: impl Into<Vec<u8>>) -> Self {
        self.next_page_token = token.into();
        self
    }

    /// Only list workflows of this type.
    pub fn with_workflow_type(mut self, workflow_type: impl Into<String>) -> Self {
        self.workflow_type = Some(workflow_type.into());
        self
    }

    /// Only list workflows on this task queue.
    pub fn with_task_queue(mut self, task_queue: impl Into<String>) -> Self {
        self.task_queue = Some(task_queue.into());
        self
    }

    /// Only list workflows in one of these statuses.
    pub fn with_status_filter(
        mut self,
        statuses: impl IntoIterator<Item = WorkflowStatus>,
    ) -> Self {
        self.status_filter = statuses.into_iter().collect();
        self
    }

    /// Sets the sort order.
    pub fn with_sort_order(mut self, sort_order: ListWorkflowsSortOrder) -> Self {
        self.sort_order = Some(sort_order);
        self
    }
}

impl SearchWorkflowsOptions {
    /// Sets the maximum number of results per page.
    pub fn with_page_size(mut self, page_size: i32) -> Self {
        self.page_size = page_size;
        self
    }

    /// Continues from the pagination token of a previous page.
    pub fn with_next_page_token(mut self, token: impl Into<Vec<u8>>) -> Self {
        self.next_page_token = token.into();
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_workflow_execution() {
        let exec = WorkflowExecution::new("test-workflow", "test-run");
        assert_eq!(exec.workflow_id, "test-workflow");
        assert_eq!(exec.run_id, "test-run");
        assert_eq!(exec.to_string(), "test-workflow:test-run");
    }

    #[test]
    fn test_payload_json() {
        #[derive(Serialize, Deserialize, PartialEq, Debug)]
        struct TestData {
            value: i32,
        }

        let data = TestData { value: 42 };
        let payload = Payload::from_json(&data).unwrap();
        assert!(!payload.is_empty());
        assert_eq!(
            payload.get_metadata_string("encoding"),
            Some("json".to_string())
        );

        let decoded: TestData = payload.to_json().unwrap();
        assert_eq!(decoded, data);
    }

    #[test]
    fn test_payload_string() {
        let payload = Payload::from_string("hello world");
        assert_eq!(payload.as_string().unwrap(), "hello world");
        assert_eq!(
            payload.get_metadata_string("encoding"),
            Some("utf-8".to_string())
        );
    }

    #[test]
    fn test_workflow_status() {
        assert!(WorkflowStatus::Completed.is_terminal());
        assert!(WorkflowStatus::Failed.is_terminal());
        assert!(!WorkflowStatus::Running.is_terminal());
        assert!(WorkflowStatus::Running.is_running());
    }

    #[test]
    fn test_retry_policy_default() {
        let policy = RetryPolicy::default();
        assert_eq!(policy.initial_interval_seconds, 1);
        assert_eq!(policy.backoff_coefficient, 2.0);
        assert_eq!(policy.max_attempts, 0); // Unlimited
    }
}
