//! Rust types that wrap the protocol buffer messages for workflows, tasks,
//! and events, with conversions to and from the wire types.
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
use std::time::SystemTime;

use crate::error::{Error, Result};
use crate::proto::orcher::v1 as proto;

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

impl From<proto::WorkflowExecution> for WorkflowExecution {
    fn from(proto: proto::WorkflowExecution) -> Self {
        Self {
            workflow_id: proto.workflow_id,
            run_id: proto.execution_id,
        }
    }
}

impl From<WorkflowExecution> for proto::WorkflowExecution {
    fn from(exec: WorkflowExecution) -> Self {
        Self {
            workflow_id: exec.workflow_id,
            execution_id: exec.run_id,
            ..Default::default()
        }
    }
}

impl fmt::Display for WorkflowExecution {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}:{}", self.workflow_id, self.run_id)
    }
}

// There is no `From` conversion between `Payload` and `proto::Payload`. Convert at
// the call site: both have the same fields, `data` and `metadata`.

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

impl From<i32> for WorkflowStatus {
    fn from(value: i32) -> Self {
        use proto::WorkflowStatus as ProtoStatus;
        match value {
            x if x == ProtoStatus::Running as i32 => Self::Running,
            x if x == ProtoStatus::Completed as i32 => Self::Completed,
            x if x == ProtoStatus::Failed as i32 => Self::Failed,
            x if x == ProtoStatus::Canceled as i32 => Self::Cancelled,
            x if x == ProtoStatus::Terminated as i32 => Self::Terminated,
            x if x == ProtoStatus::TimedOut as i32 => Self::TimedOut,
            x if x == ProtoStatus::RestartedFresh as i32 => Self::RestartedFresh,
            _ => Self::Running, // Unknown values map to the non-terminal Running
        }
    }
}

impl From<WorkflowStatus> for i32 {
    fn from(status: WorkflowStatus) -> Self {
        use proto::WorkflowStatus as ProtoStatus;
        match status {
            WorkflowStatus::Running => ProtoStatus::Running as i32,
            WorkflowStatus::Completed => ProtoStatus::Completed as i32,
            WorkflowStatus::Failed => ProtoStatus::Failed as i32,
            WorkflowStatus::Cancelled => ProtoStatus::Canceled as i32,
            WorkflowStatus::Terminated => ProtoStatus::Terminated as i32,
            WorkflowStatus::TimedOut => ProtoStatus::TimedOut as i32,
            WorkflowStatus::RestartedFresh => ProtoStatus::RestartedFresh as i32,
        }
    }
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

/// Retry policy for workflows and tasks.
///
/// The default starts at 1 second, doubles each attempt up to 60 seconds, and
/// retries without limit.
#[derive(Debug, Clone, Serialize, Deserialize)]
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

impl From<proto::RetryPolicy> for RetryPolicy {
    fn from(proto: proto::RetryPolicy) -> Self {
        Self {
            initial_interval_seconds: proto.initial_interval.map_or(1, |d| d.seconds),
            backoff_coefficient: proto.backoff_coefficient,
            max_interval_seconds: proto.maximum_interval.map_or(60, |d| d.seconds),
            max_attempts: proto.maximum_attempts,
            non_retryable_error_types: proto.non_retryable_error_types,
        }
    }
}

impl From<RetryPolicy> for proto::RetryPolicy {
    fn from(policy: RetryPolicy) -> Self {
        Self {
            initial_interval: Some(prost_types::Duration {
                seconds: policy.initial_interval_seconds,
                nanos: 0,
            }),
            backoff_coefficient: policy.backoff_coefficient,
            maximum_interval: Some(prost_types::Duration {
                seconds: policy.max_interval_seconds,
                nanos: 0,
            }),
            maximum_attempts: policy.max_attempts,
            non_retryable_error_types: policy.non_retryable_error_types,
            ..Default::default()
        }
    }
}

/// A failure, with its type, message, stack trace, and optional cause.
///
/// Displays as `[error_type] message`, followed by a `Caused by:` line per
/// cause.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct Failure {
    /// Error message
    pub message: String,
    /// The failure that caused this one, if any
    pub source: Option<Box<Failure>>,
    /// Stack trace
    pub stack_trace: String,
    /// Error type name
    pub error_type: String,
}

impl Failure {
    /// Creates a failure with no cause and no stack trace.
    pub fn new(message: impl Into<String>, error_type: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            source: None,
            stack_trace: String::new(),
            error_type: error_type.into(),
        }
    }

    /// Sets the failure that caused this one.
    pub fn with_source(mut self, source: Failure) -> Self {
        self.source = Some(Box::new(source));
        self
    }

    /// Sets the stack trace.
    pub fn with_stack_trace(mut self, stack_trace: impl Into<String>) -> Self {
        self.stack_trace = stack_trace.into();
        self
    }
}

impl From<proto::Failure> for Failure {
    fn from(proto: proto::Failure) -> Self {
        Self {
            message: proto.message,
            source: proto.cause.map(|c| Box::new(Self::from(*c))),
            stack_trace: proto.stack_trace,
            error_type: proto.failure_type,
        }
    }
}

impl From<Failure> for proto::Failure {
    fn from(failure: Failure) -> Self {
        Self {
            message: failure.message,
            source: String::new(),
            stack_trace: failure.stack_trace,
            cause: failure.source.map(|s| Box::new(proto::Failure::from(*s))),
            failure_type: failure.error_type,
            details: vec![],
            non_retryable: false,
            ..Default::default()
        }
    }
}

impl fmt::Display for Failure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "[{}] {}", self.error_type, self.message)?;
        if let Some(source) = &self.source {
            write!(f, "\nCaused by: {}", source)?;
        }
        Ok(())
    }
}

/// Header metadata attached to workflows and tasks.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Header {
    /// Header fields
    pub fields: HashMap<String, Vec<u8>>,
}

impl Header {
    /// Creates an empty header.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a string field, stored as UTF-8 bytes.
    pub fn with_string(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.fields.insert(key.into(), value.into().into_bytes());
        self
    }

    /// Returns a field as a string, or `None` if it is absent or not UTF-8.
    pub fn get_string(&self, key: &str) -> Option<String> {
        self.fields
            .get(key)
            .and_then(|bytes| String::from_utf8(bytes.clone()).ok())
    }

    /// Adds a binary field.
    pub fn with_bytes(mut self, key: impl Into<String>, value: Vec<u8>) -> Self {
        self.fields.insert(key.into(), value);
        self
    }

    /// Returns a field's raw bytes.
    pub fn get_bytes(&self, key: &str) -> Option<&[u8]> {
        self.fields.get(key).map(|v| v.as_slice())
    }
}

impl From<proto::Header> for Header {
    fn from(proto: proto::Header) -> Self {
        Self {
            fields: proto.fields,
        }
    }
}

impl From<Header> for proto::Header {
    fn from(header: Header) -> Self {
        Self {
            fields: header.fields,
            ..Default::default()
        }
    }
}

/// Annotations for storing non-indexed workflow metadata.
///
/// Use annotations for correlation IDs, deployment tags, environment info,
/// or any key-value data you want to read back but don't need to filter on.
/// For metadata you want to search or filter on, use labels instead.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Annotations {
    /// Annotation key-value pairs
    pub fields: HashMap<String, String>,
}

/// Organization context for multi-tenancy.
///
/// The organization is the unit for quotas, billing attribution, and
/// isolation. On the wire its ID travels in the `x-organization-id` header,
/// which a client sends once configured with
/// [`WorkflowClient::with_organization_id`](crate::client::WorkflowClient::with_organization_id).
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
#[non_exhaustive]
pub struct OrganizationContext {
    /// Organization ID (UUID string)
    pub organization_id: Option<String>,
}

impl OrganizationContext {
    /// Creates a context for the given organization ID.
    pub fn new(organization_id: impl Into<String>) -> Self {
        Self {
            organization_id: Some(organization_id.into()),
        }
    }

    /// Returns whether an organization ID is set.
    pub fn is_set(&self) -> bool {
        self.organization_id.is_some()
    }

    /// Returns the organization ID, if set.
    pub fn id(&self) -> Option<&str> {
        self.organization_id.as_deref()
    }
}

impl Annotations {
    /// Creates an empty annotations map.
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a key-value annotation.
    pub fn with_value(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.fields.insert(key.into(), value.into());
        self
    }

    /// Returns the value for `key`, if present.
    pub fn get(&self, key: &str) -> Option<&String> {
        self.fields.get(key)
    }
}

impl From<HashMap<String, String>> for Annotations {
    fn from(fields: HashMap<String, String>) -> Self {
        Self { fields }
    }
}

impl From<Annotations> for HashMap<String, String> {
    fn from(annotations: Annotations) -> Self {
        annotations.fields
    }
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

impl WorkflowExecutionInfo {
    /// Converts from the proto `WorkflowExecutionInfo`.
    pub(crate) fn from_proto(info: proto::WorkflowExecutionInfo) -> Self {
        let start_time = info.start_time.map(|t| {
            SystemTime::UNIX_EPOCH + std::time::Duration::new(t.seconds as u64, t.nanos as u32)
        });
        let close_time = info.close_time.map(|t| {
            SystemTime::UNIX_EPOCH + std::time::Duration::new(t.seconds as u64, t.nanos as u32)
        });
        let execution_duration_seconds = info.execution_time.map(|d| d.seconds);
        let parent_execution = info.parent_execution.map(|p| ParentExecutionInfo {
            namespace: p.namespace,
            workflow_id: p.workflow_id,
            execution_id: p.execution_id,
        });

        Self {
            workflow_id: info.workflow_id,
            execution_id: info.execution_id,
            workflow_type: info.workflow_type,
            task_queue: info.task_queue,
            namespace: info.namespace,
            status: WorkflowStatus::from(info.status),
            start_time,
            close_time,
            execution_duration_seconds,
            journal_length: info.journal_length,
            parent_execution,
            labels: info.labels,
            annotations: info.annotations,
            tags: info.tags,
            started_by: info.started_by,
            attempt: info.attempt,
            cron_schedule: info.cron_schedule,
            state_transition_count: info.state_transition_count,
        }
    }
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
    /// Converts from the proto `ListWorkflowsResponse`.
    pub(crate) fn from_list_response(resp: proto::ListWorkflowsResponse) -> Self {
        Self {
            executions: resp
                .executions
                .into_iter()
                .map(WorkflowExecutionInfo::from_proto)
                .collect(),
            next_page_token: resp.next_page_token,
        }
    }

    /// Converts from the proto `SearchWorkflowsResponse`.
    pub(crate) fn from_search_response(resp: proto::SearchWorkflowsResponse) -> Self {
        Self {
            executions: resp
                .executions
                .into_iter()
                .map(WorkflowExecutionInfo::from_proto)
                .collect(),
            next_page_token: resp.next_page_token,
        }
    }

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

impl From<ListWorkflowsSortOrder> for i32 {
    fn from(order: ListWorkflowsSortOrder) -> Self {
        use proto::ListWorkflowsSortOrder as ProtoOrder;
        match order {
            ListWorkflowsSortOrder::StartTimeAsc => ProtoOrder::StartTimeAsc as i32,
            ListWorkflowsSortOrder::StartTimeDesc => ProtoOrder::StartTimeDesc as i32,
            ListWorkflowsSortOrder::CloseTimeAsc => ProtoOrder::CloseTimeAsc as i32,
            ListWorkflowsSortOrder::CloseTimeDesc => ProtoOrder::CloseTimeDesc as i32,
        }
    }
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

impl WorkflowExecutionDescription {
    /// Converts from the proto `DescribeWorkflowExecutionResponse`.
    ///
    /// Fails if the response has no `execution_info`.
    pub(crate) fn from_proto(proto: proto::DescribeWorkflowExecutionResponse) -> Result<Self> {
        let info = proto
            .execution_info
            .ok_or_else(|| Error::internal("DescribeWorkflowExecution: missing execution_info"))?;

        let status = WorkflowStatus::from(info.status);

        let start_time = info.start_time.map(|t| {
            SystemTime::UNIX_EPOCH + std::time::Duration::new(t.seconds as u64, t.nanos as u32)
        });
        let close_time = info.close_time.map(|t| {
            SystemTime::UNIX_EPOCH + std::time::Duration::new(t.seconds as u64, t.nanos as u32)
        });

        let parent_execution = info.parent_execution.map(|p| ParentExecutionInfo {
            namespace: p.namespace,
            workflow_id: p.workflow_id,
            execution_id: p.execution_id,
        });

        let execution_config = proto.execution_config.map(|c| WorkflowExecutionConfig {
            task_queue: c.task_queue,
            execution_timeout_seconds: c.execution_timeout.map(|d| d.seconds),
            run_timeout_seconds: c.run_timeout.map(|d| d.seconds),
            default_task_timeout_seconds: c.default_task_timeout.map(|d| d.seconds),
            retry_policy: c.retry_policy.map(RetryPolicy::from),
            cron_schedule: c.cron_schedule,
        });

        Ok(Self {
            execution: WorkflowExecution::new(info.workflow_id, info.execution_id),
            workflow_type: info.workflow_type,
            task_queue: info.task_queue,
            status,
            start_time,
            close_time,
            journal_length: info.journal_length,
            attempt: info.attempt,
            labels: info.labels,
            annotations: info.annotations,
            tags: info.tags,
            started_by: info.started_by,
            cron_schedule: info.cron_schedule,
            state_transition_count: info.state_transition_count,
            parent_execution,
            pending_tasks: proto.pending_tasks,
            pending_timers: proto.pending_timers,
            pending_events: proto.pending_events,
            execution_config,
        })
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

    #[test]
    fn test_failure() {
        let failure = Failure::new("test error", "TestError").with_stack_trace("line 1\nline 2");
        assert_eq!(failure.message, "test error");
        assert_eq!(failure.error_type, "TestError");
        assert!(failure.to_string().contains("TestError"));
    }

    #[test]
    fn test_header() {
        let header = Header::new()
            .with_string("key1", "value1")
            .with_bytes("key2", vec![1, 2, 3]);

        assert_eq!(header.get_string("key1"), Some("value1".to_string()));
        assert_eq!(header.get_bytes("key2"), Some(&[1u8, 2, 3][..]));
    }

    #[test]
    fn test_annotations() {
        let annotations = Annotations::new()
            .with_value("count", "42")
            .with_value("name", "test");

        assert!(annotations.get("count").is_some());
        assert!(annotations.get("name").is_some());
        assert_eq!(annotations.get("count").unwrap(), "42");
        assert_eq!(annotations.get("name").unwrap(), "test");
    }

    #[test]
    fn test_organization_context() {
        let ctx = OrganizationContext::default();
        assert!(!ctx.is_set());
        assert!(ctx.id().is_none());

        let ctx = OrganizationContext::new("org_abc123");
        assert!(ctx.is_set());
        assert_eq!(ctx.id(), Some("org_abc123"));
        assert_eq!(ctx.organization_id, Some("org_abc123".to_string()));
    }

    #[test]
    fn test_organization_context_serialization() {
        let ctx = OrganizationContext::new("org_test");
        let json = serde_json::to_string(&ctx).unwrap();
        assert!(json.contains("org_test"));

        let decoded: OrganizationContext = serde_json::from_str(&json).unwrap();
        assert_eq!(decoded.id(), Some("org_test"));
    }
}
