//! Requests that ORCHER Core sends to a language SDK.
//!
//! When Core polls work from the server, it turns it into an [`ExecutionRequest`] and hands
//! it to the language SDK, which runs the workflow code and replies with an
//! `ExecutionResult`.
//!
//! ## Terminology
//!
//! - **ExecutionRequest**: a request to run workflow code.
//! - **RequestJob**: one work item within a request.
//! - **ExecutionResult**: the language SDK's response.
//!
//! ## Flow
//!
//! ```text
//! Server → WorkflowExecutionTask → ExecutionRequest → Language SDK
//!                                                            ↓
//!                                                    ExecutionResult
//!                                                            ↓
//!                                                      Commands → Server
//! ```

use crate::types::{Payload, WorkflowExecution};
use serde::{Deserialize, Serialize};
use std::time::SystemTime;

/// A request to run workflow code in a language SDK.
///
/// This is the main interface between ORCHER Core and the language SDKs. Core builds a
/// request and sends it to the SDK, which processes the jobs and returns an `ExecutionResult`.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionRequest {
    /// Unique identifier of the workflow run.
    pub run_id: String,

    /// Identifiers of the workflow execution.
    pub execution: WorkflowExecution,

    /// When this request was created.
    pub timestamp: SystemTime,

    /// Jobs for the language SDK to process, in order.
    pub jobs: Vec<RequestJob>,

    /// Number of entries in the execution journal when the request was built.
    pub journal_length: usize,

    /// Whether the workflow is replaying recorded history rather than running for the
    /// first time.
    pub is_replaying: bool,

    /// Whether the workflow should continue as a fresh run. Always `false` from [`Self::new`].
    pub continue_as_new: bool,
}

impl ExecutionRequest {
    /// Creates a request timestamped with the current time, with `continue_as_new` unset.
    pub fn new(
        run_id: String,
        execution: WorkflowExecution,
        jobs: Vec<RequestJob>,
        journal_length: usize,
        is_replaying: bool,
    ) -> Self {
        Self {
            run_id,
            execution,
            timestamp: SystemTime::now(),
            jobs,
            journal_length,
            is_replaying,
            continue_as_new: false,
        }
    }

    /// Appends a job to the end of the request.
    pub fn add_job(&mut self, job: RequestJob) {
        self.jobs.push(job);
    }

    /// Returns `true` if the request has at least one job.
    pub fn has_jobs(&self) -> bool {
        !self.jobs.is_empty()
    }

    /// Returns the number of jobs in the request.
    pub fn job_count(&self) -> usize {
        self.jobs.len()
    }

    /// Returns `true` if the request's only job is a cache eviction.
    pub fn is_eviction_only(&self) -> bool {
        self.jobs.len() == 1 && matches!(self.jobs[0], RequestJob::EvictFromCache(_))
    }

    /// Returns `true` if this is the workflow's first execution: not replaying and with an
    /// empty journal.
    pub fn is_first_execution(&self) -> bool {
        !self.is_replaying && self.journal_length == 0
    }
}

/// One work item within an [`ExecutionRequest`].
///
/// Each job is something the workflow code must handle. The language SDK processes jobs
/// one at a time, in order.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum RequestJob {
    /// Start the workflow execution.
    StartWorkflow(StartWorkflowJob),

    /// A timer has elapsed.
    FireTimer(FireTimerJob),

    /// A task finished with a result or an error.
    CompleteTask(CompleteTaskJob),

    /// An event was sent to the workflow.
    HandleEvent(HandleEventJob),

    /// A client sent a query.
    ProcessQuery(ProcessQueryJob),

    /// Cancellation of the workflow was requested.
    CancelWorkflow(CancelWorkflowJob),

    /// A client sent an update to the workflow's state.
    UpdateState(UpdateStateJob),

    /// Drop the workflow from the SDK's cache. Runs no workflow code.
    EvictFromCache(EvictFromCacheJob),

    /// A child workflow started.
    ChildWorkflowStarted(ChildWorkflowStartedJob),

    /// A child workflow completed.
    ChildWorkflowCompleted(ChildWorkflowCompletedJob),

    /// A child workflow failed.
    ChildWorkflowFailed(ChildWorkflowFailedJob),

    /// A child workflow timed out.
    ChildWorkflowTimedOut(ChildWorkflowTimedOutJob),

    /// A child workflow was canceled.
    ChildWorkflowCanceled(ChildWorkflowCanceledJob),

    /// A child workflow was terminated.
    ChildWorkflowTerminated(ChildWorkflowTerminatedJob),

    /// A step completed. Covers every kind of step: closures, tasks, child workflows and
    /// side effects.
    CompleteStep(CompleteStepJob),
}

impl RequestJob {
    /// Returns the job's type name, for logging.
    pub fn job_type(&self) -> &str {
        match self {
            RequestJob::StartWorkflow(_) => "StartWorkflow",
            RequestJob::FireTimer(_) => "FireTimer",
            RequestJob::CompleteTask(_) => "CompleteTask",
            RequestJob::HandleEvent(_) => "HandleEvent",
            RequestJob::ProcessQuery(_) => "ProcessQuery",
            RequestJob::CancelWorkflow(_) => "CancelWorkflow",
            RequestJob::UpdateState(_) => "UpdateState",
            RequestJob::EvictFromCache(_) => "EvictFromCache",
            RequestJob::ChildWorkflowStarted(_) => "ChildWorkflowStarted",
            RequestJob::ChildWorkflowCompleted(_) => "ChildWorkflowCompleted",
            RequestJob::ChildWorkflowFailed(_) => "ChildWorkflowFailed",
            RequestJob::ChildWorkflowTimedOut(_) => "ChildWorkflowTimedOut",
            RequestJob::ChildWorkflowCanceled(_) => "ChildWorkflowCanceled",
            RequestJob::ChildWorkflowTerminated(_) => "ChildWorkflowTerminated",
            RequestJob::CompleteStep(_) => "CompleteStep",
        }
    }

    /// Returns `true` if handling this job runs workflow code. Only cache eviction does not.
    pub fn requires_execution(&self) -> bool {
        !matches!(self, RequestJob::EvictFromCache(_))
    }
}

/// Starts the workflow execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StartWorkflowJob {
    /// Workflow type name.
    pub workflow_type: String,

    /// Unique workflow ID.
    pub workflow_id: String,

    /// Task queue the workflow runs on.
    pub task_queue: String,

    /// Workflow input as a JSON value.
    ///
    /// Keeping it as JSON rather than bytes lets it cross FFI boundaries without an extra
    /// encode and decode.
    pub input: serde_json::Value,

    /// Workflow headers (metadata).
    pub headers: Vec<(String, Payload)>,

    /// When the workflow was scheduled.
    pub scheduled_time: SystemTime,

    /// Namespace the workflow belongs to.
    pub namespace: String,
}

/// Reports that a timer has elapsed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FireTimerJob {
    /// Timer sequence number, checked on replay for determinism.
    pub sequence: u32,

    /// Timer ID the workflow gave when it started the timer.
    pub timer_id: String,

    /// When the timer was scheduled.
    pub scheduled_time: SystemTime,

    /// When the timer fired.
    pub fired_time: SystemTime,
}

/// Reports that a task finished with a result or an error.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompleteTaskJob {
    /// Task sequence number, checked on replay for determinism.
    pub sequence: u32,

    /// Task ID.
    pub task_id: String,

    /// Task type name.
    pub task_type: String,

    /// Outcome of the task.
    pub result: TaskExecutionResult,
}

/// Outcome of a task execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum TaskExecutionResult {
    /// The task completed successfully.
    Success {
        /// Output returned by the task.
        output: Payload,
    },

    /// The task failed.
    Failed {
        /// Error message.
        message: String,

        /// Error details.
        details: Option<Payload>,

        /// Whether the error is retryable.
        retryable: bool,
    },

    /// The task was cancelled.
    Cancelled {
        /// Why the task was cancelled.
        reason: String,
    },
}

/// Delivers an event sent to the workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct HandleEventJob {
    /// Event sequence number, checked on replay for determinism.
    pub sequence: u32,

    /// Event name.
    pub event_name: String,

    /// Event payload.
    pub payload: Payload,

    /// Event headers (metadata).
    pub headers: Vec<(String, Payload)>,

    /// Event ID, used for deduplication.
    pub event_id: String,

    /// When the event was sent.
    pub sent_time: SystemTime,
}

/// Asks the workflow to answer a client query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ProcessQueryJob {
    /// Query ID, echoed back in the response.
    pub query_id: String,

    /// Query name.
    pub query_type: String,

    /// Query arguments.
    pub arguments: Vec<Payload>,

    /// Query headers (metadata).
    pub headers: Vec<(String, Payload)>,
}

/// Reports that cancellation of the workflow was requested.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelWorkflowJob {
    /// Why cancellation was requested.
    pub reason: String,

    /// Additional details about the cancellation.
    pub details: Option<Payload>,

    /// When cancellation was requested.
    pub requested_time: SystemTime,
}

/// Delivers a client update to the workflow's state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateStateJob {
    /// Update ID, echoed back in the result.
    pub update_id: String,

    /// Update name.
    pub update_name: String,

    /// Update payload.
    pub payload: Payload,

    /// Update headers (metadata).
    pub headers: Vec<(String, Payload)>,
}

/// Tells the SDK to drop a workflow from its cache.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct EvictFromCacheJob {
    /// Why the workflow is being evicted.
    pub reason: EvictionReason,

    /// Additional details.
    pub details: Option<String>,
}

/// Why a workflow was evicted from the cache.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum EvictionReason {
    /// The cache is full and needs room.
    CacheFull,

    /// The workflow has been idle too long.
    Timeout,

    /// The workflow completed or failed.
    WorkflowComplete,

    /// Eviction was requested explicitly.
    Explicit,

    /// The worker is shutting down.
    Shutdown,
}

// ============================================================================
// Child Workflow Job Types
// ============================================================================

/// Reports that a child workflow started.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChildWorkflowStartedJob {
    /// Child workflow ID.
    pub workflow_id: String,

    /// Child execution ID (its run ID).
    pub execution_id: String,

    /// Child workflow type.
    pub workflow_type: String,

    /// Namespace the child runs in.
    pub namespace: String,
}

/// Reports that a child workflow completed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChildWorkflowCompletedJob {
    /// Child workflow ID.
    pub workflow_id: String,

    /// Child execution ID (its run ID).
    pub execution_id: String,

    /// Result returned by the child workflow.
    pub result: Payload,
}

/// Reports that a child workflow failed.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChildWorkflowFailedJob {
    /// Child workflow ID.
    pub workflow_id: String,

    /// Child execution ID (its run ID).
    pub execution_id: String,

    /// Failure details.
    pub failure: ChildWorkflowFailure,
}

/// Details of a child workflow failure.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChildWorkflowFailure {
    /// Error message.
    pub message: String,

    /// Error type or category.
    pub error_type: String,

    /// Stack trace or other details.
    pub details: Option<String>,
}

/// Reports that a child workflow timed out.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChildWorkflowTimedOutJob {
    /// Child workflow ID.
    pub workflow_id: String,

    /// Child execution ID (its run ID).
    pub execution_id: String,

    /// Which timeout expired.
    pub timeout_type: ChildWorkflowTimeoutType,
}

/// Which timeout a child workflow exceeded.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ChildWorkflowTimeoutType {
    /// The start-to-close timeout. Also used when the server reports an unspecified or
    /// unknown timeout type.
    StartToClose,

    /// The execution timeout, across all runs of the workflow.
    Execution,

    /// The timeout for a single run.
    Run,
}

/// Reports that a child workflow was canceled.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChildWorkflowCanceledJob {
    /// Child workflow ID.
    pub workflow_id: String,

    /// Child execution ID (its run ID).
    pub execution_id: String,

    /// Cancellation details.
    pub details: Option<Payload>,
}

/// Reports that a child workflow was terminated.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ChildWorkflowTerminatedJob {
    /// Child workflow ID.
    pub workflow_id: String,

    /// Child execution ID (its run ID).
    pub execution_id: String,

    /// Why the child was terminated.
    pub reason: String,

    /// Termination details.
    pub details: Option<Payload>,
}

/// Reports a completed step recorded in the execution journal.
///
/// A step is a closure, task, child workflow or side effect. During replay the SDK injects
/// this recorded result into the workflow context instead of running the step again, which
/// keeps replay deterministic.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompleteStepJob {
    /// Unique step name, for example `"fetch_user_1"` or `"send_email_2"`.
    pub step_name: String,

    /// Kind of step (task, closure, child workflow or side effect), as the wire enum value.
    pub step_type: i32,

    /// Serialized result, if the step succeeded.
    pub result: Vec<u8>,

    /// Failure details, if the step failed.
    pub failure: Option<StepFailure>,

    /// Attempt number that produced this outcome.
    pub execution_attempt: i32,

    /// When the step completed.
    pub completed_at: Option<SystemTime>,

    /// How long the step took, in milliseconds.
    pub duration_ms: i64,
}

/// Details of a failed step.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepFailure {
    /// Error message.
    pub message: String,

    /// Where the error came from.
    pub source: String,

    /// Stack trace.
    pub stack_trace: String,

    /// Failure type or category.
    pub failure_type: String,
}

impl std::fmt::Display for EvictionReason {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EvictionReason::CacheFull => write!(f, "cache_full"),
            EvictionReason::Timeout => write!(f, "timeout"),
            EvictionReason::WorkflowComplete => write!(f, "workflow_complete"),
            EvictionReason::Explicit => write!(f, "explicit"),
            EvictionReason::Shutdown => write!(f, "shutdown"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_execution_request_creation() {
        let execution = WorkflowExecution {
            workflow_id: "test-workflow".to_string(),
            run_id: "run-123".to_string(),
        };

        let request = ExecutionRequest::new("run-123".to_string(), execution, vec![], 0, false);

        assert_eq!(request.run_id, "run-123");
        assert_eq!(request.journal_length, 0);
        assert!(!request.is_replaying);
        assert!(request.is_first_execution());
    }

    #[test]
    fn test_add_job() {
        let execution = WorkflowExecution {
            workflow_id: "test-workflow".to_string(),
            run_id: "run-123".to_string(),
        };

        let mut request = ExecutionRequest::new("run-123".to_string(), execution, vec![], 0, false);

        assert!(!request.has_jobs());

        let job = RequestJob::FireTimer(FireTimerJob {
            sequence: 1,
            timer_id: "timer-1".to_string(),
            scheduled_time: SystemTime::now(),
            fired_time: SystemTime::now(),
        });

        request.add_job(job);

        assert!(request.has_jobs());
        assert_eq!(request.job_count(), 1);
    }

    #[test]
    fn test_job_type_names() {
        let timer_job = RequestJob::FireTimer(FireTimerJob {
            sequence: 1,
            timer_id: "timer-1".to_string(),
            scheduled_time: SystemTime::now(),
            fired_time: SystemTime::now(),
        });

        assert_eq!(timer_job.job_type(), "FireTimer");
        assert!(timer_job.requires_execution());
    }

    #[test]
    fn test_eviction_only() {
        let execution = WorkflowExecution {
            workflow_id: "test-workflow".to_string(),
            run_id: "run-123".to_string(),
        };

        let eviction_job = RequestJob::EvictFromCache(EvictFromCacheJob {
            reason: EvictionReason::CacheFull,
            details: None,
        });

        let request = ExecutionRequest::new(
            "run-123".to_string(),
            execution,
            vec![eviction_job],
            0,
            false,
        );

        assert!(request.is_eviction_only());
    }

    #[test]
    fn test_eviction_reason_display() {
        assert_eq!(EvictionReason::CacheFull.to_string(), "cache_full");
        assert_eq!(EvictionReason::Timeout.to_string(), "timeout");
        assert_eq!(EvictionReason::Shutdown.to_string(), "shutdown");
    }

    #[test]
    fn test_task_execution_result() {
        let success = TaskExecutionResult::Success {
            output: Payload::from_json(&serde_json::json!({"result": "ok"})).unwrap(),
        };

        let failed = TaskExecutionResult::Failed {
            message: "Task failed".to_string(),
            details: None,
            retryable: true,
        };

        let cancelled = TaskExecutionResult::Cancelled {
            reason: "User cancelled".to_string(),
        };

        // Every variant must serialize.
        let _ = serde_json::to_string(&success).unwrap();
        let _ = serde_json::to_string(&failed).unwrap();
        let _ = serde_json::to_string(&cancelled).unwrap();
    }
}
