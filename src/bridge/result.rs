//! Responses that language SDKs send back to ORCHER Core after processing an
//! `ExecutionRequest`.
//!
//! ## Flow
//!
//! ```text
//! Language SDK processes ExecutionRequest
//!        ↓
//! Creates ExecutionResult with Commands
//!        ↓
//! Returns to ORCHER Core
//!        ↓
//! Core sends Commands to Server
//! ```

use crate::types::Payload;
use serde::{Deserialize, Serialize};
use std::time::Duration;

/// The language SDK's response to an `ExecutionRequest`.
///
/// Carries the outcome of running the workflow code and the commands the workflow wants
/// performed, such as scheduling tasks or starting timers.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionResult {
    /// Run ID of the request this result answers.
    pub run_id: String,

    /// Whether the workflow code ran without error.
    pub successful: bool,

    /// Commands to send to the server, in order.
    pub commands: Vec<Command>,

    /// Responses to the queries in the request, if any.
    pub query_responses: Vec<QueryResponse>,

    /// Results of the updates in the request, if any. Defaults to empty when absent on
    /// the wire.
    #[serde(default)]
    pub update_results: Vec<UpdateResponse>,

    /// The error, if execution failed.
    pub error: Option<ExecutionError>,

    /// Set when the workflow wants to restart as a fresh run.
    pub restart_fresh: Option<RestartFreshRequest>,
}

impl ExecutionResult {
    /// Creates a successful result carrying the given commands.
    pub fn success(run_id: String, commands: Vec<Command>) -> Self {
        Self {
            run_id,
            successful: true,
            commands,
            query_responses: Vec::new(),
            update_results: Vec::new(),
            error: None,
            restart_fresh: None,
        }
    }

    /// Creates a failed result carrying the given error and no commands.
    pub fn failed(run_id: String, error: ExecutionError) -> Self {
        Self {
            run_id,
            successful: false,
            commands: Vec::new(),
            query_responses: Vec::new(),
            update_results: Vec::new(),
            error: Some(error),
            restart_fresh: None,
        }
    }

    /// Appends a command.
    pub fn add_command(&mut self, command: Command) {
        self.commands.push(command);
    }

    /// Appends a query response.
    pub fn add_query_response(&mut self, response: QueryResponse) {
        self.query_responses.push(response);
    }

    /// Asks for the workflow to restart as a fresh run.
    pub fn set_restart_fresh(&mut self, request: RestartFreshRequest) {
        self.restart_fresh = Some(request);
    }

    /// Returns `true` if the result has at least one command.
    pub fn has_commands(&self) -> bool {
        !self.commands.is_empty()
    }

    /// Appends an update response.
    pub fn add_update_response(&mut self, response: UpdateResponse) {
        self.update_results.push(response);
    }

    /// Returns `true` if the result has at least one query response.
    pub fn has_query_responses(&self) -> bool {
        !self.query_responses.is_empty()
    }

    /// Returns `true` if the result has at least one update result.
    pub fn has_update_results(&self) -> bool {
        !self.update_results.is_empty()
    }
}

/// An instruction from workflow code to the server.
///
/// Commands carry the workflow's orchestration decisions and side effects.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum Command {
    /// Schedule a task.
    ScheduleTask(ScheduleTaskCommand),

    /// Start a timer.
    StartTimer(StartTimerCommand),

    /// Cancel a timer.
    CancelTimer(CancelTimerCommand),

    /// Send an event to another workflow.
    SendEvent(SendEventCommand),

    /// Complete the workflow successfully. Terminal.
    CompleteWorkflow(CompleteWorkflowCommand),

    /// Fail the workflow. Terminal.
    FailWorkflow(FailWorkflowCommand),

    /// Cancel the workflow.
    CancelWorkflowExecution(CancelWorkflowCommand),

    /// Start a child workflow.
    StartChildWorkflow(StartChildWorkflowCommand),

    /// Cancel a child workflow.
    CancelChildWorkflow(CancelChildWorkflowCommand),

    /// Request graceful cancellation of the workflow.
    RequestCancellation(RequestCancellationCommand),

    /// Restart the workflow as a fresh run. Terminal.
    RestartFresh(RestartFreshCommand),

    /// Query a child workflow.
    QueryChildWorkflow(QueryChildWorkflowCommand),

    /// Record a step's result so replay can reuse it.
    RecordStepResult(RecordStepResultCommand),

    /// Park the workflow until an external event arrives. Has no server-side command.
    WaitForEvent(WaitForEventCommand),
}

impl Command {
    /// Returns the command's type name, for logging.
    pub fn command_type(&self) -> &str {
        match self {
            Command::ScheduleTask(_) => "ScheduleTask",
            Command::StartTimer(_) => "StartTimer",
            Command::CancelTimer(_) => "CancelTimer",
            Command::SendEvent(_) => "SendEvent",
            Command::CompleteWorkflow(_) => "CompleteWorkflow",
            Command::FailWorkflow(_) => "FailWorkflow",
            Command::CancelWorkflowExecution(_) => "CancelWorkflowExecution",
            Command::StartChildWorkflow(_) => "StartChildWorkflow",
            Command::CancelChildWorkflow(_) => "CancelChildWorkflow",
            Command::RequestCancellation(_) => "RequestCancellation",
            Command::RestartFresh(_) => "RestartFresh",
            Command::QueryChildWorkflow(_) => "QueryChildWorkflow",
            Command::RecordStepResult(_) => "RecordStepResult",
            Command::WaitForEvent(_) => "WaitForEvent",
        }
    }

    /// Returns `true` if this command ends the current run: complete, fail or restart fresh.
    pub fn is_terminal(&self) -> bool {
        matches!(
            self,
            Command::CompleteWorkflow(_) | Command::FailWorkflow(_) | Command::RestartFresh(_)
        )
    }
}

/// Schedules a task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ScheduleTaskCommand {
    /// Task sequence number, checked on replay for determinism.
    pub sequence: u32,

    /// Task ID.
    pub task_id: String,

    /// Task type name.
    pub task_type: String,

    /// Task queue to schedule the task on.
    pub task_queue: String,

    /// Task input arguments.
    pub input: Vec<Payload>,

    /// How long the task may run once a worker starts it.
    ///
    /// This limits running time only; time spent waiting in the queue is governed by
    /// `queue_timeout`. Keeping the two separate means a task given 60s runs for at most
    /// 60s, and a caller can ask for a task that starts promptly but runs long.
    pub timeout: Duration,

    /// How long the task may sit in its queue before a worker picks it up.
    ///
    /// `None` means no queue limit: a caller who says nothing about queueing has not
    /// asked for the task to be killed for waiting. Defaults to `None` when absent on
    /// the wire.
    #[serde(default)]
    pub queue_timeout: Option<Duration>,

    /// Maximum time between heartbeats before the task is considered failed.
    ///
    /// `None` leaves heartbeat enforcement off for this task. A value set here is passed
    /// to the server, which fails the task when heartbeats stop arriving in time.
    ///
    /// `serde(default)` lets a command from an SDK that does not emit this field still
    /// deserialize instead of failing the whole schedule. Serde does not default a missing
    /// `Option` to `None` on its own.
    #[serde(default)]
    pub heartbeat_timeout: Option<Duration>,

    /// Retry policy for the task, if any.
    pub retry_policy: Option<TaskRetryPolicy>,

    /// Task headers (metadata).
    pub headers: Vec<(String, Payload)>,
}

/// Retry policy for a task.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TaskRetryPolicy {
    /// Maximum number of attempts.
    pub max_attempts: u32,

    /// Delay before the first retry.
    pub initial_interval: Duration,

    /// Upper bound on the delay between retries.
    pub max_interval: Duration,

    /// Multiplier applied to the delay after each retry (exponential backoff).
    pub backoff_coefficient: f64,

    /// Error types that are never retried.
    pub non_retryable_errors: Vec<String>,
}

/// Starts a timer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StartTimerCommand {
    /// Timer sequence number, checked on replay for determinism.
    pub sequence: u32,

    /// Timer ID. The matching fire job carries the same ID.
    pub timer_id: String,

    /// How long until the timer fires.
    pub duration: Duration,
}

/// Cancels a timer.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelTimerCommand {
    /// Timer sequence number, checked on replay for determinism.
    pub sequence: u32,

    /// ID of the timer to cancel.
    pub timer_id: String,
}

/// Sends an event to another workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SendEventCommand {
    /// Target workflow ID.
    pub workflow_id: String,

    /// Target run ID, when a specific run is targeted.
    pub run_id: Option<String>,

    /// Event name.
    pub event_name: String,

    /// Event payload.
    pub payload: Payload,

    /// Event headers (metadata).
    pub headers: Vec<(String, Payload)>,
}

/// Completes the workflow successfully.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CompleteWorkflowCommand {
    /// Workflow output.
    pub result: Payload,
}

/// Fails the workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct FailWorkflowCommand {
    /// Failure message.
    pub message: String,

    /// Failure details.
    pub details: Option<Payload>,

    /// Error type.
    pub error_type: String,
}

/// Cancels the workflow execution.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelWorkflowCommand {
    /// Cancellation details.
    pub details: Option<Payload>,
}

/// Starts a child workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StartChildWorkflowCommand {
    /// Sequence number, checked on replay for determinism.
    pub sequence: u32,

    /// Child workflow ID.
    pub workflow_id: String,

    /// Child workflow type.
    pub workflow_type: String,

    /// Task queue the child runs on.
    pub task_queue: String,

    /// Input arguments.
    pub input: Vec<Payload>,

    /// Child workflow timeout, if any.
    pub timeout: Option<Duration>,

    /// What happens to the child when the parent closes.
    pub orphan_policy: OrphanPolicy,
}

/// What happens to a child workflow when its parent closes.
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum OrphanPolicy {
    /// Terminate the child immediately.
    Terminate,

    /// Request graceful cancellation of the child.
    Cancel,

    /// Leave the child running.
    Abandon,
}

/// Cancels a child workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct CancelChildWorkflowCommand {
    /// Child workflow ID.
    pub workflow_id: String,

    /// Child run ID.
    pub run_id: String,
}

/// Requests graceful cancellation of the workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RequestCancellationCommand {
    /// Why cancellation is requested.
    pub reason: String,
}

/// Restarts the workflow as a fresh run with clean state.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestartFreshCommand {
    /// Workflow type for the fresh run; may differ from the current one.
    pub workflow_type: String,

    /// Input for the fresh run.
    pub input: Vec<Payload>,

    /// Task queue for the fresh run, if set.
    pub task_queue: Option<String>,

    /// Timeout for the fresh run, if set.
    pub timeout: Option<Duration>,
}

/// Queries a child workflow.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryChildWorkflowCommand {
    /// Child workflow ID.
    pub workflow_id: String,

    /// Child run ID.
    pub run_id: String,

    /// Query name.
    pub query_type: String,

    /// Query arguments.
    pub arguments: Vec<Payload>,
}

/// Records the result of a step so it is not executed again on replay.
///
/// The SDK sends this after a step (closure, task, child workflow or side effect) finishes.
/// The server stores the result, and later replays receive it as a `CompleteStep` job
/// instead of running the step again.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RecordStepResultCommand {
    /// Unique step name, for example `"fetch_user_1"`.
    pub step_name: String,

    /// Kind of step (task, closure, child workflow or side effect), as the wire enum value.
    pub step_type: i32,

    /// Serialized result, if the step succeeded.
    pub result: Vec<u8>,

    /// Failure details, if the step failed.
    pub failure: Option<StepFailure>,

    /// Attempt number that produced this outcome, for retry tracking.
    pub execution_attempt: i32,
}

/// Suspend the workflow until a named external event arrives.
///
/// This produces no server-side proto command. The engine keeps the workflow
/// claimed while it waits; when `SendEvent` delivers an `EventReceived` journal
/// entry, the claim is released and the workflow is dispatched again.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WaitForEventCommand {
    /// Deterministic step ID, for example `"event_approve_1"`.
    ///
    /// Optional on the wire, defaulting to an empty string. Core never reads it, since the
    /// command produces no proto; requiring it would turn a missing field into a JSON parse
    /// error that fails the whole workflow task instead of parking the workflow.
    #[serde(default)]
    pub step_id: String,

    /// Name of the event to wait for, for example `"approve"`.
    pub event_name: String,

    /// How long to wait, in milliseconds, if the wait is bounded.
    pub timeout_ms: Option<i64>,
}

/// Details of a failed step.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct StepFailure {
    /// Error message.
    pub message: String,

    /// Stack trace, or empty if none is available.
    pub stack_trace: String,

    /// Where the error came from, for example `"StepExecution"`.
    pub source: String,

    /// Application-specific failure type.
    pub failure_type: String,
}

/// The workflow's answer to a query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct QueryResponse {
    /// ID of the query being answered.
    pub query_id: String,

    /// Query outcome.
    pub result: QueryResult,
}

/// Outcome of a query.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum QueryResult {
    /// The query succeeded.
    Success {
        /// Query output.
        output: Payload,
    },

    /// The query failed.
    Failed {
        /// Error message.
        message: String,

        /// Error details.
        details: Option<Payload>,
    },
}

/// The workflow's response to an update.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct UpdateResponse {
    /// ID of the update being answered.
    pub update_id: String,

    /// Update outcome.
    pub result: UpdateResult,
}

/// Outcome of an update.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum UpdateResult {
    /// The update was applied.
    Completed {
        /// Update output.
        output: Payload,
    },

    /// The workflow rejected the update.
    Rejected {
        /// Why the update was rejected.
        message: String,
    },

    /// The update failed while being applied.
    Failed {
        /// Error message.
        message: String,

        /// Error details.
        details: Option<Payload>,
    },
}

/// An error raised while running workflow code.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ExecutionError {
    /// Error message.
    pub message: String,

    /// Error category.
    pub error_type: ExecutionErrorType,

    /// Stack trace or other details.
    pub details: Option<String>,

    /// Whether the error is retryable.
    pub retryable: bool,
}

impl ExecutionError {
    /// Creates a non-determinism error. Never retryable: replaying the same code against
    /// the same history fails the same way.
    pub fn non_determinism(message: String, details: Option<String>) -> Self {
        Self {
            message,
            error_type: ExecutionErrorType::NonDeterminism,
            details,
            retryable: false,
        }
    }

    /// Creates an error raised by workflow code.
    pub fn workflow_error(message: String, retryable: bool) -> Self {
        Self {
            message,
            error_type: ExecutionErrorType::WorkflowCode,
            details: None,
            retryable,
        }
    }

    /// Creates a timeout error. Not retryable.
    pub fn timeout(message: String) -> Self {
        Self {
            message,
            error_type: ExecutionErrorType::Timeout,
            details: None,
            retryable: false,
        }
    }
}

/// Category of an [`ExecutionError`].
#[derive(Debug, Clone, Copy, Serialize, Deserialize, PartialEq, Eq)]
pub enum ExecutionErrorType {
    /// Workflow code diverged from its recorded history.
    NonDeterminism,

    /// Workflow code raised an error.
    WorkflowCode,

    /// Execution timed out.
    Timeout,

    /// A task failed.
    TaskFailed,

    /// The workflow was cancelled.
    Cancelled,

    /// An internal SDK error.
    InternalError,
}

/// A request to restart the workflow as a fresh run.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct RestartFreshRequest {
    /// Workflow type for the fresh run.
    pub workflow_type: String,

    /// Input for the fresh run.
    pub input: Vec<Payload>,

    /// Task queue for the fresh run, if set.
    pub task_queue: Option<String>,

    /// Timeout for the fresh run, if set.
    pub timeout: Option<Duration>,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_execution_result_success() {
        let result = ExecutionResult::success("run-123".to_string(), vec![]);

        assert!(result.successful);
        assert!(result.error.is_none());
        assert_eq!(result.run_id, "run-123");
    }

    #[test]
    fn test_execution_result_failed() {
        let error = ExecutionError::workflow_error("Workflow failed".to_string(), true);

        let result = ExecutionResult::failed("run-123".to_string(), error);

        assert!(!result.successful);
        assert!(result.error.is_some());
    }

    #[test]
    fn test_add_command() {
        let mut result = ExecutionResult::success("run-123".to_string(), vec![]);

        assert!(!result.has_commands());

        let command = Command::StartTimer(StartTimerCommand {
            sequence: 1,
            timer_id: "timer-1".to_string(),
            duration: Duration::from_secs(60),
        });

        result.add_command(command);

        assert!(result.has_commands());
        assert_eq!(result.commands.len(), 1);
    }

    #[test]
    fn test_terminal_commands() {
        let complete = Command::CompleteWorkflow(CompleteWorkflowCommand {
            result: Payload::from_json(&serde_json::json!({})).unwrap(),
        });

        let fail = Command::FailWorkflow(FailWorkflowCommand {
            message: "Failed".to_string(),
            details: None,
            error_type: "TestError".to_string(),
        });

        let timer = Command::StartTimer(StartTimerCommand {
            sequence: 1,
            timer_id: "timer-1".to_string(),
            duration: Duration::from_secs(60),
        });

        assert!(complete.is_terminal());
        assert!(fail.is_terminal());
        assert!(!timer.is_terminal());
    }

    #[test]
    fn test_command_type_names() {
        let timer = Command::StartTimer(StartTimerCommand {
            sequence: 1,
            timer_id: "timer-1".to_string(),
            duration: Duration::from_secs(60),
        });

        assert_eq!(timer.command_type(), "StartTimer");
    }

    #[test]
    fn test_execution_error_types() {
        let non_det = ExecutionError::non_determinism("Non-determinism detected".to_string(), None);
        assert_eq!(non_det.error_type, ExecutionErrorType::NonDeterminism);
        assert!(!non_det.retryable);

        let workflow_err = ExecutionError::workflow_error("Code failed".to_string(), true);
        assert_eq!(workflow_err.error_type, ExecutionErrorType::WorkflowCode);
        assert!(workflow_err.retryable);

        let timeout = ExecutionError::timeout("Timed out".to_string());
        assert_eq!(timeout.error_type, ExecutionErrorType::Timeout);
        assert!(!timeout.retryable);
    }

    #[test]
    fn test_orphan_policy() {
        let policy = OrphanPolicy::Terminate;
        assert_eq!(policy, OrphanPolicy::Terminate);

        let serialized = serde_json::to_string(&policy).unwrap();
        let deserialized: OrphanPolicy = serde_json::from_str(&serialized).unwrap();
        assert_eq!(policy, deserialized);
    }

    #[test]
    fn test_query_response() {
        let response = QueryResponse {
            query_id: "query-1".to_string(),
            result: QueryResult::Success {
                output: Payload::from_json(&serde_json::json!({"count": 42})).unwrap(),
            },
        };

        let serialized = serde_json::to_string(&response).unwrap();
        assert!(serialized.contains("query-1"));
    }

    /// A WaitForEvent command from an SDK that sends no step id must still parse.
    /// Failing with "missing field `step_id`" would fail the whole workflow task and
    /// report the workflow as failed instead of parking it for the event.
    #[test]
    fn wait_for_event_parses_without_a_step_id() {
        let json = r#"{"WaitForEvent":{"event_name":"order_shipped","timeout_ms":900000}}"#;
        let command: Command = serde_json::from_str(json).unwrap();
        match command {
            Command::WaitForEvent(cmd) => {
                assert_eq!(cmd.event_name, "order_shipped");
                assert_eq!(cmd.timeout_ms, Some(900_000));
                assert_eq!(cmd.step_id, "");
            }
            other => panic!("expected WaitForEvent, got {:?}", other),
        }
    }

    #[test]
    fn wait_for_event_keeps_a_step_id_when_given() {
        let json = r#"{"WaitForEvent":{"step_id":"event_approve_3","event_name":"approve","timeout_ms":null}}"#;
        let command: Command = serde_json::from_str(json).unwrap();
        match command {
            Command::WaitForEvent(cmd) => {
                assert_eq!(cmd.step_id, "event_approve_3");
                assert_eq!(cmd.timeout_ms, None);
            }
            other => panic!("expected WaitForEvent, got {:?}", other),
        }
    }
}
