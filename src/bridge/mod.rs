//! Bridge between ORCHER Core and the language SDKs.
//!
//! Defines the interface that language SDKs (Rust, Python, TypeScript) implement
//! to run workflow code. Core polls work from the server, converts it to an
//! [`ExecutionRequest`], hands it to the SDK, and gets back an [`ExecutionResult`]
//! that carries orchestration [`Command`]s.
//!
//! ```text
//! Server → Poller → ExecutionRequest → Language SDK
//!                                          ↓
//!                                    ExecutionResult { commands } → Server
//! ```
//!
//! ## Key Types
//!
//! - [`ExecutionRequest`]: work sent to the SDK (run ID, jobs, journal state).
//! - [`RequestJob`]: one unit of work, such as `StartWorkflow`, `CompleteTask`, `FireTimer`
//!   or `HandleEvent`.
//! - [`ExecutionResult`]: the SDK's response (success or failure, commands, query responses).
//! - [`Command`]: an instruction to the server, such as `ScheduleTask`, `StartTimer`,
//!   `CompleteWorkflow` or `StartChildWorkflow`.
//!
//! All types are `Send + Sync + Clone` and serialize with `serde`.

pub mod convert;
pub mod request;
pub mod result;

pub use convert::{
    create_eviction_request, extract_commands, task_execution_to_request,
    task_to_execution_request, validate_execution_result,
};

pub use request::{
    CancelWorkflowJob, ChildWorkflowCanceledJob, ChildWorkflowCompletedJob, ChildWorkflowFailedJob,
    ChildWorkflowFailure, ChildWorkflowStartedJob, ChildWorkflowTerminatedJob,
    ChildWorkflowTimedOutJob, ChildWorkflowTimeoutType, CompleteStepJob, CompleteTaskJob,
    EvictFromCacheJob, EvictionReason, ExecutionRequest, FireTimerJob, HandleEventJob,
    ProcessQueryJob, RequestJob, StartWorkflowJob, StepFailure as RequestStepFailure,
    TaskExecutionResult, UpdateStateJob,
};

pub use result::{
    CancelChildWorkflowCommand, CancelTimerCommand, CancelWorkflowCommand, Command,
    CompleteWorkflowCommand, ExecutionError, ExecutionErrorType, ExecutionResult,
    FailWorkflowCommand, OrphanPolicy, QueryChildWorkflowCommand, QueryResponse, QueryResult,
    RecordStepResultCommand, RequestCancellationCommand, RestartFreshCommand, RestartFreshRequest,
    ScheduleTaskCommand, SendEventCommand, StartChildWorkflowCommand, StartTimerCommand,
    StepFailure, TaskRetryPolicy, UpdateResponse, UpdateResult, WaitForEventCommand,
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::{Payload, WorkflowExecution};
    use std::time::{Duration, SystemTime};

    #[test]
    fn test_bridge_layer_integration() {
        let execution = WorkflowExecution {
            workflow_id: "test-workflow".to_string(),
            run_id: "run-123".to_string(),
        };

        let start_job = RequestJob::StartWorkflow(StartWorkflowJob {
            workflow_type: "TestWorkflow".to_string(),
            workflow_id: "test-workflow".to_string(),
            task_queue: "test-queue".to_string(),
            input: serde_json::json!({}),
            headers: vec![],
            scheduled_time: SystemTime::now(),
            namespace: "default".to_string(),
        });

        let request =
            ExecutionRequest::new("run-123".to_string(), execution, vec![start_job], 0, false);

        // Stand in for a language SDK processing the request.
        let mut result = ExecutionResult::success("run-123".to_string(), vec![]);

        result.add_command(Command::StartTimer(StartTimerCommand {
            sequence: 1,
            timer_id: "timer-1".to_string(),
            duration: Duration::from_secs(60),
        }));

        result.add_command(Command::CompleteWorkflow(CompleteWorkflowCommand {
            result: Payload::from_json(&serde_json::json!({"status": "done"})).unwrap(),
        }));

        assert_eq!(request.run_id, result.run_id);
        assert!(result.successful);
        assert_eq!(result.commands.len(), 2);
        assert!(result.commands[1].is_terminal());
    }

    #[test]
    fn test_request_serialization() {
        let execution = WorkflowExecution {
            workflow_id: "test-workflow".to_string(),
            run_id: "run-123".to_string(),
        };

        let request = ExecutionRequest::new("run-123".to_string(), execution, vec![], 0, false);

        let json = serde_json::to_string(&request).unwrap();
        assert!(json.contains("run-123"));

        let deserialized: ExecutionRequest = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.run_id, "run-123");
    }

    #[test]
    fn test_result_serialization() {
        let result = ExecutionResult::success("run-123".to_string(), vec![]);

        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("run-123"));

        let deserialized: ExecutionResult = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.run_id, "run-123");
        assert!(deserialized.successful);
    }

    #[test]
    fn test_command_serialization() {
        let command = Command::ScheduleTask(ScheduleTaskCommand {
            sequence: 1,
            task_id: "task-1".to_string(),
            task_type: "ProcessPayment".to_string(),
            task_queue: "payments".to_string(),
            input: vec![],
            timeout: Duration::from_secs(300),
            heartbeat_timeout: Some(Duration::from_secs(30)),
            queue_timeout: None,
            retry_policy: None,
            headers: vec![],
        });

        let json = serde_json::to_string(&command).unwrap();
        assert!(json.contains("task-1"));
        assert!(json.contains("heartbeat_timeout"));

        let deserialized: Command = serde_json::from_str(&json).unwrap();
        assert_eq!(deserialized.command_type(), "ScheduleTask");
    }

    #[test]
    fn test_error_handling() {
        let error = ExecutionError::non_determinism(
            "Sequence mismatch".to_string(),
            Some("Expected timer, got task".to_string()),
        );

        let result = ExecutionResult::failed("run-123".to_string(), error);

        assert!(!result.successful);
        assert!(result.error.is_some());

        let err = result.error.as_ref().unwrap();
        assert_eq!(err.error_type, ExecutionErrorType::NonDeterminism);
        assert!(!err.retryable);
    }

    #[test]
    fn test_query_responses() {
        let mut result = ExecutionResult::success("run-123".to_string(), vec![]);

        result.add_query_response(QueryResponse {
            query_id: "query-1".to_string(),
            result: QueryResult::Success {
                output: Payload::from_json(&serde_json::json!({"count": 42})).unwrap(),
            },
        });

        assert!(result.has_query_responses());
        assert_eq!(result.query_responses.len(), 1);
    }
}
