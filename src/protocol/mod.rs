//! What a language SDK's workflow code produces: the [`Command`]s an
//! activation issues, carried in an [`ExecutionResult`], with its query and
//! update answers.
//!
//! The kernel turns a language's answer to an activation into an
//! [`ExecutionResult`] (see [`crate::kernel::encoded::execution_result`]);
//! the worker checks it and sends it to the engine.

pub mod result;
mod validate;

pub(crate) use validate::validate_execution_result;

pub use result::{
    CancelChildWorkflowCommand, CancelTimerCommand, CancelWorkflowCommand, Command,
    CompleteWorkflowCommand, ExecutionError, ExecutionErrorType, ExecutionResult,
    FailWorkflowCommand, OrphanPolicy, QueryChildWorkflowCommand, QueryResponse, QueryResult,
    RecordStepResultCommand, RequestCancellationCommand, RestartFreshCommand, RestartFreshRequest,
    ScheduleTaskCommand, SendEventCommand, StartChildWorkflowCommand, StartTimerCommand,
    StepFailure, TaskRetryPolicy, UpdateResponse, UpdateResult, WaitForEventCommand,
    WorkflowRetryPolicy,
};

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::Payload;
    use std::time::Duration;

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
