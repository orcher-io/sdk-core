//! Checks an activation's result before it is sent to the engine.

use crate::error::{Error, Result};
use crate::protocol::ExecutionResult;

/// Checks that an [`ExecutionResult`] is well formed before it is sent to the server.
///
/// # Errors
///
/// Returns [`Error::InvalidCommand`] if the run ID is empty, a successful result carries no
/// commands, query responses or update results, a failed result carries no error, or the
/// result has more than one terminal command or a terminal command that is not last.
pub(crate) fn validate_execution_result(result: &ExecutionResult) -> Result<()> {
    if result.run_id.is_empty() {
        return Err(Error::InvalidCommand {
            reason: "run_id cannot be empty".to_string(),
        });
    }

    // A successful result may carry only query or update responses and no commands.
    if result.successful {
        if result.commands.is_empty()
            && result.query_responses.is_empty()
            && result.update_results.is_empty()
        {
            return Err(Error::InvalidCommand {
                reason: "Successful result must have commands or query responses".to_string(),
            });
        }
    } else {
        if result.error.is_none() {
            return Err(Error::InvalidCommand {
                reason: "Failed result must have an error".to_string(),
            });
        }
    }

    // At most one terminal command, and it must come last: nothing can follow the end of a run.
    let terminal_count = result.commands.iter().filter(|c| c.is_terminal()).count();

    if terminal_count > 1 {
        return Err(Error::InvalidCommand {
            reason: "Cannot have multiple terminal commands".to_string(),
        });
    }

    if terminal_count == 1
        && result.commands.len() > 1
        && !result
            .commands
            .last()
            .map(|c| c.is_terminal())
            .unwrap_or(false)
    {
        return Err(Error::InvalidCommand {
            reason: "Terminal command must be last".to_string(),
        });
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::protocol::{Command, CompleteWorkflowCommand};
    use crate::types::Payload;

    #[test]
    fn test_validate_execution_result_success() {
        let mut result = ExecutionResult::success("run-123".to_string(), vec![]);
        result.add_command(Command::CompleteWorkflow(CompleteWorkflowCommand {
            result: Payload::from_json(&serde_json::json!({})).unwrap(),
        }));

        assert!(validate_execution_result(&result).is_ok());
    }

    #[test]
    fn test_validate_execution_result_empty_run_id() {
        let result = ExecutionResult::success("".to_string(), vec![]);

        assert!(validate_execution_result(&result).is_err());
    }

    #[test]
    fn test_validate_execution_result_no_commands() {
        let result = ExecutionResult::success("run-123".to_string(), vec![]);

        assert!(validate_execution_result(&result).is_err());
    }

    #[test]
    fn test_validate_execution_result_multiple_terminal_commands() {
        use crate::protocol::FailWorkflowCommand;

        let mut result = ExecutionResult::success("run-123".to_string(), vec![]);
        result.add_command(Command::CompleteWorkflow(CompleteWorkflowCommand {
            result: Payload::from_json(&serde_json::json!({})).unwrap(),
        }));
        result.add_command(Command::FailWorkflow(FailWorkflowCommand {
            message: "Failed".to_string(),
            details: None,
            error_type: "TestError".to_string(),
        }));

        assert!(validate_execution_result(&result).is_err());
    }

    #[test]
    fn test_validate_execution_result_terminal_not_last() {
        use crate::protocol::StartTimerCommand;
        use std::time::Duration;

        let mut result = ExecutionResult::success("run-123".to_string(), vec![]);
        result.add_command(Command::CompleteWorkflow(CompleteWorkflowCommand {
            result: Payload::from_json(&serde_json::json!({})).unwrap(),
        }));
        result.add_command(Command::StartTimer(StartTimerCommand {
            sequence: 1,
            timer_id: "timer-1".to_string(),
            duration: Duration::from_secs(60),
        }));

        assert!(validate_execution_result(&result).is_err());
    }
}
