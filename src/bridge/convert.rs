//! Conversions between the executor's internal types and the bridge types
//! exchanged with language SDKs.
//!
//! ## Conversion flow
//!
//! ```text
//! WorkflowExecutionTask → ExecutionRequest → Language SDK
//!                                                  ↓
//!                                          ExecutionResult
//!                                                  ↓
//!                                             Commands
//!                                                  ↓
//!                                          gRPC Response
//! ```

use crate::bridge::{
    CancelWorkflowJob, ChildWorkflowCanceledJob, ChildWorkflowCompletedJob, ChildWorkflowFailedJob,
    ChildWorkflowFailure, ChildWorkflowStartedJob, ChildWorkflowTerminatedJob,
    ChildWorkflowTimedOutJob, ChildWorkflowTimeoutType, CompleteStepJob, EvictFromCacheJob,
    EvictionReason, ExecutionRequest, ExecutionResult, FireTimerJob, HandleEventJob,
    ProcessQueryJob, RequestJob, RequestStepFailure, StartWorkflowJob, UpdateStateJob,
};
use crate::error::{Error, Result};
use crate::poller::{TaskExecutionTask, WorkflowExecutionTask};
use crate::proto::orcher::v1::{EntryType, JournalEntry};
use crate::types::{Payload, WorkflowExecution};
use std::time::SystemTime;

/// Converts a workflow task polled from the server into an [`ExecutionRequest`] for the SDK.
///
/// The request always starts with a `StartWorkflow` job (built from the task's input when the
/// journal does not supply one), followed by the jobs derived from the journal, and then one
/// job per pending query and update.
///
/// # Errors
///
/// Returns [`Error::InvalidEvent`] if the journal contains an unknown entry type.
pub fn task_to_execution_request(task: WorkflowExecutionTask) -> Result<ExecutionRequest> {
    tracing::debug!(
        ">>> BRIDGE CONVERT: task_to_execution_request called with task.workflow_type = '{}'",
        task.workflow_type
    );
    tracing::debug!(
        ">>> BRIDGE CONVERT: task.execution.workflow_id = '{}'",
        task.execution.workflow_id
    );
    tracing::debug!(
        ">>> BRIDGE CONVERT: task.task_queue = '{}'",
        task.task_queue
    );

    let mut jobs = journal_to_jobs(&task.journal)?;

    // The SDK needs a StartWorkflow job to run the workflow function at all. The journal never
    // produces one (see `journal_to_jobs`), so it is built here from the task's input.
    let has_start_workflow = jobs
        .iter()
        .any(|job| matches!(job, RequestJob::StartWorkflow(_)));

    if !has_start_workflow {
        // The input stays a JSON value so it crosses the FFI boundary without re-encoding.
        let start_workflow_job = StartWorkflowJob {
            workflow_type: task.workflow_type.clone(),
            workflow_id: task.execution.workflow_id.clone(),
            task_queue: task.task_queue.clone(),
            input: task.input.clone(),
            headers: vec![],
            scheduled_time: SystemTime::now(),
            namespace: "default".to_string(),
        };

        tracing::debug!(
            ">>> BRIDGE CONVERT: Created StartWorkflowJob with workflow_type = '{}', workflow_id = '{}', task_queue = '{}'",
            start_workflow_job.workflow_type,
            start_workflow_job.workflow_id,
            start_workflow_job.task_queue
        );

        jobs.insert(0, RequestJob::StartWorkflow(start_workflow_job));
    }

    for query in &task.queries {
        let arguments = if query.query_args.is_empty() {
            vec![]
        } else {
            vec![Payload {
                data: query.query_args.clone(),
                metadata: std::collections::HashMap::new(),
            }]
        };

        jobs.push(RequestJob::ProcessQuery(ProcessQueryJob {
            query_id: query.query_id.clone(),
            query_type: query.query_type.clone(),
            arguments,
            headers: vec![],
        }));
    }

    for update in &task.updates {
        let payload = if update.args.is_empty() {
            Payload {
                data: vec![],
                metadata: std::collections::HashMap::new(),
            }
        } else {
            Payload {
                data: update.args.clone(),
                metadata: std::collections::HashMap::new(),
            }
        };

        jobs.push(RequestJob::UpdateState(UpdateStateJob {
            update_id: update.update_id.clone(),
            update_name: update.update_type.clone(),
            payload,
            headers: vec![],
        }));
    }

    // The run is replaying if the workflow has executed before (the journal is not empty) or if
    // it is resuming after a task or step completed (CompleteStep/CompleteTask jobs are present).
    let has_completion_jobs = jobs.iter().any(|job| {
        matches!(
            job,
            RequestJob::CompleteStep(_) | RequestJob::CompleteTask(_)
        )
    });
    let is_replaying = !task.journal.is_empty() || has_completion_jobs;

    let execution_request = ExecutionRequest::new(
        task.execution.run_id.clone(),
        task.execution.clone(),
        jobs.clone(),
        task.journal.len(),
        is_replaying,
    );

    tracing::debug!(
        ">>> BRIDGE CONVERT: Created ExecutionRequest with run_id = '{}', jobs_count = {}, is_replaying = {}",
        execution_request.run_id,
        execution_request.jobs.len(),
        execution_request.is_replaying
    );

    if let Some(RequestJob::StartWorkflow(ref start_job)) = execution_request.jobs.first() {
        tracing::debug!(
            ">>> BRIDGE CONVERT: First job is StartWorkflow with workflow_type = '{}'",
            start_job.workflow_type
        );
    }

    Ok(execution_request)
}

/// Converts a polled task execution into an [`ExecutionRequest`].
///
/// The request carries no jobs, because the task executor runs tasks directly. It is marked
/// as replaying when this is a retry (attempt greater than 1).
///
/// # Errors
///
/// This conversion does not fail; the `Result` keeps the signature consistent with
/// [`task_to_execution_request`].
pub fn task_execution_to_request(task: TaskExecutionTask) -> Result<ExecutionRequest> {
    Ok(ExecutionRequest::new(
        task.execution.run_id.clone(),
        task.execution.clone(),
        vec![], // Tasks handle their own execution
        0,
        task.attempt > 1,
    ))
}

/// Converts execution journal entries into the jobs the language SDK processes.
fn journal_to_jobs(journal: &[JournalEntry]) -> Result<Vec<RequestJob>> {
    let mut jobs = Vec::new();

    for (idx, event) in journal.iter().enumerate() {
        let entry_type =
            EntryType::try_from(event.entry_type).map_err(|_| Error::InvalidEvent {
                reason: format!("Unknown entry type: {}", event.entry_type),
            })?;

        match entry_type {
            EntryType::WorkflowExecutionStarted => {
                // No job. The journal entry carries an empty input; a StartWorkflow job built
                // from it would shadow the real input, which `task_to_execution_request` takes
                // from the task itself.
            }

            EntryType::TimerFired => {
                // Use the timer_id from the entry's attributes: it is the id the workflow emitted
                // in StartTimer, so the SDK can match the fired timer to its timer()/sleep() call.
                // An id derived from the journal entry would never match, and the workflow would
                // re-emit the timer and never resume.
                if let Some(ref attributes) = event.attributes {
                    use crate::proto::orcher::v1::journal_entry::Attributes as JournalAttributes;
                    if let JournalAttributes::TimerFired(attrs) = attributes {
                        jobs.push(RequestJob::FireTimer(FireTimerJob {
                            sequence: idx as u32,
                            timer_id: attrs.timer_id.clone(),
                            scheduled_time: SystemTime::now(),
                            fired_time: SystemTime::now(),
                        }));
                    }
                }
            }

            EntryType::WorkflowExecutionCompleted => {
                continue;
            }

            EntryType::WorkflowExecutionFailed => {
                continue;
            }

            EntryType::WorkflowExecutionCancelRequested => {
                jobs.push(RequestJob::CancelWorkflow(CancelWorkflowJob {
                    reason: "Cancellation requested".to_string(),
                    details: None,
                    requested_time: SystemTime::now(),
                }));
            }

            EntryType::StepCompleted => {
                // Any kind of step: closure, task, child workflow or side effect.
                if let Some(ref attributes) = event.attributes {
                    use crate::proto::orcher::v1::journal_entry::Attributes as JournalAttributes;

                    if let JournalAttributes::StepCompleted(step_attrs) = attributes {
                        let completed_at = step_attrs.completed_at.as_ref().map(|ts| {
                            SystemTime::UNIX_EPOCH
                                + std::time::Duration::from_secs(ts.seconds as u64)
                                + std::time::Duration::from_nanos(ts.nanos as u64)
                        });

                        let failure = step_attrs.failure.as_ref().map(|f| RequestStepFailure {
                            message: f.message.clone(),
                            source: f.source.clone(),
                            stack_trace: f.stack_trace.clone(),
                            failure_type: f.failure_type.clone(),
                        });

                        jobs.push(RequestJob::CompleteStep(CompleteStepJob {
                            step_name: step_attrs.step_name.clone(),
                            step_type: step_attrs.step_type,
                            result: step_attrs.result.clone(),
                            failure,
                            execution_attempt: step_attrs.execution_attempt,
                            completed_at,
                            duration_ms: step_attrs.duration_ms,
                        }));
                    }
                }
            }

            EntryType::EventReceived => {
                if let Some(ref attributes) = event.attributes {
                    use crate::proto::orcher::v1::journal_entry::Attributes as JournalAttributes;
                    if let JournalAttributes::EventReceived(event_attrs) = attributes {
                        jobs.push(RequestJob::HandleEvent(HandleEventJob {
                            sequence: idx as u32,
                            event_name: event_attrs.event_name.clone(),
                            payload: Payload {
                                data: event_attrs.payload.clone(),
                                metadata: std::collections::HashMap::new(),
                            },
                            headers: vec![],
                            event_id: format!("{}", event.entry_id),
                            sent_time: SystemTime::now(),
                        }));
                    }
                }
            }

            EntryType::ChildWorkflowExecutionCompleted => {
                if let Some(ref attributes) = event.attributes {
                    use crate::proto::orcher::v1::journal_entry::Attributes as JournalAttributes;
                    if let JournalAttributes::ChildWorkflowExecutionCompleted(attrs) = attributes {
                        jobs.push(RequestJob::ChildWorkflowCompleted(
                            ChildWorkflowCompletedJob {
                                workflow_id: attrs.workflow_id.clone(),
                                execution_id: attrs.execution_id.clone(),
                                result: Payload {
                                    data: attrs.result.clone(),
                                    metadata: std::collections::HashMap::new(),
                                },
                            },
                        ));
                    }
                }
            }

            EntryType::ChildWorkflowExecutionFailed => {
                if let Some(ref attributes) = event.attributes {
                    use crate::proto::orcher::v1::journal_entry::Attributes as JournalAttributes;
                    if let JournalAttributes::ChildWorkflowExecutionFailed(attrs) = attributes {
                        let (msg, etype) = attrs
                            .failure
                            .as_ref()
                            .map(|f| (f.message.clone(), f.failure_type.clone()))
                            .unwrap_or_else(|| {
                                (
                                    "Child workflow failed".to_string(),
                                    "ChildWorkflowFailed".to_string(),
                                )
                            });
                        jobs.push(RequestJob::ChildWorkflowFailed(ChildWorkflowFailedJob {
                            workflow_id: attrs.workflow_id.clone(),
                            execution_id: attrs.execution_id.clone(),
                            failure: ChildWorkflowFailure {
                                message: msg,
                                error_type: etype,
                                details: None,
                            },
                        }));
                    }
                }
            }

            EntryType::ChildWorkflowExecutionStarted => {
                if let Some(ref attributes) = event.attributes {
                    use crate::proto::orcher::v1::journal_entry::Attributes as JournalAttributes;
                    if let JournalAttributes::ChildWorkflowExecutionStarted(attrs) = attributes {
                        jobs.push(RequestJob::ChildWorkflowStarted(ChildWorkflowStartedJob {
                            workflow_id: attrs.workflow_id.clone(),
                            execution_id: attrs.execution_id.clone(),
                            workflow_type: attrs.workflow_type.clone(),
                            namespace: attrs.namespace.clone(),
                        }));
                    }
                }
            }

            EntryType::ChildWorkflowExecutionTimedOut => {
                if let Some(ref attributes) = event.attributes {
                    use crate::proto::orcher::v1::journal_entry::Attributes as JournalAttributes;
                    use crate::proto::orcher::v1::TimeoutType;
                    if let JournalAttributes::ChildWorkflowExecutionTimedOut(attrs) = attributes {
                        let timeout_type = match TimeoutType::try_from(attrs.timeout_type) {
                            Ok(TimeoutType::Execution) => ChildWorkflowTimeoutType::Execution,
                            Ok(TimeoutType::Run) => ChildWorkflowTimeoutType::Run,
                            // StartToClose, unspecified and unknown values all map to
                            // the overall start-to-close timeout.
                            _ => ChildWorkflowTimeoutType::StartToClose,
                        };
                        jobs.push(RequestJob::ChildWorkflowTimedOut(
                            ChildWorkflowTimedOutJob {
                                workflow_id: attrs.workflow_id.clone(),
                                execution_id: attrs.execution_id.clone(),
                                timeout_type,
                            },
                        ));
                    }
                }
            }

            EntryType::ChildWorkflowExecutionCanceled => {
                if let Some(ref attributes) = event.attributes {
                    use crate::proto::orcher::v1::journal_entry::Attributes as JournalAttributes;
                    if let JournalAttributes::ChildWorkflowExecutionCanceled(attrs) = attributes {
                        let details = if attrs.details.is_empty() {
                            None
                        } else {
                            Some(Payload {
                                data: attrs.details.clone(),
                                metadata: std::collections::HashMap::new(),
                            })
                        };
                        jobs.push(RequestJob::ChildWorkflowCanceled(
                            ChildWorkflowCanceledJob {
                                workflow_id: attrs.workflow_id.clone(),
                                execution_id: attrs.execution_id.clone(),
                                details,
                            },
                        ));
                    }
                }
            }

            EntryType::ChildWorkflowExecutionTerminated => {
                if let Some(ref attributes) = event.attributes {
                    use crate::proto::orcher::v1::journal_entry::Attributes as JournalAttributes;
                    if let JournalAttributes::ChildWorkflowExecutionTerminated(attrs) = attributes {
                        let details = if attrs.details.is_empty() {
                            None
                        } else {
                            Some(Payload {
                                data: attrs.details.clone(),
                                metadata: std::collections::HashMap::new(),
                            })
                        };
                        jobs.push(RequestJob::ChildWorkflowTerminated(
                            ChildWorkflowTerminatedJob {
                                workflow_id: attrs.workflow_id.clone(),
                                execution_id: attrs.execution_id.clone(),
                                reason: attrs.reason.clone(),
                                details,
                            },
                        ));
                    }
                }
            }

            // Other entry types produce no job for the SDK.
            _ => {}
        }
    }

    Ok(jobs)
}

/// Builds a request that tells the SDK to evict a run from its workflow cache.
pub fn create_eviction_request(
    execution: WorkflowExecution,
    reason: EvictionReason,
) -> ExecutionRequest {
    ExecutionRequest::new(
        execution.run_id.clone(),
        execution,
        vec![RequestJob::EvictFromCache(EvictFromCacheJob {
            reason,
            details: None,
        })],
        0,
        false,
    )
}

/// Checks that an [`ExecutionResult`] is well formed before it is sent to the server.
///
/// # Errors
///
/// Returns [`Error::InvalidCommand`] if the run ID is empty, a successful result carries no
/// commands, query responses or update results, a failed result carries no error, or the
/// result has more than one terminal command or a terminal command that is not last.
pub fn validate_execution_result(result: &ExecutionResult) -> Result<()> {
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

/// Validates an [`ExecutionResult`] and returns it for submission to the server.
///
/// # Errors
///
/// Returns the same errors as [`validate_execution_result`].
pub fn extract_commands(result: ExecutionResult) -> Result<ExecutionResult> {
    validate_execution_result(&result)?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::bridge::{Command, CompleteWorkflowCommand};
    use crate::types::Payload;

    #[test]
    fn test_task_to_execution_request() {
        let task = WorkflowExecutionTask {
            execution: WorkflowExecution {
                workflow_id: "wf-123".to_string(),
                run_id: "run-456".to_string(),
            },
            workflow_type: "TestWorkflow".to_string(),
            task_queue: "test-queue".to_string(),
            input: serde_json::json!({"test": "data"}),
            journal: vec![],
            task_token: vec![1, 2, 3],
            started_event_id: 1,
            previous_started_event_id: 0,
            attempt: 1,
            stream_entry_id: None,
            queries: vec![],
            updates: vec![],
        };

        let request = task_to_execution_request(task).unwrap();

        assert_eq!(request.run_id, "run-456");
        assert_eq!(request.execution.workflow_id, "wf-123");
        assert_eq!(request.jobs.len(), 1);
        assert!(!request.is_replaying);
    }

    #[test]
    fn test_task_to_execution_request_replay() {
        // A non-empty journal means the workflow has executed before, so the run is replaying.
        let task = WorkflowExecutionTask {
            execution: WorkflowExecution {
                workflow_id: "wf-123".to_string(),
                run_id: "run-456".to_string(),
            },
            workflow_type: "TestWorkflow".to_string(),
            task_queue: "test-queue".to_string(),
            input: serde_json::json!({"test": "data"}),
            journal: vec![JournalEntry {
                entry_id: 1,
                entry_type: EntryType::TimerFired as i32,
                timestamp: None,
                version: 0,
                task_id: 0,
                attributes: None,
            }],
            task_token: vec![],
            started_event_id: 1,
            previous_started_event_id: 0,
            attempt: 2,
            stream_entry_id: None,
            queries: vec![],
            updates: vec![],
        };

        let request = task_to_execution_request(task).unwrap();
        assert!(request.is_replaying);
    }

    #[test]
    fn test_journal_to_jobs_workflow_started() {
        // WorkflowExecutionStarted produces no job: its input is empty, and
        // task_to_execution_request builds the StartWorkflow job from the task's real input.
        let journal = vec![JournalEntry {
            entry_id: 1,
            entry_type: EntryType::WorkflowExecutionStarted as i32,
            timestamp: None,
            version: 0,
            task_id: 0,
            attributes: None,
        }];

        let jobs = journal_to_jobs(&journal).unwrap();

        assert_eq!(jobs.len(), 0);
    }

    #[test]
    fn test_journal_to_jobs_timer_fired() {
        use crate::proto::orcher::v1::journal_entry::Attributes;
        use crate::proto::orcher::v1::TimerFiredEventAttributes;

        let journal = vec![JournalEntry {
            entry_id: 2,
            entry_type: EntryType::TimerFired as i32,
            timestamp: None,
            version: 0,
            task_id: 0,
            attributes: Some(Attributes::TimerFired(TimerFiredEventAttributes {
                started_event_id: 0,
                timer_id: "timer_5".to_string(),
            })),
        }];

        let jobs = journal_to_jobs(&journal).unwrap();

        assert_eq!(jobs.len(), 1);
        match &jobs[0] {
            RequestJob::FireTimer(job) => {
                // The workflow's own timer_id, not one derived from the journal entry id.
                assert_eq!(job.timer_id, "timer_5");
                assert_eq!(job.sequence, 0);
            }
            _ => panic!("Expected FireTimer job"),
        }
    }

    #[test]
    fn test_create_eviction_request() {
        let execution = WorkflowExecution {
            workflow_id: "wf-123".to_string(),
            run_id: "run-456".to_string(),
        };

        let request = create_eviction_request(execution, EvictionReason::CacheFull);

        assert_eq!(request.run_id, "run-456");
        assert_eq!(request.jobs.len(), 1);
        assert!(request.is_eviction_only());
    }

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
        use crate::bridge::FailWorkflowCommand;

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
        use crate::bridge::StartTimerCommand;
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

    #[test]
    fn test_extract_commands() {
        let mut result = ExecutionResult::success("run-123".to_string(), vec![]);
        result.add_command(Command::CompleteWorkflow(CompleteWorkflowCommand {
            result: Payload::from_json(&serde_json::json!({"status": "done"})).unwrap(),
        }));

        let extracted = extract_commands(result.clone()).unwrap();
        assert_eq!(extracted.run_id, result.run_id);
        assert_eq!(extracted.commands.len(), 1);
    }
}
