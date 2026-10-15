//! An activation's result, from the kernel protocol back to what the workflow
//! driver sends the engine.
//!
//! The language SDK answers a [`RunActivation`](crate::proto::kernel::v1::RunActivation)
//! with a [`RunActivationResult`]: every durable call the run's code made, in
//! issue order, satisfied ones marked. [`execution_result`] turns that into the
//! [`ExecutionResult`] the driver checks for determinism and sends on:
//!
//! - **Every call to a step** (a task, a timer, a child), satisfied or not,
//!   is a step the code reached, which the determinism check compares, in
//!   order, with history.
//! - **Satisfied calls** send nothing; **new calls** become commands for the
//!   engine.
//! - **Query and update answers** pass through.
//! - **A workflow failure** fails the run; **a protocol error** (the SDK could
//!   not process the activation) is [`Error::Protocol`], which hands the
//!   activation back rather than failing the run.
//!
//! **Step ids.** A run bound to a legacy step-id scheme keeps the ids its SDK
//! supplies, in the command's own fields (a task's id, a timer's id, a child's
//! workflow id) or in `Command.step_id`; the conversion is one to one, so the
//! engine receives exactly what the SDK sent before the kernel protocol. Ids
//! under the kernel's own scheme come with the engine's binding of a scheme
//! per run.

use std::time::Duration;

use crate::error::{Error, Result};
use crate::payload::Payload;
use crate::proto::kernel::v1::{
    command, query_answer, run_activation_result, update_answer, Command as KernelCommand,
    FailureCode, OrphanPolicy as KernelOrphanPolicy, Outcome, Payload as KernelPayload,
    RetryPolicy, RunActivationResult,
};
use crate::proto::prost_types;
use crate::protocol::result::{
    CancelChildWorkflowCommand, CancelTimerCommand, CancelWorkflowCommand, Command,
    CompleteWorkflowCommand, ExecutionError, ExecutionResult, FailWorkflowCommand, OrphanPolicy,
    QueryResponse, QueryResult, RecordStepResultCommand, RestartFreshCommand, ScheduleTaskCommand,
    SendEventCommand, StartChildWorkflowCommand, StartTimerCommand, StepFailure, TaskRetryPolicy,
    UpdateResponse, UpdateResult, WaitForEventCommand, WorkflowRetryPolicy,
};

/// How long a task attempt may run when the command does not say.
pub const DEFAULT_TASK_TIMEOUT: Duration = Duration::from_secs(300);

/// The default delay before a task's first retry, for a retry policy that
/// leaves it unset; the same for every SDK. A policy's attempts have no
/// default: 0 is unlimited, as the engine reads it.
pub const DEFAULT_RETRY_FIRST_DELAY: Duration = Duration::from_secs(1);
/// The default longest delay between retries; see [`DEFAULT_RETRY_FIRST_DELAY`].
pub const DEFAULT_RETRY_MAX_DELAY: Duration = Duration::from_secs(60);
/// The default factor a retry delay grows by; also taken by a factor of zero
/// or less, which is no factor at all. See [`DEFAULT_RETRY_FIRST_DELAY`].
pub const DEFAULT_RETRY_MULTIPLIER: f64 = 2.0;

/// The step type a recorded value is journaled under (`StepType::Closure`).
const CLOSURE_STEP: i32 = 2;

/// Turns the language SDK's answer to an activation of run `run_id` into the
/// result the workflow driver checks and sends.
///
/// # Errors
///
/// Returns [`Error::Protocol`] when the SDK reported it could not process the
/// activation, or answered with something the kernel cannot carry (no
/// outcome, a command of no kind, a command the engine does not take yet).
pub fn execution_result(run_id: &str, answer: RunActivationResult) -> Result<ExecutionResult> {
    let mut result =
        match answer.outcome {
            Some(run_activation_result::Outcome::Commands(commands)) => {
                let mut reached = Vec::new();
                let mut issued = Vec::new();
                for command in commands.commands {
                    // Every step the code called for is reached, answered
                    // from history or not: a step still open in history is
                    // sent again, and the determinism check compares the
                    // order of all of them with the journal's.
                    reached.extend(reached_step(&command));
                    if !command.satisfied {
                        issued.push(engine_command(command)?);
                    }
                }
                let mut result = ExecutionResult::success(run_id.to_string(), issued);
                result.set_reached_steps(reached);
                result
            }
            Some(run_activation_result::Outcome::WorkflowFailed(failure)) => {
                // Code that no longer replays its history is reported as such,
                // so the engine holds the run instead of failing it as code.
                let error = if failure.code == FailureCode::NonDeterminism as i32 {
                    ExecutionError::non_determinism(failure.message, None)
                } else {
                    ExecutionError::workflow_error(failure.message, !failure.non_retryable)
                };
                ExecutionResult::failed(run_id.to_string(), error)
            }
            Some(run_activation_result::Outcome::ProtocolError(error)) => {
                return Err(Error::Protocol(error.message));
            }
            None => return Err(Error::Protocol(
                "an activation result must carry commands, a workflow failure or a protocol error"
                    .to_string(),
            )),
        };
    result.query_responses = answer
        .queries
        .into_iter()
        .map(|answer| QueryResponse {
            query_id: answer.query_id,
            result: match answer.answer {
                Some(query_answer::Answer::Value(value)) => QueryResult::Success {
                    output: payload(value),
                },
                Some(query_answer::Answer::Failure(failure)) => QueryResult::Failed {
                    message: failure.message,
                    details: None,
                },
                None => QueryResult::Failed {
                    message: "the query handler gave no answer".to_string(),
                    details: None,
                },
            },
        })
        .collect();
    result.update_results = answer
        .updates
        .into_iter()
        .map(|answer| UpdateResponse {
            update_id: answer.update_id,
            result: match answer.answer {
                Some(update_answer::Answer::Value(value)) => UpdateResult::Completed {
                    output: payload(value),
                },
                Some(update_answer::Answer::Rejected(failure)) => UpdateResult::Rejected {
                    message: failure.message,
                },
                Some(update_answer::Answer::Failed(failure)) => UpdateResult::Failed {
                    message: failure.message,
                    details: None,
                },
                None => UpdateResult::Failed {
                    message: "the update handler gave no answer".to_string(),
                    details: None,
                },
            },
        })
        .collect();
    Ok(result)
}

/// The step a call reached, as the determinism check names it: a
/// task's, a timer's or a child's id. Recorded values and waits are not
/// steps the check compares.
fn reached_step(command: &KernelCommand) -> Option<String> {
    match command.what.as_ref()? {
        command::What::RunTask(_) | command::What::StartTimer(_) => {
            Some(command.step_id.clone()).filter(|id| !id.is_empty())
        }
        command::What::StartChild(child) => {
            Some(child.workflow_id.clone()).filter(|id| !id.is_empty())
        }
        _ => None,
    }
}

/// The engine command for a new call.
fn engine_command(command: KernelCommand) -> Result<Command> {
    let seq = u32::try_from(command.seq).unwrap_or(u32::MAX);
    let step_id = command.step_id;
    Ok(match command.what {
        Some(command::What::RunTask(task)) => Command::ScheduleTask(ScheduleTaskCommand {
            sequence: seq,
            task_id: step_id,
            task_type: task.task_type,
            task_queue: task.task_queue,
            input: task.input.into_iter().map(payload).collect(),
            timeout: task
                .attempt_timeout
                .map(duration)
                .unwrap_or(DEFAULT_TASK_TIMEOUT),
            queue_timeout: task.queue_timeout.map(duration),
            heartbeat_timeout: task.heartbeat_timeout.map(duration),
            retry_policy: task.retry_policy.map(retry_policy),
            headers: task
                .headers
                .into_iter()
                .map(|(name, value)| (name, payload(value)))
                .collect(),
        }),
        Some(command::What::StartTimer(timer)) => Command::StartTimer(StartTimerCommand {
            sequence: seq,
            timer_id: step_id,
            duration: timer.duration.map(duration).unwrap_or_default(),
        }),
        Some(command::What::CancelTimer(cancel)) => Command::CancelTimer(CancelTimerCommand {
            sequence: seq,
            timer_id: cancel.step_id,
        }),
        Some(command::What::StartChild(child)) => {
            Command::StartChildWorkflow(StartChildWorkflowCommand {
                sequence: seq,
                workflow_id: child.workflow_id,
                workflow_type: child.workflow_type,
                task_queue: child.task_queue,
                input: child.input.into_iter().map(payload).collect(),
                timeout: child.execution_timeout.map(duration),
                orphan_policy: match KernelOrphanPolicy::try_from(child.orphan_policy) {
                    Ok(KernelOrphanPolicy::Terminate) => OrphanPolicy::Terminate,
                    Ok(KernelOrphanPolicy::Abandon) => OrphanPolicy::Abandon,
                    _ => OrphanPolicy::Cancel,
                },
                retry_policy: child.retry_policy.map(workflow_retry_policy),
            })
        }
        Some(command::What::CancelChild(cancel)) => {
            Command::CancelChildWorkflow(CancelChildWorkflowCommand {
                workflow_id: cancel.step_id,
                run_id: String::new(),
            })
        }
        Some(command::What::SendEvent(event)) => Command::SendEvent(SendEventCommand {
            workflow_id: event.workflow_id,
            run_id: Some(event.run_id).filter(|id| !id.is_empty()),
            event_name: event.name,
            payload: event
                .payload
                .map(payload)
                .unwrap_or_else(|| Payload::new_data(vec![])),
            headers: event
                .headers
                .into_iter()
                .map(|(name, value)| (name, payload(value)))
                .collect(),
        }),
        Some(command::What::RecordValue(record)) => {
            let (result, failure, attempts) = recorded(record.outcome);
            Command::RecordStepResult(RecordStepResultCommand {
                step_name: step_id,
                step_type: CLOSURE_STEP,
                result,
                failure,
                execution_attempt: attempts,
            })
        }
        Some(command::What::AwaitEvent(wait)) => Command::WaitForEvent(WaitForEventCommand {
            step_id,
            event_name: wait.name,
            timeout_ms: wait.timeout.map(|t| duration(t).as_millis() as i64),
        }),
        Some(command::What::CompleteRun(complete)) => {
            Command::CompleteWorkflow(CompleteWorkflowCommand {
                result: complete
                    .result
                    .map(payload)
                    .unwrap_or_else(|| Payload::new_data(vec![])),
            })
        }
        Some(command::What::FailRun(fail)) => {
            let failure = fail.failure.unwrap_or_default();
            Command::FailWorkflow(FailWorkflowCommand {
                message: failure.message,
                details: (!failure.details.is_empty()).then(|| Payload::new_data(failure.details)),
                error_type: failure.error_type,
            })
        }
        Some(command::What::EndCancelled(cancelled)) => {
            Command::CancelWorkflowExecution(CancelWorkflowCommand {
                details: (!cancelled.details.is_empty())
                    .then(|| Payload::new_data(cancelled.details.into_bytes())),
            })
        }
        Some(command::What::RestartFresh(restart)) => Command::RestartFresh(RestartFreshCommand {
            workflow_type: restart.workflow_type,
            input: restart.input.into_iter().map(payload).collect(),
            task_queue: Some(restart.task_queue).filter(|queue| !queue.is_empty()),
            timeout: restart.execution_timeout.map(duration),
            // The kernel's RestartFresh carries no retry policy yet, so the
            // fresh run keeps the one of the run it replaces.
            retry_policy: None,
        }),
        Some(command::What::UpsertLabels(_)) => {
            return Err(Error::Protocol(
                "setting labels from a workflow is not supported by the engine yet".to_string(),
            ))
        }
        None => return Err(Error::Protocol("a command of no kind".to_string())),
    })
}

/// A recorded value's result, failure and attempts, as a step result.
fn recorded(outcome: Option<Outcome>) -> (Vec<u8>, Option<StepFailure>, i32) {
    use crate::proto::kernel::v1::outcome::Result as Recorded;
    let Some(outcome) = outcome else {
        return (vec![], None, 1);
    };
    let attempts = i32::try_from(outcome.attempts.max(1)).unwrap_or(1);
    match outcome.result {
        Some(Recorded::Value(value)) => (value.data, None, attempts),
        Some(Recorded::Failure(failure)) => (
            vec![],
            Some(StepFailure {
                message: failure.message,
                stack_trace: String::new(),
                source: String::new(),
                failure_type: failure.error_type,
            }),
            attempts,
        ),
        None => (vec![], None, attempts),
    }
}

/// A workflow's retry policy, its unset fields left unset: the engine fills
/// them with its defaults for workflow retries, which are not the task
/// defaults above.
fn workflow_retry_policy(policy: RetryPolicy) -> WorkflowRetryPolicy {
    WorkflowRetryPolicy {
        max_attempts: policy.max_attempts,
        initial_interval: policy.first_delay.map(duration),
        max_interval: policy.max_delay.map(duration),
        backoff_coefficient: policy.delay_multiplier,
        non_retryable_errors: policy.never_retry_types,
    }
}

/// A retry policy, with the kernel's defaults for the fields left unset.
fn retry_policy(policy: RetryPolicy) -> TaskRetryPolicy {
    TaskRetryPolicy {
        // 0 is unlimited, as the engine reads it: the SDK sent the policy, so
        // it is passed on as given. A policy left out takes the defaults.
        max_attempts: policy.max_attempts,
        initial_interval: policy
            .first_delay
            .map(duration)
            .unwrap_or(DEFAULT_RETRY_FIRST_DELAY),
        max_interval: policy
            .max_delay
            .map(duration)
            .unwrap_or(DEFAULT_RETRY_MAX_DELAY),
        backoff_coefficient: if policy.delay_multiplier > 0.0 {
            policy.delay_multiplier
        } else {
            DEFAULT_RETRY_MULTIPLIER
        },
        non_retryable_errors: policy.never_retry_types,
    }
}

fn payload(payload: KernelPayload) -> Payload {
    Payload {
        data: payload.data,
        metadata: payload.metadata.into_iter().collect(),
    }
}

fn duration(d: prost_types::Duration) -> Duration {
    Duration::from_secs(u64::try_from(d.seconds).unwrap_or(0))
        + Duration::from_nanos(u64::try_from(d.nanos).unwrap_or(0))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::kernel::v1::{
        outcome, AwaitEvent, CancelTimer, Commands, CompleteRun, Failure, ProtocolError,
        QueryAnswer, RecordValue, RunTask, StartChild, StartTimer, UpdateAnswer, UpsertLabels,
    };

    fn call(seq: u64, satisfied: bool, step_id: &str, what: command::What) -> KernelCommand {
        KernelCommand {
            seq,
            satisfied,
            step_id: step_id.into(),
            what: Some(what),
            ..Default::default()
        }
    }

    fn json(data: &str) -> KernelPayload {
        KernelPayload {
            data: data.as_bytes().to_vec(),
            metadata: [("encoding".to_string(), b"json".to_vec())].into(),
            ..Default::default()
        }
    }

    fn answered(commands: Vec<KernelCommand>) -> RunActivationResult {
        RunActivationResult {
            outcome: Some(run_activation_result::Outcome::Commands(Commands {
                commands,
                ..Default::default()
            })),
            ..Default::default()
        }
    }

    fn seconds(s: i64) -> Option<prost_types::Duration> {
        Some(prost_types::Duration {
            seconds: s,
            nanos: 0,
        })
    }

    /// Every call to a step is a step the code reached, in order, answered
    /// from history or not. Answered calls send nothing; new calls become
    /// commands, with the ids the SDK supplied.
    #[test]
    fn every_step_call_is_reached_and_new_ones_are_commands() {
        let result = execution_result(
            "run-1",
            answered(vec![
                call(
                    0,
                    true,
                    "charge_0",
                    command::What::RunTask(RunTask::default()),
                ),
                call(
                    1,
                    true,
                    "timer_1",
                    command::What::StartTimer(StartTimer::default()),
                ),
                call(
                    2,
                    true,
                    "",
                    command::What::StartChild(StartChild {
                        workflow_id: "order-7-child-2".into(),
                        ..Default::default()
                    }),
                ),
                call(
                    3,
                    true,
                    "note_3",
                    command::What::RecordValue(RecordValue::default()),
                ),
                call(
                    4,
                    false,
                    "ship_4",
                    command::What::RunTask(RunTask {
                        task_type: "ship".into(),
                        input: vec![json(r#"{"to":"home"}"#)],
                        ..Default::default()
                    }),
                ),
            ]),
        )
        .unwrap();
        assert!(result.successful);
        assert_eq!(
            result.reached_steps.as_deref(),
            Some(
                &[
                    "charge_0".to_string(),
                    "timer_1".into(),
                    "order-7-child-2".into(),
                    "ship_4".into()
                ][..]
            )
        );
        assert_eq!(result.commands.len(), 1);
        let Command::ScheduleTask(task) = &result.commands[0] else {
            panic!("{:?}", result.commands[0]);
        };
        assert_eq!(
            (task.task_id.as_str(), task.task_type.as_str()),
            ("ship_4", "ship")
        );
        assert_eq!(task.sequence, 4);
        assert_eq!(task.input[0].data, br#"{"to":"home"}"#);
        assert_eq!(task.timeout, DEFAULT_TASK_TIMEOUT);
        assert!(task.retry_policy.is_none());
    }

    /// A retry policy's unset delays take the kernel's defaults, the same
    /// for every SDK; its attempts are passed on as given, where 0 is
    /// unlimited, as the engine reads it.
    #[test]
    fn unset_retry_delays_take_the_kernels_defaults_and_attempts_pass_through() {
        let result = execution_result(
            "run-1",
            answered(vec![call(
                0,
                false,
                "charge_0",
                command::What::RunTask(RunTask {
                    task_type: "charge".into(),
                    retry_policy: Some(RetryPolicy {
                        max_delay: seconds(10),
                        never_retry_types: vec!["Declined".into()],
                        ..Default::default()
                    }),
                    attempt_timeout: seconds(30),
                    ..Default::default()
                }),
            )]),
        )
        .unwrap();
        let Command::ScheduleTask(task) = &result.commands[0] else {
            panic!();
        };
        let policy = task.retry_policy.as_ref().unwrap();
        assert_eq!(policy.max_attempts, 0, "unlimited, not a default");
        assert_eq!(policy.initial_interval, DEFAULT_RETRY_FIRST_DELAY);
        assert_eq!(policy.max_interval, Duration::from_secs(10));
        assert_eq!(policy.backoff_coefficient, DEFAULT_RETRY_MULTIPLIER);
        assert_eq!(policy.non_retryable_errors, ["Declined"]);
        assert_eq!(task.timeout, Duration::from_secs(30));
    }

    /// Every other kind of call maps one to one onto the command the engine
    /// takes, keeping the ids the SDK supplied.
    #[test]
    fn each_kind_of_call_maps_to_its_command() {
        let result = execution_result(
            "run-1",
            answered(vec![
                call(
                    0,
                    false,
                    "timer_0",
                    command::What::StartTimer(StartTimer {
                        duration: seconds(900),
                        ..Default::default()
                    }),
                ),
                call(
                    1,
                    false,
                    "",
                    command::What::CancelTimer(CancelTimer {
                        step_id: "timer_0".into(),
                        ..Default::default()
                    }),
                ),
                call(
                    2,
                    false,
                    "note_2",
                    command::What::RecordValue(RecordValue {
                        outcome: Some(Outcome {
                            result: Some(outcome::Result::Value(json("42"))),
                            ..Default::default()
                        }),
                        ..Default::default()
                    }),
                ),
                call(
                    3,
                    false,
                    "event_go_3",
                    command::What::AwaitEvent(AwaitEvent {
                        name: "go".into(),
                        timeout: seconds(60),
                        ..Default::default()
                    }),
                ),
                call(
                    4,
                    false,
                    "",
                    command::What::CompleteRun(CompleteRun {
                        result: Some(json("\"done\"")),
                        ..Default::default()
                    }),
                ),
            ]),
        )
        .unwrap();
        let kinds: Vec<String> = result
            .commands
            .iter()
            .map(|command| match command {
                Command::StartTimer(t) => format!("timer {} {:?}", t.timer_id, t.duration),
                Command::CancelTimer(t) => format!("cancel {}", t.timer_id),
                Command::RecordStepResult(r) => format!(
                    "record {} {} {}",
                    r.step_name,
                    r.step_type,
                    String::from_utf8_lossy(&r.result)
                ),
                Command::WaitForEvent(w) => {
                    format!("wait {} {} {:?}", w.step_id, w.event_name, w.timeout_ms)
                }
                Command::CompleteWorkflow(c) => {
                    format!("complete {}", String::from_utf8_lossy(&c.result.data))
                }
                other => format!("{other:?}"),
            })
            .collect();
        assert_eq!(
            kinds,
            [
                "timer timer_0 900s",
                "cancel timer_0",
                "record note_2 2 42",
                "wait event_go_3 go Some(60000)",
                "complete \"done\"",
            ]
        );
    }

    #[test]
    fn query_and_update_answers_pass_through() {
        let mut answer = answered(vec![]);
        answer.queries = vec![QueryAnswer {
            query_id: "q1".into(),
            answer: Some(query_answer::Answer::Value(json("1"))),
            ..Default::default()
        }];
        answer.updates = vec![UpdateAnswer {
            update_id: "u1".into(),
            answer: Some(update_answer::Answer::Rejected(Failure {
                message: "closed".into(),
                ..Default::default()
            })),
            ..Default::default()
        }];
        let result = execution_result("run-1", answer).unwrap();
        assert!(
            matches!(&result.query_responses[0].result, QueryResult::Success { output } if output.data == b"1")
        );
        assert!(
            matches!(&result.update_results[0].result, UpdateResult::Rejected { message } if message == "closed")
        );
    }

    #[test]
    fn a_workflow_failure_fails_the_run() {
        let result = execution_result(
            "run-1",
            RunActivationResult {
                outcome: Some(run_activation_result::Outcome::WorkflowFailed(Failure {
                    message: "out of stock".into(),
                    non_retryable: true,
                    ..Default::default()
                })),
                ..Default::default()
            },
        )
        .unwrap();
        assert!(!result.successful);
        let error = result.error.unwrap();
        assert_eq!(error.message, "out of stock");
        assert!(!error.retryable);
    }

    /// Code that no longer replays its history is reported as
    /// non-determinism, so the engine holds the run rather than failing it
    /// as an error in the workflow's code.
    #[test]
    fn a_non_determinism_failure_is_reported_as_such() {
        let result = execution_result(
            "run-1",
            RunActivationResult {
                outcome: Some(run_activation_result::Outcome::WorkflowFailed(Failure {
                    message: "step charge_0 is now a timer".into(),
                    code: FailureCode::NonDeterminism as i32,
                    ..Default::default()
                })),
                ..Default::default()
            },
        )
        .unwrap();
        let error = result.error.unwrap();
        assert_eq!(
            error.error_type,
            crate::protocol::result::ExecutionErrorType::NonDeterminism
        );
        assert_eq!(error.message, "step charge_0 is now a timer");
    }

    /// A restart keeps the execution timeout the workflow asked for.
    #[test]
    fn a_restart_keeps_its_execution_timeout() {
        let result = execution_result(
            "run-1",
            answered(vec![call(
                0,
                false,
                "",
                command::What::RestartFresh(crate::proto::kernel::v1::RestartFresh {
                    execution_timeout: seconds(3600),
                    ..Default::default()
                }),
            )]),
        )
        .unwrap();
        let Command::RestartFresh(restart) = &result.commands[0] else {
            panic!("{:?}", result.commands);
        };
        assert_eq!(restart.timeout, Some(Duration::from_secs(3600)));
    }

    /// A child's retry policy reaches the engine command as the SDK gave it.
    /// Its unset delays stay unset, for the engine's workflow-retry defaults
    /// rather than the task defaults.
    #[test]
    fn a_child_retry_policy_is_carried_with_its_unset_delays_unset() {
        let child = |retry_policy| {
            call(
                0,
                false,
                "",
                command::What::StartChild(StartChild {
                    workflow_id: "order-7-child-0".into(),
                    workflow_type: "ship".into(),
                    retry_policy,
                    ..Default::default()
                }),
            )
        };
        let result = execution_result(
            "run-1",
            answered(vec![
                child(Some(RetryPolicy {
                    max_attempts: 3,
                    first_delay: seconds(5),
                    delay_multiplier: 1.5,
                    never_retry_types: vec!["Declined".into()],
                    ..Default::default()
                })),
                child(None),
            ]),
        )
        .unwrap();
        let Command::StartChildWorkflow(with_policy) = &result.commands[0] else {
            panic!("{:?}", result.commands);
        };
        assert_eq!(
            with_policy.retry_policy,
            Some(WorkflowRetryPolicy {
                max_attempts: 3,
                initial_interval: Some(Duration::from_secs(5)),
                max_interval: None,
                backoff_coefficient: 1.5,
                non_retryable_errors: vec!["Declined".into()],
            })
        );
        let Command::StartChildWorkflow(without) = &result.commands[1] else {
            panic!("{:?}", result.commands);
        };
        assert_eq!(
            without.retry_policy, None,
            "unset: the child is not retried"
        );
    }

    /// What the kernel cannot carry hands the activation back, never fails
    /// the run.
    #[test]
    fn what_cannot_be_carried_is_a_protocol_error() {
        let refused = [
            RunActivationResult {
                outcome: Some(run_activation_result::Outcome::ProtocolError(
                    ProtocolError {
                        message: "unknown resolution".into(),
                        ..Default::default()
                    },
                )),
                ..Default::default()
            },
            RunActivationResult::default(),
            answered(vec![call(
                0,
                false,
                "",
                command::What::UpsertLabels(UpsertLabels::default()),
            )]),
            answered(vec![KernelCommand::default()]),
        ];
        for answer in refused {
            let error = execution_result("run-1", answer).unwrap_err();
            assert!(matches!(error, Error::Protocol(_)), "{error:?}");
        }
    }

    /// Two tasks issued together; the second has finished, the first is
    /// still running. Re-running, the code calls the first (not yet
    /// answered, so sent again in full) and then the second (answered from
    /// history). That is the order the journal recorded them in, and the
    /// determinism check must see both steps reached in it.
    #[test]
    fn steps_issued_again_count_as_reached_in_issue_order() {
        use crate::proto::orcher::v1::journal_entry::Attributes;
        use crate::proto::orcher::v1::{
            EntryType, JournalEntry, TaskCompletedEventAttributes, TaskScheduledEventAttributes,
            WorkflowExecutionStartedEventAttributes,
        };
        let entry = |entry_id: i64, entry_type: EntryType, attributes: Attributes| JournalEntry {
            entry_id,
            entry_type: entry_type as i32,
            attributes: Some(attributes),
            ..Default::default()
        };
        let scheduled = |task_id: &str, task_type: &str| {
            Attributes::TaskScheduled(TaskScheduledEventAttributes {
                task_id: task_id.into(),
                task_type: task_type.into(),
                ..Default::default()
            })
        };
        let journal = vec![
            entry(
                1,
                EntryType::WorkflowExecutionStarted,
                Attributes::WorkflowExecutionStarted(WorkflowExecutionStartedEventAttributes {
                    workflow_type: "checkout".into(),
                    ..Default::default()
                }),
            ),
            entry(2, EntryType::TaskScheduled, scheduled("charge_0", "charge")),
            entry(3, EntryType::TaskScheduled, scheduled("ship_1", "ship")),
            entry(
                4,
                EntryType::TaskCompleted,
                Attributes::TaskCompleted(TaskCompletedEventAttributes {
                    scheduled_event_id: 3,
                    ..Default::default()
                }),
            ),
        ];
        let task = |task_type: &str| {
            command::What::RunTask(RunTask {
                task_type: task_type.into(),
                ..Default::default()
            })
        };
        let result = execution_result(
            "run-1",
            answered(vec![
                call(0, false, "charge_0", task("charge")),
                call(1, true, "ship_1", task("ship")),
            ]),
        )
        .unwrap();

        assert_eq!(
            result.reached_steps.as_deref(),
            Some(&["charge_0".to_string(), "ship_1".to_string()][..])
        );
        let violations = crate::Replayer::check_activation(&journal, &result);
        assert!(violations.is_empty(), "{violations:?}");
    }
}
