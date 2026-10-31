//! A runtime that runs no workflow code: it answers each vector with the
//! answer recorded for it, put back into the kernel protocol. The runner's
//! own tests run it to show the runner passes a runtime that answers right,
//! and, with [`Answers`] other than [`Answers::Recorded`], fails one that
//! does not. It is also the smallest example of the framing.
//!
//! An answer is made from the recorded activation and the message recorded
//! after it: a call answered from history (`satisfied`, no payloads) for
//! every step the history recorded that the recorded answer does not issue
//! again, in the history's order, then a call for each command the worker
//! sent, in its order, and the query and update answers it sent.

use std::collections::{HashMap, HashSet};
use std::io::{self, Read, Write};

use orcher_proto::kernel::v1::{
    command::What, outcome, query_answer, run_activation_result, update_answer, CancelChild,
    CancelTimer, Command, Commands, CompleteRun, EndCancelled, FailRun, Failure, Outcome, Payload,
    QueryAnswer, RecordValue, RestartFresh, RetryPolicy, RunActivation, RunActivationResult,
    RunTask, SendEvent, StartChild, StartTimer, UpdateAnswer,
};
use orcher_proto::prost::Message;
use orcher_proto::{
    command::Attributes, journal_entry, query_result, update_result, Command as EngineCommand,
    CompleteWorkflowExecutionRequest, FailWorkflowExecutionRequest, PollWorkflowExecutionResponse,
    QueryResultType, RetryPolicy as EngineRetryPolicy, UpdateResultType,
};
use orcher_sdk_core::worker::record::Recorded;

use crate::framing::{read_frame, write_frame};
use crate::vectors::Vector;

/// What the reference runtime answers with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Answers {
    /// The recorded answers: every vector passes.
    Recorded,
    /// The recorded answers without their last command: a runtime that
    /// stops short.
    DropCommand,
    /// The recorded answers with every task's type changed: a runtime that
    /// calls the wrong task.
    ChangeTaskType,
    /// The recorded answers without the calls answered from history: a
    /// runtime whose code no longer takes the steps the history recorded.
    ForgetHistory,
}

impl Answers {
    /// The names `--answers` takes.
    pub const NAMES: [&'static str; 4] = [
        "recorded",
        "drop-command",
        "change-task-type",
        "forget-history",
    ];

    /// The answers named `name`, one of [`Self::NAMES`].
    pub fn named(name: &str) -> Option<Self> {
        match name {
            "recorded" => Some(Self::Recorded),
            "drop-command" => Some(Self::DropCommand),
            "change-task-type" => Some(Self::ChangeTaskType),
            "forget-history" => Some(Self::ForgetHistory),
            _ => None,
        }
    }
}

/// Answers every activation read from `input` on `output`, until `input`
/// ends; an activation of no vector is answered with a protocol error.
///
/// # Errors
///
/// The error of reading or writing a frame.
pub fn serve(
    vectors: &[Vector],
    answers: Answers,
    mut input: impl Read,
    mut output: impl Write,
) -> io::Result<()> {
    let mut by_activation = HashMap::new();
    for vector in vectors {
        if let Ok(activation) = RunActivation::decode(vector.run_activation.as_slice()) {
            by_activation.insert(key(&activation), vector);
        }
    }
    while let Some(frame) = read_frame(&mut input)? {
        let result = match RunActivation::decode(frame.as_slice()) {
            Err(e) => protocol_error(Vec::new(), format!("not a RunActivation: {e}")),
            Ok(activation) => match by_activation.get(&key(&activation)) {
                None => protocol_error(activation.activation_id, "no such vector".to_string()),
                Some(vector) => answer(vector, answers)
                    .unwrap_or_else(|why| protocol_error(activation.activation_id, why)),
            },
        };
        write_frame(&mut output, &result.encode_to_vec())?;
    }
    Ok(())
}

/// An activation's run and id, which name its vector.
fn key(activation: &RunActivation) -> (String, String, Vec<u8>) {
    let run = activation.run.clone().unwrap_or_default();
    (
        run.workflow_id,
        run.run_id,
        activation.activation_id.clone(),
    )
}

fn protocol_error(activation_id: Vec<u8>, message: String) -> RunActivationResult {
    RunActivationResult {
        activation_id,
        outcome: Some(run_activation_result::Outcome::ProtocolError(
            orcher_proto::kernel::v1::ProtocolError {
                message,
                ..Default::default()
            },
        )),
        ..Default::default()
    }
}

/// The answer to `vector`.
///
/// # Errors
///
/// Why there is none: a recorded message that does not decode, or a
/// command the kernel protocol has no call for.
pub fn answer(vector: &Vector, answers: Answers) -> Result<RunActivationResult, String> {
    let activation_id = vector.activation_id.clone();
    let completion = match &vector.expected {
        Recorded::Completion(bytes) => CompleteWorkflowExecutionRequest::decode(bytes.as_slice())
            .map_err(|e| format!("the recorded completion: {e}"))?,
        Recorded::Failure(bytes) => {
            let failure = FailWorkflowExecutionRequest::decode(bytes.as_slice())
                .map_err(|e| format!("the recorded failure: {e}"))?
                .failure
                .unwrap_or_default();
            return Ok(RunActivationResult {
                activation_id,
                outcome: Some(run_activation_result::Outcome::WorkflowFailed(Failure {
                    message: failure.message,
                    non_retryable: failure.non_retryable,
                    ..Default::default()
                })),
                ..Default::default()
            });
        }
        _ => return Err("the recorded answer is not a completion or a failure".to_string()),
    };
    let history = PollWorkflowExecutionResponse::decode(vector.recorded_activation.as_slice())
        .map_err(|e| format!("the recorded activation: {e}"))?;

    let issued = completion
        .commands
        .into_iter()
        .map(call)
        .collect::<Result<Vec<_>, _>>()?;
    let issued_steps: HashSet<String> = issued.iter().filter_map(step_of).collect();
    let mut calls: Vec<Command> = recorded_steps(&history)
        .into_iter()
        .filter(|call| step_of(call).is_some_and(|step| !issued_steps.contains(&step)))
        .collect();
    calls.extend(issued);
    for (seq, call) in calls.iter_mut().enumerate() {
        call.seq = seq as u64;
    }

    match answers {
        Answers::Recorded => {}
        Answers::DropCommand => {
            if let Some(last) = calls.iter().rposition(|call| !call.satisfied) {
                calls.remove(last);
            }
        }
        Answers::ChangeTaskType => {
            for call in &mut calls {
                if let Some(What::RunTask(task)) = &mut call.what {
                    task.task_type.push_str("-changed");
                }
            }
        }
        Answers::ForgetHistory => calls.retain(|call| !call.satisfied),
    }

    Ok(RunActivationResult {
        activation_id,
        outcome: Some(run_activation_result::Outcome::Commands(Commands {
            commands: calls,
            ..Default::default()
        })),
        queries: completion
            .query_results
            .into_iter()
            .map(|result| QueryAnswer {
                answer: match (result.result_type(), result.result) {
                    (QueryResultType::Answered, Some(query_result::Result::Answer(data))) => {
                        Some(query_answer::Answer::Value(payload(data)))
                    }
                    (_, Some(query_result::Result::ErrorMessage(message))) => {
                        Some(query_answer::Answer::Failure(failure(message)))
                    }
                    _ => None,
                },
                query_id: result.query_id,
                ..Default::default()
            })
            .collect(),
        updates: completion
            .update_results
            .into_iter()
            .map(|result| UpdateAnswer {
                answer: match (result.result_type(), result.result) {
                    (UpdateResultType::Completed, Some(update_result::Result::Answer(data))) => {
                        Some(update_answer::Answer::Value(payload(data)))
                    }
                    (
                        UpdateResultType::Rejected,
                        Some(update_result::Result::ErrorMessage(message)),
                    ) => Some(update_answer::Answer::Rejected(failure(message))),
                    (_, Some(update_result::Result::ErrorMessage(message))) => {
                        Some(update_answer::Answer::Failed(failure(message)))
                    }
                    _ => None,
                },
                update_id: result.update_id,
                ..Default::default()
            })
            .collect(),
        ..Default::default()
    })
}

/// A satisfied call for every step the history recorded, the first record
/// of each, in the history's order: the tasks, timers and children the
/// determinism check compares.
fn recorded_steps(history: &PollWorkflowExecutionResponse) -> Vec<Command> {
    let mut seen = HashSet::new();
    let mut calls = Vec::new();
    for entry in &history.journal {
        let (step_id, what) = match &entry.attributes {
            Some(journal_entry::Attributes::TaskScheduled(task)) => (
                task.task_id.clone(),
                What::RunTask(RunTask {
                    task_type: task.task_type.clone(),
                    ..Default::default()
                }),
            ),
            Some(journal_entry::Attributes::TimerStarted(timer)) => (
                timer.timer_id.clone(),
                What::StartTimer(StartTimer {
                    duration: timer.start_to_fire_timeout,
                    ..Default::default()
                }),
            ),
            Some(journal_entry::Attributes::ChildWorkflowExecutionStarted(child)) => (
                String::new(),
                What::StartChild(StartChild {
                    workflow_id: child.workflow_id.clone(),
                    workflow_type: child.workflow_type.clone(),
                    ..Default::default()
                }),
            ),
            _ => continue,
        };
        let call = Command {
            satisfied: true,
            step_id,
            what: Some(what),
            ..Default::default()
        };
        if let Some(step) = step_of(&call) {
            if !step.is_empty() && seen.insert(step) {
                calls.push(call);
            }
        }
    }
    calls
}

/// The step a call is, as the determinism check names it, kind first.
fn step_of(call: &Command) -> Option<String> {
    match call.what.as_ref()? {
        What::RunTask(_) => Some(format!("task:{}", call.step_id)),
        What::StartTimer(_) => Some(format!("timer:{}", call.step_id)),
        What::StartChild(child) => Some(format!("child:{}", child.workflow_id)),
        _ => None,
    }
}

/// The call that makes the worker send `command`.
fn call(command: EngineCommand) -> Result<Command, String> {
    let (step_id, what) = match command.attributes {
        Some(Attributes::ScheduleTask(task)) => (
            task.task_id,
            What::RunTask(RunTask {
                task_type: task.task_type,
                task_queue: task.task_queue,
                input: payloads(task.input),
                queue_timeout: task.schedule_to_start_timeout,
                attempt_timeout: task.start_to_close_timeout,
                heartbeat_timeout: task.heartbeat_timeout,
                retry_policy: task.retry_policy.map(retry_policy),
                headers: task
                    .headers
                    .into_iter()
                    .map(|(name, value)| (name, payload(value)))
                    .collect(),
                ..Default::default()
            }),
        ),
        Some(Attributes::StartTimer(timer)) => (
            timer.timer_id,
            What::StartTimer(StartTimer {
                duration: timer.start_to_fire_timeout,
                ..Default::default()
            }),
        ),
        Some(Attributes::CancelTimer(timer)) => (
            String::new(),
            What::CancelTimer(CancelTimer {
                step_id: timer.timer_id,
                ..Default::default()
            }),
        ),
        Some(Attributes::StartChildWorkflow(child)) => (
            String::new(),
            What::StartChild(StartChild {
                workflow_id: child.workflow_id,
                workflow_type: child.workflow_type,
                task_queue: child.task_queue,
                namespace: child.namespace,
                input: payloads(child.input),
                execution_timeout: child.execution_timeout,
                orphan_policy: child.orphan_policy,
                retry_policy: child.retry_policy.map(retry_policy),
                ..Default::default()
            }),
        ),
        Some(Attributes::CancelChildWorkflow(child)) => (
            String::new(),
            What::CancelChild(CancelChild {
                step_id: child.workflow_id,
                ..Default::default()
            }),
        ),
        Some(Attributes::SendEvent(event)) => (
            String::new(),
            What::SendEvent(SendEvent {
                workflow_id: event.workflow_id,
                run_id: event.run_id,
                name: event.event_name,
                payload: Some(payload(event.payload)),
                ..Default::default()
            }),
        ),
        Some(Attributes::RecordStepResult(step)) => (
            step.step_name,
            What::RecordValue(RecordValue {
                outcome: Some(Outcome {
                    result: Some(match step.failure {
                        Some(failure) => outcome::Result::Failure(Failure {
                            message: failure.message,
                            error_type: failure.failure_type,
                            ..Default::default()
                        }),
                        None => outcome::Result::Value(payload(step.result)),
                    }),
                    attempts: u32::try_from(step.execution_attempt).unwrap_or(1),
                    ..Default::default()
                }),
                ..Default::default()
            }),
        ),
        Some(Attributes::CompleteWorkflow(complete)) => (
            String::new(),
            What::CompleteRun(CompleteRun {
                result: Some(payload(complete.result)),
                ..Default::default()
            }),
        ),
        Some(Attributes::FailWorkflow(fail)) => {
            let failure = fail.failure.unwrap_or_default();
            (
                String::new(),
                What::FailRun(FailRun {
                    failure: Some(Failure {
                        message: failure.message,
                        error_type: failure.failure_type,
                        details: failure.details,
                        ..Default::default()
                    }),
                    ..Default::default()
                }),
            )
        }
        Some(Attributes::CancelWorkflow(cancel)) => (
            String::new(),
            What::EndCancelled(EndCancelled {
                details: String::from_utf8_lossy(&cancel.details).into_owned(),
                ..Default::default()
            }),
        ),
        Some(Attributes::RestartFresh(restart)) => (
            String::new(),
            What::RestartFresh(RestartFresh {
                workflow_type: restart.workflow_type,
                task_queue: restart.task_queue,
                input: payloads(restart.input),
                execution_timeout: restart.execution_timeout,
                retry_policy: restart.retry_policy.map(retry_policy),
                ..Default::default()
            }),
        ),
        other => {
            return Err(format!(
                "the recorded command {other:?} has no call in the kernel protocol"
            ))
        }
    };
    Ok(Command {
        step_id,
        what: Some(what),
        ..Default::default()
    })
}

fn retry_policy(policy: EngineRetryPolicy) -> RetryPolicy {
    RetryPolicy {
        first_delay: policy.initial_interval,
        delay_multiplier: policy.backoff_coefficient,
        max_delay: policy.maximum_interval,
        max_attempts: u32::try_from(policy.maximum_attempts).unwrap_or(0),
        never_retry_types: policy.non_retryable_error_types,
        ..Default::default()
    }
}

/// The engine carries a call's inputs as one run of bytes, the payloads'
/// data joined: as one payload, they are joined back into the same bytes.
fn payloads(data: Vec<u8>) -> Vec<Payload> {
    if data.is_empty() {
        Vec::new()
    } else {
        vec![payload(data)]
    }
}

fn payload(data: Vec<u8>) -> Payload {
    Payload {
        data,
        ..Default::default()
    }
}

fn failure(message: String) -> Failure {
    Failure {
        message,
        ..Default::default()
    }
}
