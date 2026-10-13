//! An activation's history, resolved into the outcomes the run observes.
//!
//! This is the reading every SDK did for itself: the jobs core derived from
//! the journal, plus the times each SDK read from the journal beside them, plus
//! its own decoding of failures. Here it is done once:
//!
//! - **Order.** Outcomes are ordered by when the engine recorded them, then by
//!   journal position. Position alone is wrong: the engine appends step results
//!   after the rest of the journal.
//! - **Time.** Each outcome carries its recorded time; a run's deterministic
//!   clock moves to the time of the last outcome it has consumed.
//! - **One decoding.** A failed step or child is a [`Failure`] in an
//!   [`Outcome`], never a marker string inside a result.
//!
//! Step ids are the ones the run recorded, so a run replays under the scheme it
//! started with. Assigning ids is the kernel's next step.

use crate::proto::kernel::v1::{
    outcome, resolution, CancelAsked, ChildSettled, ChildStarted, End, EventArrived, Failure, FailureCode,
    Outcome, Payload, QueryAsked, Resolution, RunActivation, RunRef, RunStarted, StepSettled,
    TimerElapsed, UpdateArrived,
};
use crate::proto::orcher::v1::{journal_entry::Attributes, EntryType, JournalEntry};
use crate::proto::prost_types::Timestamp;
use crate::worker::poll::polling::WorkflowExecutionTask;

/// Resolves the history an activation carries into the [`RunActivation`] the
/// language SDK runs the workflow against.
///
/// `namespace` is the namespace the worker polled, which the engine does not
/// repeat in the activation.
pub fn resolve(task: &WorkflowExecutionTask, namespace: &str) -> RunActivation {
    let mut resolutions: Vec<(Option<Timestamp>, i64, Resolution)> = Vec::new();
    let mut started_at = None;

    for entry in &task.journal {
        let Some((step_id, what)) = resolution_for(entry) else {
            if entry.entry_type == EntryType::WorkflowExecutionStarted as i32 {
                started_at = started_at.or(entry.timestamp);
            }
            continue;
        };
        resolutions.push((
            entry.timestamp,
            entry.entry_id,
            Resolution {
                step_id,
                at: entry.timestamp,
                // Whether this activation repeats an outcome the run already
                // saw is not known from a full history; diagnostic only.
                replaying: false,
                what: Some(what),
            },
        ));
    }
    // By recorded time, then journal position. An outcome with no recorded
    // time keeps its place after those that have one.
    resolutions.sort_by(|(a_at, a_id, _), (b_at, b_id, _)| {
        let key = |at: &Option<Timestamp>| at.as_ref().map(|t| (t.seconds, t.nanos));
        match (key(a_at), key(b_at)) {
            (Some(a), Some(b)) => a.cmp(&b).then(a_id.cmp(b_id)),
            (Some(_), None) => std::cmp::Ordering::Less,
            (None, Some(_)) => std::cmp::Ordering::Greater,
            (None, None) => a_id.cmp(b_id),
        }
    });

    let started = Resolution {
        step_id: String::new(),
        at: started_at,
        replaying: false,
        what: Some(resolution::What::Started(RunStarted {
            input: vec![json_payload(
                serde_json::to_vec(&task.input).unwrap_or_else(|_| b"null".to_vec()),
            )],
            attempt: u32::try_from(task.attempt).unwrap_or(0),
            started_at,
            ..Default::default()
        })),
    };

    RunActivation {
        activation_id: task.task_token.clone(),
        run: Some(RunRef {
            workflow_id: task.execution.workflow_id.clone(),
            run_id: task.execution.run_id.clone(),
            workflow_type: task.workflow_type.clone(),
            task_queue: task.task_queue.clone(),
            namespace: namespace.to_string(),
        }),
        resolutions: std::iter::once(started)
            .chain(resolutions.into_iter().map(|(_, _, resolution)| resolution))
            // Updates waiting to be applied in this activation. The engine
            // journals an update only once the worker has answered it, so
            // these are never also among the journaled ones above; they come
            // last, with no recorded time.
            .chain(task.updates.iter().map(|update| Resolution {
                step_id: String::new(),
                at: None,
                replaying: false,
                what: Some(resolution::What::Update(UpdateArrived {
                    update_id: update.update_id.clone(),
                    name: update.update_type.clone(),
                    args: if update.args.is_empty() {
                        vec![]
                    } else {
                        vec![json_payload(update.args.clone())]
                    },
                })),
            }))
            .collect(),
        queries: task
            .queries
            .iter()
            .map(|query| QueryAsked {
                query_id: query.query_id.clone(),
                name: query.query_type.clone(),
                args: if query.query_args.is_empty() {
                    vec![]
                } else {
                    vec![json_payload(query.query_args.clone())]
                },
            })
            .collect(),
    }
}

/// The outcome a journal entry records, if it records one the run observes,
/// with the step it belongs to as the run recorded it (empty for outcomes of
/// no step: an event, an update, a cancel request).
fn resolution_for(entry: &JournalEntry) -> Option<(String, resolution::What)> {
    let attributes = entry.attributes.as_ref()?;
    let step = |id: &str, what| (id.to_string(), what);
    let no_step = |what| (String::new(), what);
    Some(match attributes {
        Attributes::StepCompleted(step_completed) => step(
            &step_completed.step_name,
            resolution::What::Step(StepSettled {
                outcome: Some(match &step_completed.failure {
                    Some(failure) => Outcome {
                        result: Some(outcome::Result::Failure(Failure {
                            message: failure.message.clone(),
                            error_type: failure.failure_type.clone(),
                            non_retryable: failure.non_retryable,
                            code: FailureCode::Application as i32,
                            details: Vec::new(),
                        })),
                        end: End::Failed as i32,
                        attempts: attempts(step_completed.execution_attempt),
                    },
                    None => Outcome {
                        result: Some(outcome::Result::Value(json_payload(
                            step_completed.result.clone(),
                        ))),
                        end: End::Completed as i32,
                        attempts: attempts(step_completed.execution_attempt),
                    },
                }),
            }),
        ),
        Attributes::TimerFired(fired) => {
            step(&fired.timer_id, resolution::What::Timer(TimerElapsed {}))
        }
        Attributes::EventReceived(event) => no_step(resolution::What::Event(EventArrived {
            event_id: entry.entry_id.to_string(),
            name: event.event_name.clone(),
            payload: Some(json_payload(event.payload.clone())),
        })),
        Attributes::UpdateAccepted(update) => no_step(resolution::What::Update(UpdateArrived {
            update_id: update.update_id.clone(),
            name: update.update_type.clone(),
            // The arguments as the client sent them, in one payload: the
            // engine journals them together.
            args: if update.args.is_empty() {
                vec![]
            } else {
                vec![json_payload(update.args.clone())]
            },
        })),
        Attributes::WorkflowExecutionCancelRequested(cancel) => {
            no_step(resolution::What::Cancel(CancelAsked {
                reason: cancel.cause.clone(),
                requested_at: entry.timestamp,
            }))
        }
        // Started, not yet settled: what history holds for a child the run
        // started, by the workflow id it was started under.
        Attributes::ChildWorkflowExecutionStarted(child) => step(
            &child.workflow_id,
            resolution::What::ChildStarted(ChildStarted {
                child_workflow_id: child.workflow_id.clone(),
                child_run_id: child.execution_id.clone(),
            }),
        ),
        Attributes::ChildWorkflowExecutionCompleted(child) => child_settled(
            &child.workflow_id,
            &child.execution_id,
            Outcome {
                result: Some(outcome::Result::Value(json_payload(child.result.clone()))),
                end: End::Completed as i32,
                attempts: 0,
            },
        ),
        Attributes::ChildWorkflowExecutionFailed(child) => {
            let (message, error_type, non_retryable) = child
                .failure
                .as_ref()
                .map(|f| (f.message.clone(), f.failure_type.clone(), f.non_retryable))
                .unwrap_or_else(|| {
                    (
                        "Child workflow failed".to_string(),
                        "ChildWorkflowFailed".to_string(),
                        false,
                    )
                });
            child_settled(
                &child.workflow_id,
                &child.execution_id,
                ended(
                    End::Failed,
                    FailureCode::Application,
                    message,
                    error_type,
                    non_retryable,
                ),
            )
        }
        Attributes::ChildWorkflowExecutionCanceled(child) => child_settled(
            &child.workflow_id,
            &child.execution_id,
            ended(
                End::Cancelled,
                FailureCode::Cancelled,
                "Child workflow was canceled".to_string(),
                String::new(),
                false,
            ),
        ),
        Attributes::ChildWorkflowExecutionTerminated(child) => child_settled(
            &child.workflow_id,
            &child.execution_id,
            ended(
                End::Terminated,
                FailureCode::Terminated,
                "Child workflow was terminated".to_string(),
                String::new(),
                false,
            ),
        ),
        Attributes::ChildWorkflowExecutionTimedOut(child) => child_settled(
            &child.workflow_id,
            &child.execution_id,
            ended(
                End::TimedOut,
                FailureCode::TimedOut,
                "Child workflow timed out".to_string(),
                String::new(),
                false,
            ),
        ),
        _ => return None,
    })
}

/// A settled child, whose step the run recorded by the child's workflow id.
fn child_settled(workflow_id: &str, run_id: &str, outcome: Outcome) -> (String, resolution::What) {
    (
        workflow_id.to_string(),
        resolution::What::Child(ChildSettled {
            child_workflow_id: workflow_id.to_string(),
            child_run_id: run_id.to_string(),
            outcome: Some(outcome),
        }),
    )
}

fn ended(
    end: End,
    code: FailureCode,
    message: String,
    error_type: String,
    non_retryable: bool,
) -> Outcome {
    Outcome {
        result: Some(outcome::Result::Failure(Failure {
            message,
            error_type,
            non_retryable,
            code: code as i32,
            details: Vec::new(),
        })),
        end: end as i32,
        attempts: 0,
    }
}

fn attempts(execution_attempt: i32) -> u32 {
    u32::try_from(execution_attempt).unwrap_or(0)
}

/// Bytes the engine holds as JSON, as a payload saying so.
fn json_payload(data: Vec<u8>) -> Payload {
    Payload {
        data,
        metadata: [("encoding".to_string(), b"json".to_vec())].into(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::orcher::v1::{
        Failure as WireFailure, QueryRequest, StepCompletedEventAttributes,
        TimerFiredEventAttributes, WorkflowExecutionCancelRequestedEventAttributes,
        WorkflowExecutionStartedEventAttributes,
    };
    use crate::types::WorkflowExecution;

    fn at(ms: i64) -> Option<Timestamp> {
        Some(Timestamp {
            seconds: ms / 1000,
            nanos: ((ms % 1000) * 1_000_000) as i32,
        })
    }

    fn entry(id: i64, ms: i64, entry_type: EntryType, attributes: Attributes) -> JournalEntry {
        JournalEntry {
            entry_id: id,
            timestamp: at(ms),
            entry_type: entry_type as i32,
            attributes: Some(attributes),
            ..Default::default()
        }
    }

    fn task(journal: Vec<JournalEntry>) -> WorkflowExecutionTask {
        WorkflowExecutionTask {
            execution: WorkflowExecution::new("order-7", "run-1"),
            workflow_type: "checkout".into(),
            task_queue: "orders".into(),
            input: serde_json::json!({"n": 1}),
            journal,
            started_event_id: 1,
            previous_started_event_id: 0,
            attempt: 1,
            task_token: b"act-1".to_vec(),
            stream_entry_id: None,
            queries: vec![],
            updates: vec![],
        }
    }

    fn started(ms: i64) -> JournalEntry {
        entry(
            1,
            ms,
            EntryType::WorkflowExecutionStarted,
            Attributes::WorkflowExecutionStarted(WorkflowExecutionStartedEventAttributes::default()),
        )
    }

    /// A child the run started is resolved as started, under the id history
    /// holds for it, before it settles: code that started it without naming
    /// it keeps that id while it still runs.
    #[test]
    fn a_child_still_running_is_resolved_as_started() {
        use crate::proto::orcher::v1::ChildWorkflowExecutionStartedEventAttributes;
        let journal = vec![
            started(1_000),
            entry(
                2,
                2_000,
                EntryType::ChildWorkflowExecutionStarted,
                Attributes::ChildWorkflowExecutionStarted(
                    ChildWorkflowExecutionStartedEventAttributes {
                        workflow_id: "child_1".into(),
                        execution_id: "run-9".into(),
                        workflow_type: "ship".into(),
                        ..Default::default()
                    },
                ),
            ),
        ];
        let activation = resolve(&task(journal), "default");
        let child = &activation.resolutions[1];
        assert_eq!(child.step_id, "child_1");
        assert_eq!(child.at, at(2_000));
        let Some(resolution::What::ChildStarted(started)) = &child.what else {
            panic!("{:?}", child.what);
        };
        assert_eq!(
            (started.child_workflow_id.as_str(), started.child_run_id.as_str()),
            ("child_1", "run-9")
        );
    }

    /// The engine appends step results after the rest of the journal. A cancel
    /// request recorded before a step finished is observed before it.
    #[test]
    fn outcomes_are_observed_in_recorded_order_not_journal_position() {
        let journal = vec![
            started(1_000),
            entry(
                2,
                3_000,
                EntryType::WorkflowExecutionCancelRequested,
                Attributes::WorkflowExecutionCancelRequested(
                    WorkflowExecutionCancelRequestedEventAttributes {
                        cause: "user".into(),
                        ..Default::default()
                    },
                ),
            ),
            // Appended after the cancel, recorded before it.
            entry(
                3,
                2_000,
                EntryType::StepCompleted,
                Attributes::StepCompleted(StepCompletedEventAttributes {
                    step_name: "charge_0".into(),
                    result: b"\"ok\"".to_vec(),
                    execution_attempt: 1,
                    ..Default::default()
                }),
            ),
        ];
        let activation = resolve(&task(journal), "default");
        let kinds: Vec<&str> = activation
            .resolutions
            .iter()
            .map(|r| match r.what.as_ref().unwrap() {
                resolution::What::Started(_) => "started",
                resolution::What::Step(_) => "step",
                resolution::What::Cancel(_) => "cancel",
                _ => "other",
            })
            .collect();
        assert_eq!(kinds, ["started", "step", "cancel"]);
        assert_eq!(activation.resolutions[1].step_id, "charge_0");
        assert_eq!(activation.resolutions[1].at, at(2_000));
    }

    /// A failed step is a Failure in its Outcome, never a marker in a result.
    #[test]
    fn a_failed_step_is_one_failure_shape() {
        let journal = vec![
            started(1_000),
            entry(
                2,
                2_000,
                EntryType::StepCompleted,
                Attributes::StepCompleted(StepCompletedEventAttributes {
                    step_name: "charge_0".into(),
                    failure: Some(WireFailure {
                        message: "card declined".into(),
                        failure_type: "Declined".into(),
                        non_retryable: true,
                        ..Default::default()
                    }),
                    execution_attempt: 3,
                    ..Default::default()
                }),
            ),
        ];
        let activation = resolve(&task(journal), "default");
        let Some(resolution::What::Step(step)) = &activation.resolutions[1].what else {
            panic!("not a step: {:?}", activation.resolutions[1]);
        };
        let outcome = step.outcome.as_ref().unwrap();
        assert_eq!(outcome.end, End::Failed as i32);
        assert_eq!(outcome.attempts, 3);
        let Some(outcome::Result::Failure(failure)) = &outcome.result else {
            panic!("not a failure: {outcome:?}");
        };
        assert_eq!(failure.message, "card declined");
        assert_eq!(failure.error_type, "Declined");
        assert!(failure.non_retryable);
    }

    #[test]
    fn the_run_starts_first_with_its_input_and_names_its_run() {
        let mut task = task(vec![
            started(1_000),
            entry(
                2,
                4_000,
                EntryType::TimerFired,
                Attributes::TimerFired(TimerFiredEventAttributes {
                    timer_id: "timer_1".into(),
                    ..Default::default()
                }),
            ),
        ]);
        task.queries = vec![QueryRequest {
            query_id: "q1".into(),
            query_type: "status".into(),
            query_args: vec![],
        }];
        let activation = resolve(&task, "payments");

        let run = activation.run.as_ref().unwrap();
        assert_eq!(
            (
                run.workflow_id.as_str(),
                run.run_id.as_str(),
                run.namespace.as_str()
            ),
            ("order-7", "run-1", "payments")
        );
        assert_eq!(activation.activation_id, b"act-1");
        let Some(resolution::What::Started(start)) = &activation.resolutions[0].what else {
            panic!("the run does not start first");
        };
        assert_eq!(activation.resolutions[0].at, at(1_000));
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&start.input[0].data).unwrap(),
            serde_json::json!({"n": 1})
        );
        assert_eq!(activation.resolutions[1].step_id, "timer_1");
        assert_eq!(activation.queries[0].name, "status");
    }

    /// An update waiting to be applied reaches the language after the
    /// journaled outcomes; the engine journals it only once answered.
    #[test]
    fn a_pending_update_is_delivered_last() {
        let mut task = task(vec![started(1_000)]);
        task.updates = vec![crate::proto::orcher::v1::UpdateRequest {
            update_id: "u1".into(),
            update_type: "rename".into(),
            args: br#"["new name"]"#.to_vec(),
        }];
        let activation = resolve(&task, "default");
        let last = activation.resolutions.last().unwrap();
        let Some(resolution::What::Update(update)) = &last.what else {
            panic!("the update was not delivered: {:?}", activation.resolutions);
        };
        assert_eq!(
            (update.update_id.as_str(), update.name.as_str()),
            ("u1", "rename")
        );
        assert_eq!(update.args[0].data, br#"["new name"]"#);
        assert!(last.at.is_none() && !last.replaying);
    }
}
