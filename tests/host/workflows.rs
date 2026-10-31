//! Workflow activations handed to the language as the kernel protocol's
//! messages, against a fake engine.

use crate::common::typed::TypedWork;
use std::time::Duration;

use crate::common::{activation, Engine};
use orcher_proto::kernel::v1::{
    command, resolution, run_activation_result, Command, Commands, Failure, ProtocolError,
    RunActivationResult, RunTask, StartChild, StepIdScheme,
};
use orcher_proto::orcher::v1::command::Attributes;
use orcher_sdk_core::{Host, HostConfig, Registrations};

async fn workflow_worker(engine: &Engine) -> Host {
    let addr = engine.serve().await;
    Host::start(
        HostConfig::new(format!("http://{addr}"), "orders")
            .with_shutdown_grace(std::time::Duration::from_millis(300)),
        Registrations::new(["checkout"], [] as [&str; 0], [] as [&str; 0]),
    )
    .await
    .unwrap()
}

async fn eventually(what: &str, mut condition: impl FnMut() -> bool) {
    for _ in 0..300 {
        if condition() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("{what} did not happen in time");
}

fn call(seq: u64, satisfied: bool, step_id: &str, task_type: &str) -> Command {
    Command {
        seq,
        satisfied,
        step_id: step_id.into(),
        what: Some(command::What::RunTask(RunTask {
            task_type: task_type.into(),
            ..Default::default()
        })),
        ..Default::default()
    }
}

/// The language receives the history resolved: the run starting with its
/// input, then the recorded task's outcome, with no journal.
#[tokio::test]
async fn an_activation_reaches_the_language_resolved() {
    let engine = Engine::default();
    engine.script().activations.push_back(activation("act-1"));
    let host = workflow_worker(&engine).await;

    let run = host.typed().next_activation().await.expect("an activation");
    assert_eq!(run.activation_id, b"act-1");
    assert_eq!(run.run.as_ref().unwrap().workflow_type, "checkout");
    let kinds: Vec<&str> = run
        .resolutions
        .iter()
        .map(|r| match r.what.as_ref().unwrap() {
            resolution::What::Started(_) => "started",
            resolution::What::Step(_) => "step",
            _ => "other",
        })
        .collect();
    assert_eq!(kinds, ["started", "step"]);
    assert_eq!(run.resolutions[1].step_id, "charge_0");
    host.shutdown().await;
}

/// The answer reaches the engine as commands: the satisfied call checked
/// against history and sent as nothing, the new call as a task with the id
/// the SDK supplied.
#[tokio::test]
async fn an_answer_reaches_the_engine_as_commands() {
    let engine = Engine::default();
    engine.script().activations.push_back(activation("act-1"));
    let host = workflow_worker(&engine).await;
    let run = host.typed().next_activation().await.unwrap();

    host.typed()
        .complete_activation(RunActivationResult {
            activation_id: run.activation_id,
            outcome: Some(run_activation_result::Outcome::Commands(Commands {
                commands: vec![
                    call(0, true, "charge_0", "charge"),
                    call(1, false, "ship_1", "ship"),
                ],
                ..Default::default()
            })),
            ..Default::default()
        })
        .await
        .unwrap();

    eventually("the completion", || {
        !engine.script().workflow_completions.is_empty()
    })
    .await;
    let completion = engine.script().workflow_completions[0].clone();
    assert_eq!(completion.commands.len(), 1, "{:?}", completion.commands);
    let Some(Attributes::ScheduleTask(task)) = &completion.commands[0].attributes else {
        panic!("{:?}", completion.commands[0]);
    };
    assert_eq!(
        (task.task_id.as_str(), task.task_type.as_str()),
        ("ship_1", "ship")
    );
    // Counted on the wire: one activation with its journal, one report
    // carrying the one command sent (sdk-core#43).
    eventually("the report counted", || {
        host.wire_metrics().work.workflow_reports == 1
    })
    .await;
    let work = host.wire_metrics().work;
    assert_eq!(work.activations, 1);
    assert!(work.activation_journal_entries >= 1, "{work:?}");
    assert_eq!(work.workflow_commands, 1);
    assert_eq!(work.workflow_failures, 0);
    host.shutdown().await;
}

/// Code that no longer issues the recorded step is held, not applied: the
/// kernel's determinism check sees the satisfied calls the language sent.
#[tokio::test]
async fn an_answer_that_disagrees_with_history_is_not_applied() {
    let engine = Engine::default();
    engine.script().activations.push_back(activation("act-1"));
    let host = workflow_worker(&engine).await;
    let run = host.typed().next_activation().await.unwrap();

    // The journal recorded a `charge` task at charge_0; the code now issues a
    // `refund` there.
    host.typed()
        .complete_activation(RunActivationResult {
            activation_id: run.activation_id,
            outcome: Some(run_activation_result::Outcome::Commands(Commands {
                commands: vec![call(0, false, "charge_0", "refund")],
                ..Default::default()
            })),
            ..Default::default()
        })
        .await
        .unwrap();

    eventually("the activation handed back", || {
        !engine.script().releases.is_empty()
    })
    .await;
    assert!(engine.script().workflow_completions.is_empty());
    assert!(engine.script().workflow_failures.is_empty());
    host.shutdown().await;
}

/// A protocol error hands the activation back at once; it does not fail the
/// run.
#[tokio::test]
async fn a_protocol_error_hands_the_activation_back() {
    let engine = Engine::default();
    engine.script().activations.push_back(activation("act-1"));
    let host = workflow_worker(&engine).await;
    let run = host.typed().next_activation().await.unwrap();

    host.typed()
        .complete_activation(RunActivationResult {
            activation_id: run.activation_id,
            outcome: Some(run_activation_result::Outcome::ProtocolError(
                ProtocolError {
                    message: "unknown resolution".into(),
                    ..Default::default()
                },
            )),
            ..Default::default()
        })
        .await
        .unwrap();

    eventually("the release", || !engine.script().releases.is_empty()).await;
    assert!(engine.script().workflow_failures.is_empty());
    assert!(engine.script().workflow_completions.is_empty());
    host.shutdown().await;
}

/// A workflow failure fails the run.
#[tokio::test]
async fn a_workflow_failure_fails_the_run() {
    let engine = Engine::default();
    engine.script().activations.push_back(activation("act-1"));
    let host = workflow_worker(&engine).await;
    let run = host.typed().next_activation().await.unwrap();

    host.typed()
        .complete_activation(RunActivationResult {
            activation_id: run.activation_id,
            outcome: Some(run_activation_result::Outcome::WorkflowFailed(Failure {
                message: "out of stock".into(),
                ..Default::default()
            })),
            ..Default::default()
        })
        .await
        .unwrap();

    eventually("the failure", || {
        !engine.script().workflow_failures.is_empty()
    })
    .await;
    let failure = engine.script().workflow_failures[0]
        .failure
        .clone()
        .unwrap();
    assert!(
        failure.message.contains("out of stock"),
        "{}",
        failure.message
    );
    host.shutdown().await;
}

fn failed(activation_id: Vec<u8>, calls: Vec<Command>) -> RunActivationResult {
    RunActivationResult {
        activation_id,
        outcome: Some(run_activation_result::Outcome::WorkflowFailed(Failure {
            message: "out of stock".into(),
            ..Default::default()
        })),
        calls_before_failure: Some(Commands {
            commands: calls,
            ..Default::default()
        }),
        ..Default::default()
    }
}

/// Code that fails having reached the recorded step fails the run.
#[tokio::test]
async fn a_failure_after_every_recorded_step_fails_the_run() {
    let engine = Engine::default();
    engine.script().activations.push_back(activation("act-1"));
    let host = workflow_worker(&engine).await;
    let run = host.typed().next_activation().await.unwrap();

    host.typed()
        .complete_activation(failed(
            run.activation_id,
            vec![call(0, true, "charge_0", "charge")],
        ))
        .await
        .unwrap();

    eventually("the failure", || {
        !engine.script().workflow_failures.is_empty()
    })
    .await;
    assert!(engine.script().releases.is_empty());
    host.shutdown().await;
}

/// Code that fails without reaching the recorded step no longer replays its
/// history, so the run is held, not failed: the kernel's determinism check
/// sees the calls made before the failure.
#[tokio::test]
async fn a_failure_that_left_a_recorded_step_behind_is_held() {
    let engine = Engine::default();
    engine.script().activations.push_back(activation("act-1"));
    let host = workflow_worker(&engine).await;
    let run = host.typed().next_activation().await.unwrap();

    host.typed()
        .complete_activation(failed(run.activation_id, vec![]))
        .await
        .unwrap();

    eventually("the activation handed back", || {
        !engine.script().releases.is_empty()
    })
    .await;
    assert!(engine.script().workflow_failures.is_empty());
    assert!(engine.script().workflow_completions.is_empty());
    host.shutdown().await;
}

/// An answer to an activation the host does not hold is refused.
#[tokio::test]
async fn an_answer_to_no_held_activation_is_refused() {
    let engine = Engine::default();
    let host = workflow_worker(&engine).await;
    let error = host
        .typed()
        .complete_activation(RunActivationResult {
            activation_id: b"never-handed-out".to_vec(),
            ..Default::default()
        })
        .await
        .unwrap_err();
    assert!(error.to_string().contains("no activation"), "{error}");
    host.shutdown().await;
}

/// A worker that reports the kernel's step-id scheme says so on every poll;
/// a run the engine bound to it starts with the scheme, its lineage and the
/// seed derived from it, and the kernel names its steps: a task by its call
/// sequence, a child started without a workflow id from the lineage.
#[tokio::test]
async fn a_run_under_the_kernel_scheme_gets_its_ids_from_the_kernel() {
    let engine = Engine::default();
    let mut fresh = activation("act-1");
    fresh.journal.truncate(1);
    fresh.step_ids = StepIdScheme::Kernel as i32;
    fresh.history_lineage = "lineage-1".into();
    engine.script().activations.push_back(fresh);
    let addr = engine.serve().await;
    let host = Host::start(
        HostConfig::new(format!("http://{addr}"), "orders")
            .with_shutdown_grace(Duration::from_millis(300))
            .with_step_ids([
                orcher_sdk_core::StepIdScheme::Kernel,
                orcher_sdk_core::StepIdScheme::RustV0,
            ]),
        Registrations::new(["checkout"], [] as [&str; 0], [] as [&str; 0]),
    )
    .await
    .unwrap();

    let run = host.typed().next_activation().await.expect("an activation");
    assert_eq!(
        engine.script().polled_step_ids[0],
        [StepIdScheme::Kernel as i32, StepIdScheme::RustV0 as i32]
    );
    let Some(resolution::What::Started(started)) = &run.resolutions[0].what else {
        panic!("the run does not start first: {:?}", run.resolutions);
    };
    assert_eq!(started.step_ids, StepIdScheme::Kernel as i32);
    assert_eq!(started.history_lineage, "lineage-1");
    assert_eq!(
        started.random_seed,
        orcher_sdk_core::kernel::random::seed_for_lineage("lineage-1")
    );

    host.typed()
        .complete_activation(RunActivationResult {
            activation_id: run.activation_id,
            outcome: Some(run_activation_result::Outcome::Commands(Commands {
                commands: vec![
                    call(0, false, "", "charge"),
                    Command {
                        seq: 1,
                        what: Some(command::What::StartChild(StartChild {
                            workflow_type: "ship".into(),
                            ..Default::default()
                        })),
                        ..Default::default()
                    },
                ],
                ..Default::default()
            })),
            ..Default::default()
        })
        .await
        .unwrap();

    eventually("the completion", || {
        !engine.script().workflow_completions.is_empty()
    })
    .await;
    let completion = engine.script().workflow_completions[0].clone();
    let Some(Attributes::ScheduleTask(task)) = &completion.commands[0].attributes else {
        panic!("{:?}", completion.commands);
    };
    assert_eq!(task.task_id, "task-0");
    let Some(Attributes::StartChildWorkflow(child)) = &completion.commands[1].attributes else {
        panic!("{:?}", completion.commands);
    };
    assert_eq!(child.workflow_id, "lineage-1-child-1");
    host.shutdown().await;
}

/// A worker that reports no scheme reports none, and a run the engine bound
/// to none starts with no seed, its SDK supplying its ids as before.
#[tokio::test]
async fn a_worker_reporting_no_scheme_runs_as_before() {
    let engine = Engine::default();
    engine.script().activations.push_back(activation("act-1"));
    let host = workflow_worker(&engine).await;

    let run = host.typed().next_activation().await.expect("an activation");
    assert!(engine.script().polled_step_ids[0].is_empty());
    let Some(resolution::What::Started(started)) = &run.resolutions[0].what else {
        panic!("the run does not start first: {:?}", run.resolutions);
    };
    assert_eq!(started.step_ids, StepIdScheme::Unspecified as i32);
    assert!(started.random_seed.is_empty());
    assert!(started.history_lineage.is_empty());
    host.shutdown().await;
}
