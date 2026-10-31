//! The host's work as encoded protocol messages: what a language binding
//! exchanges with it.

use std::time::Duration;

use crate::common::{activation, task, Engine};
use orcher_proto::host::v1::{
    task_outcome, HeartbeatAnswer, TaskAssignment, TaskHeartbeat, TaskOutcome,
};
use orcher_proto::kernel::v1::{
    command, run_activation_result, Command, Commands, Payload, RunActivation, RunActivationResult,
    RunTask,
};
use orcher_proto::prost::Message;
use orcher_sdk_core::{Error, Host, HostConfig, Registrations};

async fn worker(engine: &Engine) -> Host {
    let addr = engine.serve().await;
    Host::start(
        HostConfig::new(format!("http://{addr}"), "orders")
            .with_shutdown_grace(Duration::from_millis(300)),
        Registrations::new(["checkout"], ["charge"], [] as [&str; 0]),
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

/// An activation crosses encoded, and its encoded answer reaches the engine.
#[tokio::test]
async fn an_activation_and_its_answer_cross_encoded() {
    let engine = Engine::default();
    engine.script().activations.push_back(activation("act-1"));
    let host = worker(&engine).await;
    let encoded = host.encoded();

    let bytes = encoded.next_activation().await.expect("an activation");
    let activation = RunActivation::decode(bytes.as_slice()).expect("a RunActivation");
    assert_eq!(activation.activation_id, b"act-1");

    let answer = RunActivationResult {
        activation_id: activation.activation_id,
        // The recorded task, answered from history, then a new one.
        outcome: Some(run_activation_result::Outcome::Commands(Commands {
            commands: vec![
                call(0, true, "charge_0", "charge"),
                call(1, false, "ship_1", "ship"),
            ],
            ..Default::default()
        })),
        ..Default::default()
    };
    encoded
        .complete_activation(&answer.encode_to_vec())
        .await
        .unwrap();
    eventually("the answer reached the engine", || {
        !engine.script().workflow_completions.is_empty()
    })
    .await;
    host.shutdown().await;
}

/// A task, its heartbeat and its outcome cross encoded.
#[tokio::test]
async fn a_task_its_heartbeat_and_its_outcome_cross_encoded() {
    let engine = Engine::default();
    engine.script().tasks.push_back(task("charge_0", "token-1"));
    let host = worker(&engine).await;
    let encoded = host.encoded();

    let bytes = encoded.next_task().await.expect("a task");
    let assignment = TaskAssignment::decode(bytes.as_slice()).expect("a TaskAssignment");
    assert_eq!(assignment.token, b"token-1");

    let answer = encoded
        .heartbeat(
            &TaskHeartbeat {
                token: assignment.token.clone(),
                ..Default::default()
            }
            .encode_to_vec(),
        )
        .unwrap();
    let answer = HeartbeatAnswer::decode(answer.as_slice()).expect("a HeartbeatAnswer");
    assert!(!answer.cancel_requested);

    let outcome = TaskOutcome {
        token: assignment.token,
        result: Some(task_outcome::Result::Value(Payload {
            data: b"{}".to_vec(),
            ..Default::default()
        })),
        ..Default::default()
    };
    encoded
        .complete_task(&outcome.encode_to_vec())
        .await
        .unwrap();
    eventually("the outcome reached the engine", || {
        !engine.script().completed.is_empty()
    })
    .await;
    host.shutdown().await;
}

/// Bytes that are not the message an operation takes are refused as a
/// protocol error, and nothing is sent.
#[tokio::test]
async fn bytes_that_are_not_the_message_are_a_protocol_error() {
    let engine = Engine::default();
    let host = worker(&engine).await;
    let encoded = host.encoded();
    let garbage = [0xff, 0xff, 0xff];

    assert!(matches!(
        encoded.complete_activation(&garbage).await,
        Err(Error::Protocol(_))
    ));
    assert!(matches!(
        encoded.complete_task(&garbage).await,
        Err(Error::Protocol(_))
    ));
    assert!(matches!(
        encoded.heartbeat(&garbage),
        Err(Error::Protocol(_))
    ));
    assert!(matches!(
        encoded.complete_actor_operation(&garbage).await,
        Err(Error::Protocol(_))
    ));
    assert!(engine.script().workflow_completions.is_empty());
    assert!(engine.script().completed.is_empty());
    host.shutdown().await;
}
