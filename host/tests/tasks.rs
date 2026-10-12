//! Tasks handed to the language by pull, against a fake engine.

mod common;

use std::time::Duration;

use common::{task, Engine};
use orcher_sdk_core::proto::host::v1::{task_outcome, TaskOutcome};
use orcher_sdk_core::proto::kernel::v1::{Failure, Payload};
use orcher_sdk_host::{Host, HostConfig, Registrations};

async fn task_worker(engine: &Engine) -> Host {
    let addr = engine.serve().await;
    Host::start(
        HostConfig::new(format!("http://{addr}"), "orders").with_namespace("payments"),
        Registrations::new([] as [&str; 0], ["charge"], [] as [&str; 0]),
    )
    .await
    .unwrap()
}

/// Waits a bounded time for `condition`.
async fn eventually(what: &str, mut condition: impl FnMut() -> bool) {
    for _ in 0..200 {
        if condition() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("{what} did not happen in time");
}

fn value(data: &[u8]) -> Option<task_outcome::Result> {
    Some(task_outcome::Result::Value(Payload {
        data: data.to_vec(),
        metadata: Default::default(),
    }))
}

/// A task the engine hands out reaches the language whole.
#[tokio::test]
async fn a_task_reaches_the_language_whole() {
    let engine = Engine::default();
    engine.script().tasks.push_back(task("charge_0", "token-1"));
    let host = task_worker(&engine).await;

    let assignment = host.next_task().await.expect("a task");
    assert_eq!(assignment.token, b"token-1");
    assert_eq!(assignment.task_id, "charge_0");
    assert_eq!(assignment.task_type, "charge");
    assert_eq!(assignment.attempt, 2);
    let run = assignment.run.as_ref().unwrap();
    assert_eq!(
        (run.workflow_id.as_str(), run.run_id.as_str()),
        ("order-7", "run-1")
    );
    assert_eq!(run.namespace, "payments");
    assert_eq!(assignment.input[0].data, br#"{"amount":7}"#);
    assert_eq!(assignment.attempt_timeout.as_ref().unwrap().seconds, 30);

    host.complete_task(TaskOutcome {
        token: assignment.token.clone(),
        result: value(b"\"charged\""),
    })
    .await
    .unwrap();
    eventually("the completion", || !engine.script().completed.is_empty()).await;
    assert_eq!(engine.script().completed[0].task_token, b"token-1");
    host.shutdown().await;
}

/// A task's own failure reaches the engine as the task's failure: with the
/// type and retryability the language gave, and its message as written,
/// never relabelled as an error of the SDK.
#[tokio::test]
async fn a_task_failure_is_reported_as_the_tasks_own() {
    let engine = Engine::default();
    engine.script().tasks.push_back(task("charge_0", "token-1"));
    let host = task_worker(&engine).await;

    let assignment = host.next_task().await.unwrap();
    host.complete_task(TaskOutcome {
        token: assignment.token,
        result: Some(task_outcome::Result::Failure(Failure {
            message: "card declined".into(),
            error_type: "Declined".into(),
            non_retryable: true,
            ..Default::default()
        })),
    })
    .await
    .unwrap();

    eventually("the failure", || !engine.script().failed.is_empty()).await;
    let failure = engine.script().failed[0].failure.clone().unwrap();
    assert_eq!(failure.message, "card declined");
    assert_eq!(failure.failure_type, "Declined");
    assert!(failure.non_retryable);
    host.shutdown().await;
}

/// A heartbeat carries its details to the engine, and a cancellation request
/// comes back through it and through the cancellation wait.
#[tokio::test]
async fn a_cancellation_request_reaches_a_heartbeating_task() {
    let engine = Engine::default();
    engine.script().tasks.push_back(task("charge_0", "token-1"));
    engine.script().cancel_requested = true;
    let host = task_worker(&engine).await;
    let assignment = host.next_task().await.unwrap();

    let waiting = {
        let host = host.clone();
        let token = assignment.token.clone();
        tokio::spawn(async move { host.task_cancelled(&token).await })
    };
    let details = Payload {
        data: b"half way".to_vec(),
        metadata: Default::default(),
    };
    let mut answered = false;
    for _ in 0..200 {
        if host
            .heartbeat(&assignment.token, Some(details.clone()))
            .cancel_requested
        {
            answered = true;
            break;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert!(answered, "the heartbeat never saw the cancellation request");
    assert!(
        tokio::time::timeout(Duration::from_secs(2), waiting)
            .await
            .unwrap()
            .unwrap(),
        "the cancellation wait did not report the request"
    );
    eventually("a heartbeat with details", || {
        engine
            .script()
            .heartbeats
            .iter()
            .any(|beat| beat.details == b"half way")
    })
    .await;
    host.shutdown().await;
}

/// Once a task's outcome is reported, its cancellation wait ends with `false`
/// and its heartbeats are no longer the host's to answer.
#[tokio::test]
async fn a_reported_task_is_no_longer_held() {
    let engine = Engine::default();
    engine.script().tasks.push_back(task("charge_0", "token-1"));
    let host = task_worker(&engine).await;
    let assignment = host.next_task().await.unwrap();

    let waiting = {
        let host = host.clone();
        let token = assignment.token.clone();
        tokio::spawn(async move { host.task_cancelled(&token).await })
    };
    tokio::time::sleep(Duration::from_millis(20)).await;
    host.complete_task(TaskOutcome {
        token: assignment.token.clone(),
        result: value(b"1"),
    })
    .await
    .unwrap();
    assert!(!tokio::time::timeout(Duration::from_secs(2), waiting)
        .await
        .unwrap()
        .unwrap());
    assert!(!host.task_cancelled(&assignment.token).await);
    assert!(!host.heartbeat(&assignment.token, None).cancel_requested);
    host.shutdown().await;
}

/// An outcome with neither a value nor a failure is refused, and nothing is
/// reported for it.
#[tokio::test]
async fn an_empty_outcome_is_refused() {
    let engine = Engine::default();
    engine.script().tasks.push_back(task("charge_0", "token-1"));
    let host = task_worker(&engine).await;
    let assignment = host.next_task().await.unwrap();
    let error = host
        .complete_task(TaskOutcome {
            token: assignment.token,
            result: None,
        })
        .await
        .unwrap_err();
    assert!(error.to_string().contains("value or a failure"), "{error}");
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert!(engine.script().completed.is_empty() && engine.script().failed.is_empty());
    host.shutdown().await;
}

/// A worker that runs no tasks hands out none, at once.
#[tokio::test]
async fn a_worker_without_tasks_hands_out_none() {
    let engine = Engine::default();
    let addr = engine.serve().await;
    let host = Host::start(
        HostConfig::new(format!("http://{addr}"), "orders"),
        Registrations::new(["checkout"], [] as [&str; 0], [] as [&str; 0]),
    )
    .await
    .unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(100), host.next_task())
            .await
            .unwrap()
            .is_none()
    );
    host.shutdown().await;
}

/// After shutdown, pulls end instead of waiting forever.
#[tokio::test]
async fn pulls_end_after_shutdown() {
    let engine = Engine::default();
    let host = task_worker(&engine).await;
    host.shutdown().await;
    assert!(
        tokio::time::timeout(Duration::from_secs(5), host.next_task())
            .await
            .expect("a pull after shutdown waited forever")
            .is_none()
    );
}
