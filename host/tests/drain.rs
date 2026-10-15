//! A stopping worker drains: the language finishes and reports what it holds
//! within the shutdown grace, what it did not finish is handed back where the
//! engine can take it, and shutdown ends in bounded time.

mod common;

use std::time::{Duration, Instant};

use common::{activation, task, Engine};
use orcher_sdk_core::proto::host::v1::{task_outcome, TaskOutcome};
use orcher_sdk_core::proto::kernel::v1::Payload;
use orcher_sdk_host::{Host, HostConfig, Registrations};

async fn worker(engine: &Engine, grace: Duration) -> Host {
    let addr = engine.serve().await;
    Host::start(
        HostConfig::new(format!("http://{addr}"), "orders").with_shutdown_grace(grace),
        Registrations::new(["checkout"], ["charge"], [] as [&str; 0]),
    )
    .await
    .unwrap()
}

/// A task the language finishes during the grace is reported, and shutdown
/// ends as soon as nothing is outstanding rather than waiting out the grace.
#[tokio::test]
async fn work_finished_within_the_grace_is_reported() {
    let engine = Engine::default();
    engine.script().tasks.push_back(task("charge_0", "token-1"));
    let host = worker(&engine, Duration::from_secs(5)).await;
    let assignment = host.next_task().await.expect("a task");
    let watching = host.clone();
    let token = assignment.token.clone();
    let cancelled = tokio::spawn(async move { watching.task_cancelled(&token).await });

    let asked = Instant::now();
    let stopping = host.clone();
    let shutdown = tokio::spawn(async move { stopping.shutdown().await });
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert!(!shutdown.is_finished(), "stopped with a task outstanding");
    assert!(
        !cancelled.is_finished(),
        "the task was asked to cancel before the grace ended"
    );

    host.complete_task(TaskOutcome {
        token: assignment.token,
        result: Some(task_outcome::Result::Value(Payload {
            data: b"{}".to_vec(),
            ..Default::default()
        })),
        ..Default::default()
    })
    .await
    .unwrap();
    tokio::time::timeout(Duration::from_secs(5), shutdown)
        .await
        .expect("shutdown in time")
        .unwrap();
    assert!(
        asked.elapsed() < Duration::from_secs(3),
        "waited out the grace"
    );
    assert_eq!(engine.script().completed.len(), 1, "the result was lost");
    assert!(
        !cancelled.await.unwrap(),
        "a reported task was asked to cancel"
    );
}

/// A workflow activation still unanswered when the grace ends is handed back
/// to the engine, for another worker to run at once, rather than left
/// claimed until it times out; it is not failed.
#[tokio::test]
async fn an_unanswered_activation_is_handed_back() {
    let engine = Engine::default();
    engine.script().activations.push_back(activation("act-1"));
    let host = worker(&engine, Duration::from_millis(300)).await;
    host.next_activation().await.expect("an activation");

    tokio::time::timeout(Duration::from_secs(10), host.shutdown())
        .await
        .expect("shutdown in time");
    let script = engine.script();
    assert_eq!(
        script.releases.len(),
        1,
        "the activation was not handed back"
    );
    assert!(script.workflow_failures.is_empty());
    assert!(script.workflow_completions.is_empty());
}

/// A task still running when the grace ends is asked to cancel, and
/// shutdown still ends in bounded time.
#[tokio::test]
async fn a_task_unfinished_at_the_grace_is_asked_to_cancel() {
    let engine = Engine::default();
    engine.script().tasks.push_back(task("charge_0", "token-1"));
    let host = worker(&engine, Duration::from_millis(300)).await;
    let assignment = host.next_task().await.expect("a task");
    let watching = host.clone();
    let cancelled = tokio::spawn(async move { watching.task_cancelled(&assignment.token).await });

    let asked = Instant::now();
    tokio::time::timeout(Duration::from_secs(15), host.shutdown())
        .await
        .expect("shutdown in time");
    let cancelled = tokio::time::timeout(Duration::from_secs(2), cancelled)
        .await
        .is_ok_and(|cancelled| cancelled.unwrap());
    assert!(cancelled, "the task was not asked to cancel");
    assert!(
        asked.elapsed() < Duration::from_secs(9),
        "shutdown took {:?}",
        asked.elapsed()
    );
    assert!(engine.script().failed.is_empty(), "the task was failed");
}

/// An idle worker stops at once: every driver is told together, and none
/// waits out the grace.
#[tokio::test]
async fn an_idle_worker_stops_at_once() {
    let engine = Engine::default();
    let host = worker(&engine, Duration::from_secs(5)).await;
    tokio::time::sleep(Duration::from_millis(100)).await;

    let asked = Instant::now();
    host.shutdown().await;
    assert!(
        asked.elapsed() < Duration::from_secs(2),
        "an idle worker took {:?}",
        asked.elapsed()
    );
    // A second call returns at once.
    let again = Instant::now();
    host.shutdown().await;
    assert!(again.elapsed() < Duration::from_millis(100));
}
