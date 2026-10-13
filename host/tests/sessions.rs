//! Sessions, run by the host against a fake engine: the built-in session
//! tasks never reach the language, and a session's queue is polled from its
//! creation to its end.

mod common;

use std::time::{Duration, Instant};

use common::{task, Engine};
use orcher_sdk_core::proto::orcher::v1::PollTaskExecutionResponse;
use orcher_sdk_host::{Host, HostConfig, Registrations};

async fn session_worker(engine: &Engine, max_sessions: usize) -> Host {
    let addr = engine.serve().await;
    let host = Host::start(
        HostConfig::new(format!("http://{addr}"), "orders")
            .with_shutdown_grace(std::time::Duration::from_millis(300))
            .with_max_sessions(max_sessions),
        Registrations::new([] as [&str; 0], ["charge"], [] as [&str; 0]),
    )
    .await
    .unwrap();
    // The language pulls tasks all along, as a worker does: the host runs a
    // session task when it reaches the front.
    let puller = host.clone();
    tokio::spawn(async move { while puller.next_task().await.is_some() {} });
    host
}

async fn eventually(what: &str, mut condition: impl FnMut() -> bool) {
    for _ in 0..500 {
        if condition() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    panic!("{what} did not happen in time");
}

fn session_task(task_type: &str, token: &str, input: &str) -> PollTaskExecutionResponse {
    PollTaskExecutionResponse {
        task_id: format!("{task_type}_0"),
        task_type: task_type.into(),
        input: input.as_bytes().to_vec(),
        ..task(task_type, token)
    }
}

fn create(
    token: &str,
    session_id: &str,
    creation_ms: u64,
    execution_ms: u64,
) -> PollTaskExecutionResponse {
    session_task(
        "__orcher_create_session",
        token,
        &format!(
            r#"{{"session_id":"{session_id}","creation_timeout_ms":{creation_ms},"execution_timeout_ms":{execution_ms},"max_concurrent_tasks":1,"heartbeat_interval_ms":5000}}"#
        ),
    )
}

fn complete(token: &str, session_id: &str) -> PollTaskExecutionResponse {
    session_task(
        "__orcher_complete_session",
        token,
        &format!(r#"{{"session_id":"{session_id}"}}"#),
    )
}

/// The creation task's result, once the engine has it.
async fn opened(engine: &Engine, token: &str) -> serde_json::Value {
    eventually("the session creation's result", || {
        engine
            .script()
            .completed
            .iter()
            .any(|done| done.task_token == token.as_bytes())
    })
    .await;
    let script = engine.script();
    let done = script
        .completed
        .iter()
        .find(|done| done.task_token == token.as_bytes())
        .unwrap();
    serde_json::from_slice(&done.result).unwrap()
}

fn session_polls(engine: &Engine) -> usize {
    engine
        .script()
        .polled_queues
        .iter()
        .filter(|queue| queue.contains("__session__"))
        .count()
}

/// Whether the session queue is still polled, over a short wait.
async fn session_queue_polled(engine: &Engine) -> bool {
    let before = session_polls(engine);
    tokio::time::sleep(Duration::from_millis(400)).await;
    session_polls(engine) > before
}

/// A session's creation is answered by the host with its queue, in the shape
/// every SDK's workflow code reads; the queue is polled and its tasks reach
/// the language; the completion ends the polling. Neither built-in task
/// reaches the language.
#[tokio::test]
async fn a_session_is_opened_used_and_completed_by_the_host() {
    let engine = Engine::default();
    engine
        .script()
        .tasks
        .push_back(create("create-1", "session_0", 30_000, 600_000));
    let addr = engine.serve().await;
    let host = Host::start(
        HostConfig::new(format!("http://{addr}"), "orders")
            .with_shutdown_grace(std::time::Duration::from_millis(300)),
        Registrations::new([] as [&str; 0], ["charge"], [] as [&str; 0]),
    )
    .await
    .unwrap();
    let language = host.clone();
    let first = tokio::spawn(async move { language.next_task().await });

    let info = opened(&engine, "create-1").await;
    let queue = info["session_queue"].as_str().unwrap().to_string();
    assert_eq!(info["session_id"], "session_0");
    assert!(queue.starts_with("orders__session__"), "{queue}");
    assert_eq!(info["worker_identity"], queue["orders__session__".len()..]);
    assert_eq!(info["state"], "Open");

    // The session's task reaches the language from the session queue.
    engine
        .script()
        .session_tasks
        .push_back(PollTaskExecutionResponse {
            task_queue: queue.clone(),
            ..task("charge_1", "on-session")
        });
    let handed = tokio::time::timeout(Duration::from_secs(5), first)
        .await
        .expect("the session's task in time")
        .unwrap()
        .expect("a task");
    assert_eq!(
        handed.token, b"on-session",
        "a built-in task reached the language"
    );
    assert_eq!(handed.run.unwrap().task_queue, queue);

    engine
        .script()
        .tasks
        .push_back(complete("complete-1", "session_0"));
    let puller = host.clone();
    tokio::spawn(async move { while puller.next_task().await.is_some() {} });
    assert_eq!(opened(&engine, "complete-1").await, serde_json::json!({}));
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert!(
        !session_queue_polled(&engine).await,
        "the session queue was still polled after its completion"
    );
    host.shutdown().await;
}

/// A creation that finds every slot taken waits for one, then fails with a
/// type the workflow can tell apart, a little before its creation timeout.
#[tokio::test]
async fn a_creation_with_no_free_slot_fails_before_its_timeout() {
    let engine = Engine::default();
    {
        let mut script = engine.script();
        script
            .tasks
            .push_back(create("create-1", "a", 30_000, 600_000));
        script
            .tasks
            .push_back(create("create-2", "b", 900, 600_000));
    }
    let started = Instant::now();
    let host = session_worker(&engine, 1).await;

    eventually("the second creation's failure", || {
        !engine.script().failed.is_empty()
    })
    .await;
    let waited = started.elapsed();
    let failed = engine.script().failed[0].clone();
    assert_eq!(failed.task_token, b"create-2");
    let failure = failed.failure.unwrap();
    assert_eq!(failure.failure_type, "ResourceExhausted");
    assert!(
        failure.message.contains("1 of 1 in use"),
        "{}",
        failure.message
    );
    assert!(
        waited >= Duration::from_millis(400),
        "gave up at once: {waited:?}"
    );
    host.shutdown().await;
}

/// A creation waiting for a slot takes the one a completed session frees.
#[tokio::test]
async fn a_creation_takes_the_slot_a_completed_session_frees() {
    let engine = Engine::default();
    {
        let mut script = engine.script();
        script
            .tasks
            .push_back(create("create-1", "a", 30_000, 600_000));
        script
            .tasks
            .push_back(create("create-2", "b", 10_000, 600_000));
    }
    let host = session_worker(&engine, 1).await;
    opened(&engine, "create-1").await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        engine.script().completed.len(),
        1,
        "the second took a taken slot"
    );

    engine.script().tasks.push_back(complete("complete-1", "a"));
    assert_eq!(opened(&engine, "create-2").await["session_id"], "b");
    assert!(engine.script().failed.is_empty());
    host.shutdown().await;
}

/// A session past its execution timeout is closed: its queue stops being
/// polled and its slot is freed.
#[tokio::test]
async fn a_session_past_its_execution_timeout_is_closed() {
    let engine = Engine::default();
    engine
        .script()
        .tasks
        .push_back(create("create-1", "a", 30_000, 300));
    let host = session_worker(&engine, 1).await;
    opened(&engine, "create-1").await;

    tokio::time::sleep(Duration::from_millis(600)).await;
    assert!(
        !session_queue_polled(&engine).await,
        "the session queue was still polled past the session's execution timeout"
    );
    // Its slot is free: a new session opens at once.
    engine
        .script()
        .tasks
        .push_back(create("create-2", "b", 30_000, 600_000));
    assert_eq!(opened(&engine, "create-2").await["session_id"], "b");
    host.shutdown().await;
}

/// Completing a session this worker does not hold (a retried completion)
/// succeeds.
#[tokio::test]
async fn completing_an_unknown_session_succeeds() {
    let engine = Engine::default();
    engine
        .script()
        .tasks
        .push_back(complete("complete-1", "never-opened"));
    let host = session_worker(&engine, 1).await;
    assert_eq!(opened(&engine, "complete-1").await, serde_json::json!({}));
    host.shutdown().await;
}
