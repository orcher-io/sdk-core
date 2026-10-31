//! The worker under test: a separate process running this checkout's host,
//! with the workloads' code as its "language".
//!
//! It runs in its own process so that its CPU time, allocations and memory
//! are the worker's alone, not the load generator's or the proxy's. The
//! suite drives it over stdin and stdout: `snapshot` prints a [`Snapshot`],
//! `stop` shuts the host down, prints a last one and exits.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use orcher_proto::host::v1::{task_outcome, TaskAssignment, TaskHeartbeat, TaskOutcome};
use orcher_proto::kernel::v1::RunActivation;
use orcher_proto::prost::Message;
use orcher_sdk_core::{EncodedHost, Host, HostConfig, Registrations};
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, BufReader};

use crate::program::{self, TaskSpec};
use crate::stats::ProcessStats;

/// How the worker is set up.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct WorkerSetup {
    pub server_url: String,
    pub task_queue: String,
    /// Registers the workflow type.
    pub workflows: bool,
    /// Registers the task type.
    pub tasks: bool,
    pub workflow_concurrency: u32,
    pub task_concurrency: u32,
    pub workflow_pollers: usize,
    pub task_pollers: usize,
}

/// What the worker has done, and what it has cost so far.
#[derive(Debug, Clone, Copy, Default, Serialize, Deserialize)]
pub struct Snapshot {
    pub process: ProcessStats,
    /// Activations the language pulled.
    pub activations: u64,
    /// Activations answered with the run's end.
    pub runs_ended: u64,
    /// Of those, runs failed.
    pub runs_failed: u64,
    /// Answers the host refused.
    pub completion_errors: u64,
    /// Tasks the language pulled.
    pub tasks_started: u64,
    /// Tasks whose outcome was reported.
    pub tasks_completed: u64,
    /// Heartbeats the tasks' own code recorded.
    pub heartbeats_recorded: u64,
}

#[derive(Default)]
struct Counters {
    activations: AtomicU64,
    runs_ended: AtomicU64,
    runs_failed: AtomicU64,
    completion_errors: AtomicU64,
    tasks_started: AtomicU64,
    tasks_completed: AtomicU64,
    heartbeats_recorded: AtomicU64,
}

impl Counters {
    fn snapshot(&self) -> Snapshot {
        let get = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        Snapshot {
            process: ProcessStats::now(),
            activations: get(&self.activations),
            runs_ended: get(&self.runs_ended),
            runs_failed: get(&self.runs_failed),
            completion_errors: get(&self.completion_errors),
            tasks_started: get(&self.tasks_started),
            tasks_completed: get(&self.tasks_completed),
            heartbeats_recorded: get(&self.heartbeats_recorded),
        }
    }
}

/// The prefix of every line the worker prints for the suite.
pub const LINE_PREFIX: &str = "BENCH ";

/// Runs the worker until told to stop.
pub async fn run(setup: WorkerSetup) -> Result<(), String> {
    let config = HostConfig::new(&setup.server_url, &setup.task_queue)
        .with_concurrency(setup.workflow_concurrency, setup.task_concurrency, 1)
        .with_pollers(setup.workflow_pollers, setup.task_pollers, 1)
        .with_shutdown_grace(Duration::from_secs(2))
        .with_sdk("orcher-bench", env!("CARGO_PKG_VERSION"));
    let registrations = Registrations::new(
        setup.workflows.then_some(program::WORKFLOW_TYPE),
        setup.tasks.then_some(program::TASK_TYPE),
        Vec::<String>::new(),
    );
    let host = Host::start(config, registrations)
        .await
        .map_err(|error| format!("the host did not start: {error}"))?;
    let counters = Arc::new(Counters::default());

    let mut language = Vec::new();
    if setup.workflows {
        for _ in 0..setup.workflow_concurrency.max(1) {
            language.push(tokio::spawn(run_workflows(host.clone(), counters.clone())));
        }
    }
    if setup.tasks {
        for _ in 0..setup.task_concurrency.max(1) {
            language.push(tokio::spawn(run_tasks(host.clone(), counters.clone())));
        }
    }
    print_line("ready", &counters.snapshot());

    let mut lines = BufReader::new(tokio::io::stdin()).lines();
    while let Ok(Some(line)) = lines.next_line().await {
        match line.trim() {
            "snapshot" => print_line("snapshot", &counters.snapshot()),
            "stop" => break,
            _ => {}
        }
    }
    // Stopped by the suite, or the suite went away: either way, stop.
    let before_shutdown = counters.snapshot();
    host.shutdown().await;
    for pull in language {
        pull.abort();
    }
    print_line("stopped", &before_shutdown);
    Ok(())
}

fn print_line(kind: &str, snapshot: &Snapshot) {
    println!(
        "{LINE_PREFIX}{kind} {}",
        serde_json::to_string(snapshot).expect("a snapshot encodes")
    );
}

// The worker pulls its work as encoded messages and decodes them with its own
// generated code, as a language SDK does.

async fn run_workflows(host: Host, counters: Arc<Counters>) {
    let host = host.encoded();
    while let Some(bytes) = host.next_activation().await {
        counters.activations.fetch_add(1, Ordering::Relaxed);
        let activation =
            RunActivation::decode(bytes.as_slice()).expect("the host sends a RunActivation");
        let answer = program::answer(&activation);
        if answer.ended {
            counters.runs_ended.fetch_add(1, Ordering::Relaxed);
        }
        if answer.failed {
            counters.runs_failed.fetch_add(1, Ordering::Relaxed);
        }
        if host
            .complete_activation(&answer.result.encode_to_vec())
            .await
            .is_err()
        {
            counters.completion_errors.fetch_add(1, Ordering::Relaxed);
        }
    }
}

async fn run_tasks(host: Host, counters: Arc<Counters>) {
    let host = host.encoded();
    while let Some(bytes) = host.next_task().await {
        counters.tasks_started.fetch_add(1, Ordering::Relaxed);
        let task =
            TaskAssignment::decode(bytes.as_slice()).expect("the host sends a TaskAssignment");
        let outcome = run_task(&host, &counters, &task).await;
        if host.complete_task(&outcome.encode_to_vec()).await.is_ok() {
            counters.tasks_completed.fetch_add(1, Ordering::Relaxed);
        } else {
            counters.completion_errors.fetch_add(1, Ordering::Relaxed);
        }
    }
}

async fn run_task(host: &EncodedHost, counters: &Counters, task: &TaskAssignment) -> TaskOutcome {
    let spec: TaskSpec = task
        .input
        .first()
        .and_then(|payload| serde_json::from_slice(&payload.data).ok())
        .unwrap_or_default();
    let run_for = Duration::from_millis(spec.sleep_ms);
    if spec.heartbeat_every_ms > 0 && !run_for.is_zero() {
        let every = Duration::from_millis(spec.heartbeat_every_ms);
        let until = tokio::time::Instant::now() + run_for;
        while tokio::time::Instant::now() < until {
            tokio::time::sleep(every.min(until - tokio::time::Instant::now())).await;
            let heartbeat = TaskHeartbeat {
                token: task.token.clone(),
                ..Default::default()
            };
            let _ = host.heartbeat(&heartbeat.encode_to_vec());
            counters.heartbeats_recorded.fetch_add(1, Ordering::Relaxed);
        }
    } else if !run_for.is_zero() {
        tokio::time::sleep(run_for).await;
    }
    TaskOutcome {
        token: task.token.clone(),
        result: Some(task_outcome::Result::Value(program::json(
            program::task_result(spec.result_bytes),
        ))),
        ..Default::default()
    }
}
