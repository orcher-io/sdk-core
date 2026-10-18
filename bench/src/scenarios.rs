//! The workloads, and what is measured while each runs.
//!
//! Every scenario gets a fresh worker process on a fresh task queue. Once the
//! worker is up, the Postgres statistics are reset and the proxy's and
//! worker's counters read, so registration is not counted; the workload then
//! runs to its end (or deadline), and everything is read again. The worker is
//! sampled a few times a second meanwhile, for the claimed-but-not-started
//! work and the peaks of memory and tasks.

use std::process::Stdio;
use std::sync::Arc;
use std::time::{Duration, Instant};

use orcher_sdk_core::client::WorkflowClient;
use orcher_sdk_core::Error;
use serde::{Deserialize, Serialize};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader, Lines};
use tokio::process::{Child, ChildStdin, ChildStdout};
use tokio::sync::Mutex;

use crate::pg::{Pg, PgStats};
use crate::program::{self, Program, TaskSpec};
use crate::proxy::{Proxy, ProxyStats, Upstream};
use crate::worker::{Snapshot, WorkerSetup, LINE_PREFIX};

/// How often the worker is sampled while a workload runs.
const SAMPLE_EVERY: Duration = Duration::from_millis(200);

/// A workload.
#[derive(Debug, Clone)]
pub struct Scenario {
    pub name: &'static str,
    pub about: &'static str,
    /// How long the worker sits idle, measured on its own, before any run.
    pub idle_secs: u64,
    /// Runs started together, each running `program`.
    pub runs: u64,
    pub program: Program,
    /// Events each run waits for. One is sent to it every `event_every_ms`
    /// until it ends: an event that arrives before the run waits for it is
    /// journaled but does not wake the run on engine 0.5.5
    /// (orcher-io/orcher#376), so sending exactly as many can leave a run
    /// waiting forever.
    pub events_per_run: u64,
    pub event_every_ms: u64,
    /// Tasks run on a second, task-only worker, which is the one measured;
    /// the workflow worker beside it is not.
    pub split_tasks: bool,
    pub task_concurrency: u32,
    pub deadline_secs: u64,
}

impl Scenario {
    fn new(name: &'static str, about: &'static str, program: Program) -> Self {
        Self {
            name,
            about,
            idle_secs: 0,
            runs: 1,
            program,
            events_per_run: 0,
            event_every_ms: 0,
            split_tasks: false,
            task_concurrency: 100,
            deadline_secs: 1200,
        }
    }

    fn runs(mut self, runs: u64) -> Self {
        self.runs = runs;
        self
    }

    fn deadline(mut self, secs: u64) -> Self {
        self.deadline_secs = secs;
        self
    }
}

fn noop() -> TaskSpec {
    TaskSpec::default()
}

/// Every workload, in the order the suite runs them.
pub fn all() -> Vec<Scenario> {
    let sequential = |name, n| {
        Scenario::new(
            name,
            "one run of n no-op tasks, one after another (the journal is read whole on every activation)",
            Program::Sequence { n, task: noop() },
        )
        .deadline(3600)
    };
    let payload = |name, bytes| {
        Scenario::new(
            name,
            "one run of 5 tasks in sequence, each returning a result of this size",
            Program::Sequence {
                n: 5,
                task: TaskSpec {
                    result_bytes: bytes,
                    ..noop()
                },
            },
        )
    };
    let heartbeat = |name, k: u32| Scenario {
        about:
            "k tasks running at once for 15 s each, heartbeat timeout 3 s, on a task-only worker",
        split_tasks: true,
        task_concurrency: k,
        ..Scenario::new(
            name,
            "",
            Program::FanOut {
                n: u64::from(k),
                task: TaskSpec {
                    sleep_ms: 15_000,
                    heartbeat_timeout_ms: 3_000,
                    ..noop()
                },
            },
        )
    };
    vec![
        Scenario {
            idle_secs: 10,
            runs: 0,
            ..Scenario::new(
                "idle",
                "a worker with workflows and tasks registered, and nothing to do, for 10 s",
                Program::Sequence { n: 0, task: noop() },
            )
        },
        sequential("sequential-10", 10),
        sequential("sequential-100", 100),
        sequential("sequential-300", 300),
        sequential("sequential-1000", 1000),
        payload("payload-1k", 1 << 10),
        payload("payload-10k", 10 << 10),
        payload("payload-100k", 100 << 10),
        payload("payload-1m", 1 << 20),
        // 1 s, not shorter: a timer shorter than the engine's delay in storing
        // it fails the completion on engine 0.5.5 (orcher-io/orcher#375).
        Scenario::new(
            "timers",
            "20 runs of 10 timers of 1 s in sequence",
            Program::Timers { n: 10, ms: 1000 },
        )
        .runs(20),
        Scenario {
            events_per_run: 20,
            event_every_ms: 50,
            ..Scenario::new(
                "events",
                "20 runs each waiting for 20 events in sequence; one is sent to each run every 50 ms until it ends",
                Program::Events { n: 20 },
            )
            .runs(20)
        },
        Scenario::new(
            "fanout-1000",
            "one run issuing 1,000 no-op tasks at once and waiting for all",
            Program::FanOut {
                n: 1000,
                task: noop(),
            },
        )
        .deadline(3600),
        Scenario::new(
            "fanout-10000",
            "one run issuing 10,000 no-op tasks at once and waiting for all",
            Program::FanOut {
                n: 10_000,
                task: noop(),
            },
        )
        .deadline(7200),
        Scenario::new(
            "concurrent-100x10",
            "100 runs at once, each of 10 no-op tasks in sequence",
            Program::Sequence {
                n: 10,
                task: noop(),
            },
        )
        .runs(100),
        Scenario {
            idle_secs: 10,
            split_tasks: true,
            ..Scenario::new(
                "footprint-task-worker",
                "a task-only worker: 10 s idle, then 1,000 tasks of 20 ms through 100 slots",
                Program::FanOut {
                    n: 1000,
                    task: TaskSpec {
                        sleep_ms: 20,
                        ..noop()
                    },
                },
            )
        },
        heartbeat("heartbeat-100", 100),
        heartbeat("heartbeat-1000", 1000),
    ]
}

/// What a scenario measured.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ScenarioResult {
    pub name: String,
    pub about: String,
    pub upstream: Option<Upstream>,
    /// Set when the scenario could not be run at all.
    pub error: Option<String>,
    pub runs_started: u64,
    pub runs_completed: u64,
    pub runs_failed: u64,
    /// Runs still unfinished at the deadline.
    pub runs_unfinished: u64,
    /// Events sent to the runs.
    #[serde(default)]
    pub events_sent: u64,
    pub success_rate: f64,
    /// Recorded for rates only: it is a laptop's wall clock.
    pub wall_ms: u64,
    pub idle: Option<Phase>,
    pub busy: Phase,
    /// The RPCs of the unmeasured workflow worker of a split scenario.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub helper_rpc: Option<ProxyStats>,
    pub claimed_waiting: ClaimedWaiting,
    pub pg: PgStats,
}

/// What the measured worker did and cost over one phase.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct Phase {
    pub secs: f64,
    pub rpc: ProxyStats,
    pub worker: WorkerCost,
}

/// The measured worker's cost over a phase.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct WorkerCost {
    pub activations: u64,
    pub runs_ended: u64,
    pub tasks: u64,
    pub heartbeats_recorded: u64,
    pub completion_errors: u64,
    pub cpu_us: u64,
    pub allocations: u64,
    pub allocated_bytes: u64,
    /// Live heap at the end of the phase.
    pub live_heap_bytes: u64,
    /// The highest live heap sampled during the phase.
    pub peak_live_heap_bytes: u64,
    pub max_rss_bytes: u64,
    /// Tokio tasks alive at the end of the phase, and the most sampled.
    pub tokio_alive_tasks: u64,
    pub peak_tokio_alive_tasks: u64,
}

/// Work the engine handed the worker that the language had not started,
/// sampled while the workload ran.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ClaimedWaiting {
    pub samples: u64,
    pub peak_activations: u64,
    pub peak_tasks: u64,
    pub mean_tasks: f64,
}

/// Where the suite runs.
pub struct Bench {
    /// The engine's gRPC address, `host:port`.
    pub engine: String,
    pub pg: Pg,
    pub upstream: Upstream,
}

impl Bench {
    /// Runs one scenario.
    pub async fn run(&self, scenario: &Scenario) -> ScenarioResult {
        let mut result = ScenarioResult {
            name: scenario.name.to_string(),
            about: scenario.about.to_string(),
            upstream: Some(self.upstream),
            ..Default::default()
        };
        if let Err(error) = self.measure(scenario, &mut result).await {
            result.error = Some(error);
        }
        result
    }

    async fn measure(
        &self,
        scenario: &Scenario,
        result: &mut ScenarioResult,
    ) -> Result<(), String> {
        let proxy = Proxy::start(self.engine.clone(), self.upstream)
            .await
            .map_err(|e| format!("the proxy did not start: {e}"))?;
        let queue = format!("bench-{}-{}", scenario.name, short_id());
        let task_queue = if scenario.split_tasks {
            format!("{queue}-tasks")
        } else {
            String::new()
        };
        let program = with_task_queue(&scenario.program, &task_queue);

        // The measured worker goes through the proxy; a split scenario's
        // workflow worker talks to the engine directly and is not measured.
        let measured = Worker::spawn(WorkerSetup {
            server_url: proxy.url(),
            task_queue: if scenario.split_tasks {
                task_queue.clone()
            } else {
                queue.clone()
            },
            workflows: !scenario.split_tasks,
            tasks: true,
            workflow_concurrency: 100,
            task_concurrency: scenario.task_concurrency,
            workflow_pollers: 4,
            task_pollers: 4,
        })
        .await?;
        // Its RPCs are counted on a proxy of its own, apart from the
        // measured worker's, to see what went wrong when a run does not end.
        let helper_proxy = Proxy::start(self.engine.clone(), self.upstream)
            .await
            .map_err(|e| format!("the proxy did not start: {e}"))?;
        let helper = if scenario.split_tasks {
            Some(
                Worker::spawn(WorkerSetup {
                    server_url: helper_proxy.url(),
                    task_queue: queue.clone(),
                    workflows: true,
                    tasks: false,
                    workflow_concurrency: 100,
                    task_concurrency: 1,
                    workflow_pollers: 4,
                    task_pollers: 1,
                })
                .await?,
            )
        } else {
            None
        };

        let helper_before = helper_proxy.stats();
        let outcome = self
            .workload(scenario, &program, &queue, &proxy, &measured, result)
            .await;
        if helper.is_some() {
            result.helper_rpc = Some(helper_proxy.stats().since(&helper_before));
        }
        measured.stop().await;
        if let Some(helper) = helper {
            helper.stop().await;
        }
        outcome
    }

    async fn workload(
        &self,
        scenario: &Scenario,
        program: &Program,
        queue: &str,
        proxy: &Proxy,
        worker: &Worker,
        result: &mut ScenarioResult,
    ) -> Result<(), String> {
        let client = WorkflowClient::connect(format!("http://{}", self.engine))
            .await
            .map_err(|e| format!("the client did not connect: {e}"))?
            .with_max_message_bytes(64 << 20);

        // Let registration and the first polls settle, then start counting.
        tokio::time::sleep(Duration::from_secs(1)).await;
        self.pg.reset().await?;

        if scenario.idle_secs > 0 {
            proxy.reset_peaks();
            let rpc = proxy.stats();
            let before = worker.snapshot().await?;
            let started = Instant::now();
            tokio::time::sleep(Duration::from_secs(scenario.idle_secs)).await;
            let after = worker.snapshot().await?;
            result.idle = Some(Phase {
                secs: started.elapsed().as_secs_f64(),
                rpc: proxy.stats().since(&rpc),
                worker: cost(&before, &after, &[after]),
            });
        }

        proxy.reset_peaks();
        let rpc = proxy.stats();
        let before = worker.snapshot().await?;
        let started = Instant::now();
        let deadline = tokio::time::Instant::now() + Duration::from_secs(scenario.deadline_secs);

        let input = serde_json::to_vec(program).map_err(|e| e.to_string())?;
        let mut runs = Vec::new();
        let events_sent = Arc::new(std::sync::atomic::AtomicU64::new(0));
        for i in 0..scenario.runs {
            let client = client.clone();
            let input = input.clone();
            let workflow_id = format!("{queue}-{i}");
            let queue = queue.to_string();
            let events = (scenario.events_per_run > 0)
                .then(|| (scenario.event_every_ms, events_sent.clone()));
            runs.push(tokio::spawn(async move {
                run_one(client, workflow_id, queue, input, events, deadline).await
            }));
        }
        result.runs_started = scenario.runs;

        let sampling = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let sampler = {
            let sampling = sampling.clone();
            let worker = worker.clone();
            let proxy = proxy.clone();
            let rpc = rpc.clone();
            tokio::spawn(async move {
                let mut samples = Vec::new();
                let mut waiting = ClaimedWaiting::default();
                let mut total_tasks = 0u64;
                while sampling.load(std::sync::atomic::Ordering::Relaxed) {
                    tokio::time::sleep(SAMPLE_EVERY).await;
                    let stats = proxy.stats().since(&rpc);
                    let Ok(snapshot) = worker.snapshot().await else {
                        break;
                    };
                    let activations = stats
                        .activations_claimed()
                        .saturating_sub(snapshot.activations - before.activations);
                    let tasks = stats
                        .tasks_claimed()
                        .saturating_sub(snapshot.tasks_started - before.tasks_started);
                    waiting.samples += 1;
                    waiting.peak_activations = waiting.peak_activations.max(activations);
                    waiting.peak_tasks = waiting.peak_tasks.max(tasks);
                    total_tasks += tasks;
                    samples.push(snapshot);
                }
                if waiting.samples > 0 {
                    waiting.mean_tasks = total_tasks as f64 / waiting.samples as f64;
                }
                (samples, waiting)
            })
        };

        for run in runs {
            match run.await {
                Ok(RunEnd::Completed) => result.runs_completed += 1,
                Ok(RunEnd::Failed) => result.runs_failed += 1,
                Ok(RunEnd::Unfinished) | Err(_) => result.runs_unfinished += 1,
            }
        }
        result.wall_ms = started.elapsed().as_millis() as u64;
        result.events_sent = events_sent.load(std::sync::atomic::Ordering::Relaxed);
        sampling.store(false, std::sync::atomic::Ordering::Relaxed);
        let (samples, waiting) = sampler.await.unwrap_or_default();
        let after = worker.snapshot().await?;
        result.busy = Phase {
            secs: started.elapsed().as_secs_f64(),
            rpc: proxy.stats().since(&rpc),
            worker: cost(&before, &after, &samples),
        };
        result.claimed_waiting = waiting;
        result.success_rate = if result.runs_started == 0 {
            1.0
        } else {
            result.runs_completed as f64 / result.runs_started as f64
        };
        result.pg = self.pg.read().await?;
        Ok(())
    }
}

enum RunEnd {
    Completed,
    Failed,
    Unfinished,
}

async fn run_one(
    client: WorkflowClient,
    workflow_id: String,
    queue: String,
    input: Vec<u8>,
    events: Option<(u64, Arc<std::sync::atomic::AtomicU64>)>,
    deadline: tokio::time::Instant,
) -> RunEnd {
    let handle = match client
        .start_workflow_raw(&workflow_id, program::WORKFLOW_TYPE, &queue, input)
        .await
    {
        Ok(handle) => handle,
        Err(_) => return RunEnd::Failed,
    };
    let run_id = handle.run_id().to_string();
    let sender = events.map(|(every_ms, sent)| {
        let client = client.clone();
        let workflow_id = workflow_id.clone();
        tokio::spawn(async move {
            for i in 0u64.. {
                tokio::time::sleep(Duration::from_millis(every_ms)).await;
                if client
                    .send_event(&workflow_id, None, program::EVENT_NAME, i)
                    .await
                    .is_ok()
                {
                    sent.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                }
            }
        })
    });
    let end = client
        .get_workflow_result_until(&workflow_id, &run_id, Some(deadline))
        .await;
    if let Some(sender) = sender {
        sender.abort();
    }
    match end {
        Ok(_) => RunEnd::Completed,
        Err(Error::Timeout { .. }) => RunEnd::Unfinished,
        Err(_) => RunEnd::Failed,
    }
}

fn cost(before: &Snapshot, after: &Snapshot, samples: &[Snapshot]) -> WorkerCost {
    WorkerCost {
        activations: after.activations - before.activations,
        runs_ended: after.runs_ended - before.runs_ended,
        tasks: after.tasks_completed - before.tasks_completed,
        heartbeats_recorded: after.heartbeats_recorded - before.heartbeats_recorded,
        completion_errors: after.completion_errors - before.completion_errors,
        cpu_us: after.process.cpu_us() - before.process.cpu_us(),
        allocations: after.process.allocations - before.process.allocations,
        allocated_bytes: after.process.allocated_bytes - before.process.allocated_bytes,
        live_heap_bytes: after.process.live_heap_bytes,
        peak_live_heap_bytes: samples
            .iter()
            .chain(std::iter::once(after))
            .map(|s| s.process.live_heap_bytes)
            .max()
            .unwrap_or(0),
        max_rss_bytes: after.process.max_rss_bytes,
        tokio_alive_tasks: after.process.tokio_alive_tasks,
        peak_tokio_alive_tasks: samples
            .iter()
            .chain(std::iter::once(after))
            .map(|s| s.process.tokio_alive_tasks)
            .max()
            .unwrap_or(0),
    }
}

fn with_task_queue(program: &Program, queue: &str) -> Program {
    let set = |task: &TaskSpec| TaskSpec {
        task_queue: queue.to_string(),
        ..task.clone()
    };
    match program {
        Program::Sequence { n, task } => Program::Sequence {
            n: *n,
            task: set(task),
        },
        Program::FanOut { n, task } => Program::FanOut {
            n: *n,
            task: set(task),
        },
        other => other.clone(),
    }
}

fn short_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()[..8].to_string()
}

/// A worker process, driven over its stdin and stdout.
#[derive(Clone)]
struct Worker {
    inner: Arc<Mutex<WorkerIo>>,
}

struct WorkerIo {
    child: Child,
    stdin: ChildStdin,
    lines: Lines<BufReader<ChildStdout>>,
}

impl Worker {
    async fn spawn(setup: WorkerSetup) -> Result<Self, String> {
        let exe = std::env::current_exe().map_err(|e| e.to_string())?;
        let mut child = tokio::process::Command::new(exe)
            .arg("worker")
            .arg(serde_json::to_string(&setup).map_err(|e| e.to_string())?)
            .stdin(Stdio::piped())
            .stdout(Stdio::piped())
            .stderr(Stdio::inherit())
            .kill_on_drop(true)
            .spawn()
            .map_err(|e| format!("the worker did not start: {e}"))?;
        let stdin = child.stdin.take().ok_or("no worker stdin")?;
        let stdout = child.stdout.take().ok_or("no worker stdout")?;
        let mut io = WorkerIo {
            child,
            stdin,
            lines: BufReader::new(stdout).lines(),
        };
        tokio::time::timeout(Duration::from_secs(30), io.expect("ready"))
            .await
            .map_err(|_| "the worker did not get ready in 30 s".to_string())??;
        Ok(Self {
            inner: Arc::new(Mutex::new(io)),
        })
    }

    async fn snapshot(&self) -> Result<Snapshot, String> {
        let mut io = self.inner.lock().await;
        io.send("snapshot").await?;
        io.expect("snapshot").await
    }

    async fn stop(&self) {
        let mut io = self.inner.lock().await;
        if io.send("stop").await.is_ok() {
            let _ = tokio::time::timeout(Duration::from_secs(30), io.expect("stopped")).await;
        }
        if tokio::time::timeout(Duration::from_secs(10), io.child.wait())
            .await
            .is_err()
        {
            let _ = io.child.kill().await;
        }
    }
}

impl WorkerIo {
    async fn send(&mut self, line: &str) -> Result<(), String> {
        self.stdin
            .write_all(format!("{line}\n").as_bytes())
            .await
            .map_err(|e| format!("the worker went away: {e}"))?;
        self.stdin.flush().await.map_err(|e| e.to_string())
    }

    async fn expect(&mut self, kind: &str) -> Result<Snapshot, String> {
        let prefix = format!("{LINE_PREFIX}{kind} ");
        loop {
            let line = self
                .lines
                .next_line()
                .await
                .map_err(|e| e.to_string())?
                .ok_or_else(|| format!("the worker exited before saying {kind}"))?;
            if let Some(json) = line.strip_prefix(&prefix) {
                return serde_json::from_str(json).map_err(|e| e.to_string());
            }
        }
    }
}
