//! The workflow driver: polls for workflow activations, checks them against
//! replay, hands them to the language SDK and reports the commands it returns.

use crate::worker::metrics::WorkerMetrics as SharedWorkerMetrics;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

use crate::error::{Error, Result};
use crate::proto::orcher::v1::{self as proto};
use crate::protocol::{
    validate_execution_result, Command as BridgeCommand, ExecutionErrorType, ExecutionResult,
    QueryResponse, QueryResult as BridgeQueryResult, UpdateResponse,
    UpdateResult as BridgeUpdateResult, WorkflowRetryPolicy,
};
use crate::state::replayer::{JournalSteps, ReplayConfig, Replayer, NON_DETERMINISM_FAILURE_TYPE};
use crate::transport::channel::ChannelManager;
use crate::types::WorkflowExecution;
use crate::worker::governor::{Credits, InFlight, SlotBudget, SlotPermit};
use crate::worker::lifecycle::Leftover;
use crate::worker::poll::completion::{
    activation_token, Caller, Completer, CompletionRetryConfig, EagerTasks, Report,
};
use crate::worker::poll::polling::{PollerConfig, WorkflowExecutionPoller, WorkflowExecutionTask};

use super::task::EagerTaskInjector;
use super::{announce_shutdown, ShutdownHandle};

/// Configuration for the workflow driver.
#[derive(Clone)]
#[non_exhaustive]
pub struct WorkflowDriverConfig {
    /// Server address.
    pub server_url: String,

    /// Namespace.
    pub namespace: String,

    /// Task queue to poll.
    pub task_queue: String,

    /// Maximum number of concurrent workflow executions.
    pub max_concurrent_executions: usize,

    /// Identity of this driver.
    pub identity: String,

    /// Poll timeout.
    pub poll_timeout: Duration,

    /// Whether to enable strict determinism checking.
    pub strict_determinism: bool,

    /// Number of concurrent pollers.
    pub poller_count: usize,

    /// Organization ID for multi-tenancy (optional).
    ///
    /// When set, the driver sends it to the server in the `x-organization-id`
    /// header, which enables organization-level quotas and billing attribution.
    pub organization_id: Option<String>,

    /// API key for server authentication (optional).
    ///
    /// When set, all gRPC requests include an `authorization: Bearer <key>` header.
    pub api_key: Option<String>,

    /// TLS configuration for secure connections (optional).
    pub tls_config: Option<crate::transport::TlsConfig>,

    /// The code release this worker is running (optional).
    ///
    /// Sent on every poll. The server records it on a workflow execution the
    /// first time this worker claims it, so the execution keeps running against
    /// the code it started on. The value is opaque (a git SHA, an image digest,
    /// a release tag); the server never parses or orders it.
    ///
    /// Defaults from `ORCHER_VERSION_ID`, since the value normally comes from
    /// the build rather than being written by hand. Unset declares nothing.
    pub version_id: Option<String>,

    /// The largest gRPC message the driver sends or receives, and tells the
    /// engine it can receive. [`crate::transport::limits::default_max_message_bytes`]
    /// unless set: `ORCHER_MAX_MESSAGE_BYTES`, or 32 MiB, the engine's
    /// default. A result too large to send fails its task or workflow, saying
    /// so, rather than being sent and refused.
    pub max_message_bytes: usize,

    /// How long an activation found non-deterministic is held before it is
    /// handed back to the engine to be tried again. Each further violation
    /// of the same run doubles it, up to
    /// [`non_determinism_retry_max`](Self::non_determinism_retry_max).
    /// Default: two seconds.
    ///
    /// Such an activation is not applied and the run is not failed: it stays
    /// open, its journal unchanged, until code that replays the journal
    /// picks it up. A worker shutting down hands back what it holds at once.
    pub non_determinism_retry: Duration,

    /// The longest an activation found non-deterministic is held before it
    /// is tried again; see
    /// [`non_determinism_retry`](Self::non_determinism_retry). Default: one
    /// minute.
    pub non_determinism_retry_max: Duration,

    /// The worker protocol version reported on every workflow completion and
    /// failure; see [`crate::worker_protocol`]. Default:
    /// [`crate::WORKER_PROTOCOL_VERSION`].
    ///
    /// The engine reads it on the activation it answers, so it must match
    /// what the language SDK really does with every activation it is given.
    pub protocol_version: u32,
    /// Where this driver's calls to the engine are recorded: the worker's
    /// wire measurements, shared with its other drivers. A driver has
    /// measurements of its own unless given these.
    pub wire: std::sync::Arc<crate::observability::WireMetrics>,
}

// Written by hand so the secret is never printed; destructured so that a new
// field must be added here to compile.
impl std::fmt::Debug for WorkflowDriverConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            server_url,
            namespace,
            task_queue,
            max_concurrent_executions,
            identity,
            poll_timeout,
            strict_determinism,
            poller_count,
            organization_id,
            api_key,
            tls_config,
            version_id,
            max_message_bytes,
            non_determinism_retry,
            non_determinism_retry_max,
            protocol_version,
            wire: _,
        } = self;
        f.debug_struct("WorkflowDriverConfig")
            .field("server_url", server_url)
            .field("namespace", namespace)
            .field("task_queue", task_queue)
            .field("max_concurrent_executions", max_concurrent_executions)
            .field("identity", identity)
            .field("poll_timeout", poll_timeout)
            .field("strict_determinism", strict_determinism)
            .field("poller_count", poller_count)
            .field("organization_id", organization_id)
            .field("api_key", &crate::transport::credentials::redacted(api_key))
            .field("tls_config", tls_config)
            .field("version_id", version_id)
            .field("max_message_bytes", max_message_bytes)
            .field("non_determinism_retry", non_determinism_retry)
            .field("non_determinism_retry_max", non_determinism_retry_max)
            .field("protocol_version", protocol_version)
            .finish()
    }
}

impl Default for WorkflowDriverConfig {
    fn default() -> Self {
        Self {
            server_url: "http://localhost:50051".to_string(),
            namespace: "default".to_string(),
            task_queue: "default".to_string(),
            max_concurrent_executions: 100,
            identity: format!("workflow-driver-{}", uuid::Uuid::new_v4()),
            poll_timeout: Duration::from_secs(60),
            strict_determinism: false,
            poller_count: 4,
            organization_id: None,
            api_key: None,
            tls_config: None,
            version_id: None,
            max_message_bytes: crate::transport::limits::default_max_message_bytes(),
            non_determinism_retry: Duration::from_secs(2),
            non_determinism_retry_max: Duration::from_secs(60),
            protocol_version: crate::WORKER_PROTOCOL_VERSION,
            wire: crate::observability::WireMetrics::new(),
        }
    }
}

/// A workflow activation for the language SDK to run.
#[derive(Debug)]
pub struct WorkflowWork {
    /// The polled task.
    pub task: WorkflowExecutionTask,

    /// The activation token the engine returned on the poll.
    ///
    /// Hand it back unchanged in [`WorkflowWorkResult::stream_entry_id`].
    pub stream_entry_id: Option<String>,
}

/// The result of a workflow activation, sent back by the language SDK.
#[derive(Debug)]
pub struct WorkflowWorkResult {
    /// Workflow ID.
    pub workflow_id: String,

    /// Run ID (the execution ID).
    pub run_id: String,

    /// The task token from the polled task.
    ///
    /// Sent as the activation token when `stream_entry_id` is not set.
    pub task_token: Vec<u8>,

    /// The activation token from the work, handed back unchanged.
    pub stream_entry_id: Option<String>,

    /// The execution result: commands or an error.
    pub result: Result<ExecutionResult>,
}

/// An activation to hand back, named by the token its poll carried.
/// A workflow retry policy as the engine reads it. A delay left unset stays
/// unset, so the engine applies its own workflow-retry default.
fn workflow_retry_policy(policy: &WorkflowRetryPolicy) -> proto::RetryPolicy {
    let duration = |d: Duration| crate::proto::prost_types::Duration {
        seconds: d.as_secs() as i64,
        nanos: d.subsec_nanos() as i32,
    };
    proto::RetryPolicy {
        initial_interval: policy.initial_interval.map(duration),
        backoff_coefficient: policy.backoff_coefficient,
        maximum_interval: policy.max_interval.map(duration),
        maximum_attempts: i32::try_from(policy.max_attempts).unwrap_or(i32::MAX),
        non_retryable_error_types: policy.non_retryable_errors.clone(),
        ..Default::default()
    }
}

fn leftover(task: WorkflowExecutionTask) -> Leftover {
    Leftover {
        token: activation_token(task.task_token, task.stream_entry_id.as_deref()),
        workflow_id: task.execution.workflow_id,
        run_id: task.execution.run_id,
        retried: false,
    }
}

/// An activation handed to the language SDK, as its result is checked.
struct Recorded {
    workflow_type: String,
    /// The steps its journal recorded, when commands are verified.
    steps: Option<JournalSteps>,
}

/// How long to hold the `attempt`th activation in a row of one run found
/// non-deterministic: `initial`, doubled for each attempt after the first,
/// at most `max`.
fn non_determinism_backoff(initial: Duration, max: Duration, attempt: u32) -> Duration {
    let doublings = attempt.saturating_sub(1).min(16);
    initial.saturating_mul(1 << doublings).min(max)
}

/// A pause, for a log line: whole seconds, or milliseconds under one.
fn humanize(pause: Duration) -> String {
    if pause >= Duration::from_secs(1) {
        format!("{}s", pause.as_secs())
    } else {
        format!("{}ms", pause.as_millis())
    }
}

/// A workflow driver: polls for workflow activations and delegates running
/// them to the language SDK.
///
/// sdk-core handles polling and gRPC; the language SDK runs the handlers.
pub struct WorkflowDriver {
    config: Arc<WorkflowDriverConfig>,

    channel_manager: Arc<ChannelManager>,

    replay_config: ReplayConfig,

    /// Each activation handed to the language SDK, by run, until its result
    /// comes back: what its journal recorded, to check the result against.
    journal_steps: std::sync::Mutex<HashMap<String, Recorded>>,

    /// How many activations of each run in a row were found
    /// non-deterministic, which sets how long the next is held before it is
    /// tried again. Forgotten once one is not.
    non_deterministic: std::sync::Mutex<HashMap<String, u32>>,

    /// Shutdown signal receiver.
    shutdown: tokio::sync::watch::Receiver<bool>,

    /// Shutdown signal sender, kept for initializing the pollers.
    pub(crate) shutdown_sender: tokio::sync::watch::Sender<bool>,

    /// Receives activations from the pollers.
    task_receiver: mpsc::Receiver<WorkflowExecutionTask>,

    /// Sends work to the language SDK.
    work_sender: mpsc::Sender<WorkflowWork>,

    /// Receives results from the language SDK.
    result_receiver: mpsc::Receiver<WorkflowWorkResult>,

    /// Where to inject eagerly returned tasks, straight into the task driver's
    /// work pipeline. Set with `with_eager_task_injector` after construction.
    eager_task_injector: Option<EagerTaskInjector>,
    /// Shared worker counters, when the language SDK provided them. The
    /// heartbeat reports what these say this worker is running.
    metrics: Option<Arc<SharedWorkerMetrics>>,
    /// How hard to try to deliver each completion and failure report.
    completion_retry: CompletionRetryConfig,
    /// The activations this driver may have claimed, shared with its pollers.
    credits: Arc<Credits>,
}

impl WorkflowDriver {
    /// Create a workflow driver.
    ///
    /// Returns the driver and the channels the language SDK uses to talk to it:
    /// - `work_receiver`: the language SDK receives work from this channel.
    /// - `result_sender`: the language SDK sends results to this channel.
    pub async fn new(
        config: WorkflowDriverConfig,
    ) -> Result<(
        Self,
        mpsc::Receiver<WorkflowWork>,
        mpsc::Sender<WorkflowWorkResult>,
    )> {
        let config = Arc::new(config);

        let mut channel_manager = match config.tls_config {
            Some(ref tls) => ChannelManager::with_tls(config.server_url.clone(), tls.clone()),
            None => ChannelManager::new(config.server_url.clone()),
        }
        .with_wire_metrics(std::sync::Arc::clone(&config.wire))
        .with_max_message_bytes(config.max_message_bytes);
        let _channel = channel_manager
            .get()
            .await
            .map_err(|e| Error::connection(format!("Failed to create channel: {}", e)))?;

        // Channels to and from the language SDK.
        let (work_sender, work_receiver) = mpsc::channel(config.max_concurrent_executions);
        let (result_sender, result_receiver) = mpsc::channel(config.max_concurrent_executions);

        // Channel from the pollers.
        let (task_sender, task_receiver) = mpsc::channel(config.max_concurrent_executions);
        let (shutdown_sender, shutdown) = tokio::sync::watch::channel(false);

        let poller_count = config.poller_count.max(1);
        // One scaler for all of them: an idle driver holds one poll, a busy
        // one up to `poller_count`.
        let scaler =
            std::sync::Arc::new(crate::worker::poll::scaler::PollerScaler::new(poller_count));
        // One credit per activation the driver can run: its pollers claim no
        // more than that.
        let credits = Arc::new(Credits::new(config.max_concurrent_executions));
        tracing::info!(
            poller_count = poller_count,
            namespace = %config.namespace,
            task_queue = %config.task_queue,
            "Starting workflow driver pollers"
        );

        for poller_idx in 0..poller_count {
            let poller_config = PollerConfig {
                namespace: config.namespace.clone(),
                task_queue: config.task_queue.clone(),
                poll_timeout_seconds: config.poll_timeout.as_secs() as i64,
                max_concurrent_polls: config.max_concurrent_executions / poller_count,
                identity: format!("{}-wf-driver-poller-{}", config.identity, poller_idx),
                organization_id: config.organization_id.clone(),
                api_key: config.api_key.clone(),
                version_id: config.version_id.clone(),
                auto_heartbeat: false,
                tls_config: config.tls_config.clone(),
                max_message_bytes: config.max_message_bytes,
                wire: std::sync::Arc::clone(&config.wire),
            };

            let poller = WorkflowExecutionPoller::new(
                poller_config,
                &config.server_url,
                shutdown_sender.clone(),
                task_sender.clone(),
            )
            .await?
            .with_scaler(std::sync::Arc::clone(&scaler), poller_idx)
            .with_credits(Arc::clone(&credits));

            let poller_id = poller_idx;
            tokio::spawn(async move {
                let mut poller_mut = poller;
                if let Err(e) = poller_mut.poll_loop().await {
                    tracing::error!(poller_id = poller_id, error = %e, "Workflow driver poller error");
                }
            });
        }

        let replay_config = ReplayConfig {
            strict_mode: config.strict_determinism,
            ..ReplayConfig::default()
        };

        let driver = Self {
            config,
            channel_manager: Arc::new(channel_manager),
            replay_config,
            journal_steps: Default::default(),
            non_deterministic: Default::default(),
            shutdown,
            shutdown_sender,
            task_receiver,
            work_sender,
            result_receiver,
            eager_task_injector: None,
            metrics: None,
            completion_retry: CompletionRetryConfig::default(),
            credits,
        };

        Ok((driver, work_receiver, result_sender))
    }

    /// Forward eagerly returned tasks straight to a task driver's work channel.
    ///
    /// The server may return tasks eagerly in answer to a
    /// `complete_workflow_execution` call. With an injector set, they skip a
    /// poll round-trip and go directly to the `TaskDriver`'s work channel.
    ///
    /// Pass [`TaskDriver::eager_task_injector`](super::TaskDriver::eager_task_injector): the tasks are then heartbeated
    /// like the ones the task driver polls, and the engine is told so. A bare
    /// work sender is accepted too, but nothing heartbeats its tasks.
    pub fn with_eager_task_injector(mut self, injector: impl Into<EagerTaskInjector>) -> Self {
        self.eager_task_injector = Some(injector.into());
        self
    }

    /// Share the worker's counters, so the heartbeat can report what this
    /// driver is running.
    pub fn with_metrics(mut self, metrics: Arc<SharedWorkerMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Set how hard to retry reporting the outcome of each activation.
    ///
    /// The default keeps trying for up to thirty seconds; see
    /// [`CompletionRetryConfig`].
    pub fn with_completion_retry(mut self, retry: CompletionRetryConfig) -> Self {
        self.completion_retry = retry;
        self
    }

    /// Return a handle that stops this driver's [`run`](Self::run) from elsewhere.
    ///
    /// Take it before handing the driver to the task that runs it: `run`
    /// borrows the driver for as long as it runs, so [`shutdown`](Self::shutdown)
    /// cannot be called on it then. Stopping through the handle lets `run`
    /// send the results already handed back and give completions still being
    /// retried the shutdown grace; dropping the running future instead
    /// abandons them.
    pub fn shutdown_handle(&self) -> ShutdownHandle {
        ShutdownHandle(self.shutdown_sender.clone())
    }

    /// What sends this driver's completions and failure reports.
    fn completer(&self) -> Completer {
        Completer {
            caller: Arc::new(Caller::from(&*self.config)),
            channel_manager: Arc::clone(&self.channel_manager),
            retry: self.completion_retry.clone(),
            shutdown: self.shutdown.clone(),
            eager_task_injector: self.eager_task_injector.clone().map(|injector| EagerTasks {
                sender: injector.sender,
                heartbeats: injector.heartbeats,
                default_queue: self.config.task_queue.clone(),
            }),
            heartbeats: None,
        }
    }

    /// Run the driver until it stops.
    ///
    /// For each activation, the driver:
    /// 1. Receives it from a poller.
    /// 2. Validates its journal by replaying it.
    /// 3. Sends it to the language SDK.
    /// 4. Receives the result from the language SDK.
    /// 5. Reports the result to the server over gRPC.
    ///
    /// Returns once shut down through [`shutdown_handle`](Self::shutdown_handle),
    /// or once the language SDK has dropped its result sender and no more
    /// work can reach it (it has also dropped its work receiver, or the
    /// pollers have stopped).
    pub async fn run(&mut self) -> Result<()> {
        tracing::info!(
            namespace = %self.config.namespace,
            task_queue = %self.config.task_queue,
            identity = %self.config.identity,
            "Starting workflow driver"
        );

        let slots = SlotBudget::for_reports(
            self.completion_retry.max_in_flight,
            self.config.max_concurrent_executions,
        );
        // Each report is sent from its own task. Sent inline, one completion
        // being retried against an engine that is restarting would hold up
        // every other completion and the hand-off of new work behind it.
        let mut in_flight = InFlight::new("Workflow driver", "workflow completions");
        let completer = self.completer();
        let metrics = self.metrics.clone();
        let wire = Arc::clone(&self.config.wire);
        let credits = Arc::clone(&self.credits);
        let send = |report: Report, succeeded: bool| {
            // The activation's credit is given back once its report has been
            // sent, or given up on: until then it is still this driver's
            // work. One held to be tried again later is not run meanwhile,
            // so its credit goes back at once.
            let credit = credits
                .give_back(report.token())
                .filter(|_| !report.waits());
            let completer = completer.clone();
            let metrics = metrics.clone();
            let wire = Arc::clone(&wire);
            async move {
                // An activation released or held to be retried is handed back, not
                // reported.
                let reported = match &report {
                    Report::Complete { commands, .. } => Some((commands.len(), false)),
                    Report::Fail { .. } => Some((0, true)),
                    Report::Release(_) | Report::Retry { .. } => None,
                };
                completer.send(report).await;
                if let Some((commands, failed)) = reported {
                    wire.record_workflow_report(commands, failed);
                }
                // Counted when the outcome has reached the engine, or been
                // given up on: until then the activation is still this
                // worker's work.
                if let Some(metrics) = metrics {
                    metrics.workflow_finished(succeeded);
                }
                drop(credit);
            }
        };
        // A slot for the next report, taken before anything that may produce
        // one is accepted. While every slot is held by a completion still
        // being sent, neither results nor new work are taken, so back-pressure
        // reaches the language SDK and the pollers, which wait.
        let mut slot: Option<SlotPermit> = None;
        // Room in the work channel, reserved before a polled task is taken.
        // Nothing in this loop may wait on a send: a full work channel would
        // stop it reading results, and a language SDK that frees room for
        // new work only once its results are read would then wait on it for
        // good.
        let mut room: Option<mpsc::OwnedPermit<WorkflowWork>> = None;
        let (mut tasks_closed, mut results_closed, mut work_closed) = (false, false, false);
        // The drain deadline, set once shutdown is asked for. From then on the
        // driver drains rather than stopping at once. Stopping at once would
        // drop whatever a poll in flight brought back (the engine has already
        // claimed it, so it would wait out the claim timeout) and every result
        // for work already handed over.
        let mut draining: Option<tokio::time::Instant> = None;
        // Activations handed to the language SDK whose results have not come
        // back. This is an upper bound, not an exact count: work left in the
        // SDK's channels when it stops, or dropped by it without an answer,
        // never comes back. So shutdown waits on it only while the SDK is still
        // taking work (and so still running what it was given), and only
        // within the grace.
        let mut outstanding: usize = 0;
        // Whether the pollers have been told to stop because the SDK stopped
        // taking work.
        let mut stranded = false;

        loop {
            tokio::select! {
                _ = self.shutdown.changed(), if draining.is_none() => {
                    if *self.shutdown.borrow_and_update() {
                        tracing::info!(
                            outstanding,
                            grace_ms = self.completion_retry.shutdown_grace.as_millis() as u64,
                            "Workflow driver shutdown requested; handing over activations \
                             already polled and waiting for results"
                        );
                        draining = Some(
                            tokio::time::Instant::now() + self.completion_retry.shutdown_grace,
                        );
                        announce_shutdown(
                            &mut in_flight,
                            &completer,
                            vec![self.config.task_queue.clone()],
                        );
                    }
                }

                _ = tokio::time::sleep_until(draining.unwrap_or_else(tokio::time::Instant::now)),
                    if draining.is_some() =>
                {
                    tracing::warn!(
                        outstanding,
                        pollers_stopped = tasks_closed,
                        "Shutdown grace over; stopping the workflow driver"
                    );
                    break;
                }

                permit = slots.acquire(), if slot.is_none() => {
                    slot = Some(permit);
                }

                reserved = self.work_sender.clone().reserve_owned(),
                    if room.is_none() && !work_closed =>
                {
                    match reserved {
                        Ok(permit) => room = Some(permit),
                        Err(_) => work_closed = true,
                    }
                }

                // Watched separately: once room is reserved, the reservation
                // above is no longer waiting and would never notice a close.
                _ = self.work_sender.closed(), if !work_closed => {
                    work_closed = true;
                }

                // Receive activations from the pollers and hand them to the
                // language SDK.
                task = self.task_receiver.recv(),
                    if slot.is_some() && room.is_some() && !tasks_closed =>
                {
                    match (task, room.take()) {
                        (Some(task), Some(room)) => {
                            match self.handle_polled_task(task, room) {
                                Some(report) => in_flight.spawn(send(report, false), slot.take()),
                                // Not counted if the language SDK dropped its
                                // work receiver meanwhile: no result will come.
                                None if !self.work_sender.is_closed() => outstanding += 1,
                                None => {}
                            }
                        }
                        _ => tasks_closed = true,
                    }
                }

                // Receive results from the language SDK and report them.
                result = self.result_receiver.recv(), if slot.is_some() && !results_closed => {
                    match result {
                        Some(result) => {
                            tracing::debug!(
                                workflow_id = %result.workflow_id,
                                run_id = %result.run_id,
                                "Received workflow result from language SDK"
                            );
                            outstanding = outstanding.saturating_sub(1);
                            let (report, succeeded) = self.handle_execution_result(result);
                            // An activation held to be tried again later does
                            // not keep new work waiting meanwhile.
                            let slot = slot.take().filter(|_| !report.waits());
                            in_flight.spawn(send(report, succeeded), slot);
                        }
                        None => results_closed = true,
                    }
                }

                // Reap finished reports so the set does not grow without bound.
                Some(finished) = in_flight.join_next(), if !in_flight.is_empty() => {
                    in_flight.reaped(finished);
                }
            }

            if work_closed && !stranded {
                stranded = true;
                tracing::info!("Language SDK stopped taking workflow work");
                room = None;
                // Nothing a poll brings back after this point can run here,
                // so the pollers stop, abandoning any poll in flight.
                self.strand_polled(&mut in_flight, &completer);
            }

            if results_closed && (tasks_closed || work_closed) {
                tracing::info!("Language SDK and pollers gone, stopping the workflow driver");
                // Stop the pollers too. Left running, they would keep claiming
                // activations that nothing will ever run, each held until the
                // engine's claim timeout.
                let _ = self.shutdown_sender.send(true);
                break;
            }

            // Drained: no activation can arrive to be handed over (every
            // poller has stopped, or the language SDK takes no more work), and
            // none handed over can still be answered. Once the SDK has stopped
            // taking work, its results are not waited for: a binding that stops
            // its loops before shutting the driver down has already waited for
            // what it was running, and `outstanding` may count work it never
            // took.
            if draining.is_some()
                && (tasks_closed || work_closed)
                && (outstanding == 0 || results_closed || work_closed)
            {
                tracing::info!("Workflow driver drained");
                break;
            }
        }

        self.strand_polled(&mut in_flight, &completer);

        // Results the language SDK has already handed back are sent rather
        // than dropped with the channel: each is a finished activation, and if
        // dropped it would sit claimed until the engine's claim timeout.
        while let Ok(result) = self.result_receiver.try_recv() {
            let (report, succeeded) = self.handle_execution_result(result);
            in_flight.spawn(send(report, succeeded), None);
        }
        // One grace covers the whole shutdown, draining included, so `run`
        // returns within one grace of being asked to stop, not two.
        let stop_by = draining
            .unwrap_or_else(|| tokio::time::Instant::now() + self.completion_retry.shutdown_grace);
        in_flight.finish(stop_by).await;

        tracing::info!("Workflow driver stopped");
        Ok(())
    }

    /// Stop taking activations from the pollers.
    ///
    /// Closing the channel tells any poller still waiting on a poll to
    /// abandon it, since nothing it brought back could be handed over. An
    /// activation already received but not handed over is claimed by this
    /// worker, so it is handed back for the next poll to take at once, rather
    /// than after the claim times out. Each release is sent like a report,
    /// and so gets what is left of the shutdown grace.
    fn strand_polled(&mut self, in_flight: &mut InFlight, completer: &Completer) {
        self.task_receiver.close();
        while let Ok(task) = self.task_receiver.try_recv() {
            tracing::warn!(
                workflow_id = %task.execution.workflow_id,
                run_id = %task.execution.run_id,
                "Workflow activation received but never handed over; handing it back to the \
                 engine"
            );
            let completer = completer.clone();
            let release = Report::Release(leftover(task));
            drop(self.credits.give_back(release.token()));
            in_flight.spawn(async move { completer.send(release).await }, None);
        }
    }

    /// Validate a polled activation and hand it to the language SDK.
    ///
    /// Returns the report to send when the activation cannot be handed over
    /// at all; the activation counts as finished, unsuccessfully, once that
    /// report is sent.
    ///
    /// `room` is the space in the work channel reserved for it, so handing
    /// it over never waits.
    fn handle_polled_task(
        &self,
        task: WorkflowExecutionTask,
        room: mpsc::OwnedPermit<WorkflowWork>,
    ) -> Option<Report> {
        // Counted here rather than at the poll: this is where the activation
        // is handed to the language SDK, and it is in progress until its
        // result comes back.
        if let Some(metrics) = &self.metrics {
            metrics.workflow_started();
        }
        self.config.wire.record_activation(task.journal.len());
        let workflow_id = task.execution.workflow_id.clone();
        let run_id = task.execution.run_id.clone();
        let workflow_type = task.workflow_type.clone();
        let stream_entry_id = task.stream_entry_id.clone();

        tracing::debug!(
            workflow_id = %workflow_id,
            run_id = %run_id,
            workflow_type = %workflow_type,
            "Driver received workflow task"
        );

        // Replay the journal to catch determinism violations before the
        // language SDK runs anything.
        let execution = WorkflowExecution {
            workflow_id: workflow_id.clone(),
            run_id: run_id.clone(),
        };
        let token = activation_token(task.task_token.clone(), stream_entry_id.as_deref());

        let replay_result = {
            let mut replayer = Replayer::new(self.replay_config.clone());
            match replayer.replay(execution, task.journal.clone()) {
                Ok(result) => result,
                Err(e) => {
                    tracing::error!(
                        workflow_id = %workflow_id,
                        error = %e,
                        "Replay validation failed"
                    );
                    return Some(Report::Fail {
                        workflow_id,
                        run_id,
                        token,
                        message: format!("Replay validation failed: {}", e),
                        failure_type: "WorkflowExecutionError".to_string(),
                        non_retryable: false,
                    });
                }
            }
        };

        if !replay_result.success || replay_result.has_critical_violations() {
            tracing::error!(
                workflow_id = %workflow_id,
                violations = replay_result.violations.len(),
                "Replay failed with determinism violations"
            );
            return Some(Report::Fail {
                workflow_id,
                run_id,
                token,
                message: format!(
                    "Workflow determinism violation: {} violation(s) detected",
                    replay_result.violations.len()
                ),
                failure_type: "WorkflowExecutionError".to_string(),
                non_retryable: false,
            });
        }

        // The language SDK dropped its work receiver after the room was
        // reserved. The activation is claimed by this worker, so it is handed
        // back rather than left to the claim timeout. This is checked before
        // sending, because what is sent into a closed channel cannot be
        // recovered.
        if self.work_sender.is_closed() {
            tracing::warn!(
                workflow_id = %workflow_id,
                run_id = %run_id,
                "Language SDK stopped taking work; workflow activation not handed over, \
                 handing it back to the engine"
            );
            return Some(Report::Release(leftover(task)));
        }

        // What the journal recorded, to check the commands that come back
        // against. The engine claims a run for one activation at a time, so
        // the run names it; one never answered is replaced by the run's next.
        self.journal_steps
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .insert(
                run_id.clone(),
                Recorded {
                    workflow_type: workflow_type.clone(),
                    steps: self
                        .replay_config
                        .verify_commands
                        .then(|| JournalSteps::from_journal(&task.journal)),
                },
            );

        let work = WorkflowWork {
            task,
            stream_entry_id,
        };

        if room.send(work).is_closed() {
            self.journal_steps
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .remove(&run_id);
            // The receiver was dropped between the check above and this send.
            // The activation is lost with the channel, so it stays claimed
            // until the engine's claim timeout hands it out again.
            tracing::error!(
                workflow_id = %workflow_id,
                run_id = %run_id,
                "Language SDK stopped taking work; workflow activation not handed over, it \
                 stays claimed until the engine's claim timeout"
            );
        }
        None
    }

    /// Turn a result from the language SDK into the report to send, and
    /// whether the activation succeeded.
    fn handle_execution_result(&self, result: WorkflowWorkResult) -> (Report, bool) {
        let ran = result.result.is_ok();
        let report = self.report_for(result);
        let succeeded = ran && !report.waits();
        (report, succeeded)
    }

    fn report_for(&self, result: WorkflowWorkResult) -> Report {
        let workflow_id = result.workflow_id;
        let run_id = result.run_id;
        let stream_entry_id = result.stream_entry_id.unwrap_or_default();
        let token = activation_token(result.task_token, Some(&stream_entry_id));
        let recorded = self
            .journal_steps
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&run_id);
        let failed = |message: String, non_retryable: bool| Report::Fail {
            workflow_id: workflow_id.clone(),
            run_id: run_id.clone(),
            token: token.clone(),
            message,
            failure_type: "WorkflowExecutionError".to_string(),
            non_retryable,
        };
        // A failure of this worker's, not one the workflow code raised.
        let fail = |message: String| failed(message, false);
        // Running the same code against the same journal would do the same
        // again, and failing the run would end it for good, though a deploy
        // of the code that recorded it could carry it on. So the activation
        // is not applied: it is handed back to be tried again, later.
        let non_deterministic = |found: String| {
            let workflow_type = recorded
                .as_ref()
                .map(|r| r.workflow_type.as_str())
                .unwrap_or("unknown");
            self.retry_non_deterministic(
                Leftover {
                    workflow_id: workflow_id.clone(),
                    run_id: run_id.clone(),
                    token: token.clone(),
                    retried: true,
                },
                workflow_type,
                found,
            )
        };

        let execution_result = match result.result {
            Ok(execution_result) => execution_result,
            Err(e @ Error::DeterminismViolation { .. }) => return non_deterministic(e.to_string()),
            // The language could not process the activation: it is not the
            // run's failure, so the activation is handed back at once, for
            // this or another worker to take again.
            Err(Error::Protocol(reason)) => {
                tracing::error!(
                    workflow_id = %workflow_id,
                    run_id = %run_id,
                    %reason,
                    "The language SDK could not process the activation; handing it back"
                );
                return Report::Release(Leftover {
                    workflow_id: workflow_id.clone(),
                    run_id: run_id.clone(),
                    token: token.clone(),
                    retried: false,
                });
            }
            Err(e) => {
                self.forget_non_deterministic(&run_id);
                return fail(e.to_string());
            }
        };

        if let Err(e) = validate_execution_result(&execution_result) {
            tracing::error!(
                workflow_id = %workflow_id,
                error = %e,
                "Execution result validation failed"
            );
            self.forget_non_deterministic(&run_id);
            return fail(format!("Invalid execution result: {}", e));
        }

        // The result must not contradict what the journal recorded. Sent
        // anyway, the engine would match a reused step id to the step it
        // already has and serve its result as the new one's, or carry on a
        // run whose recorded steps the code has left behind.
        if let Some(steps) = recorded.as_ref().and_then(|r| r.steps.as_ref()) {
            let violations = steps.check_activation(&execution_result);
            if !violations.is_empty() {
                for violation in &violations {
                    tracing::debug!(
                        workflow_id = %workflow_id,
                        run_id = %run_id,
                        step_id = violation.step_id.as_deref().unwrap_or_default(),
                        entry_id = violation.event_id,
                        "{violation}"
                    );
                }
                return non_deterministic(
                    violations
                        .iter()
                        .map(|violation| {
                            let found = violation.to_string();
                            found
                                .strip_prefix("Non-deterministic workflow: ")
                                .map(str::to_string)
                                .unwrap_or(found)
                        })
                        .collect::<Vec<_>>()
                        .join("; "),
                );
            }
        }

        if !execution_result.successful {
            let error_msg = execution_result
                .error
                .as_ref()
                .map(|e| e.message.clone())
                .unwrap_or_else(|| "Unknown error".to_string());
            if let Some(ExecutionErrorType::NonDeterminism) =
                execution_result.error.as_ref().map(|e| e.error_type)
            {
                return non_deterministic(error_msg);
            }
            self.forget_non_deterministic(&run_id);
            // Whether the workflow code raised it as non-retryable, as the
            // language reported it. The engine honours the mark only from a
            // worker whose protocol version says the language means it.
            let non_retryable = execution_result
                .error
                .as_ref()
                .is_some_and(|error| !error.retryable);
            return failed(error_msg, non_retryable);
        }
        self.forget_non_deterministic(&run_id);

        let query_results =
            self.convert_query_responses_to_proto(&execution_result.query_responses);
        let update_results =
            self.convert_update_responses_to_proto(&execution_result.update_results);

        match self.convert_commands_to_proto(execution_result.commands, &self.config.task_queue) {
            Ok(commands) => Report::Complete {
                workflow_id,
                run_id,
                token,
                stream_entry_id,
                commands,
                query_results,
                update_results,
            },
            Err(e) => fail(format!("Command conversion failed: {}", e)),
        }
    }

    /// The report for an activation found non-deterministic: hand it back to
    /// the engine after a pause that grows with each violation of the run in
    /// a row, and log, once per activation, what was found and what to do.
    fn retry_non_deterministic(
        &self,
        leftover: Leftover,
        workflow_type: &str,
        found: String,
    ) -> Report {
        let attempt = {
            let mut runs = self
                .non_deterministic
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner());
            // Bounded: runs another worker carried on are never forgotten here.
            if runs.len() >= 10_000 && !runs.contains_key(&leftover.run_id) {
                runs.clear();
            }
            let attempt = runs.entry(leftover.run_id.clone()).or_insert(0);
            *attempt = attempt.saturating_add(1);
            *attempt
        };
        let after = non_determinism_backoff(
            self.config.non_determinism_retry,
            self.config.non_determinism_retry_max,
            attempt,
        );
        tracing::error!(
            workflow_type = %workflow_type,
            workflow_id = %leftover.workflow_id,
            run_id = %leftover.run_id,
            attempt,
            retry_in_ms = after.as_millis() as u64,
            failure_type = NON_DETERMINISM_FAILURE_TYPE,
            "Workflow {workflow_type:?} (id {:?}, run {:?}) is not deterministic: {found}. The \
             workflow code no longer replays this run's journal: it changed incompatibly while \
             the run was in flight, or it is not deterministic. This activation is not \
             applied and the run stays open; it is tried again in {}. To carry the run on, \
             deploy the code that recorded it (or code that replays its journal the same way), \
             and let in-flight runs finish before deploying an incompatible change.",
            leftover.workflow_id,
            leftover.run_id,
            humanize(after),
        );
        Report::Retry { leftover, after }
    }

    /// The run's activation was not found non-deterministic: the next one
    /// that is starts the pause over.
    fn forget_non_deterministic(&self, run_id: &str) {
        self.non_deterministic
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(run_id);
    }

    /// Convert bridge commands to proto commands.
    fn convert_commands_to_proto(
        &self,
        commands: Vec<BridgeCommand>,
        default_task_queue: &str,
    ) -> Result<Vec<crate::proto::orcher::v1::Command>> {
        use crate::proto::orcher::v1::{
            command::Attributes, CancelChildWorkflowCommandAttributes,
            CancelTimerCommandAttributes, CancelWorkflowCommandAttributes, Command as ProtoCommand,
            CommandType, CompleteWorkflowCommandAttributes, FailWorkflowCommandAttributes,
            OrphanPolicy as ProtoOrphanPolicy, RecordStepResultCommandAttributes,
            RestartFreshCommandAttributes, RetryPolicy, ScheduleTaskCommandAttributes,
            SendEventCommandAttributes, StartChildWorkflowCommandAttributes,
            StartTimerCommandAttributes,
        };

        let mut proto_commands = Vec::new();

        for command in commands {
            let proto_command = match command {
                BridgeCommand::RecordStepResult(cmd) => ProtoCommand {
                    command_type: CommandType::RecordStepResult as i32,
                    attributes: Some(Attributes::RecordStepResult(
                        RecordStepResultCommandAttributes {
                            step_name: cmd.step_name,
                            step_type: cmd.step_type,
                            result: cmd.result,
                            failure: cmd.failure.map(|f| crate::proto::orcher::v1::Failure {
                                message: f.message,
                                source: f.source,
                                stack_trace: f.stack_trace,
                                cause: None,
                                failure_type: f.failure_type,
                                details: vec![],
                                non_retryable: false,
                                ..Default::default()
                            }),
                            execution_attempt: cmd.execution_attempt,
                            ..Default::default()
                        },
                    )),
                    ..Default::default()
                },

                BridgeCommand::ScheduleTask(cmd) => {
                    let retry_policy = cmd.retry_policy.as_ref().map(|rp| RetryPolicy {
                        initial_interval: Some(prost_types::Duration {
                            seconds: rp.initial_interval.as_secs() as i64,
                            nanos: (rp.initial_interval.as_nanos() % 1_000_000_000) as i32,
                        }),
                        backoff_coefficient: rp.backoff_coefficient,
                        maximum_interval: Some(prost_types::Duration {
                            seconds: rp.max_interval.as_secs() as i64,
                            nanos: (rp.max_interval.as_nanos() % 1_000_000_000) as i32,
                        }),
                        maximum_attempts: rp.max_attempts as i32,
                        non_retryable_error_types: rp.non_retryable_errors.clone(),
                        ..Default::default()
                    });

                    let headers = cmd
                        .headers
                        .iter()
                        .map(|(k, v)| (k.clone(), v.data.clone()))
                        .collect();

                    ProtoCommand {
                        command_type: CommandType::ScheduleTask as i32,
                        attributes: Some(Attributes::ScheduleTask(ScheduleTaskCommandAttributes {
                            task_id: cmd.task_id,
                            task_type: cmd.task_type,
                            task_queue: if cmd.task_queue.is_empty() {
                                default_task_queue.to_string()
                            } else {
                                cmd.task_queue
                            },
                            input: {
                                let mut all_data = Vec::new();
                                for payload in &cmd.input {
                                    all_data.extend(&payload.data);
                                }
                                all_data
                            },
                            // The queue-wait limit is a separate option. Absent, the task
                            // waits in the queue as long as it needs to. Inheriting the
                            // run limit here would double the effective total timeout.
                            schedule_to_start_timeout: cmd.queue_timeout.map(|q| {
                                prost_types::Duration {
                                    seconds: q.as_secs() as i64,
                                    nanos: (q.as_nanos() % 1_000_000_000) as i32,
                                }
                            }),
                            start_to_close_timeout: Some(prost_types::Duration {
                                seconds: cmd.timeout.as_secs() as i64,
                                nanos: (cmd.timeout.as_nanos() % 1_000_000_000) as i32,
                            }),
                            heartbeat_timeout: cmd.heartbeat_timeout.map(|hb| {
                                prost_types::Duration {
                                    seconds: hb.as_secs() as i64,
                                    nanos: (hb.as_nanos() % 1_000_000_000) as i32,
                                }
                            }),
                            retry_policy,
                            headers,
                            ..Default::default()
                        })),
                        ..Default::default()
                    }
                }

                BridgeCommand::StartTimer(cmd) => ProtoCommand {
                    command_type: CommandType::StartTimer as i32,
                    attributes: Some(Attributes::StartTimer(StartTimerCommandAttributes {
                        timer_id: cmd.timer_id,
                        start_to_fire_timeout: Some(prost_types::Duration {
                            seconds: cmd.duration.as_secs() as i64,
                            nanos: (cmd.duration.as_nanos() % 1_000_000_000) as i32,
                        }),
                        ..Default::default()
                    })),
                    ..Default::default()
                },

                BridgeCommand::CancelTimer(cmd) => ProtoCommand {
                    command_type: CommandType::CancelTimer as i32,
                    attributes: Some(Attributes::CancelTimer(CancelTimerCommandAttributes {
                        timer_id: cmd.timer_id,
                        ..Default::default()
                    })),
                    ..Default::default()
                },

                BridgeCommand::CompleteWorkflow(cmd) => ProtoCommand {
                    command_type: CommandType::CompleteWorkflow as i32,
                    attributes: Some(Attributes::CompleteWorkflow(
                        CompleteWorkflowCommandAttributes {
                            result: cmd.result.data,
                            ..Default::default()
                        },
                    )),
                    ..Default::default()
                },

                BridgeCommand::FailWorkflow(cmd) => ProtoCommand {
                    command_type: CommandType::FailWorkflow as i32,
                    attributes: Some(Attributes::FailWorkflow(FailWorkflowCommandAttributes {
                        failure: Some(crate::proto::orcher::v1::Failure {
                            message: cmd.message,
                            source: "WorkflowHandler".to_string(),
                            stack_trace: String::new(),
                            cause: None,
                            failure_type: cmd.error_type,
                            details: cmd.details.map(|d| d.data).unwrap_or_default(),
                            non_retryable: false,
                            ..Default::default()
                        }),
                        ..Default::default()
                    })),
                    ..Default::default()
                },

                BridgeCommand::CancelWorkflowExecution(cmd) => ProtoCommand {
                    command_type: CommandType::CancelWorkflow as i32,
                    attributes: Some(Attributes::CancelWorkflow(
                        CancelWorkflowCommandAttributes {
                            details: cmd.details.map(|d| d.data).unwrap_or_default(),
                            ..Default::default()
                        },
                    )),
                    ..Default::default()
                },

                BridgeCommand::RestartFresh(cmd) => ProtoCommand {
                    command_type: CommandType::RestartFresh as i32,
                    attributes: Some(Attributes::RestartFresh(RestartFreshCommandAttributes {
                        workflow_type: cmd.workflow_type,
                        task_queue: cmd
                            .task_queue
                            .unwrap_or_else(|| default_task_queue.to_string()),
                        input: {
                            let mut all_data = Vec::new();
                            for payload in &cmd.input {
                                all_data.extend(&payload.data);
                            }
                            all_data
                        },
                        execution_timeout: cmd.timeout.map(|t| prost_types::Duration {
                            seconds: t.as_secs() as i64,
                            nanos: (t.as_nanos() % 1_000_000_000) as i32,
                        }),
                        run_timeout: None,
                        task_timeout: None,
                        retry_policy: cmd.retry_policy.as_ref().map(workflow_retry_policy),
                        annotations: Default::default(),
                        labels: Default::default(),
                        ..Default::default()
                    })),
                    ..Default::default()
                },

                // WaitForEvent is a client-side parking hint with no proto command.
                // The engine keeps the workflow claimed while it waits; delivering
                // the event releases the claim and the workflow is dispatched again.
                BridgeCommand::WaitForEvent(_) => continue,

                BridgeCommand::SendEvent(cmd) => {
                    let payload: Vec<u8> = cmd.payload.data;
                    ProtoCommand {
                        command_type: CommandType::SendEvent as i32,
                        attributes: Some(Attributes::SendEvent(SendEventCommandAttributes {
                            workflow_id: cmd.workflow_id,
                            run_id: cmd.run_id.unwrap_or_default(),
                            event_name: cmd.event_name,
                            payload,
                            headers: Default::default(),
                            ..Default::default()
                        })),
                        ..Default::default()
                    }
                }

                BridgeCommand::StartChildWorkflow(cmd) => {
                    let orphan_policy = match cmd.orphan_policy {
                        crate::protocol::OrphanPolicy::Terminate => ProtoOrphanPolicy::Terminate,
                        crate::protocol::OrphanPolicy::Cancel => ProtoOrphanPolicy::Cancel,
                        crate::protocol::OrphanPolicy::Abandon => ProtoOrphanPolicy::Abandon,
                    };

                    ProtoCommand {
                        command_type: CommandType::StartChildWorkflow as i32,
                        attributes: Some(Attributes::StartChildWorkflow(
                            StartChildWorkflowCommandAttributes {
                                workflow_id: cmd.workflow_id,
                                workflow_type: cmd.workflow_type,
                                task_queue: if cmd.task_queue.is_empty() {
                                    default_task_queue.to_string()
                                } else {
                                    cmd.task_queue
                                },
                                input: {
                                    let mut all_data = Vec::new();
                                    for payload in &cmd.input {
                                        all_data.extend(&payload.data);
                                    }
                                    all_data
                                },
                                execution_timeout: cmd.timeout.map(|t| prost_types::Duration {
                                    seconds: t.as_secs() as i64,
                                    nanos: (t.as_nanos() % 1_000_000_000) as i32,
                                }),
                                run_timeout: None,
                                task_timeout: None,
                                orphan_policy: orphan_policy as i32,
                                retry_policy: cmd.retry_policy.as_ref().map(workflow_retry_policy),
                                // A child runs in its parent worker's
                                // namespace. Left empty, the server would file
                                // the child under a namespace named "default",
                                // where no worker in this namespace could ever
                                // claim it.
                                namespace: self.config.namespace.clone(),
                                annotations: Default::default(),
                                labels: Default::default(),
                                ..Default::default()
                            },
                        )),
                        ..Default::default()
                    }
                }

                BridgeCommand::CancelChildWorkflow(cmd) => ProtoCommand {
                    command_type: CommandType::CancelChildWorkflow as i32,
                    attributes: Some(Attributes::CancelChildWorkflow(
                        CancelChildWorkflowCommandAttributes {
                            workflow_id: cmd.workflow_id,
                            run_id: cmd.run_id,
                            ..Default::default()
                        },
                    )),
                    ..Default::default()
                },

                other => {
                    tracing::warn!(
                        command_type = other.command_type(),
                        "Unsupported command type in conversion"
                    );
                    return Err(Error::internal(format!(
                        "Unsupported command type: {}",
                        other.command_type()
                    )));
                }
            };

            proto_commands.push(proto_command);
        }

        Ok(proto_commands)
    }

    /// Convert bridge query responses to proto `QueryResult`s.
    fn convert_query_responses_to_proto(
        &self,
        responses: &[QueryResponse],
    ) -> Vec<proto::QueryResult> {
        responses
            .iter()
            .map(|resp| {
                let (result_type, result) = match &resp.result {
                    BridgeQueryResult::Success { output } => (
                        proto::QueryResultType::Answered as i32,
                        Some(proto::query_result::Result::Answer(output.data.clone())),
                    ),
                    BridgeQueryResult::Failed { message, .. } => (
                        proto::QueryResultType::Failed as i32,
                        Some(proto::query_result::Result::ErrorMessage(message.clone())),
                    ),
                };

                proto::QueryResult {
                    query_id: resp.query_id.clone(),
                    result_type,
                    result,
                    ..Default::default()
                }
            })
            .collect()
    }

    /// Convert bridge update responses to proto `UpdateResult`s.
    fn convert_update_responses_to_proto(
        &self,
        responses: &[UpdateResponse],
    ) -> Vec<proto::UpdateResult> {
        responses
            .iter()
            .map(|resp| {
                let (result_type, result) = match &resp.result {
                    BridgeUpdateResult::Completed { output } => (
                        proto::UpdateResultType::Completed as i32,
                        Some(proto::update_result::Result::Answer(output.data.clone())),
                    ),
                    BridgeUpdateResult::Rejected { message } => (
                        proto::UpdateResultType::Rejected as i32,
                        Some(proto::update_result::Result::ErrorMessage(message.clone())),
                    ),
                    BridgeUpdateResult::Failed { message, .. } => (
                        proto::UpdateResultType::Failed as i32,
                        Some(proto::update_result::Result::ErrorMessage(message.clone())),
                    ),
                };

                proto::UpdateResult {
                    update_id: resp.update_id.clone(),
                    result_type,
                    result,
                    ..Default::default()
                }
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A driver connected to a bare HTTP/2 server that is sent nothing.
    async fn idle_driver() -> WorkflowDriver {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                tokio::spawn(async move {
                    if let Ok(mut connection) = h2::server::handshake(socket).await {
                        while connection.accept().await.is_some() {}
                    }
                });
            }
        });
        let (driver, _work, _results) = WorkflowDriver::new(WorkflowDriverConfig {
            server_url: format!("http://{addr}"),
            ..Default::default()
        })
        .await
        .unwrap();
        driver
    }

    /// A language that could not process an activation hands it back: it is
    /// released at once, not reported as the run's failure.
    #[tokio::test]
    async fn a_protocol_error_releases_the_activation() {
        let driver = idle_driver().await;
        let report = driver.report_for(WorkflowWorkResult {
            workflow_id: "order-7".into(),
            run_id: "run-1".into(),
            task_token: b"act-1".to_vec(),
            stream_entry_id: None,
            result: Err(Error::Protocol("unknown resolution".into())),
        });
        assert!(
            matches!(&report, Report::Release(leftover) if leftover.run_id == "run-1"),
            "{report:?}"
        );
    }

    /// A workflow failure reaches the engine marked non-retryable exactly when
    /// the language answered it so: the mark the workflow code raised.
    #[tokio::test]
    async fn a_workflow_failure_carries_the_mark_the_language_gave_it() {
        use crate::proto::kernel::v1::{run_activation_result, Failure, RunActivationResult};

        let driver = idle_driver().await;
        for non_retryable in [true, false] {
            let answer = RunActivationResult {
                outcome: Some(run_activation_result::Outcome::WorkflowFailed(Failure {
                    message: "out of stock".into(),
                    non_retryable,
                    ..Default::default()
                })),
                ..Default::default()
            };
            let report = driver.report_for(WorkflowWorkResult {
                workflow_id: "order-7".into(),
                run_id: "run-1".into(),
                task_token: b"act-1".to_vec(),
                stream_entry_id: None,
                result: crate::kernel::execution_result("run-1", answer),
            });
            match report {
                Report::Fail {
                    message,
                    non_retryable: sent,
                    ..
                } => {
                    assert_eq!(message, "out of stock");
                    assert_eq!(sent, non_retryable);
                }
                other => panic!("{other:?}"),
            }
        }
    }

    /// The retry policy of a child and of a fresh run reaches the engine, a
    /// delay left unset staying unset for the engine's workflow default; no
    /// policy is sent as none.
    #[tokio::test]
    async fn child_and_restart_retry_policies_reach_the_engine() {
        use crate::protocol::{RestartFreshCommand, StartChildWorkflowCommand};
        use proto::command::Attributes;

        let policy = WorkflowRetryPolicy {
            max_attempts: 4,
            initial_interval: Some(Duration::from_millis(1500)),
            max_interval: None,
            backoff_coefficient: 3.0,
            non_retryable_errors: vec!["Declined".into()],
        };
        let expected = proto::RetryPolicy {
            initial_interval: Some(crate::proto::prost_types::Duration {
                seconds: 1,
                nanos: 500_000_000,
            }),
            backoff_coefficient: 3.0,
            maximum_interval: None,
            maximum_attempts: 4,
            non_retryable_error_types: vec!["Declined".into()],
            ..Default::default()
        };
        let driver = idle_driver().await;
        let commands = driver
            .convert_commands_to_proto(
                vec![
                    BridgeCommand::StartChildWorkflow(StartChildWorkflowCommand {
                        workflow_id: "order-7-child-0".into(),
                        workflow_type: "ship".into(),
                        retry_policy: Some(policy.clone()),
                        ..Default::default()
                    }),
                    BridgeCommand::StartChildWorkflow(StartChildWorkflowCommand {
                        workflow_id: "order-7-child-1".into(),
                        workflow_type: "ship".into(),
                        ..Default::default()
                    }),
                    BridgeCommand::RestartFresh(RestartFreshCommand {
                        workflow_type: "checkout".into(),
                        retry_policy: Some(policy),
                        ..Default::default()
                    }),
                    BridgeCommand::RestartFresh(RestartFreshCommand {
                        workflow_type: "checkout".into(),
                        ..Default::default()
                    }),
                ],
                "default",
            )
            .unwrap();
        let sent: Vec<Option<proto::RetryPolicy>> = commands
            .into_iter()
            .map(|command| match command.attributes {
                Some(Attributes::StartChildWorkflow(child)) => child.retry_policy,
                Some(Attributes::RestartFresh(restart)) => restart.retry_policy,
                other => panic!("{other:?}"),
            })
            .collect();
        assert_eq!(sent, [Some(expected.clone()), None, Some(expected), None]);
    }

    #[test]
    fn test_workflow_driver_config_default() {
        let config = WorkflowDriverConfig::default();
        assert_eq!(config.namespace, "default");
        assert_eq!(config.task_queue, "default");
        assert_eq!(config.max_concurrent_executions, 100);
        assert_eq!(config.poller_count, 4);
    }

    /// A run found non-deterministic again and again is tried at a pace
    /// that slows to the cap and stays there.
    #[test]
    fn the_pause_before_a_non_deterministic_activation_is_retried_doubles_to_its_cap() {
        let config = WorkflowDriverConfig::default();
        let pause = |attempt| {
            non_determinism_backoff(
                config.non_determinism_retry,
                config.non_determinism_retry_max,
                attempt,
            )
        };
        let seconds: Vec<u64> = (1..=8).map(|n| pause(n).as_secs()).collect();
        assert_eq!(seconds, vec![2, 4, 8, 16, 32, 60, 60, 60]);
        assert_eq!(pause(u32::MAX), Duration::from_secs(60));
        assert_eq!(humanize(Duration::from_millis(50)), "50ms");
        assert_eq!(humanize(Duration::from_secs(60)), "60s");
    }
}
