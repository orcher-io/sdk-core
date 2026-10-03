//! Driver mode for workflow, task, and actor execution.
//!
//! In driver mode, sdk-core handles polling and gRPC communication, and the
//! language SDK runs the user's handlers.
//!
//! ## Architecture
//!
//! Each unit of work goes through the same sequence:
//! 1. sdk-core polls for work (`WorkflowExecutionTask`, `TaskExecutionTask`).
//! 2. sdk-core sends the work to the language SDK over a channel.
//! 3. The language SDK runs its handler.
//! 4. The language SDK sends the result back to sdk-core over a channel.
//! 5. sdk-core reports the result to the server over gRPC.

use crate::poller::metrics::WorkerMetrics as SharedWorkerMetrics;
use std::collections::HashMap;
use std::future::Future;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

use crate::bridge::{
    validate_execution_result, Command as BridgeCommand, ExecutionErrorType, ExecutionResult,
    QueryResponse, QueryResult as BridgeQueryResult, UpdateResponse,
    UpdateResult as BridgeUpdateResult,
};
use crate::error::{Error, Result};
use crate::poller::channel::{Breaker, ChannelManager};
use crate::poller::completion::{
    activation_token, note_connection_failure, Caller, Completer, CompletionRetryConfig,
    EagerTasks, Report, TaskIds, TaskReport,
};
use crate::poller::heartbeat::{TaskHeartbeat, TaskHeartbeats};
use crate::poller::lifecycle::{self, Leftover};
use crate::poller::polling::{
    ActorOperationPoller, ActorPollerConfig, PollerConfig, TaskExecutionPoller, TaskExecutionTask,
    WorkflowExecutionPoller, WorkflowExecutionTask,
};
use crate::proto::orcher::v1::{
    self as proto, actor_service_client::ActorServiceClient, ActorOperation,
    CompleteActorOperationRequest, ExecutionStatus, HeartbeatRequest, PollTaskExecutionResponse,
    WorkerMetrics, WorkerStatus,
};
use crate::state::cache::WorkflowCache;
use crate::state::replayer::{
    CommandCoverage, JournalSteps, ReplayConfig, Replayer, NON_DETERMINISM_FAILURE_TYPE,
};
use crate::types::WorkflowExecution;

/// Convert an eagerly returned `PollTaskExecutionResponse` into a `TaskWork`.
///
/// The result can go straight to the language SDK's work channel, which saves a
/// poll round-trip. When `heartbeats` is given, it heartbeats the task from here
/// on; otherwise nothing heartbeats it.
pub(crate) fn proto_task_to_task_work(
    proto: PollTaskExecutionResponse,
    default_queue: &str,
    heartbeats: Option<&TaskHeartbeats>,
) -> TaskWork {
    let task_queue = if proto.task_queue.is_empty() {
        default_queue.to_string()
    } else {
        proto.task_queue
    };
    let task = TaskExecutionTask {
        execution: crate::types::WorkflowExecution {
            workflow_id: proto.workflow_id,
            run_id: proto.execution_id,
        },
        task_id: proto.task_id,
        task_type: proto.task_type,
        task_queue,
        input: proto.input,
        task_token: proto.task_token,
        attempt: proto.attempt,
        start_to_close_timeout: proto
            .start_to_close_timeout
            .map(|d| Duration::new(d.seconds as u64, d.nanos as u32)),
        heartbeat_timeout: proto
            .heartbeat_timeout
            .map(|d| Duration::new(d.seconds as u64, d.nanos as u32)),
    };
    let heartbeat = match heartbeats {
        Some(heartbeats) => heartbeats.start(&task),
        None => TaskHeartbeat::detached(),
    };
    TaskWork { task, heartbeat }
}

/// Where a workflow driver hands the tasks the engine returns eagerly on a
/// workflow completion.
///
/// It holds the task driver's work channel and the heartbeats the task driver
/// keeps for the tasks it runs. Get one from [`TaskDriver::eager_task_injector`].
#[derive(Debug, Clone)]
pub struct EagerTaskInjector {
    pub(crate) sender: mpsc::Sender<TaskWork>,
    pub(crate) heartbeats: Option<TaskHeartbeats>,
}

/// Build an injector from a bare work channel.
///
/// Nothing heartbeats the eager tasks sent through it, so the workflow driver
/// does not tell the engine that they are heartbeated.
impl From<mpsc::Sender<TaskWork>> for EagerTaskInjector {
    fn from(sender: mpsc::Sender<TaskWork>) -> Self {
        Self {
            sender,
            heartbeats: None,
        }
    }
}

/// Configuration for the workflow driver.
#[derive(Debug, Clone)]
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

    /// Workflow cache capacity.
    pub cache_capacity: usize,

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
    pub tls_config: Option<super::TlsConfig>,

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
    /// engine it can receive. [`crate::limits::default_max_message_bytes`]
    /// unless set: `ORCHER_MAX_MESSAGE_BYTES`, or 32 MiB, the engine's
    /// default. A result too large to send fails its task or workflow, saying
    /// so, rather than being sent and refused.
    pub max_message_bytes: usize,
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
            cache_capacity: 1000,
            strict_determinism: false,
            poller_count: 4,
            organization_id: None,
            api_key: None,
            tls_config: None,
            version_id: None,
            max_message_bytes: crate::limits::default_max_message_bytes(),
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

/// A handle that stops a running [`WorkflowDriver`] or [`TaskDriver`].
///
/// Get one from [`WorkflowDriver::shutdown_handle`] or
/// [`TaskDriver::shutdown_handle`].
#[derive(Clone, Debug)]
pub struct ShutdownHandle(tokio::sync::watch::Sender<bool>);

impl ShutdownHandle {
    /// Ask the driver to stop.
    ///
    /// The text below says "activations". For a task driver, read "tasks",
    /// and read the task's start-to-close timeout for the claim timeout.
    ///
    /// The driver's pollers start no new polls. A poll already out is left to
    /// finish, because the engine may have claimed the activation it brings
    /// back and cancelling the call would not release it; that activation is
    /// handed to the language SDK. While the SDK still takes work, the driver also
    /// waits for the results of activations it has handed over. All of this,
    /// and sending the completions that follow, happens within one shutdown
    /// grace ([`CompletionRetryConfig::shutdown_grace`], five seconds by
    /// default) from this call: `run` returns by then.
    ///
    /// The driver also tells the engine, once, that this process is shutting
    /// down (`ShutdownWorker`, naming the queues the driver polls). An engine
    /// from 0.5.0 on answers the driver's open long polls at once with
    /// nothing and hands its later polls nothing, so an idle worker stops
    /// within moments. An older engine does not know the call; there an idle
    /// worker takes the whole grace to stop, as its pollers sit in long polls
    /// that answer only when work arrives and are abandoned only once the
    /// grace is over. A worker whose SDK has already stopped taking work
    /// stops at once either way.
    ///
    /// An activation that a poll brings back but that can no longer be handed
    /// to the SDK is handed back to the engine (`ReleaseWorkflowExecution`),
    /// within what is left of the grace, for the next poll to take. An engine
    /// older than 0.5.0 cannot take it back, so it waits out its claim timeout. Tasks
    /// have no such call: one not handed over waits out its start-to-close
    /// timeout.
    ///
    /// Call this at the first sign of shutdown, and keep taking work and
    /// returning results until `run` returns; only then stop the language
    /// side's loops. Stopping them first strands whatever polls bring back
    /// in the meantime: the engine has it claimed, and it waits out the
    /// engine's claim timeout.
    pub fn shutdown(&self) {
        let _ = self.0.send(true);
    }
}

/// The reports being sent, each from its own task.
struct InFlight {
    set: tokio::task::JoinSet<()>,
    /// Which driver sends them, capitalized to start a log sentence:
    /// "Workflow driver".
    driver: &'static str,
    /// What they are, in lowercase for the middle of a log sentence:
    /// "workflow completions".
    what: &'static str,
}

impl InFlight {
    fn new(driver: &'static str, what: &'static str) -> Self {
        Self {
            set: tokio::task::JoinSet::new(),
            driver,
            what,
        }
    }

    /// Run `send` in a task of its own, holding `slot` until it has sent its
    /// report or given up on it.
    fn spawn(
        &mut self,
        send: impl Future<Output = ()> + Send + 'static,
        slot: Option<tokio::sync::OwnedSemaphorePermit>,
    ) {
        self.set.spawn(async move {
            send.await;
            drop(slot);
        });
    }

    fn reaped(&self, finished: std::result::Result<(), tokio::task::JoinError>) {
        if let Err(e) = finished {
            if e.is_panic() {
                tracing::error!(error = %e, "Sending {} panicked", self.what);
            }
        }
    }

    /// Give reports still being sent until `stop_by` to finish, then abort the rest.
    ///
    /// Each report stops retrying once its own grace is up. This deadline also
    /// bounds the wait for a report stuck inside a call, and for one started
    /// late in the shutdown.
    async fn finish(mut self, stop_by: tokio::time::Instant) {
        if self.set.is_empty() {
            return;
        }
        tracing::info!(
            pending = self.set.len(),
            grace_ms = stop_by
                .saturating_duration_since(tokio::time::Instant::now())
                .as_millis() as u64,
            "Waiting for {} still being sent",
            self.what
        );
        let what = self.what;
        let set = &mut self.set;
        let drained = tokio::time::timeout_at(stop_by, async {
            while let Some(finished) = set.join_next().await {
                if let Err(e) = finished {
                    if e.is_panic() {
                        tracing::error!(error = %e, "Sending {what} panicked");
                    }
                }
            }
        })
        .await;
        if drained.is_err() {
            tracing::warn!(
                abandoned = self.set.len(),
                "Shutdown grace over; abandoning {} still being sent",
                self.what
            );
            self.set.abort_all();
            self.set.detach_all();
        }
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        // Reports are still outstanding here only when the driver's run was
        // dropped rather than shut down: its task was aborted, or the runtime
        // stopped. Dropping the set aborts them, so log it; otherwise they
        // would vanish silently.
        if !self.set.is_empty() {
            tracing::warn!(
                abandoned = self.set.len(),
                "{} stopped without a shutdown; abandoning {} still being sent. Stop it \
                 through its shutdown handle to let them finish",
                self.driver,
                self.what
            );
        }
    }
}

/// An activation to hand back, named by the token its poll carried.
fn leftover(task: WorkflowExecutionTask) -> Leftover {
    Leftover {
        token: activation_token(task.task_token, task.stream_entry_id.as_deref()),
        workflow_id: task.execution.workflow_id,
        run_id: task.execution.run_id,
    }
}

/// Tell the engine this driver is shutting down.
///
/// The notice is sent from a task of its own so the run loop keeps draining
/// meanwhile. Because it is held in `in_flight`, it gets no more than the
/// shutdown grace, whatever its own timeout.
fn announce_shutdown(in_flight: &mut InFlight, completer: &Completer, task_queues: Vec<String>) {
    let caller = Arc::clone(&completer.caller);
    let channel_manager = Arc::clone(&completer.channel_manager);
    let timeout = lifecycle::SHUTDOWN_NOTICE_TIMEOUT.min(completer.retry.shutdown_grace);
    in_flight.spawn(
        async move {
            lifecycle::announce_shutdown(
                &caller,
                &channel_manager,
                lifecycle::shutdown_queues(task_queues),
                timeout,
            )
            .await;
        },
        None,
    );
}

/// A workflow driver: polls for workflow activations and delegates running
/// them to the language SDK.
///
/// sdk-core handles polling and gRPC; the language SDK runs the handlers.
pub struct WorkflowDriver {
    config: Arc<WorkflowDriverConfig>,

    channel_manager: Arc<ChannelManager>,

    /// Workflow cache. Held by the driver but not read by it.
    #[allow(dead_code)]
    cache: Arc<WorkflowCache>,

    replay_config: ReplayConfig,

    /// The steps the journal of each activation handed to the language SDK
    /// recorded, by run, until its result comes back: the result's commands
    /// are checked against them.
    journal_steps: std::sync::Mutex<HashMap<String, JournalSteps>>,

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
            };

            let poller = WorkflowExecutionPoller::new(
                poller_config,
                &config.server_url,
                shutdown_sender.clone(),
                task_sender.clone(),
            )
            .await?;

            let poller_id = poller_idx;
            tokio::spawn(async move {
                let mut poller_mut = poller;
                if let Err(e) = poller_mut.poll_loop().await {
                    tracing::error!(poller_id = poller_id, error = %e, "Workflow driver poller error");
                }
            });
        }

        let cache = Arc::new(WorkflowCache::new(config.cache_capacity));
        let replay_config = ReplayConfig {
            strict_mode: config.strict_determinism,
            ..ReplayConfig::default()
        };

        let driver = Self {
            config,
            channel_manager: Arc::new(channel_manager),
            cache,
            replay_config,
            journal_steps: Default::default(),
            shutdown,
            shutdown_sender,
            task_receiver,
            work_sender,
            result_receiver,
            eager_task_injector: None,
            metrics: None,
            completion_retry: CompletionRetryConfig::default(),
        };

        Ok((driver, work_receiver, result_sender))
    }

    /// Forward eagerly returned tasks straight to a task driver's work channel.
    ///
    /// The server may return tasks eagerly in answer to a
    /// `complete_workflow_execution` call. With an injector set, they skip a
    /// poll round-trip and go directly to the `TaskDriver`'s work channel.
    ///
    /// Pass [`TaskDriver::eager_task_injector`]: the tasks are then heartbeated
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

        let slots = Arc::new(tokio::sync::Semaphore::new(
            self.completion_retry
                .max_in_flight
                .unwrap_or(self.config.max_concurrent_executions)
                .max(1),
        ));
        // Each report is sent from its own task. Sent inline, one completion
        // being retried against an engine that is restarting would hold up
        // every other completion and the hand-off of new work behind it.
        let mut in_flight = InFlight::new("Workflow driver", "workflow completions");
        let completer = self.completer();
        let metrics = self.metrics.clone();
        let send = |report: Report, succeeded: bool| {
            let completer = completer.clone();
            let metrics = metrics.clone();
            async move {
                completer.send(report).await;
                // Counted when the outcome has reached the engine, or been
                // given up on: until then the activation is still this
                // worker's work.
                if let Some(metrics) = metrics {
                    metrics.workflow_finished(succeeded);
                }
            }
        };
        // A slot for the next report, taken before anything that may produce
        // one is accepted. While every slot is held by a completion still
        // being sent, neither results nor new work are taken, so back-pressure
        // reaches the language SDK and the pollers, which wait.
        let mut slot: Option<tokio::sync::OwnedSemaphorePermit> = None;
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

                permit = Arc::clone(&slots).acquire_owned(), if slot.is_none() => {
                    slot = permit.ok();
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
                            in_flight.spawn(send(report, succeeded), slot.take());
                        }
                        None => results_closed = true,
                    }
                }

                // Reap finished reports so the set does not grow without bound.
                Some(finished) = in_flight.set.join_next(), if !in_flight.set.is_empty() => {
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
        if self.replay_config.verify_commands {
            self.journal_steps
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .insert(run_id.clone(), JournalSteps::from_journal(&task.journal));
        }

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
        let succeeded = result.result.is_ok();
        (self.report_for(result), succeeded)
    }

    fn report_for(&self, result: WorkflowWorkResult) -> Report {
        let workflow_id = result.workflow_id;
        let run_id = result.run_id;
        let stream_entry_id = result.stream_entry_id.unwrap_or_default();
        let token = activation_token(result.task_token, Some(&stream_entry_id));
        let journal_steps = self
            .journal_steps
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .remove(&run_id);
        let fail_as = |message: String, failure_type: &str, non_retryable: bool| Report::Fail {
            workflow_id: workflow_id.clone(),
            run_id: run_id.clone(),
            token: token.clone(),
            message,
            failure_type: failure_type.to_string(),
            non_retryable,
        };
        let fail = |message: String| fail_as(message, "WorkflowExecutionError", false);
        // Running the same code against the same journal would issue the
        // same commands again, so this is not retryable.
        let non_deterministic =
            |message: String| fail_as(message, NON_DETERMINISM_FAILURE_TYPE, true);

        let execution_result = match result.result {
            Ok(execution_result) => execution_result,
            Err(e @ Error::DeterminismViolation { .. }) => return non_deterministic(e.to_string()),
            Err(e) => return fail(e.to_string()),
        };

        if let Err(e) = validate_execution_result(&execution_result) {
            tracing::error!(
                workflow_id = %workflow_id,
                error = %e,
                "Execution result validation failed"
            );
            return fail(format!("Invalid execution result: {}", e));
        }

        if !execution_result.successful {
            let error_msg = execution_result
                .error
                .as_ref()
                .map(|e| e.message.clone())
                .unwrap_or_else(|| "Unknown error".to_string());
            return match execution_result.error.as_ref().map(|e| e.error_type) {
                Some(ExecutionErrorType::NonDeterminism) => non_deterministic(error_msg),
                _ => fail(error_msg),
            };
        }

        // The commands must not contradict what the journal recorded. Sent
        // anyway, the engine would match a reused step id to the step it
        // already has and serve its result as the new one's.
        if let Some(journal_steps) = journal_steps {
            let violations =
                journal_steps.check(&execution_result.commands, CommandCoverage::Activation);
            if !violations.is_empty() {
                for violation in &violations {
                    tracing::error!(
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
                        .map(ToString::to_string)
                        .collect::<Vec<_>>()
                        .join("; "),
                );
            }
        }

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
                            }),
                            execution_attempt: cmd.execution_attempt,
                        },
                    )),
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
                        })),
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
                    })),
                },

                BridgeCommand::CancelTimer(cmd) => ProtoCommand {
                    command_type: CommandType::CancelTimer as i32,
                    attributes: Some(Attributes::CancelTimer(CancelTimerCommandAttributes {
                        timer_id: cmd.timer_id,
                    })),
                },

                BridgeCommand::CompleteWorkflow(cmd) => ProtoCommand {
                    command_type: CommandType::CompleteWorkflow as i32,
                    attributes: Some(Attributes::CompleteWorkflow(
                        CompleteWorkflowCommandAttributes {
                            result: cmd.result.data,
                        },
                    )),
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
                        }),
                    })),
                },

                BridgeCommand::CancelWorkflowExecution(cmd) => ProtoCommand {
                    command_type: CommandType::CancelWorkflow as i32,
                    attributes: Some(Attributes::CancelWorkflow(
                        CancelWorkflowCommandAttributes {
                            details: cmd.details.map(|d| d.data).unwrap_or_default(),
                        },
                    )),
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
                        retry_policy: None,
                        annotations: Default::default(),
                        labels: Default::default(),
                    })),
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
                        })),
                    }
                }

                BridgeCommand::StartChildWorkflow(cmd) => {
                    let orphan_policy = match cmd.orphan_policy {
                        crate::bridge::OrphanPolicy::Terminate => ProtoOrphanPolicy::Terminate,
                        crate::bridge::OrphanPolicy::Cancel => ProtoOrphanPolicy::Cancel,
                        crate::bridge::OrphanPolicy::Abandon => ProtoOrphanPolicy::Abandon,
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
                                retry_policy: None,
                                // A child runs in its parent worker's
                                // namespace. Left empty, the server would file
                                // the child under a namespace named "default",
                                // where no worker in this namespace could ever
                                // claim it.
                                namespace: self.config.namespace.clone(),
                                annotations: Default::default(),
                                labels: Default::default(),
                            },
                        )),
                    }
                }

                BridgeCommand::CancelChildWorkflow(cmd) => ProtoCommand {
                    command_type: CommandType::CancelChildWorkflow as i32,
                    attributes: Some(Attributes::CancelChildWorkflow(
                        CancelChildWorkflowCommandAttributes {
                            workflow_id: cmd.workflow_id,
                            run_id: cmd.run_id,
                        },
                    )),
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
                }
            })
            .collect()
    }

    /// Ask the driver to stop; the same as [`ShutdownHandle::shutdown`].
    pub fn shutdown(&self) {
        let _ = self.shutdown_sender.send(true);
    }
}

/// Configuration for the task driver.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct TaskDriverConfig {
    /// Server address.
    pub server_url: String,

    /// Namespace.
    pub namespace: String,

    /// Task queue to poll.
    pub task_queue: String,

    /// Maximum number of concurrent task executions.
    pub max_concurrent_executions: usize,

    /// Identity of this driver.
    pub identity: String,

    /// Poll timeout.
    pub poll_timeout: Duration,

    /// Whether to enable heartbeats.
    pub enable_heartbeat: bool,

    /// Heartbeat interval.
    pub heartbeat_interval: Duration,

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
    pub tls_config: Option<super::TlsConfig>,

    /// The code release this worker is running (optional).
    ///
    /// Sent on every poll; see [`WorkflowDriverConfig::version_id`].
    pub version_id: Option<String>,

    /// Whether to heartbeat every task automatically. On by default.
    ///
    /// When on, every task handed to the language SDK is heartbeated on a
    /// timer, at a third or less of its heartbeat timeout, until its result is
    /// sent. The polls tell the engine so, and the engine then applies a
    /// default heartbeat timeout to a task that sets none: a task whose worker
    /// dies, or whose poll response is lost, is retried instead of staying
    /// started forever. When off, only the heartbeats the task's own code
    /// records are sent, and the engine is not told.
    pub auto_heartbeat: bool,

    /// The largest gRPC message the driver sends or receives, and tells the
    /// engine it can receive. [`crate::limits::default_max_message_bytes`]
    /// unless set: `ORCHER_MAX_MESSAGE_BYTES`, or 32 MiB, the engine's
    /// default. A result too large to send fails its task or workflow, saying
    /// so, rather than being sent and refused.
    pub max_message_bytes: usize,
}

impl Default for TaskDriverConfig {
    fn default() -> Self {
        Self {
            server_url: "http://localhost:50051".to_string(),
            namespace: "default".to_string(),
            task_queue: "default".to_string(),
            max_concurrent_executions: 100,
            identity: format!("task-driver-{}", uuid::Uuid::new_v4()),
            poll_timeout: Duration::from_secs(60),
            enable_heartbeat: true,
            heartbeat_interval: Duration::from_secs(30),
            poller_count: 4,
            organization_id: None,
            api_key: None,
            tls_config: None,
            version_id: None,
            auto_heartbeat: true,
            max_message_bytes: crate::limits::default_max_message_bytes(),
        }
    }
}

/// A task execution for the language SDK to run.
#[derive(Debug)]
pub struct TaskWork {
    /// The polled task.
    pub task: TaskExecutionTask,

    /// The task's heartbeat, running from the moment it was handed over.
    ///
    /// Keep it until the task's result is handed back: dropping every clone
    /// stops the heartbeats, and a task still running is then timed out. The
    /// task's code records its own heartbeats, and learns of cancellation,
    /// through this handle.
    pub heartbeat: TaskHeartbeat,
}

/// The result of a task execution, sent back by the language SDK.
#[derive(Debug)]
pub struct TaskWorkResult {
    /// The task token from the polled task, which identifies the task to complete.
    pub task_token: Vec<u8>,

    /// The task output or error.
    ///
    /// An error built from a [`TaskFailure`](crate::error::TaskFailure) is
    /// reported with the type it names and its non-retryable mark, which the
    /// engine's retry decision reads; any other error is reported as a
    /// retryable `TaskExecutionError`.
    pub result: Result<Vec<u8>>,
}

/// A request to add or remove a session queue on a running [`TaskDriver`].
#[derive(Debug, Clone)]
pub enum SessionQueueChange {
    /// Start polling this session queue.
    Add(String),
    /// Stop polling this session queue. Its poller stops on its next iteration.
    Remove(String),
}

/// A task driver: polls for tasks and delegates running them to the language SDK.
pub struct TaskDriver {
    config: Arc<TaskDriverConfig>,

    /// Channel manager for gRPC connections, shared by the reports in flight.
    channel_manager: Arc<ChannelManager>,

    /// Shutdown signal receiver.
    shutdown: tokio::sync::watch::Receiver<bool>,

    /// Shutdown signal sender.
    shutdown_sender: tokio::sync::watch::Sender<bool>,

    /// Receives tasks from the pollers.
    task_receiver: mpsc::Receiver<TaskExecutionTask>,

    /// Sender side of the task channel, kept for spawning session queue
    /// pollers. Dropped once shutdown starts, so that the channel closes
    /// when the last poller has stopped and the driver knows no more tasks
    /// can arrive.
    task_sender: Option<mpsc::Sender<TaskExecutionTask>>,

    /// Sends work to the language SDK.
    work_sender: mpsc::Sender<TaskWork>,

    /// Receives results from the language SDK.
    result_receiver: mpsc::Receiver<TaskWorkResult>,

    /// Receives session queue add and remove requests.
    session_queue_rx: mpsc::UnboundedReceiver<SessionQueueChange>,
    /// Every session queue polled so far. They are named, along with the
    /// driver's own queue, when it tells the engine it is shutting down. A
    /// removed queue is kept, because its poller runs until the shutdown.
    session_queues: Arc<parking_lot::Mutex<Vec<String>>>,
    /// Shared worker counters, when the language SDK provided them. The
    /// heartbeat reports what these say this worker is running.
    metrics: Option<Arc<SharedWorkerMetrics>>,
    /// How hard to try to deliver each completion and failure report.
    completion_retry: CompletionRetryConfig,
    /// The heartbeats of the tasks handed to the language SDK.
    heartbeats: TaskHeartbeats,
}

impl TaskDriver {
    /// Create a task driver.
    ///
    /// Returns `(driver, work_rx, result_tx, session_queue_tx)` where:
    /// - `work_rx` receives task work items for the language SDK.
    /// - `result_tx` sends task results back from the language SDK.
    /// - `session_queue_tx` sends session queue add and remove requests to the driver.
    pub async fn new(
        config: TaskDriverConfig,
    ) -> Result<(
        Self,
        mpsc::Receiver<TaskWork>,
        mpsc::Sender<TaskWorkResult>,
        mpsc::UnboundedSender<SessionQueueChange>,
    )> {
        let config = Arc::new(config);

        let channel_manager = match config.tls_config {
            Some(ref tls) => ChannelManager::with_tls(config.server_url.clone(), tls.clone()),
            None => ChannelManager::new(config.server_url.clone()),
        }
        .with_max_message_bytes(config.max_message_bytes);

        // Channels to and from the language SDK.
        // The work channel has one place more than the executions allowed.
        // The run loop always holds one place reserved for the next polled
        // task, so that handing it over never waits; that place is the loop's
        // own look-ahead, not room anyone else can use. Without the extra
        // place, the workflow driver's eager tasks, which are offered with
        // `try_send` and never wait, would find the channel full whenever the
        // SDK had only one execution free (always, when only one is allowed),
        // and each would sit until the engine took it back.
        let (work_sender, work_receiver) =
            mpsc::channel(config.max_concurrent_executions.saturating_add(1));
        let (result_sender, result_receiver) = mpsc::channel(config.max_concurrent_executions);

        // Channel from the pollers.
        let (task_sender, task_receiver) = mpsc::channel(config.max_concurrent_executions);
        let (shutdown_sender, shutdown) = tokio::sync::watch::channel(false);

        let poller_count = config.poller_count.max(1);
        tracing::info!(
            poller_count = poller_count,
            namespace = %config.namespace,
            task_queue = %config.task_queue,
            "Starting task driver pollers"
        );

        for poller_idx in 0..poller_count {
            let poller_config = PollerConfig {
                namespace: config.namespace.clone(),
                task_queue: config.task_queue.clone(),
                poll_timeout_seconds: config.poll_timeout.as_secs() as i64,
                max_concurrent_polls: config.max_concurrent_executions / poller_count,
                identity: format!("{}-task-driver-poller-{}", config.identity, poller_idx),
                organization_id: config.organization_id.clone(),
                api_key: config.api_key.clone(),
                version_id: config.version_id.clone(),
                auto_heartbeat: config.auto_heartbeat,
                tls_config: config.tls_config.clone(),
                max_message_bytes: config.max_message_bytes,
            };

            let poller = TaskExecutionPoller::new(
                poller_config,
                &config.server_url,
                shutdown_sender.clone(),
                task_sender.clone(),
            )
            .await?;

            let poller_id = poller_idx;
            tokio::spawn(async move {
                let mut poller_mut = poller;
                if let Err(e) = poller_mut.poll_loop().await {
                    tracing::error!(poller_id = poller_id, error = %e, "Task driver poller error");
                }
            });
        }

        let (session_queue_tx, session_queue_rx) = mpsc::unbounded_channel();

        let channel_manager = Arc::new(channel_manager);
        let heartbeats = TaskHeartbeats::new(
            Caller::from(&*config),
            Arc::clone(&channel_manager),
            config.auto_heartbeat,
        );
        let driver = Self {
            config,
            channel_manager,
            shutdown,
            shutdown_sender,
            task_receiver,
            task_sender: Some(task_sender),
            work_sender,
            result_receiver,
            session_queue_rx,
            session_queues: Arc::default(),
            metrics: None,
            completion_retry: CompletionRetryConfig::default(),
            heartbeats,
        };

        Ok((driver, work_receiver, result_sender, session_queue_tx))
    }

    /// Share the worker's counters, so the heartbeat can report the tasks
    /// this driver is running.
    pub fn with_metrics(mut self, metrics: Arc<SharedWorkerMetrics>) -> Self {
        self.metrics = Some(metrics);
        self
    }

    /// Set how hard to retry reporting the outcome of each task, and the
    /// shutdown grace to stop within.
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
    /// hand over tasks a poll already brought back, send the results handed
    /// back, and give reports still being retried the shutdown grace;
    /// dropping the running future instead abandons them. See
    /// [`ShutdownHandle::shutdown`].
    pub fn shutdown_handle(&self) -> ShutdownHandle {
        ShutdownHandle(self.shutdown_sender.clone())
    }

    /// Start a poller for a session-specific queue.
    ///
    /// Session queues are worker-specific (for example,
    /// `default__session__worker_abc123`). Only the worker that created the
    /// session polls its queue, which ensures all session tasks run on that
    /// worker.
    ///
    /// # Errors
    ///
    /// Returns an error if the driver is already shutting down, or if the
    /// session poller cannot be created.
    pub async fn add_session_queue(&self, session_queue: String) -> Result<()> {
        let task_sender = self.task_sender.clone().ok_or_else(|| {
            Error::worker_error("Task driver is shutting down; no new session queue is polled")
        })?;
        self.session_queues.lock().push(session_queue.clone());
        let poller = session_poller(
            &self.config,
            self.shutdown_sender.clone(),
            task_sender,
            &session_queue,
        )
        .await?;
        tokio::spawn(run_session_poller(poller, session_queue));
        Ok(())
    }

    /// Start polling a session queue without waiting for the poller to
    /// connect. Called from the run loop, which must not wait on anything
    /// but its own channels.
    fn start_session_poller(&self, session_queue: String) {
        let Some(task_sender) = self.task_sender.clone() else {
            tracing::info!(
                session_queue = %session_queue,
                "Task driver shutting down; not polling the new session queue"
            );
            return;
        };
        self.session_queues.lock().push(session_queue.clone());
        let config = Arc::clone(&self.config);
        let shutdown_sender = self.shutdown_sender.clone();
        tokio::spawn(async move {
            match session_poller(&config, shutdown_sender, task_sender, &session_queue).await {
                Ok(poller) => run_session_poller(poller, session_queue).await,
                Err(e) => tracing::error!(
                    session_queue = %session_queue,
                    error = %e,
                    "Failed to add session queue poller"
                ),
            }
        });
    }

    /// What sends this driver's completions and failure reports.
    fn completer(&self) -> Completer {
        Completer {
            caller: Arc::new(Caller::from(&*self.config)),
            channel_manager: Arc::clone(&self.channel_manager),
            retry: self.completion_retry.clone(),
            shutdown: self.shutdown.clone(),
            eager_task_injector: None,
            heartbeats: Some(self.heartbeats.clone()),
        }
    }

    /// Run the driver until it stops.
    ///
    /// Hands polled tasks to the language SDK and reports each result it
    /// hands back. Every report is sent from a task of its own and retried
    /// with the same task token while the answer may change, so a slow or
    /// failing report holds up neither the others nor new work.
    ///
    /// Returns once shut down through [`shutdown_handle`](Self::shutdown_handle)
    /// or [`shutdown`](Self::shutdown), or once the language SDK has dropped
    /// both its result sender and its work receiver.
    pub async fn run(&mut self) -> Result<()> {
        tracing::info!(
            namespace = %self.config.namespace,
            task_queue = %self.config.task_queue,
            identity = %self.config.identity,
            "Starting task driver"
        );

        let slots = Arc::new(tokio::sync::Semaphore::new(
            self.completion_retry
                .max_in_flight
                .unwrap_or(self.config.max_concurrent_executions)
                .max(1),
        ));
        // Each report is sent from its own task. Sent inline, one report
        // being retried, or merely slow to be answered, would hold up every
        // other result and the hand-off of every polled task behind it.
        let mut in_flight = InFlight::new("Task driver", "task completions");
        let completer = self.completer();
        let metrics = self.metrics.clone();
        let eager_work = self.work_sender.clone();
        let default_queue = self.config.task_queue.clone();
        let heartbeats = self.heartbeats.clone();
        let send = |report: TaskReport, succeeded: bool| {
            let completer = completer.clone();
            let metrics = metrics.clone();
            let eager_work = eager_work.clone();
            let default_queue = default_queue.clone();
            let heartbeats = heartbeats.clone();
            async move {
                let token = report.token().to_vec();
                completer
                    .send_task(report, Some(&eager_work), &default_queue)
                    .await;
                // The task is heartbeated until its outcome has reached the
                // engine or been given up on: a report retried for a while
                // must not find its attempt timed out for lack of heartbeats.
                heartbeats.finish(&token);
                // Counted when the outcome has reached the engine, or been
                // given up on: until then the task is still this worker's
                // work.
                if let Some(metrics) = metrics {
                    metrics.task_finished(succeeded);
                }
            }
        };
        // A slot for the next report, taken before a result is accepted and
        // before a polled task is taken. While every slot is held by a report
        // still being sent, neither is taken, so the language SDK and the
        // pollers wait instead of the backlog growing.
        let mut slot: Option<tokio::sync::OwnedSemaphorePermit> = None;
        // Room in the work channel, reserved before a polled task is taken.
        // Nothing in this loop may wait on a send: a full work channel would
        // stop it reading results, and a language SDK that frees room for
        // new work only once its results are read would then wait on it for
        // good.
        let mut room: Option<mpsc::OwnedPermit<TaskWork>> = None;
        let (mut tasks_closed, mut results_closed, mut work_closed) = (false, false, false);
        let mut sessions_closed = false;
        // The drain deadline, set once shutdown is asked for. From then on the
        // driver drains rather than stopping at once. Stopping at once would
        // drop whatever a poll in flight brought back (the engine has already
        // started it, so it would wait out its start-to-close timeout) and
        // every result for work already handed over.
        let mut draining: Option<tokio::time::Instant> = None;
        // Tasks handed to the language SDK whose results have not come back,
        // keyed by task token, with the ids their report is logged under. The
        // token is a credential, so it is never logged. This is an upper bound
        // on what is outstanding, not an exact count: work the SDK drops
        // without an answer never comes back. So shutdown waits on it only
        // while the SDK still takes work, and only within the grace.
        let mut handed: HashMap<Vec<u8>, TaskIds> = HashMap::new();
        // Whether the pollers have been told to stop because the SDK stopped
        // taking work.
        let mut stranded = false;

        loop {
            tokio::select! {
                _ = self.shutdown.changed(), if draining.is_none() => {
                    if *self.shutdown.borrow_and_update() {
                        tracing::info!(
                            outstanding = handed.len(),
                            grace_ms = self.completion_retry.shutdown_grace.as_millis() as u64,
                            "Task driver shutdown requested; handing over tasks already polled \
                             and waiting for results"
                        );
                        draining = Some(
                            tokio::time::Instant::now() + self.completion_retry.shutdown_grace,
                        );
                        // From here on only the pollers hold the task channel
                        // open, so it closes once the last one has stopped.
                        self.task_sender = None;
                        let mut queues = self.session_queues.lock().clone();
                        queues.push(self.config.task_queue.clone());
                        announce_shutdown(&mut in_flight, &completer, queues);
                    }
                }

                _ = tokio::time::sleep_until(draining.unwrap_or_else(tokio::time::Instant::now)),
                    if draining.is_some() =>
                {
                    tracing::warn!(
                        outstanding = handed.len(),
                        pollers_stopped = tasks_closed,
                        "Shutdown grace over; stopping the task driver"
                    );
                    break;
                }

                permit = Arc::clone(&slots).acquire_owned(), if slot.is_none() => {
                    slot = permit.ok();
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

                task = self.task_receiver.recv(),
                    if slot.is_some() && room.is_some() && !tasks_closed =>
                {
                    match (task, room.take()) {
                        (Some(task), Some(room)) => self.hand_over(task, room, &mut handed),
                        _ => tasks_closed = true,
                    }
                }

                result = self.result_receiver.recv(), if slot.is_some() && !results_closed => {
                    match result {
                        Some(result) => {
                            self.heartbeats.reporting(&result.task_token);
                            let (report, succeeded) = task_report(result, &mut handed);
                            in_flight.spawn(send(report, succeeded), slot.take());
                        }
                        None => results_closed = true,
                    }
                }

                change = self.session_queue_rx.recv(), if !sessions_closed => {
                    match change {
                        Some(SessionQueueChange::Add(queue)) => self.start_session_poller(queue),
                        Some(SessionQueueChange::Remove(queue)) => {
                            // A removed queue's poller is not stopped here. It
                            // stops on a later poll iteration, when it sees the
                            // shutdown signal or the server returns no work for
                            // the queue.
                            tracing::info!(
                                session_queue = %queue,
                                "Session queue removal noted (poller will wind down)"
                            );
                        }
                        None => sessions_closed = true,
                    }
                }

                // Reap finished reports so the set does not grow without bound.
                Some(finished) = in_flight.set.join_next(), if !in_flight.set.is_empty() => {
                    in_flight.reaped(finished);
                }
            }

            if work_closed && !stranded {
                stranded = true;
                tracing::info!("Language SDK stopped taking task work");
                room = None;
                // Nothing a poll brings back after this point can run here,
                // so the pollers stop, abandoning any poll in flight.
                self.strand_polled();
            }

            if results_closed && work_closed {
                tracing::info!("Language SDK gone, stopping the task driver");
                // Stop the pollers too. Left running, they would keep starting
                // tasks that nothing will ever run, each held until its
                // start-to-close timeout.
                let _ = self.shutdown_sender.send(true);
                break;
            }

            // Drained: no task can arrive to be handed over (every poller
            // has stopped, or the language SDK takes no more work), and none
            // handed over can still be answered. Once the SDK has stopped
            // taking work, its results are not waited for.
            if draining.is_some()
                && (tasks_closed || work_closed)
                && (handed.is_empty() || results_closed || work_closed)
            {
                tracing::info!("Task driver drained");
                break;
            }
        }

        self.task_sender = None;
        self.strand_polled();

        // Results the language SDK has already handed back are sent rather
        // than dropped with the channel: each is a finished task, and if
        // dropped it would wait out its start-to-close timeout.
        while let Ok(result) = self.result_receiver.try_recv() {
            self.heartbeats.reporting(&result.task_token);
            let (report, succeeded) = task_report(result, &mut handed);
            in_flight.spawn(send(report, succeeded), None);
        }
        // One grace covers the whole shutdown, draining included.
        let stop_by = draining
            .unwrap_or_else(|| tokio::time::Instant::now() + self.completion_retry.shutdown_grace);
        in_flight.finish(stop_by).await;
        // Tasks still running at this point will not have their results sent.
        // They are left to the engine, which times them out once their
        // heartbeats stop, and retries them.
        self.heartbeats.stop_all();

        tracing::info!("Task driver stopped");
        Ok(())
    }

    /// Hand a polled task to the language SDK through the room reserved for
    /// it, so handing it over never waits.
    fn hand_over(
        &self,
        task: TaskExecutionTask,
        room: mpsc::OwnedPermit<TaskWork>,
        handed: &mut HashMap<Vec<u8>, TaskIds>,
    ) {
        // Counted here rather than at the poll: this is where the task is
        // handed to the language SDK, and it is in progress until its report
        // is sent.
        if let Some(metrics) = &self.metrics {
            metrics.task_started();
        }
        let token = task.task_token.clone();
        let ids = TaskIds {
            task_id: task.task_id.clone(),
            workflow_id: task.execution.workflow_id.clone(),
        };
        tracing::debug!(
            task_id = %ids.task_id,
            workflow_id = %ids.workflow_id,
            "Handing task to language SDK"
        );
        // The task is heartbeated from here until its result is sent. If the
        // hand-over fails, the work is dropped along with its heartbeat handle,
        // which stops the heartbeats.
        let heartbeat = self.heartbeats.start(&task);
        if room.send(TaskWork { task, heartbeat }).is_closed() {
            // The language SDK dropped its work receiver after the room was
            // reserved.
            tracing::error!(
                task_id = %ids.task_id,
                workflow_id = %ids.workflow_id,
                "Language SDK stopped taking work; task not handed over, it waits out its \
                 timeouts"
            );
            if let Some(metrics) = &self.metrics {
                metrics.task_finished(false);
            }
        } else {
            handed.insert(token, ids);
        }
    }

    /// Stop taking tasks from the pollers.
    ///
    /// Closing the channel tells any poller still waiting on a poll to
    /// abandon it, since nothing it brought back could be handed over. A task
    /// already received but not handed over is logged by name: the engine has
    /// started it, so it waits out its start-to-close timeout.
    fn strand_polled(&mut self) {
        self.task_receiver.close();
        while let Ok(task) = self.task_receiver.try_recv() {
            tracing::error!(
                task_id = %task.task_id,
                workflow_id = %task.execution.workflow_id,
                "Task received but never handed over; it waits out its start-to-close timeout"
            );
        }
    }

    /// Return a cloned sender for the work channel.
    ///
    /// Use this to give a `WorkflowDriver` a handle so it can inject eager tasks
    /// directly into this driver's language-SDK work channel after workflow
    /// completions. Nothing heartbeats tasks injected this way; prefer
    /// [`eager_task_injector`](Self::eager_task_injector).
    ///
    /// ```rust,ignore
    /// let task_work_tx = task_driver.clone_work_sender();
    /// workflow_driver = workflow_driver.with_eager_task_injector(task_work_tx);
    /// ```
    pub fn clone_work_sender(&self) -> mpsc::Sender<TaskWork> {
        self.work_sender.clone()
    }

    /// Return where a [`WorkflowDriver`] should hand the tasks the engine
    /// returns eagerly on its completions.
    ///
    /// The injector sends to this driver's work channel, and the tasks are
    /// heartbeated the same way as the ones this driver polls.
    ///
    /// ```rust,ignore
    /// workflow_driver = workflow_driver.with_eager_task_injector(task_driver.eager_task_injector());
    /// ```
    pub fn eager_task_injector(&self) -> EagerTaskInjector {
        EagerTaskInjector {
            sender: self.work_sender.clone(),
            heartbeats: Some(self.heartbeats.clone()),
        }
    }

    /// Ask the driver to stop.
    ///
    /// The same as [`ShutdownHandle::shutdown`], for a driver not yet
    /// running.
    pub fn shutdown(&self) {
        let _ = self.shutdown_sender.send(true);
    }
}

/// The failure type reported for a task whose error names none.
const GENERIC_TASK_FAILURE: &str = "TaskExecutionError";

/// Turn a result from the language SDK into the report to send, and whether
/// the task succeeded.
fn task_report(
    result: TaskWorkResult,
    handed: &mut HashMap<Vec<u8>, TaskIds>,
) -> (TaskReport, bool) {
    // A task the workflow driver handed over eagerly never passed through
    // this driver, so its ids are not known here.
    let task = handed.remove(&result.task_token).unwrap_or_default();
    let token = result.task_token;
    match result.result {
        Ok(output) => (
            TaskReport::Complete {
                token,
                result: output,
                task,
            },
            true,
        ),
        Err(e) => {
            // The engine decides whether to retry from the failure type it is
            // given, so the type the task raised is sent when the SDK gave
            // one. An error that is only a message is reported with the
            // generic type.
            let (failure_type, non_retryable) = match e.task_failure() {
                Some(failure) => (
                    failure
                        .error_type
                        .clone()
                        .filter(|t| !t.is_empty())
                        .unwrap_or_else(|| GENERIC_TASK_FAILURE.to_string()),
                    failure.non_retryable,
                ),
                None => (GENERIC_TASK_FAILURE.to_string(), false),
            };
            (
                TaskReport::Fail {
                    token,
                    message: e.to_string(),
                    failure_type,
                    non_retryable,
                    task,
                },
                false,
            )
        }
    }
}

/// Build a poller for a session queue.
async fn session_poller(
    config: &TaskDriverConfig,
    shutdown_sender: tokio::sync::watch::Sender<bool>,
    task_sender: mpsc::Sender<TaskExecutionTask>,
    session_queue: &str,
) -> Result<TaskExecutionPoller> {
    let poller_config = PollerConfig {
        namespace: config.namespace.clone(),
        task_queue: session_queue.to_string(),
        poll_timeout_seconds: config.poll_timeout.as_secs() as i64,
        max_concurrent_polls: 1,
        identity: format!("{}-session-poller-{}", config.identity, session_queue),
        organization_id: config.organization_id.clone(),
        api_key: config.api_key.clone(),
        version_id: config.version_id.clone(),
        auto_heartbeat: config.auto_heartbeat,
        tls_config: config.tls_config.clone(),
        max_message_bytes: config.max_message_bytes,
    };
    TaskExecutionPoller::new(
        poller_config,
        &config.server_url,
        shutdown_sender,
        task_sender,
    )
    .await
}

async fn run_session_poller(mut poller: TaskExecutionPoller, session_queue: String) {
    tracing::info!(
        session_queue = %session_queue,
        "Started session queue poller"
    );
    if let Err(e) = poller.poll_loop().await {
        tracing::error!(
            session_queue = %session_queue,
            error = %e,
            "Session queue poller error"
        );
    }
}

// ---------------------------------------------------------------------------
// Actor Driver
// ---------------------------------------------------------------------------

/// Events emitted by the ActorDriver to notify the language SDK.
#[derive(Debug, Clone)]
pub enum ActorDriverEvent {
    /// Server requested re-registration of actor handlers.
    ReRegistrationRequired,
}

/// Configuration for the actor driver.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ActorDriverConfig {
    /// Server address.
    pub server_url: String,

    /// Namespace.
    pub namespace: String,

    /// Worker ID that identifies this actor worker.
    pub service_id: String,

    /// Maximum number of concurrent actor operation executions.
    pub max_concurrent_executions: usize,

    /// Identity of this driver.
    pub identity: String,

    /// Poll timeout.
    pub poll_timeout: Duration,

    /// Number of concurrent pollers.
    pub poller_count: usize,

    /// Organization ID for multi-tenancy (optional).
    pub organization_id: Option<String>,

    /// API key for server authentication (optional).
    pub api_key: Option<String>,

    /// Whether to send heartbeats. Defaults to `true`.
    pub enable_heartbeat: bool,

    /// Heartbeat interval. Defaults to 10 seconds.
    pub heartbeat_interval: Duration,

    /// Registration ID returned by handler registration. Heartbeats require it.
    pub registration_id: Option<String>,

    /// TLS configuration for secure connections (optional).
    pub tls_config: Option<super::TlsConfig>,

    /// The largest gRPC message the driver sends or receives, and tells the
    /// engine it can receive. [`crate::limits::default_max_message_bytes`]
    /// unless set: `ORCHER_MAX_MESSAGE_BYTES`, or 32 MiB, the engine's
    /// default. A result too large to send fails its task or workflow, saying
    /// so, rather than being sent and refused.
    pub max_message_bytes: usize,
}

impl Default for ActorDriverConfig {
    fn default() -> Self {
        Self {
            server_url: "http://localhost:50051".to_string(),
            namespace: "default".to_string(),
            service_id: "default".to_string(),
            max_concurrent_executions: 100,
            identity: format!("actor-driver-{}", uuid::Uuid::new_v4()),
            poll_timeout: Duration::from_secs(30),
            poller_count: 4,
            organization_id: None,
            api_key: None,
            enable_heartbeat: true,
            heartbeat_interval: Duration::from_secs(10),
            registration_id: None,
            tls_config: None,
            max_message_bytes: crate::limits::default_max_message_bytes(),
        }
    }
}

/// An actor operation for the language SDK to run.
#[derive(Debug)]
pub struct ActorWork {
    /// The polled actor operation, as the proto type.
    pub operation: ActorOperation,
}

/// The result of an actor operation, sent back by the language SDK.
#[derive(Debug)]
pub struct ActorWorkResult {
    /// Operation ID.
    pub operation_id: String,

    /// Execution ID.
    pub execution_id: String,

    /// The operation output or error.
    pub result: Result<Vec<u8>>,
}

/// An actor driver: polls for actor operations and delegates running them to
/// the language SDK.
///
/// It follows the same pattern as [`TaskDriver`]: no replay, no cache, plain
/// bytes in and out. The driver creates `poller_count` `ActorOperationPoller`s,
/// receives operations from them, sends work to the language SDK, and reports
/// results over gRPC. It also sends the actor worker's periodic heartbeat.
pub struct ActorDriver {
    config: Arc<ActorDriverConfig>,

    /// Channel manager for gRPC connections, shared by the heartbeat and the
    /// reports in flight.
    channel_manager: Arc<ChannelManager>,

    /// Shutdown signal receiver.
    shutdown: tokio::sync::watch::Receiver<bool>,

    /// Shutdown signal sender.
    shutdown_sender: tokio::sync::watch::Sender<bool>,

    /// Receives operations from the pollers.
    operation_receiver: mpsc::Receiver<ActorOperation>,

    /// Sends work to the language SDK.
    work_sender: mpsc::Sender<ActorWork>,

    /// Receives results from the language SDK.
    result_receiver: mpsc::Receiver<ActorWorkResult>,

    /// Operations currently being run, reported in the heartbeat.
    operations_in_progress: Arc<AtomicU64>,

    /// Operations completed successfully, reported in the heartbeat.
    operations_completed: Arc<AtomicU64>,

    /// Operations that failed, reported in the heartbeat.
    operations_failed: Arc<AtomicU64>,

    /// Notifies the language SDK of driver events.
    event_sender: mpsc::Sender<ActorDriverEvent>,

    /// How hard to try to deliver each completion and failure report.
    completion_retry: CompletionRetryConfig,
}

impl ActorDriver {
    /// Create an actor driver.
    ///
    /// Returns the driver and the channels the language SDK uses to talk to it:
    /// - `work_receiver`: the language SDK receives work from this channel.
    /// - `result_sender`: the language SDK sends results to this channel.
    /// - `event_receiver`: the language SDK receives driver events, such as a
    ///   re-registration request, from this channel.
    pub async fn new(
        config: ActorDriverConfig,
    ) -> Result<(
        Self,
        mpsc::Receiver<ActorWork>,
        mpsc::Sender<ActorWorkResult>,
        mpsc::Receiver<ActorDriverEvent>,
    )> {
        let config = Arc::new(config);

        let channel_manager = match config.tls_config {
            Some(ref tls) => ChannelManager::with_tls(config.server_url.clone(), tls.clone()),
            None => ChannelManager::new(config.server_url.clone()),
        }
        .with_max_message_bytes(config.max_message_bytes);

        // Channels to and from the language SDK.
        let (work_sender, work_receiver) = mpsc::channel(config.max_concurrent_executions);
        let (result_sender, result_receiver) = mpsc::channel(config.max_concurrent_executions);
        let (event_sender, event_receiver) = mpsc::channel(10);

        // Channel from the pollers to the driver.
        let (operation_sender, operation_receiver) =
            mpsc::channel(config.max_concurrent_executions);
        let (shutdown_sender, shutdown) = tokio::sync::watch::channel(false);

        let poller_count = config.poller_count.max(1);
        tracing::info!(
            poller_count = poller_count,
            namespace = %config.namespace,
            service_id = %config.service_id,
            "Starting actor driver pollers"
        );

        for poller_idx in 0..poller_count {
            let poller_config = ActorPollerConfig {
                service_id: config.service_id.clone(),
                poll_timeout_ms: config.poll_timeout.as_millis() as u64,
                max_operations: (config.max_concurrent_executions / poller_count).max(1) as u32,
                identity: format!("{}-actor-driver-poller-{}", config.identity, poller_idx),
                organization_id: config.organization_id.clone(),
                api_key: config.api_key.clone(),
                tls_config: config.tls_config.clone(),
                max_message_bytes: config.max_message_bytes,
            };

            let poller = ActorOperationPoller::new(
                poller_config,
                &config.server_url,
                shutdown_sender.clone(),
                operation_sender.clone(),
            )
            .await?;

            let poller_id = poller_idx;
            tokio::spawn(async move {
                let mut poller_mut = poller;
                if let Err(e) = poller_mut.poll_loop().await {
                    tracing::error!(poller_id = poller_id, error = %e, "Actor driver poller error");
                }
            });
        }

        let driver = Self {
            config,
            channel_manager: Arc::new(channel_manager),
            shutdown,
            shutdown_sender,
            operation_receiver,
            work_sender,
            result_receiver,
            operations_in_progress: Arc::new(AtomicU64::new(0)),
            operations_completed: Arc::new(AtomicU64::new(0)),
            operations_failed: Arc::new(AtomicU64::new(0)),
            event_sender,
            completion_retry: CompletionRetryConfig::default(),
        };

        Ok((driver, work_receiver, result_sender, event_receiver))
    }

    /// Set how hard to retry reporting the outcome of each operation.
    ///
    /// The default keeps trying for up to thirty seconds; see
    /// [`CompletionRetryConfig`].
    pub fn with_completion_retry(mut self, retry: CompletionRetryConfig) -> Self {
        self.completion_retry = retry;
        self
    }

    /// What sends this driver's completions and failure reports.
    fn completer(&self) -> Completer {
        Completer {
            caller: Arc::new(Caller::from(&*self.config)),
            channel_manager: Arc::clone(&self.channel_manager),
            retry: self.completion_retry.clone(),
            shutdown: self.shutdown.clone(),
            eager_task_injector: None,
            heartbeats: None,
        }
    }

    /// Run the driver until it stops.
    ///
    /// Returns once shut down, or once the language SDK has dropped its result
    /// sender and no more operations can reach it. Unlike the workflow and task
    /// drivers, it stops at once on shutdown rather than draining polls in
    /// flight; results already handed back still get the shutdown grace.
    pub async fn run(&mut self) -> Result<()> {
        tracing::info!(
            namespace = %self.config.namespace,
            service_id = %self.config.service_id,
            identity = %self.config.identity,
            enable_heartbeat = self.config.enable_heartbeat,
            heartbeat_interval_secs = self.config.heartbeat_interval.as_secs(),
            "Starting actor driver"
        );

        let mut heartbeat_ticker = tokio::time::interval(self.config.heartbeat_interval);
        heartbeat_ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // An interval's first tick completes immediately; consume it so the
        // first heartbeat goes out one interval after start.
        heartbeat_ticker.tick().await;

        let slots = Arc::new(tokio::sync::Semaphore::new(
            self.completion_retry
                .max_in_flight
                .unwrap_or(self.config.max_concurrent_executions)
                .max(1),
        ));
        // Each report is sent, and retried, from its own task. Retried
        // inline, one report would hold up every other result and the
        // hand-off of new operations for as long as it kept trying.
        let mut in_flight = InFlight::new("Actor driver", "actor operation completions");
        let completer = self.completer();
        // A slot for the next report, taken before a result is accepted, so
        // a backlog of reports holds the language SDK back rather than
        // growing without bound.
        let mut slot: Option<tokio::sync::OwnedSemaphorePermit> = None;
        // Room in the work channel, reserved before an operation is taken, so
        // handing one over never waits. The task driver's run loop explains why.
        let mut room: Option<mpsc::OwnedPermit<ActorWork>> = None;
        let (mut operations_closed, mut results_closed, mut work_closed) = (false, false, false);

        loop {
            tokio::select! {
                // Unlike the workflow and task drivers, this driver does not
                // tell the engine it is shutting down. The engine's shutdown
                // covers workflow and task polls only, and this driver polls
                // neither, so its notice would name no queue. A notice naming
                // no queue covers every queue of this identity, which would
                // end the polls of the process's other drivers while they may
                // still be draining. This driver stops at once anyway,
                // abandoning its polls.
                _ = self.shutdown.changed() => {
                    if *self.shutdown.borrow_and_update() {
                        tracing::info!("Actor driver shutdown requested");
                        break;
                    }
                }

                permit = Arc::clone(&slots).acquire_owned(), if slot.is_none() => {
                    slot = permit.ok();
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

                // Receive operations from the pollers and hand them to the
                // language SDK.
                operation = self.operation_receiver.recv(),
                    if room.is_some() && !operations_closed =>
                {
                    match (operation, room.take()) {
                        (Some(operation), Some(room)) => {
                            let op_id = operation.operation_id.clone();
                            if room.send(ActorWork { operation }).is_closed() {
                                tracing::error!(
                                    operation_id = %op_id,
                                    "Failed to send actor work to language SDK"
                                );
                            } else {
                                self.operations_in_progress.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        _ => operations_closed = true,
                    }
                }

                // Receive results from the language SDK and report them.
                result = self.result_receiver.recv(), if slot.is_some() && !results_closed => {
                    match result {
                        Some(result) => {
                            let request = self.handle_actor_result(result);
                            let completer = completer.clone();
                            in_flight.spawn(
                                async move {
                                    completer.send_actor(request).await;
                                },
                                slot.take(),
                            );
                        }
                        None => results_closed = true,
                    }
                }

                _ = heartbeat_ticker.tick(), if self.config.enable_heartbeat => {
                    self.send_heartbeat().await;
                }

                // Reap finished reports so the set does not grow without bound.
                Some(finished) = in_flight.set.join_next(), if !in_flight.set.is_empty() => {
                    in_flight.reaped(finished);
                }
            }

            if results_closed && (operations_closed || work_closed) {
                tracing::info!("All actor driver channels closed, shutting down");
                break;
            }
        }

        // Results already handed back are sent rather than dropped with the
        // channel, and reports still being retried get the shutdown grace.
        while let Ok(result) = self.result_receiver.try_recv() {
            let request = self.handle_actor_result(result);
            let completer = completer.clone();
            in_flight.spawn(
                async move {
                    completer.send_actor(request).await;
                },
                None,
            );
        }
        in_flight
            .finish(tokio::time::Instant::now() + self.completion_retry.shutdown_grace)
            .await;

        tracing::info!("Actor driver stopped");
        Ok(())
    }

    /// Count an actor operation's result from the language SDK, and build
    /// the report to send for it.
    fn handle_actor_result(&self, result: ActorWorkResult) -> CompleteActorOperationRequest {
        self.operations_in_progress.fetch_sub(1, Ordering::Relaxed);
        let (output, status, error_message) = match result.result {
            Ok(output) => {
                self.operations_completed.fetch_add(1, Ordering::Relaxed);
                (output, ExecutionStatus::Success, String::new())
            }
            Err(e) => {
                self.operations_failed.fetch_add(1, Ordering::Relaxed);
                (vec![], ExecutionStatus::Error, e.to_string())
            }
        };
        CompleteActorOperationRequest {
            service_id: self.config.service_id.clone(),
            operation_id: result.operation_id,
            execution_id: result.execution_id,
            result: output,
            status: status as i32,
            error_message,
            error_code: String::new(),
            duration_ms: 0,
        }
    }

    /// Attach the worker's credentials to an outbound request.
    ///
    /// Polling carries these headers already; reporting the outcome has to
    /// carry them too, or a server with authentication enabled answers
    /// Unauthenticated and the result is dropped.
    fn credentialed<T>(&self, body: T) -> tonic::Request<T> {
        crate::poller::credentials::credentialed_request(
            body,
            self.config.api_key.as_deref(),
            self.config.organization_id.as_deref(),
        )
    }

    /// Send one heartbeat to the server, and pass on a re-registration request.
    async fn send_heartbeat(&self) {
        let in_progress = self.operations_in_progress.load(Ordering::Relaxed);
        let completed = self.operations_completed.load(Ordering::Relaxed);
        let failed = self.operations_failed.load(Ordering::Relaxed);

        let request = HeartbeatRequest {
            service_id: self.config.service_id.clone(),
            registration_id: self.config.registration_id.clone().unwrap_or_default(),
            status: WorkerStatus::Healthy as i32,
            metrics: Some(WorkerMetrics {
                operations_in_progress: in_progress as u32,
                operations_completed: completed,
                operations_failed: failed,
                avg_duration_ms: 0.0,
                cpu_usage: 0.0,
                memory_usage_bytes: 0,
            }),
        };

        // Heartbeats are periodic, so a failed one is not retried: the next
        // tick sends another.
        let (channel, generation) = match self.channel_manager.connection(Breaker::Respect).await {
            Ok(connected) => connected,
            Err(e) => {
                tracing::warn!(
                    service_id = %self.config.service_id,
                    error = ?e,
                    "Skipping heartbeat — channel not ready"
                );
                return;
            }
        };

        let result = {
            let mut client = ActorServiceClient::new(channel);
            client.heartbeat(self.credentialed(request)).await
        };

        match result {
            Ok(response) => {
                let response = response.into_inner();
                if response.re_register {
                    tracing::warn!(
                        service_id = %self.config.service_id,
                        "Server requested re-registration"
                    );
                    let _ = self
                        .event_sender
                        .try_send(ActorDriverEvent::ReRegistrationRequired);
                }
                tracing::debug!(
                    service_id = %self.config.service_id,
                    "Heartbeat sent"
                );
            }
            Err(e) => {
                tracing::error!(
                    service_id = %self.config.service_id,
                    error = %e,
                    "Heartbeat failed"
                );
                // The connection is shared with the reports in flight, so,
                // as with them, the connection is blamed only when no server
                // answered at all.
                note_connection_failure(&self.channel_manager, &e, generation);
            }
        }
    }

    /// Ask the driver to stop.
    pub fn shutdown(&self) {
        let _ = self.shutdown_sender.send(true);
    }
}

#[cfg(test)]
#[path = "driver_tests.rs"]
mod engine_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_workflow_driver_config_default() {
        let config = WorkflowDriverConfig::default();
        assert_eq!(config.namespace, "default");
        assert_eq!(config.task_queue, "default");
        assert_eq!(config.max_concurrent_executions, 100);
        assert_eq!(config.poller_count, 4);
    }

    #[test]
    fn test_task_driver_config_default() {
        let config = TaskDriverConfig::default();
        assert_eq!(config.namespace, "default");
        assert_eq!(config.task_queue, "default");
        assert_eq!(config.max_concurrent_executions, 100);
        assert_eq!(config.poller_count, 4);
        assert!(config.enable_heartbeat);
    }

    #[test]
    fn test_actor_driver_config_default() {
        let config = ActorDriverConfig::default();
        assert_eq!(config.namespace, "default");
        assert_eq!(config.service_id, "default");
        assert_eq!(config.max_concurrent_executions, 100);
        assert_eq!(config.poller_count, 4);
        assert_eq!(config.poll_timeout, Duration::from_secs(30));
    }
}
