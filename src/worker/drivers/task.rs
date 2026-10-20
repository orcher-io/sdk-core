//! The task driver: polls for task executions, heartbeats them while the
//! language SDK runs them, and reports their results. Tasks the engine hands
//! back eagerly on a workflow completion join it through [`EagerTaskInjector`].

use crate::worker::metrics::WorkerMetrics as SharedWorkerMetrics;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;

use crate::error::{Error, Result};
use crate::proto::orcher::v1::PollTaskExecutionResponse;
use crate::transport::channel::ChannelManager;
use crate::worker::governor::{InFlight, SlotBudget, SlotPermit};
use crate::worker::heartbeat::{TaskHeartbeat, TaskHeartbeats};
use crate::worker::poll::completion::{
    Caller, Completer, CompletionRetryConfig, TaskIds, TaskReport,
};
use crate::worker::poll::polling::{PollerConfig, TaskExecutionPoller, TaskExecutionTask};

use super::{announce_shutdown, ShutdownHandle};

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

/// Configuration for the task driver.
#[derive(Clone)]
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
    pub tls_config: Option<crate::transport::TlsConfig>,

    /// The code release this worker is running (optional).
    ///
    /// Sent on every poll; see [`WorkflowDriverConfig::version_id`](super::WorkflowDriverConfig::version_id).
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
    /// engine it can receive. [`crate::transport::limits::default_max_message_bytes`]
    /// unless set: `ORCHER_MAX_MESSAGE_BYTES`, or 32 MiB, the engine's
    /// default. A result too large to send fails its task or workflow, saying
    /// so, rather than being sent and refused.
    pub max_message_bytes: usize,
    /// Where this driver's calls to the engine are recorded: the worker's
    /// wire measurements, shared with its other drivers. A driver has
    /// measurements of its own unless given these.
    pub wire: std::sync::Arc<crate::observability::WireMetrics>,
}

// Written by hand so the secret is never printed; destructured so that a new
// field must be added here to compile.
impl std::fmt::Debug for TaskDriverConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            server_url,
            namespace,
            task_queue,
            max_concurrent_executions,
            identity,
            poll_timeout,
            enable_heartbeat,
            heartbeat_interval,
            poller_count,
            organization_id,
            api_key,
            tls_config,
            version_id,
            auto_heartbeat,
            max_message_bytes,
            wire: _,
        } = self;
        f.debug_struct("TaskDriverConfig")
            .field("server_url", server_url)
            .field("namespace", namespace)
            .field("task_queue", task_queue)
            .field("max_concurrent_executions", max_concurrent_executions)
            .field("identity", identity)
            .field("poll_timeout", poll_timeout)
            .field("enable_heartbeat", enable_heartbeat)
            .field("heartbeat_interval", heartbeat_interval)
            .field("poller_count", poller_count)
            .field("organization_id", organization_id)
            .field("api_key", &crate::transport::credentials::redacted(api_key))
            .field("tls_config", tls_config)
            .field("version_id", version_id)
            .field("auto_heartbeat", auto_heartbeat)
            .field("max_message_bytes", max_message_bytes)
            .finish()
    }
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
            max_message_bytes: crate::transport::limits::default_max_message_bytes(),
            wire: crate::observability::WireMetrics::new(),
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

/// A session starting or ending on a running [`TaskDriver`]'s session queue.
///
/// Changes are counted: a queue is polled from its first `Add` until as many
/// `Remove`s have arrived, so every session a worker hosts can share one
/// queue.
#[derive(Debug, Clone)]
pub enum SessionQueueChange {
    /// A session starts on this queue; the queue is polled.
    Add(String),
    /// A session on this queue ends. When it was the last, the queue's
    /// poller stops; a poll already out is left to finish, and a task it
    /// brings is still handed over.
    Remove(String),
}

/// The session queues a task driver polls.
///
/// Every session a worker hosts shares one queue (it is named for the worker,
/// not the session), so a queue is polled from its first session's start
/// until its last session's end: one poller per queue, stopped when its last
/// session is removed, not at the driver's shutdown.
#[derive(Default)]
struct SessionQueues(parking_lot::Mutex<SessionQueuesInner>);

#[derive(Default)]
struct SessionQueuesInner {
    /// Each queue with sessions: how many, and what stops its poller.
    open: HashMap<String, (usize, tokio::sync::watch::Sender<bool>)>,
    /// Each queue with a poller running, and how many. A poller stopped
    /// because its sessions ended may still have a poll out; until it ends,
    /// its queue is named when the driver tells the engine it is shutting
    /// down, so the engine ends that poll too.
    polled: HashMap<String, usize>,
}

impl SessionQueues {
    /// Counts a session on `queue`. Returns the stop for a new poller when
    /// the queue had no session; `None` when it is already polled.
    fn add(&self, queue: &str) -> Option<tokio::sync::watch::Sender<bool>> {
        let mut inner = self.0.lock();
        if let Some((sessions, _)) = inner.open.get_mut(queue) {
            *sessions += 1;
            return None;
        }
        let (stop, _) = tokio::sync::watch::channel(false);
        inner.open.insert(queue.to_string(), (1, stop.clone()));
        *inner.polled.entry(queue.to_string()).or_default() += 1;
        Some(stop)
    }

    /// Counts a session off `queue`, stopping its poller when it was the
    /// last. Returns whether the poller was stopped.
    fn remove(&self, queue: &str) -> bool {
        let mut inner = self.0.lock();
        let Some((sessions, _)) = inner.open.get_mut(queue) else {
            return false;
        };
        *sessions -= 1;
        if *sessions > 0 {
            return false;
        }
        if let Some((_, stop)) = inner.open.remove(queue) {
            let _ = stop.send(true);
        }
        true
    }

    /// A poller on `queue` has ended, or never started.
    fn poller_ended(&self, queue: &str) {
        let mut inner = self.0.lock();
        if let Some(pollers) = inner.polled.get_mut(queue) {
            *pollers -= 1;
            if *pollers == 0 {
                inner.polled.remove(queue);
            }
        }
    }

    /// The queues with a poller running.
    fn polled(&self) -> Vec<String> {
        let mut queues: Vec<String> = self.0.lock().polled.keys().cloned().collect();
        queues.sort();
        queues
    }
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
    /// The session queues polled, with the sessions using each and what
    /// stops its poller.
    session_queues: Arc<SessionQueues>,
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
        .with_wire_metrics(std::sync::Arc::clone(&config.wire))
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
        // One scaler for all of them: an idle driver holds one poll, a busy
        // one up to `poller_count`.
        let scaler =
            std::sync::Arc::new(crate::worker::poll::scaler::PollerScaler::new(poller_count));
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
                wire: std::sync::Arc::clone(&config.wire),
            };

            let poller = TaskExecutionPoller::new(
                poller_config,
                &config.server_url,
                shutdown_sender.clone(),
                task_sender.clone(),
            )
            .await?
            .with_scaler(std::sync::Arc::clone(&scaler), poller_idx);

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
        let Some(stop) = self.session_queues.add(&session_queue) else {
            return Ok(());
        };
        let stopped = follow_shutdown(&self.shutdown_sender, &stop);
        match session_poller(&self.config, stop, task_sender, &session_queue).await {
            Ok(poller) => {
                tokio::spawn(run_session_poller(
                    poller,
                    session_queue,
                    Arc::clone(&self.session_queues),
                    stopped,
                ));
                Ok(())
            }
            Err(e) => {
                self.session_queues.remove(&session_queue);
                self.session_queues.poller_ended(&session_queue);
                Err(e)
            }
        }
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
        let Some(stop) = self.session_queues.add(&session_queue) else {
            return;
        };
        let stopped = follow_shutdown(&self.shutdown_sender, &stop);
        let config = Arc::clone(&self.config);
        let queues = Arc::clone(&self.session_queues);
        tokio::spawn(async move {
            match session_poller(&config, stop, task_sender, &session_queue).await {
                Ok(poller) => run_session_poller(poller, session_queue, queues, stopped).await,
                Err(e) => {
                    tracing::error!(
                        session_queue = %session_queue,
                        error = %e,
                        "Failed to add session queue poller"
                    );
                    queues.remove(&session_queue);
                    queues.poller_ended(&session_queue);
                }
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

        let slots = SlotBudget::for_reports(
            self.completion_retry.max_in_flight,
            self.config.max_concurrent_executions,
        );
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
        let mut slot: Option<SlotPermit> = None;
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
                        let mut queues = self.session_queues.polled();
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
                            // A poll already out is left to finish, as at
                            // shutdown: the engine may have started the task
                            // it carries, which is still handed over.
                            if self.session_queues.remove(&queue) {
                                tracing::info!(
                                    session_queue = %queue,
                                    "Last session on the queue ended; its poller stops"
                                );
                            }
                        }
                        None => sessions_closed = true,
                    }
                }

                // Reap finished reports so the set does not grow without bound.
                Some(finished) = in_flight.join_next(), if !in_flight.is_empty() => {
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

    /// Return where a [`WorkflowDriver`](super::WorkflowDriver) should hand the tasks the engine
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
        wire: std::sync::Arc::clone(&config.wire),
    };
    TaskExecutionPoller::new(
        poller_config,
        &config.server_url,
        shutdown_sender,
        task_sender,
    )
    .await
}

/// Stops a session poller when the driver shuts down. The poller's own stop
/// is separate, so its queue's last session ending stops it alone. Returns
/// what the poller holds while it runs: once it is dropped, this stops
/// following.
fn follow_shutdown(
    driver: &tokio::sync::watch::Sender<bool>,
    stop: &tokio::sync::watch::Sender<bool>,
) -> tokio::sync::watch::Receiver<bool> {
    let mut driver = driver.subscribe();
    let stop = stop.clone();
    let running = stop.subscribe();
    tokio::spawn(async move {
        tokio::select! {
            _ = driver.wait_for(|stopped| *stopped) => {
                let _ = stop.send(true);
            }
            _ = stop.closed() => {}
        }
    });
    running
}

async fn run_session_poller(
    mut poller: TaskExecutionPoller,
    session_queue: String,
    queues: Arc<SessionQueues>,
    running: tokio::sync::watch::Receiver<bool>,
) {
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
    queues.poller_ended(&session_queue);
    drop(poller);
    drop(running);
    tracing::info!(session_queue = %session_queue, "Session queue poller stopped");
}

#[cfg(test)]
#[path = "task_tests.rs"]
mod engine_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_task_driver_config_default() {
        let config = TaskDriverConfig::default();
        assert_eq!(config.namespace, "default");
        assert_eq!(config.task_queue, "default");
        assert_eq!(config.max_concurrent_executions, 100);
        assert_eq!(config.poller_count, 4);
        assert!(config.enable_heartbeat);
    }
}
