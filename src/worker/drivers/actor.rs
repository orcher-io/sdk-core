//! The actor driver: polls for actor operations, hands them to the language
//! SDK and reports their results, and sends the actor worker's heartbeat.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;

use crate::error::Result;
use crate::transport::channel::{Breaker, ChannelManager};
use crate::worker::governor::{InFlight, SlotBudget, SlotPermit};
use crate::worker::poll::completion::{
    note_connection_failure, Caller, Completer, CompletionRetryConfig,
};
use crate::worker::poll::polling::{ActorOperationPoller, ActorPollerConfig};
use orcher_proto::orcher::v1::{
    actor_service_client::ActorServiceClient, ActorOperation, CompleteActorOperationRequest,
    ExecutionStatus, HeartbeatRequest, WorkerMetrics, WorkerStatus,
};

use super::ShutdownHandle;

/// Sets the registration a running [`ActorDriver`]'s heartbeats name, when
/// the actor service has registered the worker's handlers again.
#[derive(Clone, Debug)]
pub struct RegistrationHandle(Arc<parking_lot::Mutex<Option<String>>>);

impl RegistrationHandle {
    /// From now on, heartbeats name `registration_id`.
    pub fn set(&self, registration_id: impl Into<String>) {
        *self.0.lock() = Some(registration_id.into());
    }

    /// The registration heartbeats name.
    pub fn get(&self) -> Option<String> {
        self.0.lock().clone()
    }
}

/// Events emitted by the ActorDriver to notify the language SDK.
#[derive(Debug, Clone)]
pub enum ActorDriverEvent {
    /// Server requested re-registration of actor handlers.
    ReRegistrationRequired,
}

/// Configuration for the actor driver.
#[derive(Clone)]
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
    pub tls_config: Option<crate::transport::TlsConfig>,

    /// How long a connect may take.
    pub connect_timeout: std::time::Duration,

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
impl std::fmt::Debug for ActorDriverConfig {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            server_url,
            namespace,
            service_id,
            max_concurrent_executions,
            identity,
            poll_timeout,
            poller_count,
            organization_id,
            api_key,
            enable_heartbeat,
            heartbeat_interval,
            registration_id,
            tls_config,
            connect_timeout,
            max_message_bytes,
            wire: _,
        } = self;
        f.debug_struct("ActorDriverConfig")
            .field("server_url", server_url)
            .field("namespace", namespace)
            .field("service_id", service_id)
            .field("max_concurrent_executions", max_concurrent_executions)
            .field("identity", identity)
            .field("poll_timeout", poll_timeout)
            .field("poller_count", poller_count)
            .field("organization_id", organization_id)
            .field("api_key", &crate::transport::credentials::redacted(api_key))
            .field("enable_heartbeat", enable_heartbeat)
            .field("heartbeat_interval", heartbeat_interval)
            .field("registration_id", registration_id)
            .field("tls_config", tls_config)
            .field("connect_timeout", connect_timeout)
            .field("max_message_bytes", max_message_bytes)
            .finish()
    }
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
            connect_timeout: crate::client::connection::DEFAULT_CONNECT_TIMEOUT,
            max_message_bytes: crate::transport::limits::default_max_message_bytes(),
            wire: crate::observability::WireMetrics::new(),
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
/// It follows the same pattern as [`TaskDriver`](super::TaskDriver): no replay, no cache, plain
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

    /// The registration heartbeats name; replaced on re-registration.
    registration: RegistrationHandle,

    /// When each operation handed to the language SDK was handed over, by
    /// operation id, so its report says how long it ran.
    handed_at: HashMap<String, Instant>,
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
        .with_connect_timeout(config.connect_timeout)
        .with_wire_metrics(std::sync::Arc::clone(&config.wire))
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
        // One scaler for all of them: an idle driver holds one poll, a busy
        // one up to `poller_count`.
        let scaler =
            std::sync::Arc::new(crate::worker::poll::scaler::PollerScaler::new(poller_count));
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
                connect_timeout: config.connect_timeout,
                max_message_bytes: config.max_message_bytes,
                wire: std::sync::Arc::clone(&config.wire),
            };

            let poller = ActorOperationPoller::new(
                poller_config,
                &config.server_url,
                shutdown_sender.clone(),
                operation_sender.clone(),
            )
            .await?
            .with_scaler(std::sync::Arc::clone(&scaler), poller_idx);

            let poller_id = poller_idx;
            tokio::spawn(async move {
                let mut poller_mut = poller;
                if let Err(e) = poller_mut.poll_loop().await {
                    tracing::error!(poller_id = poller_id, error = %e, "Actor driver poller error");
                }
            });
        }

        let registration = RegistrationHandle(Arc::new(parking_lot::Mutex::new(
            config.registration_id.clone(),
        )));
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
            registration,
            handed_at: HashMap::new(),
        };

        Ok((driver, work_receiver, result_sender, event_receiver))
    }

    /// Return a handle that stops this driver's [`run`](Self::run) from
    /// elsewhere. Take it before handing the driver to the task that runs
    /// it: `run` borrows the driver for as long as it runs.
    pub fn shutdown_handle(&self) -> ShutdownHandle {
        ShutdownHandle(self.shutdown_sender.clone())
    }

    /// Return a handle that changes the registration this driver's
    /// heartbeats name, for when the worker's handlers are registered again
    /// after [`ActorDriverEvent::ReRegistrationRequired`]. Without it the
    /// heartbeats keep naming the registration the actor service dropped.
    pub fn registration_handle(&self) -> RegistrationHandle {
        self.registration.clone()
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
    /// sender and no more operations can reach it. On shutdown its pollers
    /// stop at once, abandoning polls in flight, and it keeps taking the
    /// results of the operations already handed over until they are all in or
    /// the shutdown grace is over.
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

        let slots = SlotBudget::for_reports(
            self.completion_retry.max_in_flight,
            self.config.max_concurrent_executions,
        );
        // Each report is sent, and retried, from its own task. Retried
        // inline, one report would hold up every other result and the
        // hand-off of new operations for as long as it kept trying.
        let mut in_flight = InFlight::new("Actor driver", "actor operation completions");
        let completer = self.completer();
        // A slot for the next report, taken before a result is accepted, so
        // a backlog of reports holds the language SDK back rather than
        // growing without bound.
        let mut slot: Option<SlotPermit> = None;
        // Room in the work channel, reserved before an operation is taken, so
        // handing one over never waits. The task driver's run loop explains why.
        let mut room: Option<mpsc::OwnedPermit<ActorWork>> = None;
        let (mut operations_closed, mut results_closed, mut work_closed) = (false, false, false);
        // The drain deadline, set once shutdown is asked for. The pollers
        // stop at once (they share the signal); operations already handed
        // over still have their results taken and reported until then.
        let mut draining: Option<tokio::time::Instant> = None;

        loop {
            tokio::select! {
                // Unlike the workflow and task drivers, this driver does not
                // tell the engine it is shutting down. The engine's shutdown
                // covers workflow and task polls only, and this driver polls
                // neither, so its notice would name no queue. A notice naming
                // no queue covers every queue of this identity, which would
                // end the polls of the process's other drivers while they may
                // still be draining. Its pollers stop at once anyway,
                // abandoning their polls.
                _ = self.shutdown.changed(), if draining.is_none() => {
                    if *self.shutdown.borrow_and_update() {
                        tracing::info!(
                            outstanding = self.handed_at.len(),
                            grace_ms = self.completion_retry.shutdown_grace.as_millis() as u64,
                            "Actor driver shutdown requested; waiting for the results of the \
                             operations handed over"
                        );
                        draining = Some(
                            tokio::time::Instant::now() + self.completion_retry.shutdown_grace,
                        );
                    }
                }

                _ = tokio::time::sleep_until(draining.unwrap_or_else(tokio::time::Instant::now)),
                    if draining.is_some() =>
                {
                    tracing::warn!(
                        outstanding = self.handed_at.len(),
                        "Shutdown grace over; stopping the actor driver"
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

                // Receive operations from the pollers and hand them to the
                // language SDK.
                operation = self.operation_receiver.recv(),
                    if room.is_some() && !operations_closed =>
                {
                    match (operation, room.take()) {
                        (Some(operation), Some(room)) => {
                            let op_id = operation.operation_id.clone();
                            self.handed_at.insert(op_id.clone(), Instant::now());
                            if room.send(ActorWork { operation }).is_closed() {
                                self.handed_at.remove(&op_id);
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
                Some(finished) = in_flight.join_next(), if !in_flight.is_empty() => {
                    in_flight.reaped(finished);
                }
            }

            if results_closed && (operations_closed || work_closed) {
                tracing::info!("All actor driver channels closed, shutting down");
                break;
            }

            // Drained: every operation handed over has been answered.
            if draining.is_some() && (self.handed_at.is_empty() || results_closed) {
                tracing::info!("Actor driver drained");
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
        // One grace covers the whole shutdown, draining included.
        let stop_by = draining
            .unwrap_or_else(|| tokio::time::Instant::now() + self.completion_retry.shutdown_grace);
        in_flight.finish(stop_by).await;

        tracing::info!("Actor driver stopped");
        Ok(())
    }

    /// Count an actor operation's result from the language SDK, and build
    /// the report to send for it.
    /// The report of an operation's result. A failure the language SDK
    /// built as a [`TaskFailure`](crate::TaskFailure) is reported with its
    /// type as the error code; any other error with core's own code.
    fn handle_actor_result(&mut self, result: ActorWorkResult) -> CompleteActorOperationRequest {
        self.operations_in_progress.fetch_sub(1, Ordering::Relaxed);
        let duration_ms = self
            .handed_at
            .remove(&result.operation_id)
            .map_or(0, |handed| handed.elapsed().as_millis() as u64);
        let (output, status, error_message, error_code) = match result.result {
            Ok(output) => {
                self.operations_completed.fetch_add(1, Ordering::Relaxed);
                (
                    output,
                    ExecutionStatus::Success,
                    String::new(),
                    String::new(),
                )
            }
            Err(e) => {
                self.operations_failed.fetch_add(1, Ordering::Relaxed);
                let error_code = match e.task_failure() {
                    Some(failure) => failure.error_type.clone().unwrap_or_default(),
                    None => e.code().as_str().to_string(),
                };
                (vec![], ExecutionStatus::Error, e.to_string(), error_code)
            }
        };
        CompleteActorOperationRequest {
            service_id: self.config.service_id.clone(),
            operation_id: result.operation_id,
            execution_id: result.execution_id,
            result: output,
            status: status as i32,
            error_message,
            error_code,
            duration_ms,
            ..Default::default()
        }
    }

    /// Attach the worker's credentials to an outbound request.
    ///
    /// Polling carries these headers already; reporting the outcome has to
    /// carry them too, or a server with authentication enabled answers
    /// Unauthenticated and the result is dropped.
    fn credentialed<T>(&self, body: T) -> tonic::Request<T> {
        crate::transport::credentials::credentialed_request(
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
            registration_id: self.registration.get().unwrap_or_default(),
            status: WorkerStatus::Healthy as i32,
            metrics: Some(WorkerMetrics {
                operations_in_progress: in_progress as u32,
                operations_completed: completed,
                operations_failed: failed,
                avg_duration_ms: 0.0,
                cpu_usage: 0.0,
                memory_usage_bytes: 0,
                ..Default::default()
            }),
            ..Default::default()
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
}

#[cfg(test)]
mod tests {
    use super::*;

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
