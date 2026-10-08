//! Worker registration driver for SDK workers.
//!
//! Handles the worker lifecycle: register on startup, send periodic heartbeats,
//! and deregister on shutdown. The server uses this data to show which workers
//! are live and how loaded they are.
//!
//! Unlike `WorkflowDriver`/`TaskDriver`, this driver does no polling — it only
//! manages the registration lifecycle.

use crate::poller::metrics::WorkerMetrics;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

use crate::error::{Error, Result};
use crate::poller::channel::ChannelManager;
use crate::poller::driver::ShutdownHandle;
use crate::proto::orcher::v1::{
    worker_service_client::WorkerServiceClient, DeregisterWorkerRequest, RegisterWorkerRequest,
    WorkerCapabilities, WorkerHeartbeatRequest, WorkerLoadMetrics, WorkerRegistrationStatus,
};

/// Configuration for worker registration.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct WorkerRegistrationConfig {
    /// Server address.
    pub server_url: String,

    /// Unique identifier of this service instance.
    pub service_id: String,

    /// Human-readable worker identity.
    pub identity: String,

    /// Task queue this worker polls.
    pub task_queue: String,

    /// Namespace the worker belongs to.
    pub namespace: String,

    /// Workflow types this worker can execute.
    pub workflow_types: Vec<String>,

    /// Task types this worker can execute.
    pub task_types: Vec<String>,

    /// Maximum concurrent workflow executions.
    pub max_concurrent_workflows: u32,

    /// Maximum concurrent task executions.
    pub max_concurrent_tasks: u32,

    /// Interval between worker heartbeats. Defaults to 10 seconds.
    pub heartbeat_interval: Duration,

    /// Additional metadata, such as SDK version, build id and host info.
    pub metadata: HashMap<String, String>,

    /// The code release this worker is running, if known.
    ///
    /// Declared at registration as well as on each poll, so the server can list
    /// which releases are live without waiting for a poll. Opaque; see
    /// `PollerConfig::version_id`.
    pub version_id: Option<String>,

    /// TLS configuration; `None` connects without TLS.
    pub tls_config: Option<super::TlsConfig>,

    /// API key sent as `authorization: Bearer <key>` on registration,
    /// heartbeat and deregistration.
    ///
    /// It must match the key the pollers send: a server that requires a key
    /// rejects registration without one, and registration failures are only
    /// logged.
    pub api_key: Option<String>,

    /// Organization sent as `x-organization-id`, matching the pollers.
    pub organization_id: Option<String>,

    /// The worker protocol version declared at registration; see
    /// [`crate::worker_protocol`]. Default: [`crate::WORKER_PROTOCOL_VERSION`].
    ///
    /// For observability: the engine decides per activation from
    /// [`WorkflowDriverConfig::protocol_version`](crate::WorkflowDriverConfig::protocol_version),
    /// which should be the same.
    pub protocol_version: u32,
}

/// Registration authenticates exactly as polling and reporting do; see
/// [`crate::poller::credentials::credentialed_request`]. Re-exported here for
/// callers that import it from this module.
pub use crate::poller::credentials::credentialed_request;

impl Default for WorkerRegistrationConfig {
    fn default() -> Self {
        Self {
            server_url: "http://localhost:50051".to_string(),
            service_id: uuid::Uuid::new_v4().to_string(),
            identity: format!("worker-{}", uuid::Uuid::new_v4()),
            task_queue: "default".to_string(),
            namespace: "default".to_string(),
            workflow_types: Vec::new(),
            task_types: Vec::new(),
            max_concurrent_workflows: 100,
            max_concurrent_tasks: 200,
            heartbeat_interval: Duration::from_secs(10),
            metadata: HashMap::new(),
            tls_config: None,
            version_id: None,
            api_key: None,
            organization_id: None,
            protocol_version: crate::WORKER_PROTOCOL_VERSION,
        }
    }
}

/// How long [`WorkerRegistrationDriver::run`] spends deregistering on the way
/// out, connecting included.
///
/// Deregistration is a courtesy: the server drops a registration that stops
/// heartbeating anyway. So an unreachable or slow server must not hold up the
/// worker's shutdown for longer than this.
pub const DEREGISTER_TIMEOUT: Duration = Duration::from_secs(5);

/// Drives a worker's registration: register, heartbeat, then deregister.
///
/// [`run`](Self::run) borrows the driver for as long as it runs, so take a
/// [`shutdown_handle`](Self::shutdown_handle) before starting it and stop it
/// through that. Aborting the task that runs it instead skips deregistration,
/// and the server keeps listing the worker until the registration expires.
pub struct WorkerRegistrationDriver {
    config: Arc<WorkerRegistrationConfig>,
    channel_manager: Arc<tokio::sync::Mutex<ChannelManager>>,
    registration_id: Option<String>,
    shutdown_sender: tokio::sync::watch::Sender<bool>,
    shutdown: tokio::sync::watch::Receiver<bool>,

    /// What this worker is running, shared with the drivers that run it.
    metrics: Arc<WorkerMetrics>,
}

impl WorkerRegistrationDriver {
    /// Shares the worker's counters with this driver.
    ///
    /// The heartbeat then reports what the work drivers are actually running.
    /// Without this the heartbeat reports zeros.
    pub fn with_metrics(mut self, metrics: Arc<WorkerMetrics>) -> Self {
        self.metrics = metrics;
        self
    }

    /// Creates a driver and registers the worker with the server.
    ///
    /// Registration is best-effort: if it fails, the failure is logged and
    /// the driver is still returned, and the heartbeat loop retries
    /// registration on each tick.
    ///
    /// # Errors
    ///
    /// Registration failures are not returned, so this always returns `Ok`.
    pub async fn new(config: WorkerRegistrationConfig) -> Result<Self> {
        let config = Arc::new(config);
        let channel_manager = match config.tls_config {
            Some(ref tls) => ChannelManager::with_tls(config.server_url.clone(), tls.clone()),
            None => ChannelManager::new(config.server_url.clone()),
        };
        let (shutdown_sender, shutdown) = tokio::sync::watch::channel(false);

        let mut driver = Self {
            config,
            channel_manager: Arc::new(tokio::sync::Mutex::new(channel_manager)),
            registration_id: None,
            shutdown_sender,
            shutdown,
            metrics: WorkerMetrics::new(),
        };

        // Best-effort; `send_heartbeat` retries while unregistered.
        if let Err(e) = driver.register().await {
            tracing::warn!(
                service_id = %driver.config.service_id,
                error = %e,
                "Initial worker registration failed — will retry on heartbeat"
            );
        }

        Ok(driver)
    }

    /// Register (or re-register) with the server.
    async fn register(&mut self) -> Result<()> {
        let request = RegisterWorkerRequest {
            service_id: self.config.service_id.clone(),
            identity: self.config.identity.clone(),
            task_queue: self.config.task_queue.clone(),
            namespace: self.config.namespace.clone(),
            capabilities: Some(WorkerCapabilities {
                workflow_types: self.config.workflow_types.clone(),
                task_types: self.config.task_types.clone(),
                max_concurrent_workflows: self.config.max_concurrent_workflows,
                max_concurrent_tasks: self.config.max_concurrent_tasks,
                protocol_version: self.config.protocol_version,
                ..Default::default()
            }),
            metadata: self.config.metadata.clone(),
            version_id: self.config.version_id.clone().unwrap_or_default(),
            ..Default::default()
        };

        let channel = {
            let mut mgr = self.channel_manager.lock().await;
            mgr.get().await.map_err(|e| {
                Error::Connection(format!("Failed to get channel for registration: {}", e))
            })?
        };

        let mut client = WorkerServiceClient::new(channel);
        let response = client
            .register_worker(credentialed_request(
                request,
                self.config.api_key.as_deref(),
                self.config.organization_id.as_deref(),
            ))
            .await
            .map_err(|e| Error::Connection(format!("RegisterWorker RPC failed: {}", e)))?
            .into_inner();

        if response.success {
            tracing::info!(
                service_id = %self.config.service_id,
                registration_id = %response.registration_id,
                "Worker registered successfully"
            );
            self.registration_id = Some(response.registration_id);
            Ok(())
        } else {
            Err(Error::Connection(format!(
                "RegisterWorker rejected: {}",
                response.error_message
            )))
        }
    }

    /// Runs the heartbeat loop until [`shutdown`](Self::shutdown) is called,
    /// or [`ShutdownHandle::shutdown`] on a handle from
    /// [`shutdown_handle`](Self::shutdown_handle).
    ///
    /// On shutdown the worker is deregistered, within
    /// [`DEREGISTER_TIMEOUT`]. A failed or timed-out deregistration is logged,
    /// not returned.
    pub async fn run(&mut self) -> Result<()> {
        tracing::info!(
            service_id = %self.config.service_id,
            heartbeat_interval_secs = self.config.heartbeat_interval.as_secs(),
            "Starting worker registration heartbeat loop"
        );

        let mut ticker = tokio::time::interval(self.config.heartbeat_interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        ticker.tick().await; // the first tick fires immediately; skip it

        loop {
            tokio::select! {
                _ = self.shutdown.changed() => {
                    if *self.shutdown.borrow_and_update() {
                        tracing::info!(
                            service_id = %self.config.service_id,
                            "Worker registration driver shutdown requested"
                        );
                        break;
                    }
                }

                _ = ticker.tick() => {
                    self.send_heartbeat().await;
                }
            }
        }

        match tokio::time::timeout(DEREGISTER_TIMEOUT, self.deregister()).await {
            Ok(Ok(())) => {}
            Ok(Err(e)) => tracing::warn!(
                service_id = %self.config.service_id,
                error = %e,
                "Failed to deregister worker on shutdown"
            ),
            Err(_) => tracing::warn!(
                service_id = %self.config.service_id,
                timeout_secs = DEREGISTER_TIMEOUT.as_secs(),
                "Worker deregistration timed out on shutdown"
            ),
        }

        Ok(())
    }

    /// Send a heartbeat to the server.
    async fn send_heartbeat(&mut self) {
        let registration_id = match &self.registration_id {
            Some(id) => id.clone(),
            None => {
                // Not registered yet: register instead of heartbeating.
                if let Err(e) = self.register().await {
                    tracing::warn!(
                        service_id = %self.config.service_id,
                        error = %e,
                        "Re-registration attempt failed"
                    );
                }
                return;
            }
        };

        let request = WorkerHeartbeatRequest {
            service_id: self.config.service_id.clone(),
            registration_id,
            status: WorkerRegistrationStatus::Healthy as i32,
            metrics: Some({
                let m = self.metrics.snapshot();
                WorkerLoadMetrics {
                    workflows_in_progress: m.workflows_in_progress as u32,
                    workflows_completed: m.workflows_completed,
                    workflows_failed: m.workflows_failed,
                    tasks_in_progress: m.tasks_in_progress as u32,
                    tasks_completed: m.tasks_completed,
                    tasks_failed: m.tasks_failed,
                    // Process-level usage is not collected; the server treats
                    // zero as "not reported" rather than idle.
                    cpu_usage: 0.0,
                    memory_usage_bytes: 0,
                    ..Default::default()
                }
            }),
            ..Default::default()
        };

        let result = {
            let channel = {
                let mut mgr = self.channel_manager.lock().await;
                match mgr.get().await {
                    Ok(ch) => ch,
                    Err(e) => {
                        tracing::warn!(
                            service_id = %self.config.service_id,
                            error = %e,
                            "Failed to get channel for heartbeat"
                        );
                        return;
                    }
                }
            };
            let mut client = WorkerServiceClient::new(channel);
            client
                .worker_heartbeat(credentialed_request(
                    request,
                    self.config.api_key.as_deref(),
                    self.config.organization_id.as_deref(),
                ))
                .await
        };

        match result {
            Ok(response) => {
                let resp = response.into_inner();
                if resp.re_register {
                    tracing::warn!(
                        service_id = %self.config.service_id,
                        "Server requested re-registration"
                    );
                    self.registration_id = None;
                    if let Err(e) = self.register().await {
                        tracing::warn!(
                            service_id = %self.config.service_id,
                            error = %e,
                            "Re-registration failed"
                        );
                    }
                } else {
                    tracing::debug!(
                        service_id = %self.config.service_id,
                        "Worker heartbeat sent"
                    );
                }
            }
            Err(e) => {
                tracing::warn!(
                    service_id = %self.config.service_id,
                    error = %e,
                    "Worker heartbeat failed"
                );
                let mut mgr = self.channel_manager.lock().await;
                mgr.record_failure();
            }
        }
    }

    /// Deregisters from the server; called on shutdown.
    async fn deregister(&self) -> Result<()> {
        let registration_id = match &self.registration_id {
            Some(id) => id.clone(),
            None => return Ok(()), // never registered, nothing to deregister
        };

        let request = DeregisterWorkerRequest {
            service_id: self.config.service_id.clone(),
            registration_id,
            ..Default::default()
        };

        let channel = {
            let mut mgr = self.channel_manager.lock().await;
            mgr.get().await.map_err(|e| {
                Error::Connection(format!("Failed to get channel for deregistration: {}", e))
            })?
        };

        let mut client = WorkerServiceClient::new(channel);
        client
            .deregister_worker(credentialed_request(
                request,
                self.config.api_key.as_deref(),
                self.config.organization_id.as_deref(),
            ))
            .await
            .map_err(|e| Error::Connection(format!("DeregisterWorker RPC failed: {}", e)))?;

        tracing::info!(
            service_id = %self.config.service_id,
            "Worker deregistered successfully"
        );
        Ok(())
    }

    /// Signals the driver to shut down.
    pub fn shutdown(&self) {
        let _ = self.shutdown_sender.send(true);
    }

    /// Returns a handle that stops this driver from outside.
    ///
    /// [`run`](Self::run) holds `&mut self`, so once it is running (usually in
    /// a spawned task) this handle is the only way to stop it gracefully: the
    /// loop ends and the worker deregisters. A handle used before `run` starts
    /// still takes effect, and `run` deregisters and returns at once.
    pub fn shutdown_handle(&self) -> ShutdownHandle {
        ShutdownHandle::from_sender(self.shutdown_sender.clone())
    }

    /// Returns the registration ID, if the worker is registered.
    pub fn registration_id(&self) -> Option<&str> {
        self.registration_id.as_deref()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_config_default() {
        let config = WorkerRegistrationConfig::default();
        assert_eq!(config.task_queue, "default");
        assert_eq!(config.namespace, "default");
        assert_eq!(config.heartbeat_interval, Duration::from_secs(10));
        assert_eq!(config.max_concurrent_workflows, 100);
        assert_eq!(config.max_concurrent_tasks, 200);
    }

    #[test]
    fn registration_carries_the_same_credentials_as_polling() {
        let request = credentialed_request((), Some("orch_abc"), Some("org_123"));
        let md = request.metadata();
        assert_eq!(md.get("authorization").unwrap(), "Bearer orch_abc");
        assert_eq!(md.get("x-organization-id").unwrap(), "org_123");
    }
}
