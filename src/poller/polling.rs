//! Pollers that long-poll the server for work.
//!
//! There is one poller for each kind of work:
//! - Workflow execution steps
//! - Task executions
//! - Actor operations
//!
//! ## Terminology
//!
//! - **Worker**: a process that polls for and executes workflows and tasks.
//! - **Task**: a unit of work that a workflow schedules.
//! - **Execution step**: a point where the workflow's code runs to decide what happens next.

use std::sync::Arc;
use std::time::Duration;

/// How long a poller waits before retrying after an error it has no specific
/// handling for.
///
/// Doubles from two seconds and settles at thirty. A proxy between the worker
/// and a restarting engine can answer with a non-gRPC error (an HTML 502 shows
/// up as `Internal` "protocol error") for a minute or more, so the first few
/// retries are quick and later ones stop hammering. There is deliberately no
/// ceiling on the count: a poller never gives up.
pub(crate) fn unexpected_poll_error_backoff(consecutive_errors: u32) -> Duration {
    const FIRST: u64 = 2_000;
    const CAP: u64 = 30_000;
    let doublings = consecutive_errors.saturating_sub(1).min(4);
    Duration::from_millis((FIRST << doublings).min(CAP))
}
use tokio::sync::mpsc;
use tonic::transport::Channel;

use crate::error::{Error, Result};
use crate::proto::orcher::v1::{
    actor_service_client::ActorServiceClient, execution_service_client::ExecutionServiceClient,
    ActorOperation, JournalEntry, PollActorOperationRequest, PollTaskExecutionRequest,
    PollWorkflowExecutionRequest, QueryRequest, UpdateRequest,
};
use crate::types::WorkflowExecution;

/// Configuration for a poller.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct PollerConfig {
    /// Namespace to poll from.
    pub namespace: String,

    /// Task queue to poll from.
    pub task_queue: String,

    /// How long the server may hold a poll open, in seconds.
    pub poll_timeout_seconds: i64,

    /// Maximum concurrent polls.
    #[allow(dead_code)]
    pub max_concurrent_polls: usize,

    /// Identity this poller reports to the server.
    pub identity: String,

    /// Organization ID, for a multi-tenant server.
    ///
    /// When set, the poller sends it in the `x-organization-id` header of
    /// every request.
    pub organization_id: Option<String>,

    /// API key for server authentication.
    ///
    /// When set, the poller sends an `authorization: Bearer <key>` header in
    /// every gRPC request.
    pub api_key: Option<String>,

    /// TLS for the poller's own connection.
    ///
    /// The driver that owns this poller connects with the same setting. A
    /// poller connecting without it would dial a TLS server in plaintext and
    /// fail at the handshake, while the driver reports itself connected.
    pub tls_config: Option<super::TlsConfig>,

    /// The code release this worker is running, if declared.
    ///
    /// An opaque label — a git sha, an image digest, a release tag. The server
    /// never parses or orders it; it records the value on a workflow execution
    /// the first time this worker claims it, so the execution keeps running
    /// against the code it started on.
    ///
    /// Normally comes from the build rather than being written by hand, so
    /// [`PollerConfig::from_env`] reads `ORCHER_VERSION_ID`. Leave unset and
    /// nothing is declared, which is the default and changes no behavior.
    pub version_id: Option<String>,

    /// Whether to tell the engine that this poller's tasks are heartbeated automatically.
    ///
    /// When set, each task poll tells the engine that the tasks it brings back
    /// are heartbeated on a timer until their results are sent. The
    /// [`TaskDriver`](crate::poller::TaskDriver) does that when its own
    /// `auto_heartbeat` is on, and sets this to match. The engine then holds a
    /// task that sets no heartbeat timeout to a default one, so a poller used
    /// on its own must leave this off (the default) unless whatever runs its
    /// tasks heartbeats them.
    pub auto_heartbeat: bool,

    /// The largest message the poller receives, and tells the engine it can
    /// receive. [`crate::limits::default_max_message_bytes`] unless set.
    pub max_message_bytes: usize,
}

impl Default for PollerConfig {
    fn default() -> Self {
        Self {
            namespace: "default".to_string(),
            task_queue: "default".to_string(),
            // The server holds an idle poll for up to 30 seconds; 35 adds a
            // buffer. The other language SDKs use the same value, so all
            // workers behave alike. Shorter timeouts wake up more often and
            // raise idle CPU usage.
            poll_timeout_seconds: 35,
            max_concurrent_polls: 5,
            identity: format!("rust-sdk-{}", uuid::Uuid::new_v4()),
            organization_id: None,
            api_key: None,
            version_id: None,
            tls_config: None,
            auto_heartbeat: false,
            max_message_bytes: crate::limits::default_max_message_bytes(),
        }
    }
}

impl PollerConfig {
    /// Creates a configuration from environment variables.
    ///
    /// Reads `ORCHER_VERSION_ID` into [`version_id`](Self::version_id); a
    /// blank value counts as unset. Every other field takes its default.
    pub fn from_env() -> Self {
        Self {
            // The release identity almost always comes from CI — a git sha or an
            // image digest — so reading it from the environment is the path that
            // actually gets used. Empty is treated as unset.
            version_id: std::env::var("ORCHER_VERSION_ID")
                .ok()
                .filter(|v| !v.trim().is_empty()),
            ..Self::default()
        }
    }

    /// Declares the code release this worker is running.
    ///
    /// A blank value leaves the release undeclared.
    pub fn with_version_id(mut self, version_id: impl Into<String>) -> Self {
        let v = version_id.into();
        self.version_id = if v.trim().is_empty() { None } else { Some(v) };
        self
    }
}

/// A workflow execution step polled from the server, waiting to be processed.
#[derive(Debug, Clone)]
pub struct WorkflowExecutionTask {
    /// Workflow execution identifier.
    pub execution: WorkflowExecution,

    /// Workflow type.
    pub workflow_type: String,

    /// Task queue the step was polled from.
    #[allow(dead_code)]
    pub task_queue: String,

    /// Workflow input parameters.
    pub input: serde_json::Value,

    /// Execution journal entries.
    pub journal: Vec<JournalEntry>,

    /// The activation token to report this activation's outcome with.
    ///
    /// The same bytes as `stream_entry_id`; empty from an engine that issues
    /// no activation tokens.
    pub task_token: Vec<u8>,

    /// Started event ID.
    #[allow(dead_code)]
    pub started_event_id: i64,

    /// Previous started event ID.
    #[allow(dead_code)]
    pub previous_started_event_id: i64,

    /// Attempt number.
    pub attempt: i32,

    /// The activation token, as the engine returned it on the poll.
    ///
    /// Passed back in `CompleteWorkflowExecutionRequest` so the engine applies
    /// the completion once and only for this activation.
    pub stream_entry_id: Option<String>,

    /// Pending queries to answer during this execution step.
    pub queries: Vec<QueryRequest>,

    /// Pending updates to process during this execution step.
    pub updates: Vec<UpdateRequest>,
}

/// A task execution polled from the server, waiting to be processed.
#[derive(Debug, Clone)]
pub struct TaskExecutionTask {
    /// The workflow execution that scheduled the task.
    pub execution: WorkflowExecution,

    /// Task ID.
    pub task_id: String,
    /// Task type.
    pub task_type: String,

    /// Task queue the task was polled from.
    pub task_queue: String,

    /// Serialized task input.
    pub input: Vec<u8>,

    /// Task token that names this attempt; its outcome is reported with it.
    pub task_token: Vec<u8>,

    /// Attempt number.
    pub attempt: i32,

    /// Start-to-close timeout, if the task has one.
    #[allow(dead_code)]
    pub start_to_close_timeout: Option<Duration>,
    /// Heartbeat timeout, if the task has one.
    #[allow(dead_code)]
    pub heartbeat_timeout: Option<Duration>,
}

/// Poller for workflow execution steps.
///
/// Long-polls the server for workflow execution work.
///
/// # Examples
///
/// ```rust,ignore
/// use orcher_sdk_core::poller::{WorkflowExecutionPoller, PollerConfig};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let config = PollerConfig::default();
/// let poller = WorkflowExecutionPoller::new(config, "http://localhost:50051").await?;
/// # Ok(())
/// # }
/// ```
pub struct WorkflowExecutionPoller {
    /// Poller configuration.
    config: Arc<PollerConfig>,

    /// gRPC client for `ExecutionService`.
    client: ExecutionServiceClient<Channel>,

    /// Shutdown signal.
    shutdown: tokio::sync::watch::Receiver<bool>,

    /// Channel the polled work is sent to.
    task_sender: mpsc::Sender<WorkflowExecutionTask>,

    /// Poll request built once and cloned for each poll.
    poll_request_template: PollWorkflowExecutionRequest,

    /// Server address, used to reconnect after a cool-off.
    server_url: String,

    /// Consecutive polls that returned no work.
    consecutive_idle_polls: u32,

    /// Consecutive polls that ended in a normal long-poll timeout.
    consecutive_timeouts: u32,

    /// End of the current cool-off; when set, the poller sleeps until then and reconnects.
    cooloff_until: Option<std::time::Instant>,
}

impl WorkflowExecutionPoller {
    /// Connects to the server and creates a workflow execution poller.
    ///
    /// # Arguments
    ///
    /// * `config` - Poller configuration
    /// * `server_url` - Server address
    /// * `shutdown` - Shutdown signal sender; the poller subscribes to it
    /// * `task_sender` - Channel the polled execution steps are sent to
    ///
    /// # Errors
    ///
    /// Returns an error if the address or TLS settings are invalid, or the
    /// connection fails.
    pub async fn new(
        config: PollerConfig,
        server_url: impl Into<String>,
        shutdown: tokio::sync::watch::Sender<bool>,
        task_sender: mpsc::Sender<WorkflowExecutionTask>,
    ) -> Result<Self> {
        let address = server_url.into();
        tracing::info!(
            server_url = %address,
            task_queue = %config.task_queue,
            namespace = %config.namespace,
            "WorkflowPoller: Connecting to ORCHER server..."
        );

        // Through the same builder as the driver's channel manager, so this
        // connection carries the same TLS setting as the one that was just
        // reported connected.
        let channel = super::channel::connect_channel(&address, config.tls_config.as_ref()).await?;

        tracing::info!(
            server_url = %address,
            "WorkflowPoller: Successfully connected to ORCHER server"
        );

        let shutdown_rx = shutdown.subscribe();

        // Built once; each poll clones it.
        let poll_request_template = PollWorkflowExecutionRequest {
            task_queue: config.task_queue.clone(),
            namespace: config.namespace.clone(),
            identity: config.identity.clone(),
            poll_timeout: Some(prost_types::Duration {
                seconds: config.poll_timeout_seconds,
                nanos: 0,
            }),
            // Sent on every poll so the server can bind an execution to this
            // release the first time this worker claims it. Empty means "not
            // declared".
            version_id: config.version_id.clone().unwrap_or_default(),
            // Which process this is, so a shutdown it announces stops its own
            // polls and not those of a later process reusing its identity.
            worker_instance_id: super::lifecycle::worker_instance_id().to_string(),
        };

        let poller = Self {
            client: crate::limits::sized!(
                ExecutionServiceClient::new(channel),
                config.max_message_bytes
            ),
            config: Arc::new(config),
            shutdown: shutdown_rx,
            task_sender,
            poll_request_template,
            server_url: address,
            consecutive_idle_polls: 0,
            consecutive_timeouts: 0,
            cooloff_until: None,
        };

        Ok(poller)
    }

    /// Polls for workflow execution steps until shutdown.
    ///
    /// Runs until shutdown is requested or the driver stops taking
    /// activations. Polled steps are sent through `task_sender`. Errors from
    /// the server are retried with backoff and never end the loop.
    ///
    /// # Errors
    ///
    /// Returns an error if reconnecting after a cool-off fails.
    pub async fn poll_loop(&mut self) -> Result<()> {
        tracing::info!(
            task_queue = %self.config.task_queue,
            namespace = %self.config.namespace,
            "Starting workflow execution poller"
        );

        // Cool-off settings.
        //
        // Tuned for work that arrives in bursts with idle periods between
        // them. Entering cool-off too eagerly puts pollers to sleep between
        // bursts and delays new work by 30 seconds or more.
        //
        // With a 500ms sleep after each idle poll, 100 idle polls is about 50
        // seconds of idleness before cool-off, which leaves the engine time to
        // produce new work.
        //
        // Cool-off is meant for a server that is overloaded or failing
        // repeatedly, not for ordinary idle periods.
        const MAX_CONSECUTIVE_IDLE: u32 = 100;
        const MAX_CONSECUTIVE_TIMEOUTS: u32 = 20;
        const INITIAL_COOLOFF_MS: u64 = 500;
        const MAX_COOLOFF_MS: u64 = 5_000;

        fn calc_backoff(attempts: u32) -> Duration {
            let base = INITIAL_COOLOFF_MS;
            let max = MAX_COOLOFF_MS;
            // Doubles from 500ms and stops growing at 4s, under the 5s cap.
            let shift = attempts.saturating_sub(1).min(3);
            let ms = base.saturating_mul(1u64 << shift);
            Duration::from_millis(std::cmp::min(ms, max))
        }

        // Errors the status table does not know: counted so the retry backs
        // off, reset by the next successful poll.
        let mut consecutive_errors: u32 = 0;

        // Resolves once the driver has stopped taking activations.
        let driver_gone = self.task_sender.clone();

        loop {
            // Checked before each poll, never during one. Once shut down, no
            // new poll starts; one already out is left to finish, because by
            // the time it answers the engine may already have claimed the
            // activation it carries, and cancelling the call would not give
            // that back. What it brings is handed to the driver, which keeps
            // taking activations for its shutdown grace.
            if *self.shutdown.borrow() || self.task_sender.is_closed() {
                tracing::info!("Workflow execution poller shutting down");
                break;
            }

            // In cool-off: sleep until it ends, then reconnect.
            if let Some(until) = self.cooloff_until {
                let now = std::time::Instant::now();
                if now < until {
                    let sleep_for = until.saturating_duration_since(now);
                    tracing::warn!(
                        sleep_ms = sleep_for.as_millis(),
                        "Workflow poller in cool-off"
                    );
                    self.pause(sleep_for).await;
                    if *self.shutdown.borrow() || self.task_sender.is_closed() {
                        continue;
                    }
                }

                // Recreate channel/client after cool-off
                let channel = super::channel::connect_channel(
                    &self.server_url,
                    self.config.tls_config.as_ref(),
                )
                .await
                .map_err(|e| {
                    Error::connection(format!("Failed to reconnect to {}: {}", self.server_url, e))
                })?;
                self.client = crate::limits::sized!(
                    ExecutionServiceClient::new(channel),
                    self.config.max_message_bytes
                );
                self.cooloff_until = None;
                self.consecutive_idle_polls = 0;
                self.consecutive_timeouts = 0;
            }

            // Poll for work; this waits up to `poll_timeout_seconds`.
            //
            // Abandoned only when the driver has stopped taking activations:
            // its grace is over, or it is gone. Whatever the poll brought
            // could not be handed over then anyway. A poll that has already
            // answered wins over that, so an activation that has arrived is
            // always offered to the driver.
            let result = tokio::select! {
                biased;
                result = self.poll_once_inner() => result,
                _ = driver_gone.closed() => {
                    tracing::info!(
                        task_queue = %self.config.task_queue,
                        "Workflow driver stopped taking activations; abandoning the poll in flight"
                    );
                    break;
                }
            };

            match result {
                Ok(Some(task)) => {
                    // Work arrived: reset the idle, timeout and error counts.
                    self.consecutive_idle_polls = 0;
                    self.consecutive_timeouts = 0;
                    consecutive_errors = 0;

                    tracing::info!(
                        task_queue = %self.config.task_queue,
                        workflow_id = %task.execution.workflow_id,
                        run_id = %task.execution.run_id,
                        workflow_type = %task.workflow_type,
                        "Received workflow execution task"
                    );
                    if let Err(mpsc::error::SendError(task)) = self.task_sender.send(task).await {
                        // The engine counts this activation as claimed by this
                        // worker. Handed back, it goes to the next poll
                        // immediately. An engine that cannot take it back holds
                        // it until its claim timeout, hence the warning.
                        tracing::warn!(
                            task_queue = %self.config.task_queue,
                            workflow_id = %task.execution.workflow_id,
                            run_id = %task.execution.run_id,
                            "Workflow driver stopped before this activation could be handed to it; \
                             handing it back to the engine"
                        );
                        self.release(task).await;
                        break;
                    }
                }
                Ok(None) => {
                    // No work available - server timed out after long-poll
                    tracing::trace!("No workflow execution work available (timeout)");
                    // An empty answer is still an answer: the path works again.
                    consecutive_errors = 0;
                    self.consecutive_idle_polls = self.consecutive_idle_polls.saturating_add(1);

                    // Enter cool-off after too many idle polls
                    if self.consecutive_idle_polls >= MAX_CONSECUTIVE_IDLE {
                        let backoff = calc_backoff(self.consecutive_idle_polls);
                        // Jitter 0..250ms
                        let jitter = {
                            let now = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or(Duration::from_secs(0));
                            Duration::from_millis((now.as_nanos() % 250) as u64)
                        };
                        self.cooloff_until = Some(std::time::Instant::now() + backoff + jitter);
                        tracing::warn!(
                            idle_count = self.consecutive_idle_polls,
                            cooloff_ms = (backoff + jitter).as_millis(),
                            "Workflow poller entering cool-off due to idle polls"
                        );
                        continue;
                    }

                    // Sleep briefly so an idle loop does not spin the CPU.
                    self.pause(Duration::from_millis(500)).await;
                }
                Err(e) => {
                    // Timeouts are normal during long polling; they are not warnings.
                    if e.is_normal_timeout() {
                        tracing::trace!(
                            task_queue = %self.config.task_queue,
                            "Poll timeout (normal during long-polling)"
                        );
                        self.consecutive_timeouts = self.consecutive_timeouts.saturating_add(1);

                        // Enter cool-off after too many timeouts
                        if self.consecutive_timeouts >= MAX_CONSECUTIVE_TIMEOUTS {
                            let backoff = calc_backoff(self.consecutive_timeouts);
                            // Jitter 0..250ms
                            let jitter = {
                                let now = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap_or(Duration::from_secs(0));
                                Duration::from_millis((now.as_nanos() % 250) as u64)
                            };
                            self.cooloff_until = Some(std::time::Instant::now() + backoff + jitter);
                            tracing::warn!(
                                timeout_count = self.consecutive_timeouts,
                                cooloff_ms = (backoff + jitter).as_millis(),
                                "Workflow poller entering cool-off due to timeouts"
                            );
                            continue;
                        }

                        // Sleep briefly so the loop does not spin the CPU.
                        self.pause(Duration::from_millis(500)).await;
                    } else if e.is_retryable() {
                        tracing::warn!(
                            error = %e,
                            task_queue = %self.config.task_queue,
                            "Workflow poll failed (retryable), backing off 2s"
                        );
                        // Jitter, so many pollers do not retry in lockstep.
                        let jitter_ms = (std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_millis()
                            % 1000) as u64;
                        self.pause(Duration::from_secs(2) + Duration::from_millis(jitter_ms))
                            .await;
                    } else {
                        // Never stop. A proxy in front of a restarting engine can
                        // answer with an HTML 502, which tonic reports as `Internal`
                        // "protocol error" and the status table does not treat as
                        // retryable. Returning here would end the poller for the
                        // life of the process while the task pollers, which never
                        // stop, recover. No error a poller can observe is worth
                        // going quiet forever, so back off and try again.
                        consecutive_errors = consecutive_errors.saturating_add(1);
                        let backoff = unexpected_poll_error_backoff(consecutive_errors);
                        tracing::error!(
                            error = %e,
                            task_queue = %self.config.task_queue,
                            consecutive_errors,
                            retry_in_ms = backoff.as_millis(),
                            "Workflow poll failed with an unexpected error, retrying"
                        );
                        self.pause(backoff).await;
                    }
                }
            }
        }

        tracing::info!("Workflow execution poller stopped");
        Ok(())
    }

    /// Waits `duration` between polls, or less if the poller is told to stop
    /// meanwhile, so a shutdown is not held up by a backoff of up to thirty
    /// seconds.
    async fn pause(&mut self, duration: Duration) {
        let driver_gone = self.task_sender.clone();
        let shutdown = &mut self.shutdown;
        tokio::select! {
            _ = tokio::time::sleep(duration) => {}
            _ = driver_gone.closed() => {}
            _ = async {
                // A dropped sender is not a request to stop: a poller built
                // outside a driver may have been handed one it does not keep.
                if shutdown.wait_for(|stop| *stop).await.is_err() {
                    std::future::pending::<()>().await;
                }
            } => {}
        }
    }

    /// Hand back an activation this poller received and could not pass on.
    ///
    /// The driver has stopped by then, so there is no grace left to spend:
    /// the release gets a short bound of its own, and the process may well
    /// exit before it is answered. That leaves the activation to its claim
    /// timeout, just as if no release had been sent.
    async fn release(&self, task: WorkflowExecutionTask) {
        let token = crate::poller::completion::activation_token(
            task.task_token,
            task.stream_entry_id.as_deref(),
        );
        super::lifecycle::release_on(
            self.client.clone(),
            &self.config.namespace,
            &self.config.identity,
            self.config.api_key.as_deref(),
            self.config.organization_id.as_deref(),
            super::lifecycle::Leftover {
                workflow_id: task.execution.workflow_id,
                run_id: task.execution.run_id,
                token,
                retried: false,
            },
            super::lifecycle::SHUTDOWN_NOTICE_TIMEOUT,
        )
        .await;
    }

    /// Polls once for a workflow execution step.
    async fn poll_once_inner(&mut self) -> Result<Option<WorkflowExecutionTask>> {
        // A short sleep, so no failure mode can turn the poll loop into a busy spin.
        tokio::time::sleep(Duration::from_millis(10)).await;

        // Clone the prebuilt request rather than building one for each poll.
        let request_body = self.poll_request_template.clone();

        // Give the call 10 seconds more than the long poll, so the client does
        // not time out before the server answers.
        let grpc_deadline = Duration::from_secs(self.config.poll_timeout_seconds as u64 + 10);
        let mut request = tonic::Request::new(request_body);
        request.set_timeout(grpc_deadline);

        // Organization header, if configured.
        if let Some(ref org_id) = self.config.organization_id {
            if let Ok(value) = org_id.parse() {
                request.metadata_mut().insert("x-organization-id", value);
            }
        }

        // Authorization header, if an API key is configured.
        if let Some(ref key) = self.config.api_key {
            if let Ok(value) = format!("Bearer {}", key).parse() {
                request.metadata_mut().insert("authorization", value);
            }
        }

        // How much this poller can receive, so the engine fails what will
        // not fit rather than handing it out to be refused here.
        crate::limits::state_receive_limit(&mut request, self.config.max_message_bytes);

        // This is the hot path: only work and errors are logged, not every poll.

        let start = std::time::Instant::now();
        let response = self.client.poll_workflow_execution(request).await;
        let elapsed = start.elapsed();

        let response = response.map_err(|e| {
            if e.code() == tonic::Code::DeadlineExceeded {
                // Expected during long polling; the loop treats it as a
                // normal timeout.
                tracing::trace!("Poll timeout (expected during long-poll)");
                return Error::timeout("poll workflow execution");
            }
            // A message over the size limit is said as such, not as tonic's
            // "decoded message length too large".
            let e = crate::limits::clarify(e);
            tracing::warn!(
                error_code = ?e.code(),
                error_message = %e.message(),
                elapsed_ms = elapsed.as_millis(),
                "Workflow poll gRPC error"
            );
            Error::GrpcStatus(e)
        })?;

        let poll_response = response.into_inner();

        // An empty id means the poll brought no work.
        if poll_response.workflow_id.is_empty() {
            tracing::trace!(
                elapsed_ms = elapsed.as_millis(),
                "No workflow execution work available"
            );
            return Ok(None);
        }

        tracing::debug!(
            workflow_id = %poll_response.workflow_id,
            execution_id = %poll_response.execution_id,
            workflow_type = %poll_response.workflow_type,
            elapsed_ms = elapsed.as_millis(),
            "Polled workflow execution task"
        );

        // The poll response has no task token field; the engine returns the
        // activation token in `stream_entry_id` instead. It is carried as the
        // task token too, because a failure report has nowhere else to put
        // it and language SDKs hand the task token back with every result.
        // The token must name the activation, not only the workflow, so the
        // engine can tell a repeat of a completion from a new one. An engine
        // that returns no token gets an empty one back, which it ignores.
        let task_token = poll_response.stream_entry_id.clone().into_bytes();

        let task = WorkflowExecutionTask {
            execution: WorkflowExecution::new(
                poll_response.workflow_id,
                poll_response.execution_id,
            ),
            workflow_type: poll_response.workflow_type,
            task_queue: poll_response.task_queue,
            input: serde_json::from_slice(&poll_response.input)
                .unwrap_or(serde_json::Value::Object(serde_json::Map::new())),
            journal: poll_response.journal,
            started_event_id: poll_response.started_event_id,
            previous_started_event_id: poll_response.previous_started_event_id,
            attempt: poll_response.attempt,
            task_token,
            // Echoed on completion, where engines read the activation token.
            stream_entry_id: if poll_response.stream_entry_id.is_empty() {
                None
            } else {
                Some(poll_response.stream_entry_id)
            },
            // Pending queries and updates from the server.
            queries: poll_response.queries,
            updates: poll_response.updates,
        };

        Ok(Some(task))
    }
}

/// Poller for task executions.
///
/// Long-polls the server for task execution work.
///
/// # Examples
///
/// ```rust,ignore
/// use orcher_sdk_core::poller::{TaskExecutionPoller, PollerConfig};
///
/// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
/// let config = PollerConfig::default();
/// let poller = TaskExecutionPoller::new(config, "http://localhost:50051").await?;
/// # Ok(())
/// # }
/// ```
pub struct TaskExecutionPoller {
    /// Poller configuration.
    config: Arc<PollerConfig>,

    /// gRPC client for `ExecutionService`.
    client: ExecutionServiceClient<Channel>,

    /// Shutdown signal.
    shutdown: tokio::sync::watch::Receiver<bool>,

    /// Channel the polled work is sent to.
    task_sender: mpsc::Sender<TaskExecutionTask>,

    /// Poll request built once and cloned for each poll.
    poll_request_template: PollTaskExecutionRequest,
}

impl TaskExecutionPoller {
    /// Connects to the server and creates a task execution poller.
    ///
    /// # Arguments
    ///
    /// * `config` - Poller configuration
    /// * `server_url` - Server address
    /// * `shutdown` - Shutdown signal sender; the poller subscribes to it
    /// * `task_sender` - Channel the polled tasks are sent to
    ///
    /// # Errors
    ///
    /// Returns an error if the address or TLS settings are invalid, or the
    /// connection fails.
    pub async fn new(
        config: PollerConfig,
        server_url: impl Into<String>,
        shutdown: tokio::sync::watch::Sender<bool>,
        task_sender: mpsc::Sender<TaskExecutionTask>,
    ) -> Result<Self> {
        let address = server_url.into();
        tracing::info!(
            server_url = %address,
            task_queue = %config.task_queue,
            namespace = %config.namespace,
            "TaskPoller: Connecting to ORCHER server..."
        );

        // Through the same builder as the driver's channel manager, so this
        // connection carries the same TLS setting as the one that was just
        // reported connected.
        let channel = super::channel::connect_channel(&address, config.tls_config.as_ref()).await?;

        tracing::info!(
            server_url = %address,
            "TaskPoller: Successfully connected to ORCHER server"
        );

        let shutdown_rx = shutdown.subscribe();

        // Built once; each poll clones it.
        let poll_request_template = PollTaskExecutionRequest {
            task_queue: config.task_queue.clone(),
            namespace: config.namespace.clone(),
            identity: config.identity.clone(),
            poll_timeout: Some(prost_types::Duration {
                seconds: config.poll_timeout_seconds,
                nanos: 0,
            }),
            task_queue_metadata: std::collections::HashMap::new(),
            // See the workflow poller.
            worker_instance_id: super::lifecycle::worker_instance_id().to_string(),
            task_capabilities: config.auto_heartbeat.then_some(
                crate::proto::orcher::v1::TaskCapabilities {
                    auto_heartbeat: true,
                },
            ),
        };

        let poller = Self {
            client: crate::limits::sized!(
                ExecutionServiceClient::new(channel),
                config.max_message_bytes
            ),
            config: Arc::new(config),
            shutdown: shutdown_rx,
            task_sender,
            poll_request_template,
        };

        Ok(poller)
    }

    /// Polls for task executions until shutdown.
    ///
    /// Runs until shutdown is requested or the driver stops taking tasks.
    /// Polled tasks are sent through `task_sender`. Errors from the server are
    /// retried after a pause and never end the loop, so this always returns
    /// `Ok`.
    pub async fn poll_loop(&mut self) -> Result<()> {
        tracing::info!(
            task_queue = %self.config.task_queue,
            namespace = %self.config.namespace,
            "Starting task execution poller"
        );

        tracing::debug!("Task poller: Entering main poll loop");

        // Cool-off settings.
        //
        // Tuned for work that arrives in bursts with idle periods between
        // them. Entering cool-off too eagerly puts pollers to sleep between
        // bursts and delays new work by 30 seconds or more.
        //
        // With a 500ms sleep after each idle poll, 100 idle polls is about 50
        // seconds of idleness before cool-off, which leaves the engine time to
        // produce new work.
        //
        // Cool-off is meant for a server that is overloaded or failing
        // repeatedly, not for ordinary idle periods.
        const MAX_CONSECUTIVE_IDLE: u32 = 100;
        const MAX_CONSECUTIVE_TIMEOUTS: u32 = 20;
        const INITIAL_COOLOFF_MS: u64 = 500;
        const MAX_COOLOFF_MS: u64 = 5_000;

        fn calc_backoff(attempts: u32) -> Duration {
            let base = INITIAL_COOLOFF_MS;
            let max = MAX_COOLOFF_MS;
            // Doubles from 500ms and stops growing at 4s, under the 5s cap.
            let shift = attempts.saturating_sub(1).min(3);
            let ms = base.saturating_mul(1u64 << shift);
            Duration::from_millis(std::cmp::min(ms, max))
        }

        // Idle and timeout tracking for cool-off.
        let mut consecutive_idle_polls: u32 = 0;
        let mut consecutive_timeouts: u32 = 0;
        let mut cooloff_until: Option<std::time::Instant> = None;
        let mut iteration_count: u64 = 0;

        // Resolves once the driver has stopped taking tasks.
        let driver_gone = self.task_sender.clone();

        loop {
            iteration_count += 1;

            // Checked before each poll, never during one. Once shut down, no
            // new poll starts; one already out is left to finish, because by
            // the time it answers the engine may already have started the
            // task it carries, and cancelling the call would not give that
            // back. What it brings is handed to the driver, which keeps
            // taking tasks for its shutdown grace.
            tracing::trace!(iteration = iteration_count, "Checking shutdown flag");
            if *self.shutdown.borrow() || self.task_sender.is_closed() {
                tracing::info!("Task execution poller shutting down");
                break;
            }

            // In cool-off: sleep until it ends, then reset the counters.
            if let Some(until) = cooloff_until {
                let now = std::time::Instant::now();
                if now < until {
                    let sleep_for = until.saturating_duration_since(now);
                    tracing::warn!(sleep_ms = sleep_for.as_millis(), "Task poller in cool-off");
                    self.pause(sleep_for).await;
                }
                // Leave cool-off.
                cooloff_until = None;
                consecutive_idle_polls = 0;
                consecutive_timeouts = 0;
                if *self.shutdown.borrow() || self.task_sender.is_closed() {
                    continue;
                }
            }

            // Poll for work; this waits up to `poll_timeout_seconds`.
            //
            // Abandoned only when the driver has stopped taking tasks: its
            // grace is over, or it is gone. A poll that has already answered
            // wins over that, so a task that has arrived is always offered
            // to the driver.
            let result = tokio::select! {
                biased;
                result = self.poll_once_inner() => result,
                _ = driver_gone.closed() => {
                    tracing::info!(
                        task_queue = %self.config.task_queue,
                        "Task driver stopped taking tasks; abandoning the poll in flight"
                    );
                    break;
                }
            };

            match result {
                Ok(Some(task)) => {
                    tracing::info!(
                        task_queue = %self.config.task_queue,
                        workflow_id = %task.execution.workflow_id,
                        run_id = %task.execution.run_id,
                        task_id = %task.task_id,
                        task_type = %task.task_type,
                        "Received task execution task"
                    );

                    // Work arrived: reset the idle and timeout counts.
                    consecutive_idle_polls = 0;
                    consecutive_timeouts = 0;

                    if let Err(mpsc::error::SendError(task)) = self.task_sender.send(task).await {
                        // The engine has started this task for this worker;
                        // nothing gives it back, so it waits out its
                        // start-to-close timeout. Logged as an error so the
                        // loss is visible.
                        tracing::error!(
                            task_queue = %self.config.task_queue,
                            task_id = %task.task_id,
                            workflow_id = %task.execution.workflow_id,
                            "Task driver stopped before this task could be handed to it; it \
                             waits out its start-to-close timeout"
                        );
                        break;
                    }
                }
                Ok(None) => {
                    // No work available - server timed out after long-poll
                    tracing::trace!("No task work available (timeout)");

                    // The poll answered, even with no work: reset the timeout count.
                    consecutive_timeouts = 0;

                    // Count idle polls and enter cool-off after too many.
                    consecutive_idle_polls = consecutive_idle_polls.saturating_add(1);
                    if consecutive_idle_polls >= MAX_CONSECUTIVE_IDLE {
                        let backoff = calc_backoff(consecutive_idle_polls);
                        // Jitter 0..250ms
                        let jitter = {
                            let now = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or(Duration::from_secs(0));
                            Duration::from_millis((now.as_nanos() % 250) as u64)
                        };
                        cooloff_until = Some(std::time::Instant::now() + backoff + jitter);
                        tracing::warn!(
                            idle_count = consecutive_idle_polls,
                            cooloff_ms = (backoff + jitter).as_millis(),
                            "Task poller entering cool-off due to idle polls"
                        );
                        continue;
                    }

                    // Sleep briefly so an idle loop does not spin the CPU.
                    self.pause(Duration::from_millis(500)).await;
                }
                Err(e) => {
                    // Timeouts are normal during long polling; they are not warnings.
                    if e.is_normal_timeout() {
                        tracing::trace!(
                            task_queue = %self.config.task_queue,
                            "Poll timeout (normal during long-polling)"
                        );

                        // A timeout is not an idle poll: reset the idle count.
                        consecutive_idle_polls = 0;

                        // Count normal timeouts and enter cool-off after too many.
                        consecutive_timeouts = consecutive_timeouts.saturating_add(1);
                        if consecutive_timeouts >= MAX_CONSECUTIVE_TIMEOUTS {
                            let backoff = calc_backoff(consecutive_timeouts);
                            // Jitter 0..250ms
                            let jitter = {
                                let now = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap_or(Duration::from_secs(0));
                                Duration::from_millis((now.as_nanos() % 250) as u64)
                            };
                            cooloff_until = Some(std::time::Instant::now() + backoff + jitter);
                            tracing::warn!(
                                timeout_count = consecutive_timeouts,
                                cooloff_ms = (backoff + jitter).as_millis(),
                                "Task poller entering cool-off due to timeouts"
                            );
                            continue;
                        }

                        // Sleep briefly so the loop does not spin the CPU.
                        self.pause(Duration::from_millis(500)).await;
                    } else {
                        tracing::warn!(
                            error = %e,
                            task_queue = %self.config.task_queue,
                            "Task poller error"
                        );

                        // A real error resets both counts.
                        consecutive_idle_polls = 0;
                        consecutive_timeouts = 0;

                        // Sleep on error so the loop does not spin.
                        self.pause(Duration::from_secs(2)).await;
                    }
                }
            }
        }

        tracing::info!("Task execution poller stopped");
        Ok(())
    }

    /// Waits `duration` between polls, or less if the poller is told to stop
    /// meanwhile, so a shutdown is not held up by a backoff.
    async fn pause(&mut self, duration: Duration) {
        let driver_gone = self.task_sender.clone();
        let shutdown = &mut self.shutdown;
        tokio::select! {
            _ = tokio::time::sleep(duration) => {}
            _ = driver_gone.closed() => {}
            _ = async {
                // A dropped sender is not a request to stop: a poller built
                // outside a driver may have been handed one it does not keep.
                if shutdown.wait_for(|stop| *stop).await.is_err() {
                    std::future::pending::<()>().await;
                }
            } => {}
        }
    }

    /// Polls once for a task execution.
    async fn poll_once_inner(&mut self) -> Result<Option<TaskExecutionTask>> {
        // Clone the prebuilt request rather than building one for each poll.
        let request_body = self.poll_request_template.clone();

        // Give the call 10 seconds more than the long poll, so the client does
        // not time out before the server answers.
        let grpc_deadline = Duration::from_secs(self.config.poll_timeout_seconds as u64 + 10);
        let mut request = tonic::Request::new(request_body);
        request.set_timeout(grpc_deadline);

        // Organization header, if configured.
        if let Some(ref org_id) = self.config.organization_id {
            if let Ok(value) = org_id.parse() {
                request.metadata_mut().insert("x-organization-id", value);
            }
        }

        // Authorization header, if an API key is configured.
        if let Some(ref key) = self.config.api_key {
            if let Ok(value) = format!("Bearer {}", key).parse() {
                request.metadata_mut().insert("authorization", value);
            }
        }

        // How much this poller can receive, so the engine fails what will
        // not fit rather than handing it out to be refused here.
        crate::limits::state_receive_limit(&mut request, self.config.max_message_bytes);

        let start = std::time::Instant::now();

        // The gRPC deadline alone does not guarantee the call returns, so a
        // local timeout two seconds past it keeps the poller from hanging.
        let timeout_duration = Duration::from_secs(self.config.poll_timeout_seconds as u64 + 12);

        let response =
            tokio::time::timeout(timeout_duration, self.client.poll_task_execution(request))
                .await
                .map_err(|_elapsed| {
                    tracing::warn!(
                        timeout_secs = timeout_duration.as_secs(),
                        "Task poll timeout elapsed"
                    );
                    Error::timeout("poll task execution - tokio timeout")
                })?
                .map_err(|e| {
                    if e.code() == tonic::Code::DeadlineExceeded {
                        // Expected during long polling.
                        tracing::trace!("Poll timeout (expected)");
                        return Error::timeout("poll task execution");
                    }
                    let e = crate::limits::clarify(e);
                    tracing::warn!(error = %e, code = ?e.code(), "Task poll gRPC error");
                    Error::GrpcStatus(e)
                })?;

        let elapsed = start.elapsed();
        let poll_response = response.into_inner();

        // Debug level: this fires on every poll.
        tracing::debug!(
            has_task = !poll_response.task_id.is_empty(),
            elapsed_ms = elapsed.as_millis(),
            "SDK received poll_task_execution response"
        );

        // An empty id means the poll brought no work.
        if poll_response.task_id.is_empty() {
            tracing::trace!("No task execution work available");
            return Ok(None);
        }

        tracing::info!(
            workflow_id = %poll_response.workflow_id,
            execution_id = %poll_response.execution_id,
            task_id = %poll_response.task_id,
            task_type = %poll_response.task_type,
            "Received task execution"
        );

        let task = TaskExecutionTask {
            execution: WorkflowExecution::new(
                poll_response.workflow_id,
                poll_response.execution_id,
            ),
            task_id: poll_response.task_id,
            task_type: poll_response.task_type,
            task_queue: poll_response.task_queue,
            input: poll_response.input,
            task_token: poll_response.task_token,
            attempt: poll_response.attempt,
            start_to_close_timeout: poll_response
                .start_to_close_timeout
                .map(|d| Duration::new(d.seconds as u64, d.nanos as u32)),
            heartbeat_timeout: poll_response
                .heartbeat_timeout
                .map(|d| Duration::new(d.seconds as u64, d.nanos as u32)),
        };

        Ok(Some(task))
    }
}

// ============================================================================
// Actor Operation Poller
// ============================================================================

/// Configuration for an actor operation poller.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct ActorPollerConfig {
    /// ID of this actor worker.
    pub service_id: String,

    /// How long the server may hold a poll open, in milliseconds.
    pub poll_timeout_ms: u64,

    /// Maximum operations the server returns per poll.
    pub max_operations: u32,

    /// Identity this poller reports to the server.
    pub identity: String,

    /// Organization ID, for a multi-tenant server.
    pub organization_id: Option<String>,

    /// API key for server authentication.
    pub api_key: Option<String>,

    /// TLS for the poller's own connection; see `PollerConfig::tls_config`.
    pub tls_config: Option<super::TlsConfig>,

    /// The largest message the poller receives; see
    /// `PollerConfig::max_message_bytes`.
    pub max_message_bytes: usize,
}

impl Default for ActorPollerConfig {
    fn default() -> Self {
        Self {
            service_id: "default".to_string(),
            poll_timeout_ms: 30_000,
            max_operations: 100,
            identity: format!("actor-poller-{}", uuid::Uuid::new_v4()),
            organization_id: None,
            api_key: None,
            tls_config: None,
            max_message_bytes: crate::limits::default_max_message_bytes(),
        }
    }
}

/// Poller for actor operations.
///
/// Long-polls the server for actor operations through `ActorService`.
/// Unlike the workflow and task pollers, which get one item per poll, it
/// gets a batch (`repeated ActorOperation`).
pub struct ActorOperationPoller {
    /// Poller configuration.
    config: Arc<ActorPollerConfig>,

    /// gRPC client for `ActorService`.
    client: ActorServiceClient<Channel>,

    /// Shutdown signal.
    shutdown: tokio::sync::watch::Receiver<bool>,

    /// Channel the polled operations are sent to.
    operation_sender: mpsc::Sender<ActorOperation>,

    /// Poll request built once and cloned for each poll.
    poll_request_template: PollActorOperationRequest,

    /// Server address, used to reconnect after a cool-off.
    server_url: String,

    /// Consecutive polls that returned no operations.
    consecutive_idle_polls: u32,

    /// Consecutive polls that ended in a normal long-poll timeout.
    consecutive_timeouts: u32,

    /// End of the current cool-off, if any.
    cooloff_until: Option<std::time::Instant>,
}

impl ActorOperationPoller {
    /// Connects to the server and creates an actor operation poller.
    ///
    /// # Errors
    ///
    /// Returns an error if the address or TLS settings are invalid, or the
    /// connection fails.
    pub async fn new(
        config: ActorPollerConfig,
        server_url: impl Into<String>,
        shutdown: tokio::sync::watch::Sender<bool>,
        operation_sender: mpsc::Sender<ActorOperation>,
    ) -> Result<Self> {
        let address = server_url.into();
        tracing::info!(
            server_url = %address,
            service_id = %config.service_id,
            "ActorPoller: Connecting to ORCHER server..."
        );

        // Through the same builder as the driver's channel manager, so this
        // connection carries the same TLS setting as the one that was just
        // reported connected.
        let channel = super::channel::connect_channel(&address, config.tls_config.as_ref()).await?;

        tracing::info!(
            server_url = %address,
            "ActorPoller: Successfully connected to ORCHER server"
        );

        let shutdown_rx = shutdown.subscribe();

        let poll_request_template = PollActorOperationRequest {
            service_id: config.service_id.clone(),
            max_operations: config.max_operations,
            timeout_ms: config.poll_timeout_ms,
        };

        Ok(Self {
            client: crate::limits::sized!(
                ActorServiceClient::new(channel),
                config.max_message_bytes
            ),
            config: Arc::new(config),
            shutdown: shutdown_rx,
            operation_sender,
            poll_request_template,
            server_url: address,
            consecutive_idle_polls: 0,
            consecutive_timeouts: 0,
            cooloff_until: None,
        })
    }

    /// Polls for actor operations until shutdown.
    ///
    /// Uses the same cool-off scheme as [`WorkflowExecutionPoller`]. Returns
    /// once shutdown is requested or the operation channel closes. Errors from
    /// the server are retried with backoff and never end the loop.
    ///
    /// # Errors
    ///
    /// Returns an error if reconnecting after a cool-off fails.
    pub async fn poll_loop(&mut self) -> Result<()> {
        tracing::info!(
            service_id = %self.config.service_id,
            "Starting actor operation poller"
        );

        const MAX_CONSECUTIVE_IDLE: u32 = 100;
        const MAX_CONSECUTIVE_TIMEOUTS: u32 = 20;
        const INITIAL_COOLOFF_MS: u64 = 500;
        const MAX_COOLOFF_MS: u64 = 5_000;

        fn calc_backoff(attempts: u32) -> Duration {
            let base = INITIAL_COOLOFF_MS;
            let max = MAX_COOLOFF_MS;
            let shift = attempts.saturating_sub(1).min(3);
            let ms = base.saturating_mul(1u64 << shift);
            Duration::from_millis(std::cmp::min(ms, max))
        }

        // Errors the status table does not know: counted so the retry backs
        // off, reset by the next successful poll.
        let mut consecutive_errors: u32 = 0;

        loop {
            if *self.shutdown.borrow() {
                tracing::info!("Actor operation poller shutting down");
                break;
            }

            // In cool-off: sleep until it ends, then reconnect.
            if let Some(until) = self.cooloff_until {
                let now = std::time::Instant::now();
                if now < until {
                    let sleep_for = until.saturating_duration_since(now);
                    tracing::warn!(sleep_ms = sleep_for.as_millis(), "Actor poller in cool-off");
                    tokio::time::sleep(sleep_for).await;
                }

                // Recreate channel/client after cool-off
                let channel = super::channel::connect_channel(
                    &self.server_url,
                    self.config.tls_config.as_ref(),
                )
                .await
                .map_err(|e| {
                    Error::connection(format!("Failed to reconnect to {}: {}", self.server_url, e))
                })?;
                self.client = crate::limits::sized!(
                    ActorServiceClient::new(channel),
                    self.config.max_message_bytes
                );
                self.cooloff_until = None;
                self.consecutive_idle_polls = 0;
                self.consecutive_timeouts = 0;
            }

            let result = self.poll_once_inner().await;

            match result {
                Ok(operations) if !operations.is_empty() => {
                    self.consecutive_idle_polls = 0;
                    self.consecutive_timeouts = 0;
                    consecutive_errors = 0;

                    tracing::debug!(count = operations.len(), "Received actor operations");

                    for operation in operations {
                        if let Err(e) = self.operation_sender.send(operation).await {
                            tracing::error!("Failed to send actor operation: {}", e);
                            return Ok(());
                        }
                    }
                }
                Ok(_) => {
                    // No work, but an answer: the path to the server works.
                    consecutive_errors = 0;
                    self.consecutive_idle_polls = self.consecutive_idle_polls.saturating_add(1);

                    if self.consecutive_idle_polls >= MAX_CONSECUTIVE_IDLE {
                        let backoff = calc_backoff(self.consecutive_idle_polls);
                        let jitter = {
                            let now = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .unwrap_or(Duration::from_secs(0));
                            Duration::from_millis((now.as_nanos() % 250) as u64)
                        };
                        self.cooloff_until = Some(std::time::Instant::now() + backoff + jitter);
                        tracing::warn!(
                            idle_count = self.consecutive_idle_polls,
                            cooloff_ms = (backoff + jitter).as_millis(),
                            "Actor poller entering cool-off due to idle polls"
                        );
                        continue;
                    }

                    tokio::time::sleep(Duration::from_millis(500)).await;
                }
                Err(e) => {
                    if e.is_normal_timeout() {
                        self.consecutive_timeouts = self.consecutive_timeouts.saturating_add(1);

                        if self.consecutive_timeouts >= MAX_CONSECUTIVE_TIMEOUTS {
                            let backoff = calc_backoff(self.consecutive_timeouts);
                            let jitter = {
                                let now = std::time::SystemTime::now()
                                    .duration_since(std::time::UNIX_EPOCH)
                                    .unwrap_or(Duration::from_secs(0));
                                Duration::from_millis((now.as_nanos() % 250) as u64)
                            };
                            self.cooloff_until = Some(std::time::Instant::now() + backoff + jitter);
                            tracing::warn!(
                                timeout_count = self.consecutive_timeouts,
                                cooloff_ms = (backoff + jitter).as_millis(),
                                "Actor poller entering cool-off due to timeouts"
                            );
                            continue;
                        }

                        tokio::time::sleep(Duration::from_millis(500)).await;
                    } else if e.is_retryable() {
                        tracing::warn!(
                            error = %e,
                            "Actor poll failed (retryable), backing off 2s"
                        );
                        let jitter_ms = (std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap()
                            .as_millis()
                            % 1000) as u64;
                        tokio::time::sleep(
                            Duration::from_secs(2) + Duration::from_millis(jitter_ms),
                        )
                        .await;
                    } else {
                        // Same rule as the workflow poller: an error is never a
                        // reason to stop polling for the rest of the process.
                        consecutive_errors = consecutive_errors.saturating_add(1);
                        let backoff = unexpected_poll_error_backoff(consecutive_errors);
                        tracing::error!(
                            error = %e,
                            consecutive_errors,
                            retry_in_ms = backoff.as_millis(),
                            "Actor poll failed with an unexpected error, retrying"
                        );
                        tokio::time::sleep(backoff).await;
                    }
                }
            }
        }

        tracing::info!("Actor operation poller stopped");
        Ok(())
    }

    /// Polls once for a batch of actor operations.
    async fn poll_once_inner(&mut self) -> Result<Vec<ActorOperation>> {
        tokio::time::sleep(Duration::from_millis(10)).await;

        let request_body = self.poll_request_template.clone();

        let grpc_deadline = Duration::from_millis(self.config.poll_timeout_ms + 10_000);
        let mut request = tonic::Request::new(request_body);
        request.set_timeout(grpc_deadline);

        if let Some(ref org_id) = self.config.organization_id {
            if let Ok(value) = org_id.parse() {
                request.metadata_mut().insert("x-organization-id", value);
            }
        }

        if let Some(ref key) = self.config.api_key {
            if let Ok(value) = format!("Bearer {}", key).parse() {
                request.metadata_mut().insert("authorization", value);
            }
        }

        let timeout_duration = Duration::from_millis(self.config.poll_timeout_ms + 12_000);

        let response =
            tokio::time::timeout(timeout_duration, self.client.poll_actor_operation(request))
                .await
                .map_err(|_elapsed| Error::timeout("poll actor operation - tokio timeout"))?
                .map_err(|e| {
                    if e.code() == tonic::Code::DeadlineExceeded {
                        return Error::timeout("poll actor operation");
                    }
                    Error::from(e)
                })?;

        let poll_response = response.into_inner();

        if poll_response.operations.is_empty() {
            return Ok(vec![]);
        }

        tracing::debug!(
            count = poll_response.operations.len(),
            "Polled actor operations"
        );

        Ok(poll_response.operations)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The retry delay after an unclassified error doubles from two seconds
    /// and settles at thirty, and keeps returning a delay however high the
    /// count goes: a poller backs off, it never stops.
    #[test]
    fn unexpected_error_backoff_doubles_then_settles() {
        let secs = |n: u32| unexpected_poll_error_backoff(n).as_secs();
        assert_eq!(secs(1), 2);
        assert_eq!(secs(2), 4);
        assert_eq!(secs(3), 8);
        assert_eq!(secs(4), 16);
        assert_eq!(secs(5), 30);
        assert_eq!(secs(6), 30);
        assert_eq!(secs(u32::MAX), 30);
    }

    /// The status a proxy produces while the engine restarts. The status
    /// table does not retry it, so the poller must survive it on the
    /// unexpected-error path.
    #[test]
    fn a_protocol_error_from_a_proxy_is_not_retryable_by_the_status_table() {
        let status = tonic::Status::internal(
            "protocol error: received message with invalid compression flag: 60",
        );
        let error = crate::error::Error::from(status);
        assert!(
            !error.is_retryable(),
            "if this becomes retryable the poller takes the quick-retry path, which is fine; \
             the point is that neither path may stop the poller"
        );
    }

    #[test]
    fn test_poller_config_default() {
        let config = PollerConfig::default();
        assert_eq!(config.namespace, "default");
        assert_eq!(config.task_queue, "default");
        assert_eq!(config.poll_timeout_seconds, 35);
        assert!(config.identity.starts_with("rust-sdk-"));
    }

    /// Empty must mean "not declared", not a release literally named "".
    /// The wire type is a bare string, so an empty value would otherwise bind
    /// executions to an empty release and look like a declaration.
    #[test]
    fn an_empty_version_is_treated_as_undeclared() {
        assert_eq!(PollerConfig::default().with_version_id("").version_id, None);
        assert_eq!(
            PollerConfig::default().with_version_id("   ").version_id,
            None
        );
        assert_eq!(
            PollerConfig::default().with_version_id("abc123").version_id,
            Some("abc123".to_string())
        );
    }

    /// The release normally comes from CI, so the environment is the path that
    /// actually gets used.
    #[test]
    fn from_env_reads_the_release_and_ignores_blanks() {
        // SAFETY: single-threaded test, restored below.
        unsafe { std::env::set_var("ORCHER_VERSION_ID", "sha-deadbeef") };
        assert_eq!(
            PollerConfig::from_env().version_id,
            Some("sha-deadbeef".to_string())
        );

        unsafe { std::env::set_var("ORCHER_VERSION_ID", "  ") };
        assert_eq!(PollerConfig::from_env().version_id, None);

        unsafe { std::env::remove_var("ORCHER_VERSION_ID") };
        assert_eq!(PollerConfig::from_env().version_id, None);
    }

    #[test]
    fn test_poller_config_customization() {
        let config = PollerConfig {
            namespace: "custom-ns".to_string(),
            task_queue: "custom-queue".to_string(),
            poll_timeout_seconds: 30,
            max_concurrent_polls: 10,
            identity: "test-identity".to_string(),
            organization_id: Some("org-123".to_string()),
            version_id: None,
            api_key: None,
            tls_config: None,
            auto_heartbeat: false,
            max_message_bytes: 1024,
        };

        assert_eq!(config.namespace, "custom-ns");
        assert_eq!(config.task_queue, "custom-queue");
        assert_eq!(config.poll_timeout_seconds, 30);
        assert_eq!(config.max_concurrent_polls, 10);
        assert_eq!(config.organization_id, Some("org-123".to_string()));
    }

    #[test]
    fn test_poller_config_default_organization() {
        let config = PollerConfig::default();
        assert!(config.organization_id.is_none());
    }
}
