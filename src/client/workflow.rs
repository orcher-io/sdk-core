//! Client for starting, querying, and managing workflow executions and for
//! sending them events.
//!
//! ## Terminology
//!
//! - **Worker**: a process that polls for and executes workflows and tasks.
//! - **Task**: a unit of work a workflow schedules.
//! - **Event**: an external message delivered to a running workflow.

use std::time::Duration;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;

use crate::error::{Error, Result};
use crate::proto::orcher::v1::{
    query_service_client::QueryServiceClient, update_workflow_response,
    workflow_service_client::WorkflowServiceClient, CancelWorkflowRequest,
    DescribeWorkflowExecutionRequest, GetWorkflowResultRequest, GetWorkflowStatusRequest,
    ListWorkflowsRequest, QueryWorkflowRequest, ResetWorkflowRequest, SearchWorkflowsRequest,
    SendEventRequest, StartWorkflowRequest, TerminateWorkflowRequest, UpdateWorkflowRequest,
};
use crate::types::{
    ListWorkflowsOptions, SearchWorkflowsOptions, WorkflowExecution, WorkflowExecutionDescription,
    WorkflowListPage, WorkflowStatus,
};
use crate::DEFAULT_NAMESPACE;
use std::collections::HashMap;
use tracing::debug;

/// Attaches credentials to every outgoing client RPC.
///
/// The interceptor is bound when a gRPC client is built, so every call made
/// through that client carries the headers and no call site can forget them.
/// The headers match the ones the worker sends on its own requests.
#[derive(Clone, Default)]
pub struct AuthInterceptor {
    /// Sent as `authorization: Bearer <key>`.
    pub api_key: Option<String>,
    /// Sent as `x-organization-id`, matching the worker's poll requests.
    pub organization_id: Option<String>,
}

impl tonic::service::Interceptor for AuthInterceptor {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> std::result::Result<tonic::Request<()>, tonic::Status> {
        if let Some(ref key) = self.api_key {
            match format!("Bearer {}", key).parse() {
                Ok(value) => {
                    request.metadata_mut().insert("authorization", value);
                }
                // A key that cannot be encoded as a header is a configuration
                // error; surface it instead of sending the request unauthenticated.
                Err(_) => return Err(tonic::Status::invalid_argument(
                    "api_key contains characters that cannot be sent in the authorization header",
                )),
            }
        }
        if let Some(ref org) = self.organization_id {
            if let Ok(value) = org.parse() {
                request.metadata_mut().insert("x-organization-id", value);
            }
        }
        Ok(request)
    }
}

/// How long the server is asked to hold each result long-poll.
///
/// Waiting for a result is a loop of short polls rather than one long request.
/// The server bounds its own wait anyway, and intermediaries (proxies, load
/// balancers, conntrack) drop idle streams. Short polls let a wait of any
/// length avoid pinning a server-side waiter task or tripping an idle timeout,
/// at the cost of one cheap round-trip per window.
const RESULT_POLL_WINDOW: Duration = Duration::from_secs(20);

/// Options for starting a workflow with extended configuration.
///
/// The struct is `#[non_exhaustive]` so that adding an option is not a
/// breaking change. Build one with [`StartWorkflowOpts::new`] and the `with_*`
/// methods.
#[derive(Default)]
#[non_exhaustive]
pub struct StartWorkflowOpts {
    pub workflow_id: String,
    pub workflow_type: String,
    pub task_queue: String,
    pub input_bytes: Vec<u8>,
    pub cron_schedule: Option<String>,
    /// Workflow-level retry policy, off by default.
    ///
    /// `None` means a failed workflow is not retried. A policy with
    /// `maximum_attempts > 0` retries it as a fresh run.
    pub retry_policy: Option<crate::proto::orcher::v1::RetryPolicy>,
    /// Maximum time for the entire workflow execution.
    pub execution_timeout: Option<Duration>,
    /// Maximum time for a single run of the workflow.
    pub run_timeout: Option<Duration>,
    /// Maximum time for an individual workflow task.
    pub task_timeout: Option<Duration>,
    /// What to do when `workflow_id` is already in use in the namespace.
    ///
    /// `None` leaves it to the server, which allows another run once the
    /// previous one has closed. An id whose run is still open is refused with
    /// [`Error::WorkflowAlreadyExists`] under every policy except
    /// `TerminateIfRunning`.
    pub id_reuse_policy: Option<crate::proto::orcher::v1::WorkflowIdReusePolicy>,
}

impl StartWorkflowOpts {
    /// Creates options to start `workflow_type` as `workflow_id` on
    /// `task_queue`.
    ///
    /// `input_bytes` must already be encoded. Every other option is unset.
    pub fn new(
        workflow_id: impl Into<String>,
        workflow_type: impl Into<String>,
        task_queue: impl Into<String>,
        input_bytes: impl Into<Vec<u8>>,
    ) -> Self {
        Self {
            workflow_id: workflow_id.into(),
            workflow_type: workflow_type.into(),
            task_queue: task_queue.into(),
            input_bytes: input_bytes.into(),
            ..Default::default()
        }
    }

    /// Runs the workflow on a cron schedule.
    pub fn with_cron_schedule(mut self, cron_schedule: impl Into<String>) -> Self {
        self.cron_schedule = Some(cron_schedule.into());
        self
    }

    /// Retries a failed workflow as a fresh run under this policy.
    pub fn with_retry_policy(
        mut self,
        retry_policy: crate::proto::orcher::v1::RetryPolicy,
    ) -> Self {
        self.retry_policy = Some(retry_policy);
        self
    }

    /// Bounds the whole workflow execution.
    pub fn with_execution_timeout(mut self, timeout: Duration) -> Self {
        self.execution_timeout = Some(timeout);
        self
    }

    /// Bounds a single run of the workflow.
    pub fn with_run_timeout(mut self, timeout: Duration) -> Self {
        self.run_timeout = Some(timeout);
        self
    }

    /// Bounds an individual workflow task.
    pub fn with_task_timeout(mut self, timeout: Duration) -> Self {
        self.task_timeout = Some(timeout);
        self
    }

    /// Sets what to do when `workflow_id` is already in use.
    pub fn with_id_reuse_policy(
        mut self,
        policy: crate::proto::orcher::v1::WorkflowIdReusePolicy,
    ) -> Self {
        self.id_reuse_policy = Some(policy);
        self
    }
}

/// Maps a refused start to an error.
///
/// ALREADY_EXISTS is the one refusal a start can attribute to its workflow: it
/// names the id in use and the execution holding it. Any other code stays an
/// opaque status. A NOT_FOUND here is about the namespace, not the workflow,
/// and reporting it as a missing workflow would mislead.
fn start_error(status: tonic::Status, workflow_id: &str) -> Error {
    if status.code() == tonic::Code::AlreadyExists {
        Error::from_status_for_workflow(status, workflow_id)
    } else {
        Error::from(status)
    }
}

/// Converts a std `Duration` to the proto `Duration` used in workflow requests.
fn to_proto_duration(d: Duration) -> prost_types::Duration {
    prost_types::Duration {
        seconds: d.as_secs() as i64,
        nanos: d.subsec_nanos() as i32,
    }
}

/// Client for starting and managing ORCHER workflows.
///
/// Cloning is cheap: clones share the underlying channel.
///
/// # Examples
///
/// ```no_run
/// use orcher_sdk_core::client::WorkflowClient;
///
/// #[tokio::main]
/// async fn main() -> Result<(), Box<dyn std::error::Error>> {
///     // Connect to ORCHER server
///     let client = WorkflowClient::connect("http://localhost:50051").await?;
///
///     // Start a workflow
///     let handle = client.start_workflow(
///         "order-123",
///         "OrderProcessingWorkflow",
///         "order-queue",
///         serde_json::json!({"order_id": 123}),
///     ).await?;
///
///     // Wait for result
///     let result: serde_json::Value = handle.result().await?;
///     println!("Workflow result: {:?}", result);
///
///     Ok(())
/// }
/// ```
#[derive(Clone)]
pub struct WorkflowClient {
    client: WorkflowServiceClient<InterceptedService<Channel, AuthInterceptor>>,
    /// Shares the channel with `client`.
    query_client: QueryServiceClient<InterceptedService<Channel, AuthInterceptor>>,
    /// Kept so the gRPC clients can be rebuilt when credentials change.
    channel: Channel,
    /// Credentials attached to every request.
    auth: AuthInterceptor,
    /// Namespace every operation targets.
    namespace: String,
    timeout: Duration,
    /// The largest message sent or received.
    max_message_bytes: usize,
}

impl WorkflowClient {
    /// Creates a client for the server at `address`, sending no credentials.
    ///
    /// The connection is lazy: it opens on first use and reconnects on its
    /// own if the server goes away and comes back. The connect timeout is 5
    /// seconds and the per-request timeout 180 seconds.
    ///
    /// # Errors
    ///
    /// Returns a configuration error if `address` is not a valid URI.
    ///
    /// # Arguments
    ///
    /// * `address` - Server address (e.g., "http://localhost:50051")
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use orcher_sdk_core::client::WorkflowClient;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = WorkflowClient::connect("http://localhost:50051").await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn connect(address: impl Into<String>) -> Result<Self> {
        let address = address.into();

        let channel = Channel::from_shared(address.clone())
            .map_err(|e| Error::configuration(format!("Invalid server address: {}", e)))?
            .connect_timeout(Duration::from_secs(5))
            .timeout(Duration::from_secs(180))
            .connect_lazy();

        Ok(Self::from_channel(channel))
    }

    /// Creates a client on a pre-built gRPC channel, sending no credentials.
    ///
    /// Use this to configure TLS or other channel-level settings yourself.
    /// Add credentials with [`WorkflowClient::with_api_key`] and
    /// [`WorkflowClient::with_organization_id`].
    pub fn from_channel(channel: Channel) -> Self {
        let auth = AuthInterceptor::default();
        let max_message_bytes = crate::limits::default_max_message_bytes();
        Self {
            client: crate::limits::sized!(
                WorkflowServiceClient::with_interceptor(channel.clone(), auth.clone()),
                max_message_bytes
            ),
            query_client: crate::limits::sized!(
                QueryServiceClient::with_interceptor(channel.clone(), auth.clone()),
                max_message_bytes
            ),
            channel,
            auth,
            namespace: DEFAULT_NAMESPACE.to_string(),
            timeout: Duration::from_secs(180),
            max_message_bytes,
        }
    }

    /// Sends and receives messages of up to `max_message_bytes`: a workflow's
    /// input, its result, an event's payload.
    ///
    /// [`crate::limits::default_max_message_bytes`] unless set:
    /// `ORCHER_MAX_MESSAGE_BYTES`, or 32 MiB, the engine's default. A message
    /// over the limit on either side fails with an OUT_OF_RANGE status that
    /// says which limit to raise.
    pub fn with_max_message_bytes(mut self, max_message_bytes: usize) -> Self {
        self.max_message_bytes = max_message_bytes;
        self.rebuild_clients();
        self
    }

    /// Returns the credentials this client sends.
    ///
    /// An application usually runs three gRPC clients over one channel:
    /// workflow, actor, and namespace. Build the other two with this same
    /// interceptor so an API key or organization set here is not silently
    /// missing from their calls.
    pub fn auth(&self) -> &AuthInterceptor {
        &self.auth
    }

    /// Rebuilds the gRPC clients so a credential change takes effect.
    ///
    /// The interceptor is bound when the clients are constructed, so changing
    /// `auth` alone would leave the existing clients sending the old headers.
    fn rebuild_clients(&mut self) {
        self.client = crate::limits::sized!(
            WorkflowServiceClient::with_interceptor(self.channel.clone(), self.auth.clone()),
            self.max_message_bytes
        );
        self.query_client = crate::limits::sized!(
            QueryServiceClient::with_interceptor(self.channel.clone(), self.auth.clone()),
            self.max_message_bytes
        );
    }

    /// Sends `authorization: Bearer <key>` on every request from this client.
    pub fn with_api_key(mut self, api_key: impl Into<String>) -> Self {
        self.auth.api_key = Some(api_key.into());
        self.rebuild_clients();
        self
    }

    /// Sends `x-organization-id` on every request from this client.
    pub fn with_organization_id(mut self, organization_id: impl Into<String>) -> Self {
        self.auth.organization_id = Some(organization_id.into());
        self.rebuild_clients();
        self
    }

    /// Sets the namespace every operation from this client targets.
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = namespace.into();
        self
    }

    /// Sets the default timeout for operations.
    pub fn with_timeout(mut self, timeout: Duration) -> Self {
        self.timeout = timeout;
        self
    }

    /// Waits for a workflow's result, without a deadline, and returns its raw
    /// bytes.
    ///
    /// Use this to get a result without a [`WorkflowHandle`]. It is
    /// [`WorkflowClient::get_workflow_result_until`] with no deadline.
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - ID of the workflow
    /// * `run_id` - Run ID of the execution
    ///
    /// # Returns
    ///
    /// The workflow's serialized result.
    ///
    /// # Errors
    ///
    /// Returns [`Error::WorkflowExecutionFailed`] if the workflow failed.
    pub async fn get_workflow_result_raw(
        &self,
        workflow_id: impl Into<String>,
        run_id: impl Into<String>,
    ) -> Result<Vec<u8>> {
        self.get_workflow_result_until(workflow_id, run_id, None)
            .await
    }

    /// Waits for a workflow's terminal result, re-issuing the server long-poll
    /// until `deadline`, or for as long as the workflow runs when it is `None`.
    ///
    /// Each request asks the server to wait at most 20 seconds. A
    /// timeout, whether from the server's own bound or from a transport or
    /// proxy idle timeout, just means "not finished yet, ask again". Short
    /// polls let a caller await a workflow that runs for hours or days without
    /// pinning a server-side waiter task or tripping an intermediary.
    ///
    /// A lost connection is not treated as an answer: the wait continues,
    /// with backoff, until the deadline.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Timeout`] when `deadline` passes,
    /// [`Error::WorkflowExecutionFailed`] if the workflow failed, and the
    /// server's status for errors that are real answers (not found, invalid
    /// argument, permission denied).
    pub async fn get_workflow_result_until(
        &self,
        workflow_id: impl Into<String>,
        run_id: impl Into<String>,
        deadline: Option<tokio::time::Instant>,
    ) -> Result<Vec<u8>> {
        let workflow_id = workflow_id.into();
        let run_id = run_id.into();
        // Backoff for a connection that has gone away, so a server coming back
        // up is not met with a tight retry loop from every waiting caller.
        let mut reconnect_backoff = Duration::from_millis(100);
        loop {
            let window = match deadline {
                Some(d) => {
                    let remaining = d.saturating_duration_since(tokio::time::Instant::now());
                    if remaining.is_zero() {
                        return Err(Error::Timeout {
                            operation: format!("waiting for result of workflow '{}'", workflow_id),
                        });
                    }
                    remaining.min(RESULT_POLL_WINDOW)
                }
                None => RESULT_POLL_WINDOW,
            };

            let request = GetWorkflowResultRequest {
                workflow_id: workflow_id.clone(),
                execution_id: run_id.clone(),
                namespace: self.namespace.clone(),
                timeout: Some(prost_types::Duration {
                    seconds: window.as_secs() as i64,
                    nanos: 0,
                }),
            };

            let inner = match self.client.clone().get_workflow_result(request).await {
                Ok(r) => r.into_inner(),
                Err(status) => {
                    let err = Error::from(status);
                    // `is_normal_timeout` covers the server's DeadlineExceeded, a
                    // transport or proxy DeadlineExceeded, and Cancelled("Timeout
                    // expired"). The short sleep stops a server that returns
                    // instantly from turning this into a busy loop.
                    if err.is_normal_timeout() {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        continue;
                    }

                    // A dropped connection is not an answer about the workflow.
                    // A server restarting mid-wait surfaces here as a transport
                    // error while the workflow keeps running and may well
                    // complete. Returning that error would tell the caller the
                    // work failed, and a caller that then retries could repeat
                    // something that already succeeded.
                    //
                    // So wait again, within whatever deadline the caller set.
                    // Errors that are answers (not found, invalid, denied)
                    // still return immediately.
                    if err.is_retryable() {
                        let remaining = deadline
                            .map(|d| d.saturating_duration_since(tokio::time::Instant::now()));
                        if remaining == Some(Duration::ZERO) {
                            return Err(Error::Timeout {
                                operation: format!(
                                    "waiting for result of workflow '{}'",
                                    workflow_id
                                ),
                            });
                        }
                        let pause = match remaining {
                            Some(left) => reconnect_backoff.min(left),
                            None => reconnect_backoff,
                        };
                        debug!(
                            workflow_id = %workflow_id,
                            error = %err,
                            pause_ms = pause.as_millis(),
                            "Lost the connection while waiting for a result; waiting again"
                        );
                        tokio::time::sleep(pause).await;
                        reconnect_backoff = (reconnect_backoff * 2).min(Duration::from_secs(2));
                        continue;
                    }

                    return Err(err);
                }
            };

            return match inner.outcome {
                Some(crate::proto::orcher::v1::get_workflow_result_response::Outcome::Error(
                    error,
                )) => Err(Error::WorkflowExecutionFailed { message: error }),
                Some(crate::proto::orcher::v1::get_workflow_result_response::Outcome::Result(
                    result_bytes,
                )) => Ok(result_bytes),
                None => Err(Error::internal("Workflow result missing outcome")),
            };
        }
    }

    /// Starts a workflow execution with JSON-serialized input.
    ///
    /// # Errors
    ///
    /// Returns a serialization error if `input` cannot be encoded as JSON,
    /// and [`Error::WorkflowAlreadyExists`] if `workflow_id` is held by an open
    /// run.
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - Unique identifier for this workflow execution
    /// * `workflow_type` - Type/name of the workflow to execute
    /// * `task_queue` - Task queue where the workflow will be executed
    /// * `input` - Input data for the workflow (JSON-serializable)
    ///
    /// # Returns
    ///
    /// A `WorkflowHandle` that can be used to interact with the running workflow
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use orcher_sdk_core::client::WorkflowClient;
    /// # async fn example(client: WorkflowClient) -> Result<(), Box<dyn std::error::Error>> {
    /// let handle = client.start_workflow(
    ///     "order-123",
    ///     "OrderProcessingWorkflow",
    ///     "order-queue",
    ///     serde_json::json!({"order_id": 123, "amount": 99.99}),
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn start_workflow<T: serde::Serialize>(
        &self,
        workflow_id: impl Into<String>,
        workflow_type: impl Into<String>,
        task_queue: impl Into<String>,
        input: T,
    ) -> Result<WorkflowHandle> {
        let workflow_id = workflow_id.into();
        let workflow_type = workflow_type.into();
        let task_queue = task_queue.into();

        let input_bytes = serde_json::to_vec(&input)
            .map_err(|e| Error::serialization(format!("Failed to serialize input: {}", e)))?;

        self.start_workflow_raw(workflow_id, workflow_type, task_queue, input_bytes)
            .await
    }

    /// Starts a workflow execution with already-serialized input.
    ///
    /// Use this with a custom data converter or payload envelope.
    ///
    /// # Errors
    ///
    /// Returns [`Error::WorkflowAlreadyExists`] if `workflow_id` is held by an
    /// open run.
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - Unique identifier for this workflow execution
    /// * `workflow_type` - Type/name of the workflow to execute
    /// * `task_queue` - Task queue where the workflow will be executed
    /// * `input_bytes` - Pre-serialized input bytes (JSON, Payload envelope, etc.)
    ///
    /// # Returns
    ///
    /// A `WorkflowHandle` that can be used to interact with the running workflow
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use orcher_sdk_core::client::WorkflowClient;
    /// # async fn example(client: WorkflowClient) -> Result<(), Box<dyn std::error::Error>> {
    /// let input_bytes = serde_json::to_vec(&serde_json::json!({"order_id": 123}))?;
    /// let handle = client.start_workflow_raw(
    ///     "order-123",
    ///     "OrderProcessingWorkflow",
    ///     "order-queue",
    ///     input_bytes,
    /// ).await?;
    /// # Ok(())
    /// # }
    /// ```
    pub async fn start_workflow_raw(
        &self,
        workflow_id: impl Into<String>,
        workflow_type: impl Into<String>,
        task_queue: impl Into<String>,
        input_bytes: Vec<u8>,
    ) -> Result<WorkflowHandle> {
        let workflow_id: String = workflow_id.into();
        let request = StartWorkflowRequest {
            workflow_id: workflow_id.clone(),
            workflow_type: workflow_type.into(),
            task_queue: task_queue.into(),
            namespace: self.namespace.clone(),
            input: input_bytes,
            execution_timeout: None,
            run_timeout: None,
            task_timeout: None,
            retry_policy: None,
            cron_schedule: String::new(),
            annotations: HashMap::new(),
            labels: HashMap::new(),
            request_id: uuid::Uuid::new_v4().to_string(),
            start_delay: None,
            schedule_config: None,
            workflow_id_reuse_policy: 0,
        };
        self.start_workflow_request(request).await
    }

    /// Starts a single workflow from a fully formed request.
    ///
    /// This is the general primitive over the `StartWorkflow` endpoint. The
    /// caller controls every field (retry policy, timeouts, cron, labels, and
    /// so on); `start_workflow_raw` and the higher-level SDKs build on it. The
    /// handle carries the caller's `workflow_id`, or the server-assigned one
    /// when it is left empty. Unlike the batch endpoint, this uses the
    /// single-start path and does not re-wrap the input.
    ///
    /// # Errors
    ///
    /// A workflow id already in use fails with [`Error::WorkflowAlreadyExists`],
    /// whose `run_id` names the execution holding it. Any other refusal is
    /// returned as the server's status.
    pub async fn start_workflow_request(
        &self,
        request: StartWorkflowRequest,
    ) -> Result<WorkflowHandle> {
        let requested_id = request.workflow_id.clone();
        let response = self
            .client
            .clone()
            .start_workflow(request)
            .await
            .map_err(|status| start_error(status, &requested_id))?;
        let inner = response.into_inner();
        let workflow_id = if requested_id.is_empty() {
            inner.workflow_id
        } else {
            requested_id
        };
        Ok(WorkflowHandle {
            client: self.clone(),
            execution: WorkflowExecution::new(workflow_id, inner.execution_id),
        })
    }

    /// Starts a workflow execution with the options in [`StartWorkflowOpts`].
    ///
    /// Use this for anything beyond the basic start: a cron schedule,
    /// timeouts, a retry policy, or an id reuse policy.
    ///
    /// # Errors
    ///
    /// Same as [`WorkflowClient::start_workflow_request`].
    pub async fn start_workflow_with_options(
        &self,
        opts: StartWorkflowOpts,
    ) -> Result<WorkflowHandle> {
        let request = StartWorkflowRequest {
            workflow_id: opts.workflow_id,
            workflow_type: opts.workflow_type,
            task_queue: opts.task_queue,
            namespace: self.namespace.clone(),
            input: opts.input_bytes,
            execution_timeout: opts.execution_timeout.map(to_proto_duration),
            run_timeout: opts.run_timeout.map(to_proto_duration),
            task_timeout: opts.task_timeout.map(to_proto_duration),
            retry_policy: opts.retry_policy,
            cron_schedule: opts.cron_schedule.unwrap_or_default(),
            annotations: HashMap::new(),
            labels: HashMap::new(),
            request_id: uuid::Uuid::new_v4().to_string(),
            start_delay: None,
            schedule_config: None,
            workflow_id_reuse_policy: opts.id_reuse_policy.map_or(0, |p| p as i32),
        };
        self.start_workflow_request(request).await
    }

    /// Starts several workflows in a single batch transaction.
    ///
    /// Returns one handle per started workflow.
    ///
    /// # Errors
    ///
    /// Returns the server's status unchanged. A batch has no single workflow
    /// to attribute a refusal to.
    pub async fn batch_start_workflows(
        &self,
        requests: Vec<StartWorkflowRequest>,
    ) -> Result<Vec<WorkflowHandle>> {
        use crate::proto::orcher::v1::BatchStartWorkflowRequest;

        let batch_request = BatchStartWorkflowRequest {
            workflows: requests.clone(),
        };

        let response = self
            .client
            .clone()
            .batch_start_workflow(batch_request)
            .await
            // No single workflow to attribute a NOT_FOUND to here, so the status
            // stays opaque rather than being misfiled against an arbitrary id.
            .map_err(Error::GrpcStatus)?;

        let executions = response.into_inner().executions;
        let handles = executions
            .into_iter()
            .map(|exec| WorkflowHandle {
                client: self.clone(),
                execution: WorkflowExecution::new(exec.workflow_id, exec.execution_id),
            })
            .collect();

        Ok(handles)
    }

    /// Sends an event with a JSON-serialized payload to a running workflow.
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - ID of the workflow to send event to
    /// * `run_id` - Run to target; `None` targets the latest run
    /// * `event_name` - Name of the event
    /// * `input` - Event payload
    pub async fn send_event<T: serde::Serialize>(
        &self,
        workflow_id: impl Into<String>,
        run_id: Option<String>,
        event_name: impl Into<String>,
        input: T,
    ) -> Result<()> {
        let workflow_id: String = workflow_id.into();
        let input_bytes = serde_json::to_vec(&input)
            .map_err(|e| Error::serialization(format!("Failed to serialize input: {}", e)))?;

        let request = SendEventRequest {
            workflow_id: workflow_id.clone(),
            execution_id: run_id.unwrap_or_default(),
            namespace: self.namespace.clone(),
            event_name: event_name.into(),
            payload: input_bytes,
            request_id: uuid::Uuid::new_v4().to_string(),
        };

        self.client
            .clone()
            .send_event(request)
            .await
            .map_err(|s| Error::from_status_for_workflow(s, &workflow_id))?;

        Ok(())
    }

    /// Queries a running workflow.
    ///
    /// Arguments are sent as JSON and the result is decoded from JSON.
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - ID of the workflow to query
    /// * `run_id` - Optional run ID
    /// * `query_name` - Name of the query handler
    /// * `args` - Query arguments
    pub async fn query_workflow<T: serde::Serialize, R: for<'de> serde::Deserialize<'de>>(
        &self,
        workflow_id: impl Into<String>,
        run_id: Option<String>,
        query_name: impl Into<String>,
        args: T,
    ) -> Result<R> {
        let args_bytes = serde_json::to_vec(&args)
            .map_err(|e| Error::serialization(format!("Failed to serialize args: {}", e)))?;

        let workflow_id: String = workflow_id.into();
        let request = QueryWorkflowRequest {
            workflow_id: workflow_id.clone(),
            execution_id: run_id.unwrap_or_default(),
            namespace: self.namespace.clone(),
            query_type: query_name.into(),
            query_args: args_bytes,
        };

        let response = self
            .client
            .clone()
            .query_workflow(request)
            .await
            .map_err(|s| Error::from_status_for_workflow(s, &workflow_id))?;

        let result_bytes = response.into_inner().result;

        serde_json::from_slice(&result_bytes)
            .map_err(|e| Error::deserialization(format!("Failed to deserialize result: {}", e)))
    }

    /// Sends an update to a running workflow and waits for its result.
    ///
    /// Updates are synchronous mutations: they can read and modify workflow
    /// state, and their results are journaled so replay reproduces them.
    ///
    /// # Errors
    ///
    /// Returns an internal error carrying the handler's message if the
    /// workflow rejects the update.
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - ID of the workflow to update
    /// * `run_id` - Optional run ID
    /// * `update_name` - Name of the update handler
    /// * `args` - Update arguments
    pub async fn update_workflow<T: serde::Serialize, R: for<'de> serde::Deserialize<'de>>(
        &self,
        workflow_id: impl Into<String>,
        run_id: Option<String>,
        update_name: impl Into<String>,
        args: T,
    ) -> Result<R> {
        let args_bytes = serde_json::to_vec(&args)
            .map_err(|e| Error::serialization(format!("Failed to serialize args: {}", e)))?;

        let workflow_id: String = workflow_id.into();
        let request = UpdateWorkflowRequest {
            workflow_id: workflow_id.clone(),
            execution_id: run_id.unwrap_or_default(),
            namespace: self.namespace.clone(),
            update_type: update_name.into(),
            args: args_bytes,
            update_id: String::new(),
        };

        let response = self
            .client
            .clone()
            .update_workflow(request)
            .await
            .map_err(|s| Error::from_status_for_workflow(s, &workflow_id))?;

        let inner = response.into_inner();
        match inner.outcome {
            Some(update_workflow_response::Outcome::Result(result_bytes)) => {
                serde_json::from_slice(&result_bytes).map_err(|e| {
                    Error::deserialization(format!("Failed to deserialize update result: {}", e))
                })
            }
            Some(update_workflow_response::Outcome::Failure(failure)) => Err(Error::internal(
                format!("Update rejected: {}", failure.message),
            )),
            None => Err(Error::internal("No outcome in update response")),
        }
    }

    /// Returns a workflow's current status.
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - ID of the workflow
    /// * `run_id` - Optional run ID
    pub async fn get_workflow_status(
        &self,
        workflow_id: impl Into<String>,
        run_id: Option<String>,
    ) -> Result<WorkflowStatus> {
        let workflow_id: String = workflow_id.into();
        let request = GetWorkflowStatusRequest {
            workflow_id: workflow_id.clone(),
            execution_id: run_id.unwrap_or_default(),
            namespace: self.namespace.clone(),
        };

        let response = self
            .client
            .clone()
            .get_workflow_status(request)
            .await
            .map_err(|s| Error::from_status_for_workflow(s, &workflow_id))?;

        Ok(WorkflowStatus::from(response.into_inner().status))
    }

    /// Requests cancellation of a workflow execution.
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - ID of the workflow to cancel
    /// * `run_id` - Optional run ID
    pub async fn cancel_workflow(
        &self,
        workflow_id: impl Into<String>,
        run_id: Option<String>,
    ) -> Result<()> {
        let workflow_id: String = workflow_id.into();
        let request = CancelWorkflowRequest {
            workflow_id: workflow_id.clone(),
            execution_id: run_id.unwrap_or_default(),
            namespace: self.namespace.clone(),
            reason: String::new(),
            request_id: uuid::Uuid::new_v4().to_string(),
        };

        self.client
            .clone()
            .cancel_workflow(request)
            .await
            .map_err(|s| Error::from_status_for_workflow(s, &workflow_id))?;

        Ok(())
    }

    /// Returns detailed metadata about a workflow execution.
    ///
    /// The description includes execution info, counts of pending tasks,
    /// timers, and events, and the execution's configuration.
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - ID of the workflow
    /// * `run_id` - Optional run ID
    pub async fn describe_workflow(
        &self,
        workflow_id: impl Into<String>,
        run_id: Option<String>,
    ) -> Result<WorkflowExecutionDescription> {
        let workflow_id: String = workflow_id.into();
        let request = DescribeWorkflowExecutionRequest {
            workflow_id: workflow_id.clone(),
            execution_id: run_id.unwrap_or_default(),
            namespace: self.namespace.clone(),
        };

        let response = self
            .query_client
            .clone()
            .describe_workflow_execution(request)
            .await
            .map_err(|s| Error::from_status_for_workflow(s, &workflow_id))?;

        WorkflowExecutionDescription::from_proto(response.into_inner())
    }

    /// Lists workflow executions, optionally filtered.
    ///
    /// Returns one page of workflow summaries; pass the page's
    /// `next_page_token` back to fetch the following page. A page size of 0
    /// requests 100 results.
    ///
    /// # Arguments
    ///
    /// * `options` - Filter, sort, and pagination options
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use orcher_sdk_core::client::WorkflowClient;
    /// # use orcher_sdk_core::types::ListWorkflowsOptions;
    /// # async fn example(client: WorkflowClient) -> Result<(), Box<dyn std::error::Error>> {
    /// // List OrderProcessing workflows, 50 per page
    /// let page = client
    ///     .list_workflows(
    ///         ListWorkflowsOptions::default()
    ///             .with_workflow_type("OrderProcessing")
    ///             .with_page_size(50),
    ///     )
    ///     .await?;
    ///
    /// for exec in &page.executions {
    ///     println!("{}: {:?}", exec.workflow_id, exec.status);
    /// }
    ///
    /// // Fetch the following page, if there is one
    /// if page.has_more() {
    ///     let next = client
    ///         .list_workflows(ListWorkflowsOptions::default().with_next_page_token(page.next_page_token))
    ///         .await?;
    /// }
    /// # Ok(())
    /// # }
    /// ```
    pub async fn list_workflows(&self, options: ListWorkflowsOptions) -> Result<WorkflowListPage> {
        let status_filter: Vec<i32> = options.status_filter.into_iter().map(i32::from).collect();

        let request = ListWorkflowsRequest {
            namespace: self.namespace.clone(),
            page_size: if options.page_size > 0 {
                options.page_size
            } else {
                100
            },
            next_page_token: options.next_page_token,
            workflow_type: options.workflow_type.unwrap_or_default(),
            task_queue: options.task_queue.unwrap_or_default(),
            status_filter,
            start_time_begin: None,
            start_time_end: None,
            sort_order: options.sort_order.map(i32::from).unwrap_or(0),
        };

        let response = self
            .query_client
            .clone()
            .list_workflows(request)
            .await
            // No single workflow to attribute a NOT_FOUND to here, so the status
            // stays opaque rather than being misfiled against an arbitrary id.
            .map_err(Error::GrpcStatus)?;

        Ok(WorkflowListPage::from_list_response(response.into_inner()))
    }

    /// Searches workflow executions with a SQL-like query string.
    ///
    /// The query can filter on workflow attributes, including custom labels.
    /// A page size of 0 requests 100 results.
    ///
    /// # Arguments
    ///
    /// * `query` - SQL-like query string
    /// * `options` - Pagination options
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use orcher_sdk_core::client::WorkflowClient;
    /// # use orcher_sdk_core::types::SearchWorkflowsOptions;
    /// # async fn example(client: WorkflowClient) -> Result<(), Box<dyn std::error::Error>> {
    /// let page = client.search_workflows(
    ///     "WorkflowType = 'OrderProcessing' AND Status = 'Running'",
    ///     SearchWorkflowsOptions::default(),
    /// ).await?;
    ///
    /// println!("Found {} workflows", page.executions.len());
    /// # Ok(())
    /// # }
    /// ```
    pub async fn search_workflows(
        &self,
        query: impl Into<String>,
        options: SearchWorkflowsOptions,
    ) -> Result<WorkflowListPage> {
        let request = SearchWorkflowsRequest {
            namespace: self.namespace.clone(),
            page_size: if options.page_size > 0 {
                options.page_size
            } else {
                100
            },
            next_page_token: options.next_page_token,
            query: query.into(),
            sort_fields: vec![],
        };

        let response = self
            .query_client
            .clone()
            .search_workflows(request)
            .await
            // No single workflow to attribute a NOT_FOUND to here, so the status
            // stays opaque rather than being misfiled against an arbitrary id.
            .map_err(Error::GrpcStatus)?;

        Ok(WorkflowListPage::from_search_response(
            response.into_inner(),
        ))
    }

    /// Terminates a workflow execution.
    ///
    /// # Arguments
    ///
    /// * `workflow_id` - ID of the workflow to terminate
    /// * `run_id` - Optional run ID
    /// * `reason` - Reason for termination
    pub async fn terminate_workflow(
        &self,
        workflow_id: impl Into<String>,
        run_id: Option<String>,
        reason: impl Into<String>,
    ) -> Result<()> {
        let workflow_id: String = workflow_id.into();
        let request = TerminateWorkflowRequest {
            workflow_id: workflow_id.clone(),
            execution_id: run_id.unwrap_or_default(),
            namespace: self.namespace.clone(),
            reason: reason.into(),
            details: vec![],
        };

        self.client
            .clone()
            .terminate_workflow(request)
            .await
            .map_err(|s| Error::from_status_for_workflow(s, &workflow_id))?;

        Ok(())
    }

    /// Resets a workflow to a journal point and re-executes it from there.
    ///
    /// Returns the execution id of the run the reset creates.
    pub async fn reset_workflow(
        &self,
        workflow_id: impl Into<String>,
        run_id: Option<String>,
        target_event_id: i64,
        reason: impl Into<String>,
    ) -> Result<String> {
        let workflow_id: String = workflow_id.into();
        let request = ResetWorkflowRequest {
            workflow_id: workflow_id.clone(),
            execution_id: run_id.unwrap_or_default(),
            namespace: self.namespace.clone(),
            target_event_id,
            reason: reason.into(),
        };

        let response = self
            .client
            .clone()
            .reset_workflow(request)
            .await
            .map_err(|s| Error::from_status_for_workflow(s, &workflow_id))?;

        Ok(response.into_inner().new_execution_id)
    }
}

/// Handle to one workflow execution.
///
/// Every method targets this specific run.
#[derive(Clone)]
pub struct WorkflowHandle {
    client: WorkflowClient,
    execution: WorkflowExecution,
}

impl WorkflowHandle {
    /// Creates a handle for an execution that already exists.
    ///
    /// Use this when you have a workflow id and run id and want to interact
    /// with that run without starting one.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use orcher_sdk_core::client::{WorkflowClient, WorkflowHandle};
    /// # use orcher_sdk_core::types::WorkflowExecution;
    /// # async fn example() -> Result<(), Box<dyn std::error::Error>> {
    /// let client = WorkflowClient::connect("http://localhost:50051").await?;
    /// let execution = WorkflowExecution::new("workflow-123", "run-456");
    /// let handle = WorkflowHandle::new(client, execution);
    /// # Ok(())
    /// # }
    /// ```
    pub fn new(client: WorkflowClient, execution: WorkflowExecution) -> Self {
        Self { client, execution }
    }

    /// Returns the execution identifier.
    pub fn execution(&self) -> &WorkflowExecution {
        &self.execution
    }

    /// Returns the workflow id.
    pub fn workflow_id(&self) -> &str {
        &self.execution.workflow_id
    }

    /// Returns the run id.
    pub fn run_id(&self) -> &str {
        &self.execution.run_id
    }

    /// Waits for the workflow to finish and returns its JSON-decoded result.
    ///
    /// The wait has no deadline: it lasts as long as the workflow runs, and
    /// survives lost connections. Use
    /// [`result_with_timeout`](Self::result_with_timeout) to bound it.
    ///
    /// # Errors
    ///
    /// Returns [`Error::WorkflowExecutionFailed`] if the workflow failed, and
    /// a deserialization error if the result does not decode as `T`.
    ///
    /// # Examples
    ///
    /// ```no_run
    /// # use orcher_sdk_core::client::WorkflowHandle;
    /// # async fn example(handle: WorkflowHandle) -> Result<(), Box<dyn std::error::Error>> {
    /// let result: serde_json::Value = handle.result().await?;
    /// println!("Workflow completed with result: {:?}", result);
    /// # Ok(())
    /// # }
    /// ```
    pub async fn result<T: for<'de> serde::Deserialize<'de>>(&self) -> Result<T> {
        let bytes = self.result_bytes_until(None).await?;
        serde_json::from_slice(&bytes)
            .map_err(|e| Error::deserialization(format!("Failed to deserialize result: {}", e)))
    }

    /// Waits for the workflow's result, giving up after `timeout`.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Timeout`] if the workflow has not reached a terminal
    /// state in time, and otherwise the same errors as
    /// [`result`](Self::result).
    pub async fn result_with_timeout<T: for<'de> serde::Deserialize<'de>>(
        &self,
        timeout: std::time::Duration,
    ) -> Result<T> {
        let bytes = self
            .result_bytes_until(Some(tokio::time::Instant::now() + timeout))
            .await?;
        serde_json::from_slice(&bytes)
            .map_err(|e| Error::deserialization(format!("Failed to deserialize result: {}", e)))
    }

    /// Waits for a terminal result using the client's polling loop in
    /// [`WorkflowClient::get_workflow_result_until`].
    async fn result_bytes_until(&self, deadline: Option<tokio::time::Instant>) -> Result<Vec<u8>> {
        self.client
            .get_workflow_result_until(
                self.execution.workflow_id.clone(),
                self.execution.run_id.clone(),
                deadline,
            )
            .await
    }

    /// Returns the workflow's current status.
    pub async fn status(&self) -> Result<WorkflowStatus> {
        self.client
            .get_workflow_status(
                &self.execution.workflow_id,
                Some(self.execution.run_id.clone()),
            )
            .await
    }

    /// Sends an event to this workflow.
    pub async fn send_event<T: serde::Serialize>(
        &self,
        event_name: impl Into<String>,
        input: T,
    ) -> Result<()> {
        self.client
            .send_event(
                &self.execution.workflow_id,
                Some(self.execution.run_id.clone()),
                event_name,
                input,
            )
            .await
    }

    /// Queries this workflow.
    pub async fn query<T: serde::Serialize, R: for<'de> serde::Deserialize<'de>>(
        &self,
        query_name: impl Into<String>,
        args: T,
    ) -> Result<R> {
        self.client
            .query_workflow(
                &self.execution.workflow_id,
                Some(self.execution.run_id.clone()),
                query_name,
                args,
            )
            .await
    }

    /// Sends an update to this workflow and waits for its result.
    pub async fn update<T: serde::Serialize, R: for<'de> serde::Deserialize<'de>>(
        &self,
        update_name: impl Into<String>,
        args: T,
    ) -> Result<R> {
        self.client
            .update_workflow(
                &self.execution.workflow_id,
                Some(self.execution.run_id.clone()),
                update_name,
                args,
            )
            .await
    }

    /// Requests cancellation of this workflow.
    pub async fn cancel(&self) -> Result<()> {
        self.client
            .cancel_workflow(
                &self.execution.workflow_id,
                Some(self.execution.run_id.clone()),
            )
            .await
    }

    /// Returns detailed metadata about this execution.
    ///
    /// The description includes status, timing, configuration, and counts of
    /// pending tasks, timers, and events.
    pub async fn describe(&self) -> Result<WorkflowExecutionDescription> {
        self.client
            .describe_workflow(
                &self.execution.workflow_id,
                Some(self.execution.run_id.clone()),
            )
            .await
    }

    /// Terminates this workflow.
    pub async fn terminate(&self, reason: impl Into<String>) -> Result<()> {
        self.client
            .terminate_workflow(
                &self.execution.workflow_id,
                Some(self.execution.run_id.clone()),
                reason,
            )
            .await
    }

    /// Resets this workflow to a journal point and re-executes it from there.
    ///
    /// The journal is replayed up to and including `target_event_id`, then
    /// fresh decisions are made from that point. The current execution ends
    /// with terminal status `reset`.
    ///
    /// Returns the execution id of the run the reset creates.
    pub async fn reset(&self, target_event_id: i64, reason: impl Into<String>) -> Result<String> {
        self.client
            .reset_workflow(
                &self.execution.workflow_id,
                Some(self.execution.run_id.clone()),
                target_event_id,
                reason,
            )
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tonic::service::Interceptor;

    #[test]
    fn auth_interceptor_sends_bearer_token() {
        let mut auth = AuthInterceptor {
            api_key: Some("secret-key".into()),
            organization_id: None,
        };
        let req = auth
            .call(tonic::Request::new(()))
            .expect("interceptor failed");
        assert_eq!(
            req.metadata().get("authorization").unwrap(),
            "Bearer secret-key"
        );
    }

    #[test]
    fn auth_interceptor_sends_organization_id() {
        let mut auth = AuthInterceptor {
            api_key: None,
            organization_id: Some("org-123".into()),
        };
        let req = auth
            .call(tonic::Request::new(()))
            .expect("interceptor failed");
        assert_eq!(req.metadata().get("x-organization-id").unwrap(), "org-123");
    }

    #[test]
    fn auth_interceptor_is_inert_without_credentials() {
        let mut auth = AuthInterceptor::default();
        let req = auth
            .call(tonic::Request::new(()))
            .expect("interceptor failed");
        assert!(req.metadata().get("authorization").is_none());
        assert!(req.metadata().get("x-organization-id").is_none());
    }

    #[test]
    fn auth_interceptor_rejects_unencodable_key() {
        // A newline cannot go in a header. Sending the request unauthenticated
        // instead would hide a configuration error, so the call must fail.
        let mut auth = AuthInterceptor {
            api_key: Some("bad\nkey".into()),
            organization_id: None,
        };
        assert!(auth.call(tonic::Request::new(())).is_err());
    }

    #[test]
    fn test_workflow_execution_display() {
        let execution = WorkflowExecution::new("test-workflow", "test-run");
        assert_eq!(execution.workflow_id, "test-workflow");
        assert_eq!(execution.run_id, "test-run");
    }

    /// A start refused because its id is in use says so and names the run
    /// holding the id, so a caller that means to start the workflow once can
    /// tell it apart from a failure.
    #[test]
    fn a_refused_start_names_the_run_in_the_way() {
        let mut metadata = tonic::metadata::MetadataMap::new();
        metadata.insert(
            crate::error::EXECUTION_ID_METADATA_KEY,
            "run-1".parse().unwrap(),
        );
        let err = start_error(
            tonic::Status::with_metadata(tonic::Code::AlreadyExists, "running", metadata),
            "order-1",
        );
        match err {
            Error::WorkflowAlreadyExists {
                workflow_id,
                run_id,
            } => {
                assert_eq!(workflow_id, "order-1");
                assert_eq!(run_id.as_deref(), Some("run-1"));
            }
            other => panic!("expected WorkflowAlreadyExists, got {other:?}"),
        }
    }

    /// Any other refusal of a start is about the request or its namespace,
    /// not a workflow, and stays an opaque status.
    #[test]
    fn other_start_refusals_stay_opaque() {
        let err = start_error(tonic::Status::not_found("namespace"), "order-1");
        assert!(matches!(err, Error::GrpcStatus(ref s) if s.code() == tonic::Code::NotFound));
    }
}
