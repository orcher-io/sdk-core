//! gRPC client for invoking operations on virtual actors through the Orcher
//! server.

use std::collections::HashMap;
use tonic::service::interceptor::InterceptedService;
use tonic::transport::Channel;

use crate::client::workflow::AuthInterceptor;

use crate::error::{Error, Result};
use crate::proto::orcher::v1::{
    actor_service_client::ActorServiceClient, ExecutionStatus, InvokeOperationRequest,
};

/// Low-level gRPC client for actor operations.
///
/// Payloads and results are raw bytes. Language SDKs build their typed actor
/// clients (such as `ActorInvocationClient`) on top of this one and handle
/// serialization themselves.
#[derive(Clone)]
pub struct CoreActorClient {
    client: ActorServiceClient<InterceptedService<Channel, AuthInterceptor>>,
}

impl CoreActorClient {
    /// Creates an actor client on an existing gRPC channel that sends no
    /// credentials.
    ///
    /// Use [`CoreActorClient::with_auth`] against a server that requires them.
    pub fn new(channel: Channel) -> Self {
        Self::with_auth(channel, AuthInterceptor::default())
    }

    /// Creates an actor client that sends the given credentials on every call.
    ///
    /// Pass the workflow client's own
    /// [`WorkflowClient::auth`](crate::client::workflow::WorkflowClient::auth)
    /// so actor and workflow calls share one set of credentials. An actor
    /// client built without them is refused as unauthenticated by a server
    /// that requires an API key, even while workflow calls succeed.
    pub fn with_auth(channel: Channel, auth: AuthInterceptor) -> Self {
        Self {
            client: ActorServiceClient::with_interceptor(channel, auth),
        }
    }

    /// Invokes an actor operation and waits for its result.
    ///
    /// The server queues the operation for a worker to execute and replies
    /// once it has finished.
    ///
    /// # Arguments
    ///
    /// * `actor_name` - Actor type name (e.g., "Counter")
    /// * `key` - Actor instance key (e.g., "user-123")
    /// * `operation` - Operation name (e.g., "increment")
    /// * `payload` - Serialized operation payload (JSON bytes)
    /// * `timeout_ms` - Optional timeout in milliseconds
    /// * `idempotency_key` - Optional idempotency key for deduplication
    ///
    /// # Returns
    ///
    /// The serialized result bytes on success.
    ///
    /// # Errors
    ///
    /// Returns the transport error if the RPC fails, and
    /// [`Error::WorkflowExecutionFailed`] if the operation failed, timed out,
    /// was cancelled, or came back with an unknown status.
    pub async fn invoke_operation(
        &self,
        actor_name: String,
        key: String,
        operation: String,
        payload: Vec<u8>,
        timeout_ms: Option<u64>,
        idempotency_key: Option<String>,
    ) -> Result<Vec<u8>> {
        let request = InvokeOperationRequest {
            actor_name: actor_name.clone(),
            key: key.clone(),
            operation: operation.clone(),
            payload,
            timeout_ms: timeout_ms.unwrap_or(0),
            idempotency_key: idempotency_key.unwrap_or_default(),
            metadata: HashMap::new(),
        };

        let response = self
            .client
            .clone()
            .invoke_operation(request)
            .await?
            .into_inner();

        match ExecutionStatus::try_from(response.status) {
            Ok(ExecutionStatus::Success) => Ok(response.result),
            Ok(ExecutionStatus::Error) => Err(Error::WorkflowExecutionFailed {
                message: format!(
                    "Actor operation {}.{} on key '{}' failed: {} (code: {})",
                    actor_name, operation, key, response.error_message, response.error_code
                ),
            }),
            Ok(ExecutionStatus::Timeout) => Err(Error::WorkflowExecutionFailed {
                message: format!(
                    "Actor operation {}.{} on key '{}' timed out",
                    actor_name, operation, key
                ),
            }),
            Ok(ExecutionStatus::Cancelled) => Err(Error::WorkflowExecutionFailed {
                message: format!(
                    "Actor operation {}.{} on key '{}' was cancelled",
                    actor_name, operation, key
                ),
            }),
            _ => Err(Error::WorkflowExecutionFailed {
                message: format!(
                    "Actor operation {}.{} on key '{}' returned unknown status: {}",
                    actor_name, operation, key, response.status
                ),
            }),
        }
    }

    /// Invokes an actor operation and discards its result.
    ///
    /// This makes the same RPC as [`CoreActorClient::invoke_operation`], so it
    /// still waits for the operation to finish and still returns its errors.
    /// The server has no dispatch-only mode.
    pub async fn invoke_operation_no_wait(
        &self,
        actor_name: String,
        key: String,
        operation: String,
        payload: Vec<u8>,
    ) -> Result<()> {
        let _ = self
            .invoke_operation(actor_name, key, operation, payload, None, None)
            .await?;
        Ok(())
    }
}
