//! Actors: the worker's handlers registered with the actor service, their
//! operations handed to the language by pull, and the actors' state read and
//! written on the worker's own connection.
//!
//! Every call to the actor service (registration, state, and the driver's
//! polls, completions and heartbeats) carries the worker's TLS, API key and
//! organization, over one connection for the host's calls. When the actor
//! service asks for the handlers again, the host registers them again and
//! the driver's heartbeats name the new registration.

use std::collections::{HashMap, HashSet};
use std::sync::Mutex;

use orcher_sdk_core::proto::host::v1::{actor_result, ActorMode, ActorOperation, ActorResult};
use orcher_sdk_core::proto::kernel::v1::Payload;
use orcher_sdk_core::proto::orcher::v1::actor_service_client::ActorServiceClient;
use orcher_sdk_core::proto::orcher::v1::{
    self as engine, DeleteStateRequest, GetStateRequest, ListStateKeysRequest,
    RegisterHandlersRequest, SetStateRequest,
};
use orcher_sdk_core::transport::{credentialed_request, limits};
use orcher_sdk_core::worker::{ActorWork, ActorWorkResult, RegistrationHandle};
use orcher_sdk_core::{Error, Result, TaskFailure};
use tokio::sync::mpsc;

use crate::HostConfig;

/// One operation a worker runs for an actor type.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct ActorHandler {
    /// The actor type, for example `Cart`.
    pub actor_name: String,
    /// The operation, for example `add_item`.
    pub operation: String,
    /// Whether the operation may run alongside others on the same actor (a
    /// read), rather than alone (a write).
    pub shared: bool,
}

impl ActorHandler {
    /// An operation that runs alone on its actor.
    pub fn exclusive(actor_name: impl Into<String>, operation: impl Into<String>) -> Self {
        Self {
            actor_name: actor_name.into(),
            operation: operation.into(),
            shared: false,
        }
    }

    /// An operation that may run alongside other shared operations on its
    /// actor.
    pub fn shared(actor_name: impl Into<String>, operation: impl Into<String>) -> Self {
        Self {
            shared: true,
            ..Self::exclusive(actor_name, operation)
        }
    }
}

/// A value of an actor's state, with the version it was written at.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct StateValue {
    /// The stored bytes.
    pub value: Vec<u8>,
    /// The version, for a conditional write.
    pub version: String,
}

/// Which actor's state, and on behalf of which operation.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct StateScope {
    /// The actor type.
    pub actor_name: String,
    /// The actor instance.
    pub key: String,
    /// The operation the access is part of.
    pub execution_id: String,
}

impl StateScope {
    /// The state of actor `actor_name` / `key`, for the operation run as
    /// `execution_id`.
    pub fn new(
        actor_name: impl Into<String>,
        key: impl Into<String>,
        execution_id: impl Into<String>,
    ) -> Self {
        Self {
            actor_name: actor_name.into(),
            key: key.into(),
            execution_id: execution_id.into(),
        }
    }

    /// The scope of `operation`'s actor, for that operation.
    pub fn of(operation: &ActorOperation) -> Self {
        Self::new(
            &operation.actor_name,
            &operation.key,
            &operation.execution_id,
        )
    }
}

/// The host's calls to the actor service, with the worker's credentials.
#[derive(Clone)]
pub(crate) struct ActorService {
    client: ActorServiceClient<orcher_sdk_core::transport::instrument::WireChannel>,
    api_key: Option<String>,
    organization_id: Option<String>,
}

impl ActorService {
    pub(crate) async fn connect(
        config: &HostConfig,
        wire: &std::sync::Arc<orcher_sdk_core::observability::WireMetrics>,
    ) -> Result<Self> {
        let channel = orcher_sdk_core::transport::channel::connect_channel(
            &config.server_url,
            config.tls.as_ref(),
            wire,
        )
        .await?;
        let max = limits::default_max_message_bytes();
        Ok(Self {
            client: ActorServiceClient::new(channel)
                .max_decoding_message_size(max)
                .max_encoding_message_size(max),
            api_key: config.api_key.clone(),
            organization_id: config.organization_id.clone(),
        })
    }

    fn request<T>(&self, body: T) -> tonic::Request<T> {
        credentialed_request(
            body,
            self.api_key.as_deref(),
            self.organization_id.as_deref(),
        )
    }

    /// Registers the worker's handlers; returns the registration.
    pub(crate) async fn register(
        &self,
        service_id: &str,
        handlers: &[ActorHandler],
        metadata: &HashMap<String, String>,
    ) -> Result<String> {
        let request = RegisterHandlersRequest {
            service_id: service_id.to_string(),
            handlers: handlers
                .iter()
                .map(|handler| engine::ActorHandler {
                    actor_name: handler.actor_name.clone(),
                    operation: handler.operation.clone(),
                    mode: if handler.shared {
                        engine::OperationMode::Shared
                    } else {
                        engine::OperationMode::Exclusive
                    } as i32,
                    ..Default::default()
                })
                .collect(),
            metadata: metadata.clone(),
            ..Default::default()
        };
        let response = self
            .client
            .clone()
            .register_handlers(self.request(request))
            .await
            .map_err(Error::from)?
            .into_inner();
        if !response.success {
            return Err(Error::Configuration(format!(
                "the actor service refused the worker's handlers: {}",
                response.error_message
            )));
        }
        Ok(response.registration_id)
    }

    pub(crate) async fn get(
        &self,
        scope: &StateScope,
        state_key: &str,
    ) -> Result<Option<StateValue>> {
        let response = self
            .client
            .clone()
            .get_state(self.request(GetStateRequest {
                actor_name: scope.actor_name.clone(),
                key: scope.key.clone(),
                state_key: state_key.to_string(),
                execution_id: scope.execution_id.clone(),
                ..Default::default()
            }))
            .await
            .map_err(Error::from)?
            .into_inner();
        Ok(response.exists.then_some(StateValue {
            value: response.value,
            version: response.version,
        }))
    }

    pub(crate) async fn set(
        &self,
        scope: &StateScope,
        state_key: &str,
        value: Vec<u8>,
        expected_version: Option<&str>,
    ) -> Result<String> {
        let response = self
            .client
            .clone()
            .set_state(self.request(SetStateRequest {
                actor_name: scope.actor_name.clone(),
                key: scope.key.clone(),
                state_key: state_key.to_string(),
                value,
                execution_id: scope.execution_id.clone(),
                expected_version: expected_version.unwrap_or_default().to_string(),
                ..Default::default()
            }))
            .await
            .map_err(Error::from)?
            .into_inner();
        if !response.success {
            return Err(Error::WorkerError(format!(
                "the actor service did not write {state_key}: {}",
                response.error_message
            )));
        }
        Ok(response.version)
    }

    pub(crate) async fn delete(&self, scope: &StateScope, state_key: &str) -> Result<bool> {
        let response = self
            .client
            .clone()
            .delete_state(self.request(DeleteStateRequest {
                actor_name: scope.actor_name.clone(),
                key: scope.key.clone(),
                state_key: state_key.to_string(),
                execution_id: scope.execution_id.clone(),
                ..Default::default()
            }))
            .await
            .map_err(Error::from)?
            .into_inner();
        Ok(response.existed)
    }

    pub(crate) async fn keys(&self, scope: &StateScope, prefix: &str) -> Result<Vec<String>> {
        let response = self
            .client
            .clone()
            .list_state_keys(self.request(ListStateKeysRequest {
                actor_name: scope.actor_name.clone(),
                key: scope.key.clone(),
                execution_id: scope.execution_id.clone(),
                prefix: prefix.to_string(),
                ..Default::default()
            }))
            .await
            .map_err(Error::from)?
            .into_inner();
        Ok(response.keys)
    }
}

/// What the actor service is told about the worker when it registers.
pub(crate) fn registration_metadata(config: &HostConfig) -> HashMap<String, String> {
    let mut metadata = HashMap::from([
        ("namespace".to_string(), config.namespace.clone()),
        ("task_queue".to_string(), config.task_queue.clone()),
        (
            "host_version".to_string(),
            env!("CARGO_PKG_VERSION").to_string(),
        ),
    ]);
    if !config.sdk_name.is_empty() {
        metadata.insert("sdk".to_string(), config.sdk_name.clone());
    }
    if !config.sdk_version.is_empty() {
        metadata.insert("sdk_version".to_string(), config.sdk_version.clone());
    }
    metadata
}

/// Registers the handlers again each time the driver says the actor service
/// asks for them, and points the driver's heartbeats at the new
/// registration. Ends when the driver does.
pub(crate) fn follow_re_registration(
    service: ActorService,
    service_id: String,
    handlers: Vec<ActorHandler>,
    metadata: HashMap<String, String>,
    registration: RegistrationHandle,
    mut events: mpsc::Receiver<orcher_sdk_core::worker::ActorDriverEvent>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while events.recv().await.is_some() {
            match service.register(&service_id, &handlers, &metadata).await {
                Ok(registration_id) => {
                    tracing::info!(%registration_id, "Actor handlers registered again");
                    registration.set(registration_id);
                }
                Err(error) => tracing::warn!(
                    %error,
                    "Registering the actor handlers again failed; the next heartbeat asks again"
                ),
            }
        }
    })
}

/// The host's side of the actor driver.
pub(crate) struct Actors {
    work: tokio::sync::Mutex<mpsc::Receiver<ActorWork>>,
    results: mpsc::Sender<ActorWorkResult>,
    /// The operations the language holds, by operation id.
    held: Mutex<HashSet<String>>,
    pub(crate) service: ActorService,
}

impl Actors {
    pub(crate) fn new(
        work: mpsc::Receiver<ActorWork>,
        results: mpsc::Sender<ActorWorkResult>,
        service: ActorService,
    ) -> Self {
        Self {
            work: tokio::sync::Mutex::new(work),
            results,
            held: Mutex::new(HashSet::new()),
            service,
        }
    }

    /// The next operation, once there is one; `None` once the driver has
    /// stopped.
    pub(crate) async fn next(&self) -> Option<ActorOperation> {
        let operation = self.work.lock().await.recv().await?.operation;
        self.held_operations()
            .insert(operation.operation_id.clone());
        Some(ActorOperation {
            operation_id: operation.operation_id,
            execution_id: operation.execution_id,
            actor_name: operation.actor_name,
            key: operation.key,
            operation: operation.operation,
            payload: (!operation.payload.is_empty()).then(|| Payload {
                data: operation.payload,
                metadata: [("encoding".to_string(), b"json".to_vec())].into(),
                ..Default::default()
            }),
            mode: match engine::OperationMode::try_from(operation.mode) {
                Ok(engine::OperationMode::Shared) => ActorMode::Shared,
                Ok(engine::OperationMode::Exclusive) => ActorMode::Exclusive,
                _ => ActorMode::Unspecified,
            } as i32,
            metadata: operation.metadata,
            ..Default::default()
        })
    }

    /// Reports an operation's result. A failure goes to the engine with the
    /// type the language gave it.
    pub(crate) async fn complete(&self, result: ActorResult) -> Result<()> {
        let outcome = match result.result {
            Some(actor_result::Result::Value(value)) => Ok(value.data),
            Some(actor_result::Result::Failure(failure)) => {
                let mut reported =
                    TaskFailure::new(failure.message).with_non_retryable(failure.non_retryable);
                if !failure.error_type.is_empty() {
                    reported = reported.with_type(failure.error_type);
                }
                Err(reported.into())
            }
            None => {
                return Err(Error::Configuration(
                    "an actor result must carry a value or a failure".to_string(),
                ))
            }
        };
        if !self.held_operations().remove(&result.operation_id) {
            return Err(Error::Protocol(
                "no actor operation with this id is held: it was already answered, or the \
                 worker stopped"
                    .to_string(),
            ));
        }
        self.results
            .send(ActorWorkResult {
                operation_id: result.operation_id,
                execution_id: result.execution_id,
                result: outcome,
            })
            .await
            .map_err(|_| Error::WorkerError("the actor driver has stopped".to_string()))
    }

    /// How many operations the language holds.
    pub(crate) fn held(&self) -> usize {
        self.held_operations().len()
    }

    /// Lets go of the operations the language still holds, when the worker
    /// stops: the engine cannot take them back, so they wait out their
    /// timeouts.
    pub(crate) fn abandon_held(&self) {
        let held = std::mem::take(&mut *self.held_operations());
        if !held.is_empty() {
            tracing::warn!(
                count = held.len(),
                "Actor operations unanswered at shutdown; left to time out"
            );
        }
    }

    fn held_operations(&self) -> std::sync::MutexGuard<'_, HashSet<String>> {
        self.held
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}
