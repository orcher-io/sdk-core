//! One worker's configuration, and every default its drivers are built with.
//!
//! The defaults were retyped in each SDK (the Python and TypeScript native
//! modules and the Rust SDK each hard-coded the same poll timeouts, cache
//! size, heartbeat interval and protocol version). They live here, once.

use std::collections::HashMap;
use std::time::Duration;

use orcher_sdk_core::transport::TlsConfig;
use orcher_sdk_core::worker::drivers::actor::ActorDriverConfig;
use orcher_sdk_core::worker::drivers::registration::WorkerRegistrationConfig;
use orcher_sdk_core::worker::drivers::task::TaskDriverConfig;
use orcher_sdk_core::worker::drivers::workflow::WorkflowDriverConfig;
use orcher_sdk_core::worker::BUILT_IN_TASK_TYPES;

/// How long a workflow poll waits for work.
pub const WORKFLOW_POLL_TIMEOUT: Duration = Duration::from_secs(60);
/// How long a task poll waits for work.
pub const TASK_POLL_TIMEOUT: Duration = Duration::from_secs(60);
/// How long an actor poll waits for work.
pub const ACTOR_POLL_TIMEOUT: Duration = Duration::from_secs(30);
/// How many workflow runs the workflow driver's cache holds.
pub const WORKFLOW_CACHE_CAPACITY: usize = 1000;
/// How often a running task is heartbeated for, when it does not heartbeat
/// itself.
pub const TASK_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
/// How often the worker tells the engine it is alive.
pub const WORKER_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
/// How many of each kind of work run at once, unless configured.
pub const DEFAULT_CONCURRENCY: u32 = 100;
/// How many polls each driver keeps open.
pub const POLLERS_PER_DRIVER: usize = 4;

/// One worker's configuration.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct HostConfig {
    /// The engine's gRPC address, for example `http://localhost:50051`.
    pub server_url: String,
    /// The namespace the worker serves.
    pub namespace: String,
    /// The task queue the worker polls.
    pub task_queue: String,
    /// The worker's identity, the same for every driver and every
    /// registration for the worker's whole life.
    pub identity: String,
    /// The API key every connection the worker opens sends.
    pub api_key: Option<String>,
    /// The organization every connection the worker opens names.
    pub organization_id: Option<String>,
    /// TLS for every connection the worker opens.
    pub tls: Option<TlsConfig>,
    /// The release the worker runs; runs are bound to it on first claim.
    pub version_id: Option<String>,
    /// Workflow activations run at once.
    pub max_concurrent_workflows: u32,
    /// Tasks run at once.
    pub max_concurrent_tasks: u32,
    /// Actor operations run at once.
    pub max_concurrent_actor_operations: u32,
    /// The SDK running the worker, reported to the engine, for example
    /// `orcher-sdk-ts`.
    pub sdk_name: String,
    /// The SDK's version.
    pub sdk_version: String,
}

impl HostConfig {
    /// A worker on `task_queue` of the engine at `server_url`, in the
    /// `default` namespace, with every other setting at its default. Its
    /// identity is generated here, once.
    pub fn new(server_url: impl Into<String>, task_queue: impl Into<String>) -> Self {
        Self {
            server_url: server_url.into(),
            namespace: "default".to_string(),
            task_queue: task_queue.into(),
            identity: format!("orcher-worker-{}", uuid::Uuid::new_v4()),
            api_key: None,
            organization_id: None,
            tls: None,
            version_id: None,
            max_concurrent_workflows: DEFAULT_CONCURRENCY,
            max_concurrent_tasks: DEFAULT_CONCURRENCY,
            max_concurrent_actor_operations: DEFAULT_CONCURRENCY,
            sdk_name: String::new(),
            sdk_version: String::new(),
        }
    }

    /// Serves `namespace`.
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = namespace.into();
        self
    }

    /// Uses `identity` instead of the generated one.
    pub fn with_identity(mut self, identity: impl Into<String>) -> Self {
        let identity = identity.into();
        if !identity.trim().is_empty() {
            self.identity = identity;
        }
        self
    }

    /// Sends `api_key` on every connection.
    pub fn with_api_key(mut self, api_key: impl Into<String>) -> Self {
        self.api_key = Some(api_key.into());
        self
    }

    /// Names `organization_id` on every connection.
    pub fn with_organization_id(mut self, organization_id: impl Into<String>) -> Self {
        self.organization_id = Some(organization_id.into());
        self
    }

    /// Uses `tls` for every connection.
    pub fn with_tls(mut self, tls: TlsConfig) -> Self {
        self.tls = Some(tls);
        self
    }

    /// Runs release `version_id`. A blank one means no release.
    pub fn with_version_id(mut self, version_id: impl Into<String>) -> Self {
        let version_id = version_id.into();
        self.version_id = (!version_id.trim().is_empty()).then_some(version_id);
        self
    }

    /// Runs at most these many workflow activations, tasks and actor
    /// operations at once.
    pub fn with_concurrency(mut self, workflows: u32, tasks: u32, actor_operations: u32) -> Self {
        self.max_concurrent_workflows = workflows;
        self.max_concurrent_tasks = tasks;
        self.max_concurrent_actor_operations = actor_operations;
        self
    }

    /// Reports the SDK running the worker.
    pub fn with_sdk(mut self, name: impl Into<String>, version: impl Into<String>) -> Self {
        self.sdk_name = name.into();
        self.sdk_version = version.into();
        self
    }

    /// The workflow driver's configuration.
    pub fn workflow_driver(&self) -> WorkflowDriverConfig {
        let mut config = WorkflowDriverConfig::default();
        config.server_url = self.server_url.clone();
        config.namespace = self.namespace.clone();
        config.task_queue = self.task_queue.clone();
        config.max_concurrent_executions = self.max_concurrent_workflows as usize;
        config.identity = self.identity.clone();
        config.poll_timeout = WORKFLOW_POLL_TIMEOUT;
        config.cache_capacity = WORKFLOW_CACHE_CAPACITY;
        config.strict_determinism = false;
        config.poller_count = POLLERS_PER_DRIVER;
        config.organization_id = self.organization_id.clone();
        config.api_key = self.api_key.clone();
        config.tls_config = self.tls.clone();
        config.version_id = self.version_id.clone();
        config.protocol_version = orcher_sdk_core::worker_protocol::CANCEL_REQUEST;
        config
    }

    /// The task driver's configuration.
    pub fn task_driver(&self) -> TaskDriverConfig {
        let mut config = TaskDriverConfig::default();
        config.server_url = self.server_url.clone();
        config.namespace = self.namespace.clone();
        config.task_queue = self.task_queue.clone();
        config.max_concurrent_executions = self.max_concurrent_tasks as usize;
        config.identity = self.identity.clone();
        config.poll_timeout = TASK_POLL_TIMEOUT;
        config.enable_heartbeat = true;
        config.heartbeat_interval = TASK_HEARTBEAT_INTERVAL;
        config.poller_count = POLLERS_PER_DRIVER;
        config.organization_id = self.organization_id.clone();
        config.api_key = self.api_key.clone();
        config.tls_config = self.tls.clone();
        config.version_id = self.version_id.clone();
        config.auto_heartbeat = true;
        config
    }

    /// The actor driver's configuration, for the registration the actor
    /// service gave the worker's handlers.
    pub fn actor_driver(&self, registration_id: Option<String>) -> ActorDriverConfig {
        let mut config = ActorDriverConfig::default();
        config.server_url = self.server_url.clone();
        config.namespace = self.namespace.clone();
        config.service_id = self.identity.clone();
        config.max_concurrent_executions = self.max_concurrent_actor_operations as usize;
        config.identity = self.identity.clone();
        config.poll_timeout = ACTOR_POLL_TIMEOUT;
        config.poller_count = POLLERS_PER_DRIVER;
        config.organization_id = self.organization_id.clone();
        config.api_key = self.api_key.clone();
        config.enable_heartbeat = true;
        config.heartbeat_interval = WORKER_HEARTBEAT_INTERVAL;
        config.registration_id = registration_id;
        config.tls_config = self.tls.clone();
        config
    }

    /// The worker registration's configuration, advertising what the worker
    /// registered. The built-in session tasks are advertised whenever the
    /// worker runs tasks, since it handles them.
    pub fn registration(&self, registrations: &crate::Registrations) -> WorkerRegistrationConfig {
        let runs_tasks = !registrations.task_types.is_empty();
        let task_types = registrations
            .task_types
            .iter()
            .cloned()
            .chain(
                BUILT_IN_TASK_TYPES
                    .iter()
                    .filter(|_| runs_tasks)
                    .map(|built_in| built_in.to_string()),
            )
            .collect();
        let mut metadata = HashMap::new();
        if !self.sdk_name.is_empty() {
            metadata.insert("sdk".to_string(), self.sdk_name.clone());
        }
        if !self.sdk_version.is_empty() {
            metadata.insert("sdk_version".to_string(), self.sdk_version.clone());
        }
        metadata.insert(
            "host_version".to_string(),
            env!("CARGO_PKG_VERSION").to_string(),
        );
        let mut config = WorkerRegistrationConfig::default();
        config.server_url = self.server_url.clone();
        config.service_id = self.identity.clone();
        config.identity = self.identity.clone();
        config.task_queue = self.task_queue.clone();
        config.namespace = self.namespace.clone();
        config.workflow_types = registrations.workflow_types.clone();
        config.task_types = task_types;
        config.max_concurrent_workflows = self.max_concurrent_workflows;
        config.max_concurrent_tasks = self.max_concurrent_tasks;
        config.heartbeat_interval = WORKER_HEARTBEAT_INTERVAL;
        config.metadata = metadata;
        config.version_id = self.version_id.clone();
        config.tls_config = self.tls.clone();
        config.api_key = self.api_key.clone();
        config.organization_id = self.organization_id.clone();
        config.protocol_version = orcher_sdk_core::worker_protocol::CANCEL_REQUEST;
        config
    }
}
