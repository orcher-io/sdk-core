//! One worker's configuration, and every default its drivers are built with.
//!
//! The defaults were retyped in each SDK (the Python and TypeScript native
//! modules and the Rust SDK each hard-coded the same poll timeouts,
//! heartbeat interval and protocol version). They live here, once.

use std::collections::HashMap;
use std::time::Duration;

use crate::client::ConnectOptions;
use crate::transport::TlsConfig;
use crate::worker::drivers::actor::ActorDriverConfig;
use crate::worker::drivers::registration::WorkerRegistrationConfig;
use crate::worker::drivers::task::TaskDriverConfig;
use crate::worker::drivers::workflow::WorkflowDriverConfig;
use crate::worker::{CompletionRetryConfig, BUILT_IN_TASK_TYPES};

/// How long a workflow poll waits for work.
pub const WORKFLOW_POLL_TIMEOUT: Duration = Duration::from_secs(60);
/// How long a task poll waits for work.
pub const TASK_POLL_TIMEOUT: Duration = Duration::from_secs(60);
/// How long an actor poll waits for work.
pub const ACTOR_POLL_TIMEOUT: Duration = Duration::from_secs(30);
/// How often a running task is heartbeated for, when it does not heartbeat
/// itself.
pub const TASK_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);
/// How often the worker tells the engine it is alive, unless configured.
pub const WORKER_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(10);
/// How many of each kind of work run at once, unless configured.
pub const DEFAULT_CONCURRENCY: u32 = 100;
/// How many polls each driver keeps open, unless configured.
pub const POLLERS_PER_DRIVER: usize = 4;
/// How long a stopping worker gives the language to finish the work it
/// holds, unless configured. Well inside the 30 seconds a container
/// orchestrator usually allows a process to stop.
pub const DEFAULT_SHUTDOWN_GRACE: Duration = Duration::from_secs(10);
/// How long the drivers keep running past the shutdown grace, to send the
/// last results and hand back the activations the language did not finish.
pub const RELEASE_MARGIN: Duration = Duration::from_secs(5);
/// How many sessions a worker hosts at once, unless configured.
pub const DEFAULT_MAX_SESSIONS: usize = 10;

/// One worker's configuration.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct HostConfig {
    /// How every connection the worker opens reaches the engine: its
    /// address, the namespace the worker serves, the credentials and TLS
    /// settings, the connect timeout and the message limit. The same options
    /// a client is connected with ([`crate::Connection`]).
    pub connection: ConnectOptions,
    /// The task queue the worker polls.
    pub task_queue: String,
    /// The worker's identity, the same for every driver and every
    /// registration for the worker's whole life.
    pub identity: String,
    /// The release the worker runs; runs are bound to it on first claim.
    pub version_id: Option<String>,
    /// Workflow activations run at once.
    pub max_concurrent_workflows: u32,
    /// Tasks run at once.
    pub max_concurrent_tasks: u32,
    /// Actor operations run at once.
    pub max_concurrent_actor_operations: u32,
    /// Polls the workflow driver keeps open (at least one).
    pub workflow_pollers: usize,
    /// Polls the task driver keeps open (at least one).
    pub task_pollers: usize,
    /// Polls the actor driver keeps open (at least one).
    pub actor_pollers: usize,
    /// How often the worker, and its actor service registration, tell the
    /// engine they are alive.
    pub heartbeat_interval: Duration,
    /// Sessions hosted at once. A session creation that finds every slot
    /// taken waits for one to come free, up to its creation timeout; it is
    /// not passed on to another worker.
    pub max_sessions: usize,
    /// How long a stopping worker gives the language to finish the work it
    /// holds. Workflow activations still held after it are handed back to
    /// the engine; tasks and actor operations still running are left to
    /// their timeouts, since the engine cannot take them back.
    pub shutdown_grace: Duration,
    /// The SDK running the worker, reported to the engine, for example
    /// `orcher-sdk-ts`.
    pub sdk_name: String,
    /// The SDK's version.
    pub sdk_version: String,
    /// The worker protocol version the worker reports, numbered as in
    /// [`crate::worker_protocol`]. What a version promises covers
    /// the language SDK too, so the SDK raises it once it does what the
    /// version says. Default:
    /// [`CANCEL_REQUEST`](crate::worker_protocol::CANCEL_REQUEST).
    pub protocol_version: u32,
}

impl HostConfig {
    /// A worker on `task_queue`, connected as `connection` says (an
    /// address alone is the engine there, in the `default` namespace), with
    /// every other setting at its default. Its identity is generated here,
    /// once.
    pub fn new(connection: impl Into<ConnectOptions>, task_queue: impl Into<String>) -> Self {
        Self {
            connection: connection.into(),
            task_queue: task_queue.into(),
            identity: format!("orcher-worker-{}", uuid::Uuid::new_v4()),
            version_id: None,
            max_concurrent_workflows: DEFAULT_CONCURRENCY,
            max_concurrent_tasks: DEFAULT_CONCURRENCY,
            max_concurrent_actor_operations: DEFAULT_CONCURRENCY,
            workflow_pollers: POLLERS_PER_DRIVER,
            task_pollers: POLLERS_PER_DRIVER,
            actor_pollers: POLLERS_PER_DRIVER,
            heartbeat_interval: WORKER_HEARTBEAT_INTERVAL,
            max_sessions: DEFAULT_MAX_SESSIONS,
            shutdown_grace: DEFAULT_SHUTDOWN_GRACE,
            sdk_name: String::new(),
            sdk_version: String::new(),
            protocol_version: crate::worker_protocol::CANCEL_REQUEST,
        }
    }

    /// Serves `namespace`.
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.connection.namespace = namespace.into();
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
        self.connection.api_key = Some(api_key.into());
        self
    }

    /// Names `organization_id` on every connection.
    pub fn with_organization_id(mut self, organization_id: impl Into<String>) -> Self {
        self.connection.organization_id = Some(organization_id.into());
        self
    }

    /// Uses `tls` for every connection.
    pub fn with_tls(mut self, tls: TlsConfig) -> Self {
        self.connection.tls = Some(tls);
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

    /// Keeps this many polls open per driver; zero counts as one.
    pub fn with_pollers(mut self, workflows: usize, tasks: usize, actors: usize) -> Self {
        self.workflow_pollers = workflows.max(1);
        self.task_pollers = tasks.max(1);
        self.actor_pollers = actors.max(1);
        self
    }

    /// Tells the engine the worker is alive this often.
    pub fn with_heartbeat_interval(mut self, interval: Duration) -> Self {
        self.heartbeat_interval = interval;
        self
    }

    /// Hosts at most `max_sessions` sessions at once.
    pub fn with_max_sessions(mut self, max_sessions: usize) -> Self {
        self.max_sessions = max_sessions;
        self
    }

    /// Gives the language `grace` to finish its work when the worker stops.
    pub fn with_shutdown_grace(mut self, grace: Duration) -> Self {
        self.shutdown_grace = grace;
        self
    }

    /// How the drivers report results: past the shutdown grace, they keep
    /// sending for [`RELEASE_MARGIN`].
    pub fn completion_retry(&self) -> CompletionRetryConfig {
        CompletionRetryConfig::default().with_shutdown_grace(self.shutdown_grace + RELEASE_MARGIN)
    }

    /// Reports the SDK running the worker.
    pub fn with_sdk(mut self, name: impl Into<String>, version: impl Into<String>) -> Self {
        self.sdk_name = name.into();
        self.sdk_version = version.into();
        self
    }

    /// Reports worker protocol version `version`
    /// ([`crate::worker_protocol`]): set by a language SDK that does
    /// what the version promises.
    pub fn with_protocol_version(mut self, version: u32) -> Self {
        self.protocol_version = version;
        self
    }

    /// The workflow driver's configuration.
    pub(crate) fn workflow_driver(&self) -> WorkflowDriverConfig {
        WorkflowDriverConfig {
            server_url: self.connection.url.clone(),
            namespace: self.connection.namespace.clone(),
            task_queue: self.task_queue.clone(),
            max_concurrent_executions: self.max_concurrent_workflows as usize,
            identity: self.identity.clone(),
            poll_timeout: WORKFLOW_POLL_TIMEOUT,
            strict_determinism: false,
            poller_count: self.workflow_pollers.max(1),
            organization_id: self.connection.organization_id.clone(),
            api_key: self.connection.api_key.clone(),
            tls_config: self.connection.tls.clone(),
            connect_timeout: self.connection.connect_timeout,
            version_id: self.version_id.clone(),
            protocol_version: self.protocol_version,
            max_message_bytes: self.connection.max_message_bytes,
            ..WorkflowDriverConfig::default()
        }
    }

    /// The task driver's configuration.
    pub(crate) fn task_driver(&self) -> TaskDriverConfig {
        TaskDriverConfig {
            server_url: self.connection.url.clone(),
            namespace: self.connection.namespace.clone(),
            task_queue: self.task_queue.clone(),
            max_concurrent_executions: self.max_concurrent_tasks as usize,
            identity: self.identity.clone(),
            poll_timeout: TASK_POLL_TIMEOUT,
            enable_heartbeat: true,
            heartbeat_interval: TASK_HEARTBEAT_INTERVAL,
            poller_count: self.task_pollers.max(1),
            organization_id: self.connection.organization_id.clone(),
            api_key: self.connection.api_key.clone(),
            tls_config: self.connection.tls.clone(),
            connect_timeout: self.connection.connect_timeout,
            version_id: self.version_id.clone(),
            auto_heartbeat: true,
            max_message_bytes: self.connection.max_message_bytes,
            ..TaskDriverConfig::default()
        }
    }

    /// The actor driver's configuration, for the registration the actor
    /// service gave the worker's handlers.
    pub(crate) fn actor_driver(&self, registration_id: Option<String>) -> ActorDriverConfig {
        ActorDriverConfig {
            server_url: self.connection.url.clone(),
            namespace: self.connection.namespace.clone(),
            service_id: self.identity.clone(),
            max_concurrent_executions: self.max_concurrent_actor_operations as usize,
            identity: self.identity.clone(),
            poll_timeout: ACTOR_POLL_TIMEOUT,
            poller_count: self.actor_pollers.max(1),
            organization_id: self.connection.organization_id.clone(),
            api_key: self.connection.api_key.clone(),
            enable_heartbeat: true,
            heartbeat_interval: self.heartbeat_interval,
            registration_id,
            tls_config: self.connection.tls.clone(),
            connect_timeout: self.connection.connect_timeout,
            max_message_bytes: self.connection.max_message_bytes,
            ..ActorDriverConfig::default()
        }
    }

    /// The worker registration's configuration, advertising what the worker
    /// registered. The built-in session tasks are advertised whenever the
    /// worker runs tasks, since it handles them.
    pub(crate) fn registration(
        &self,
        registrations: &crate::worker::host::Registrations,
    ) -> WorkerRegistrationConfig {
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
        WorkerRegistrationConfig {
            server_url: self.connection.url.clone(),
            service_id: self.identity.clone(),
            identity: self.identity.clone(),
            task_queue: self.task_queue.clone(),
            namespace: self.connection.namespace.clone(),
            workflow_types: registrations.workflow_types.clone(),
            task_types,
            max_concurrent_workflows: self.max_concurrent_workflows,
            max_concurrent_tasks: self.max_concurrent_tasks,
            heartbeat_interval: self.heartbeat_interval,
            metadata,
            version_id: self.version_id.clone(),
            tls_config: self.connection.tls.clone(),
            connect_timeout: self.connection.connect_timeout,
            api_key: self.connection.api_key.clone(),
            organization_id: self.connection.organization_id.clone(),
            protocol_version: self.protocol_version,
            ..WorkerRegistrationConfig::default()
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::worker::host::Registrations;
    use crate::worker_protocol::{CANCEL_REQUEST, WORKFLOW_FAILURE_RETRYABILITY};

    /// The poll counts and the heartbeat interval a worker sets reach the
    /// drivers that use them; zero polls counts as one.
    #[test]
    fn poll_counts_and_heartbeat_interval_reach_the_drivers() {
        let config = HostConfig::new("http://engine:50051", "orders")
            .with_pollers(2, 6, 0)
            .with_heartbeat_interval(std::time::Duration::from_secs(3));
        assert_eq!(config.workflow_driver().poller_count, 2);
        assert_eq!(config.task_driver().poller_count, 6);
        let actor = config.actor_driver(None);
        assert_eq!(actor.poller_count, 1);
        assert_eq!(actor.heartbeat_interval, std::time::Duration::from_secs(3));
        let registration = config.registration(&Registrations::new(
            ["checkout"],
            [] as [&str; 0],
            [] as [&str; 0],
        ));
        assert_eq!(
            registration.heartbeat_interval,
            std::time::Duration::from_secs(3)
        );
    }

    #[test]
    fn every_driver_is_built_from_one_config() {
        let config = HostConfig::new("https://engine:443", "orders")
            .with_namespace("payments")
            .with_api_key("key")
            .with_organization_id("org-1")
            .with_version_id("release-7");
        let config = HostConfig {
            connection: config
                .connection
                .with_connect_timeout(std::time::Duration::from_secs(2))
                .with_max_message_bytes(1 << 20),
            ..config
        };

        let workflow = config.workflow_driver();
        let task = config.task_driver();
        let actor = config.actor_driver(Some("registration-1".into()));
        let registration = config.registration(&Registrations::new(
            ["checkout"],
            ["charge"],
            [] as [&str; 0],
        ));

        for identity in [
            &workflow.identity,
            &task.identity,
            &actor.identity,
            &registration.identity,
        ] {
            assert_eq!(identity, &config.identity);
        }
        assert_eq!(registration.service_id, config.identity);
        assert_eq!(actor.service_id, config.identity);
        for api_key in [
            &workflow.api_key,
            &task.api_key,
            &actor.api_key,
            &registration.api_key,
        ] {
            assert_eq!(api_key.as_deref(), Some("key"));
        }
        for org in [
            &workflow.organization_id,
            &task.organization_id,
            &actor.organization_id,
            &registration.organization_id,
        ] {
            assert_eq!(org.as_deref(), Some("org-1"));
        }
        for connect_timeout in [
            workflow.connect_timeout,
            task.connect_timeout,
            actor.connect_timeout,
            registration.connect_timeout,
        ] {
            assert_eq!(connect_timeout, std::time::Duration::from_secs(2));
        }
        for max_message_bytes in [
            workflow.max_message_bytes,
            task.max_message_bytes,
            actor.max_message_bytes,
        ] {
            assert_eq!(max_message_bytes, 1 << 20);
        }
        assert_eq!(workflow.version_id.as_deref(), Some("release-7"));
        assert_eq!(task.version_id.as_deref(), Some("release-7"));
        assert_eq!(registration.version_id.as_deref(), Some("release-7"));

        assert_eq!(workflow.poll_timeout, WORKFLOW_POLL_TIMEOUT);
        assert!(!workflow.strict_determinism);
        assert_eq!(
            workflow.protocol_version,
            crate::worker_protocol::CANCEL_REQUEST
        );
        assert_eq!(task.poll_timeout, TASK_POLL_TIMEOUT);
        assert!(task.auto_heartbeat);
        assert_eq!(task.heartbeat_interval, TASK_HEARTBEAT_INTERVAL);
        assert_eq!(actor.poll_timeout, ACTOR_POLL_TIMEOUT);
        assert_eq!(actor.registration_id.as_deref(), Some("registration-1"));
    }

    /// The worker protocol version is the language SDK's to raise, and every
    /// report of it says the same: registration and the workflow driver alike.
    #[test]
    fn the_protocol_version_is_the_sdks_to_raise() {
        let registrations = Registrations::default();
        let config = HostConfig::new("http://engine", "q");
        assert_eq!(config.workflow_driver().protocol_version, CANCEL_REQUEST);
        assert_eq!(
            config.registration(&registrations).protocol_version,
            CANCEL_REQUEST
        );
        let config = config.with_protocol_version(WORKFLOW_FAILURE_RETRYABILITY);
        assert_eq!(
            config.workflow_driver().protocol_version,
            WORKFLOW_FAILURE_RETRYABILITY
        );
        assert_eq!(
            config.registration(&registrations).protocol_version,
            WORKFLOW_FAILURE_RETRYABILITY
        );
    }

    /// An identity is generated once; a blank one or a blank release is not a
    /// value.
    #[test]
    fn identity_is_stable_and_blanks_are_unset() {
        let config = HostConfig::new("http://engine", "q");
        assert_eq!(
            config.workflow_driver().identity,
            config.task_driver().identity
        );
        let kept = config.identity.clone();
        let config = config.with_identity("  ").with_version_id(" ");
        assert_eq!(config.identity, kept);
        assert_eq!(config.version_id, None);
    }
}
