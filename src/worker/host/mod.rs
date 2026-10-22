// The host returns orcher-sdk-core's `Error`, which allows this lint for the
// same reason: a rich error is worth its size on paths that are not hot.
#![allow(clippy::result_large_err)]

//! # ORCHER worker host
//!
//! One worker's plumbing, written once: building core's drivers from one
//! configuration, running the worker's lifecycle, and registering and
//! deregistering it. The Rust SDK uses this crate directly; the Python and
//! TypeScript native modules are bindings over it. Before it, each of the
//! three did this itself, with different bugs.
//!
//! The design is `docs/worker-host.md` in the sdk-core repository. The host
//! hands work to the language by pull: the language asks for the next piece
//! of work when it has room to run one, and returns its outcome. Tasks are
//! handed over this way ([`Host::next_task`]), and workflow activations as the
//! kernel protocol's messages ([`Host::next_activation`]). Sessions are run
//! by the host itself: the built-in session tasks never reach the language.
//! Actor operations are handed over by pull too
//! ([`Host::next_actor_operation`]), and the actors' state is read and written
//! through the host, on its connection, with the worker's credentials.

mod actors;
pub mod config;
mod encoded;
mod sessions;
mod tasks;
mod workflows;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use crate::observability::{WireMetrics, WireMetricsSnapshot};
use crate::proto::host::v1::{
    ActorOperation, ActorResult, HeartbeatAnswer, TaskAssignment, TaskOutcome,
};
use crate::proto::kernel::v1::{Payload, RunActivation, RunActivationResult};
use crate::worker::drivers::registration::DEREGISTER_TIMEOUT;
use crate::worker::{
    ActorDriver, NeededDrivers, ShutdownHandle, TaskDriver, WorkerMetrics,
    WorkerRegistrationDriver, WorkflowDriver,
};
use crate::{Error, Result};
use tokio::task::JoinHandle;

pub use actors::{ActorHandler, StateScope, StateValue};
pub use config::HostConfig;
pub use encoded::EncodedHost;

/// How long a driver has to stop past the shutdown grace and the release
/// margin, before it is aborted.
pub const DRIVER_STOP_TIMEOUT: Duration = Duration::from_secs(2);

/// What the worker runs: the workflow, task and actor types it registered.
#[derive(Debug, Clone, Default)]
#[non_exhaustive]
pub struct Registrations {
    /// Registered workflow types.
    pub workflow_types: Vec<String>,
    /// Registered task types, not counting the built-in session tasks.
    pub task_types: Vec<String>,
    /// Registered actor types.
    pub actor_types: Vec<String>,
    /// The operations the worker runs for its actor types; every actor type
    /// needs at least one.
    pub actor_handlers: Vec<ActorHandler>,
}

impl Registrations {
    /// The worker registered these workflow, task and actor types.
    pub fn new(
        workflow_types: impl IntoIterator<Item = impl Into<String>>,
        task_types: impl IntoIterator<Item = impl Into<String>>,
        actor_types: impl IntoIterator<Item = impl Into<String>>,
    ) -> Self {
        Self {
            workflow_types: workflow_types.into_iter().map(Into::into).collect(),
            task_types: task_types.into_iter().map(Into::into).collect(),
            actor_types: actor_types.into_iter().map(Into::into).collect(),
            actor_handlers: Vec::new(),
        }
    }

    /// The worker runs these actor operations; their actor types are added
    /// to the registered ones.
    pub fn with_actor_handlers(mut self, handlers: impl IntoIterator<Item = ActorHandler>) -> Self {
        for handler in handlers {
            if !self.actor_types.contains(&handler.actor_name) {
                self.actor_types.push(handler.actor_name.clone());
            }
            self.actor_handlers.push(handler);
        }
        self
    }

    /// The drivers these registrations need.
    pub fn needed_drivers(&self) -> NeededDrivers {
        NeededDrivers::for_registrations(&self.workflow_types, &self.task_types, &self.actor_types)
    }
}

/// A running worker host. Cheap to clone: every clone is the same worker, so
/// a language can pull work from as many places as it runs work in.
#[derive(Clone)]
pub struct Host {
    inner: Arc<Inner>,
}

struct Inner {
    config: HostConfig,
    wire: Arc<WireMetrics>,
    needed: NeededDrivers,
    tasks: Option<tasks::Tasks>,
    workflows: Option<workflows::Workflows>,
    actors: Option<actors::Actors>,
    stops: Mutex<Stops>,
}

/// What stops the worker's drivers, taken once by [`Host::shutdown`].
#[derive(Default)]
struct Stops {
    drivers: Vec<(&'static str, ShutdownHandle, JoinHandle<()>)>,
    registration: Option<(ShutdownHandle, JoinHandle<()>)>,
    /// Registers the actor handlers again when the actor service asks.
    re_registration: Option<JoinHandle<()>>,
}

impl std::fmt::Debug for Host {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Host")
            .field("identity", &self.inner.config.identity)
            .field("needed", &self.inner.needed)
            .finish()
    }
}

impl Host {
    /// Starts a worker: starts the drivers its registrations need, registers
    /// it with the engine, and keeps it registered until
    /// [`shutdown`](Self::shutdown).
    ///
    /// # Errors
    ///
    /// Returns [`Error::Configuration`] for a worker that registered nothing
    /// it could run (it would poll for nothing), or an actor type with no
    /// operation; and the actor service's refusal of the worker's actor
    /// handlers.
    pub async fn start(config: HostConfig, registrations: Registrations) -> Result<Self> {
        let needed = registrations.needed_drivers();
        if needed.is_empty() {
            return Err(Error::Configuration(
                "the worker registered no workflow, task or actor to run".to_string(),
            ));
        }
        // One worker's wire measurements, shared by all its connections.
        let wire = WireMetrics::new();
        // What the worker is running and has run, counted by the drivers and
        // reported to the engine with every worker heartbeat.
        let load = WorkerMetrics::new();
        let mut stops = Stops::default();

        let mut eager_tasks = None;
        let tasks = if needed.tasks {
            let (driver, work, results, queue_changes) = TaskDriver::new({
                let mut driver_config = config.task_driver();
                driver_config.wire = Arc::clone(&wire);
                driver_config
            })
            .await?;
            let mut driver = driver
                .with_completion_retry(config.completion_retry())
                .with_metrics(Arc::clone(&load));
            // Tasks the engine hands back with a workflow's completion reach
            // this worker's task driver without a poll.
            eager_tasks = Some(driver.eager_task_injector());
            let stop = driver.shutdown_handle();
            let running = tokio::spawn(async move {
                if let Err(error) = driver.run().await {
                    tracing::warn!(%error, "The task driver ended with an error");
                }
            });
            stops.drivers.push(("task", stop, running));
            let sessions =
                sessions::Sessions::new(config.max_sessions, queue_changes, results.clone());
            Some(tasks::Tasks::new(
                config.namespace.clone(),
                work,
                results,
                sessions,
            ))
        } else {
            None
        };

        let workflows = if needed.workflows {
            let (driver, work, results) = WorkflowDriver::new({
                let mut driver_config = config.workflow_driver();
                driver_config.wire = Arc::clone(&wire);
                driver_config
            })
            .await?;
            let driver = driver
                .with_completion_retry(config.completion_retry())
                .with_metrics(Arc::clone(&load));
            let mut driver = match eager_tasks {
                Some(injector) => driver.with_eager_task_injector(injector),
                None => driver,
            };
            let stop = driver.shutdown_handle();
            let running = tokio::spawn(async move {
                if let Err(error) = driver.run().await {
                    tracing::warn!(%error, "The workflow driver ended with an error");
                }
            });
            stops.drivers.push(("workflow", stop, running));
            Some(workflows::Workflows::new(
                config.namespace.clone(),
                work,
                results,
            ))
        } else {
            None
        };

        let actors = if needed.actors {
            if let Some(bare) = registrations.actor_types.iter().find(|actor| {
                !registrations
                    .actor_handlers
                    .iter()
                    .any(|handler| &handler.actor_name == *actor)
            }) {
                return Err(Error::Configuration(format!(
                    "actor type {bare} was registered with no operation"
                )));
            }
            let service = actors::ActorService::connect(&config, &wire).await?;
            let metadata = actors::registration_metadata(&config);
            let registration_id = service
                .register(&config.identity, &registrations.actor_handlers, &metadata)
                .await?;
            let (driver, work, results, events) = ActorDriver::new({
                let mut driver_config = config.actor_driver(Some(registration_id));
                driver_config.wire = Arc::clone(&wire);
                driver_config
            })
            .await?;
            stops.re_registration = Some(actors::follow_re_registration(
                service.clone(),
                config.identity.clone(),
                registrations.actor_handlers.clone(),
                metadata,
                driver.registration_handle(),
                events,
            ));
            let stop = driver.shutdown_handle();
            let mut driver = driver.with_completion_retry(config.completion_retry());
            let running = tokio::spawn(async move {
                if let Err(error) = driver.run().await {
                    tracing::warn!(%error, "The actor driver ended with an error");
                }
            });
            stops.drivers.push(("actor", stop, running));
            Some(actors::Actors::new(work, results, service))
        } else {
            None
        };

        let mut registration = WorkerRegistrationDriver::new({
            let mut driver_config = config.registration(&registrations);
            driver_config.wire = Arc::clone(&wire);
            driver_config
        })
        .await?
        .with_metrics(load);
        let stop = registration.shutdown_handle();
        let running = tokio::spawn(async move {
            if let Err(error) = registration.run().await {
                tracing::warn!(%error, "Worker registration ended with an error");
            }
        });
        stops.registration = Some((stop, running));

        tracing::info!(
            identity = %config.identity,
            workflows = needed.workflows,
            tasks = needed.tasks,
            actors = needed.actors,
            "Worker host started"
        );
        Ok(Self {
            inner: Arc::new(Inner {
                config,
                wire,
                needed,
                tasks,
                workflows,
                actors,
                stops: Mutex::new(stops),
            }),
        })
    }

    /// What the worker's calls to the engine have done so far, by RPC
    /// method: calls, latency, outcomes and retries. The same for every
    /// language SDK, to report as it likes.
    pub fn wire_metrics(&self) -> WireMetricsSnapshot {
        self.inner.wire.snapshot()
    }

    /// The worker's configuration.
    pub fn config(&self) -> &HostConfig {
        &self.inner.config
    }

    /// The drivers the worker runs.
    pub fn needed_drivers(&self) -> NeededDrivers {
        self.inner.needed
    }

    /// The next workflow activation to run, once there is one: the run's
    /// history resolved into the outcomes its code observes. `None` once the
    /// worker has stopped and every activation it took has been handed over,
    /// and at once for a worker that runs no workflows.
    pub async fn next_activation(&self) -> Option<RunActivation> {
        self.inner.workflows.as_ref()?.next().await
    }

    /// Sends the answer to an activation: every durable call the code made,
    /// or a workflow failure, or a protocol error. An answer the kernel
    /// cannot carry hands the activation back to the engine rather than
    /// failing the run.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Protocol`] for an answer to an activation the host
    /// does not hold (already answered, or the worker stopped), and an error
    /// once the worker has stopped.
    pub async fn complete_activation(&self, answer: RunActivationResult) -> Result<()> {
        match &self.inner.workflows {
            Some(workflows) => workflows.complete(answer).await,
            None => Err(Error::Configuration(
                "the worker runs no workflows".to_string(),
            )),
        }
    }

    /// The next task to run, once there is one. `None` once the worker has
    /// stopped and every task it took has been handed over, and at once for
    /// a worker that runs no tasks.
    pub async fn next_task(&self) -> Option<TaskAssignment> {
        self.inner.tasks.as_ref()?.next().await
    }

    /// Records a heartbeat for a running task, with progress details, and
    /// says whether the task has been asked to cancel.
    pub fn heartbeat(&self, token: &[u8], details: Option<Payload>) -> HeartbeatAnswer {
        match &self.inner.tasks {
            Some(tasks) => tasks.heartbeat(token, details),
            None => HeartbeatAnswer::default(),
        }
    }

    /// Waits until a running task is asked to cancel (`true`), or its outcome
    /// is reported (`false`).
    pub async fn task_cancelled(&self, token: &[u8]) -> bool {
        match &self.inner.tasks {
            Some(tasks) => tasks.cancelled(token).await,
            None => false,
        }
    }

    /// Reports a task's outcome.
    ///
    /// # Errors
    ///
    /// Returns an error for an outcome with neither a value nor a failure, or
    /// once the worker has stopped.
    pub async fn complete_task(&self, outcome: TaskOutcome) -> Result<()> {
        match &self.inner.tasks {
            Some(tasks) => tasks.complete(outcome).await,
            None => Err(Error::Configuration("the worker runs no tasks".to_string())),
        }
    }

    /// The next actor operation to run, once there is one. `None` once the
    /// worker has stopped, and at once for a worker that runs no actors.
    pub async fn next_actor_operation(&self) -> Option<ActorOperation> {
        self.inner.actors.as_ref()?.next().await
    }

    /// Reports an actor operation's result: its value, or its failure with
    /// the type the language gave it.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Protocol`] for a result of an operation the host does
    /// not hold, an error for a result with neither a value nor a failure,
    /// and an error once the worker has stopped.
    pub async fn complete_actor_operation(&self, result: ActorResult) -> Result<()> {
        self.actors()?.complete(result).await
    }

    /// Reads one key of an actor's state; `None` when it is not set.
    ///
    /// # Errors
    ///
    /// Returns the actor service's error.
    pub async fn get_actor_state(
        &self,
        scope: &StateScope,
        state_key: &str,
    ) -> Result<Option<StateValue>> {
        self.actors()?.service.get(scope, state_key).await
    }

    /// Writes one key of an actor's state; returns its new version. With
    /// `expected_version`, the write is refused unless the stored version
    /// is that one.
    ///
    /// # Errors
    ///
    /// Returns the actor service's error or refusal.
    pub async fn set_actor_state(
        &self,
        scope: &StateScope,
        state_key: &str,
        value: Vec<u8>,
        expected_version: Option<&str>,
    ) -> Result<String> {
        self.actors()?
            .service
            .set(scope, state_key, value, expected_version)
            .await
    }

    /// Deletes one key of an actor's state; returns whether it was set.
    ///
    /// # Errors
    ///
    /// Returns the actor service's error.
    pub async fn delete_actor_state(&self, scope: &StateScope, state_key: &str) -> Result<bool> {
        self.actors()?.service.delete(scope, state_key).await
    }

    /// The keys of an actor's state that start with `prefix`.
    ///
    /// # Errors
    ///
    /// Returns the actor service's error.
    pub async fn actor_state_keys(&self, scope: &StateScope, prefix: &str) -> Result<Vec<String>> {
        self.actors()?.service.keys(scope, prefix).await
    }

    /// Work the language holds and has not reported.
    fn outstanding(&self) -> usize {
        self.inner.tasks.as_ref().map_or(0, tasks::Tasks::running)
            + self
                .inner
                .workflows
                .as_ref()
                .map_or(0, workflows::Workflows::held)
            + self.inner.actors.as_ref().map_or(0, actors::Actors::held)
    }

    fn actors(&self) -> Result<&actors::Actors> {
        self.inner
            .actors
            .as_ref()
            .ok_or_else(|| Error::Configuration("the worker runs no actors".to_string()))
    }

    /// Stops the worker, within a bounded time:
    ///
    /// 1. Every driver stops claiming work at once. Work a poll already
    ///    brought back is still handed over, and pulls stay open for it.
    /// 2. The language has the shutdown grace
    ///    ([`HostConfig::shutdown_grace`]) to finish and report what it
    ///    holds.
    /// 3. Workflow activations still held are then handed back to the engine
    ///    for another worker. Tasks and actor operations still running are
    ///    asked to cancel and stop being heartbeated, so the engine retries
    ///    them once they time out; it cannot take them back sooner.
    /// 4. The drivers stop (aborted past [`RELEASE_MARGIN`] and
    ///    [`DRIVER_STOP_TIMEOUT`]); pulls then return `None`.
    /// 5. Sessions close and the worker deregisters.
    ///
    /// A second call returns at once.
    ///
    /// [`RELEASE_MARGIN`]: config::RELEASE_MARGIN
    pub async fn shutdown(&self) {
        let stops = std::mem::take(
            &mut *self
                .inner
                .stops
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        );
        if stops.drivers.is_empty() && stops.registration.is_none() {
            return;
        }
        let asked = tokio::time::Instant::now();
        for (_, stop, _) in &stops.drivers {
            stop.shutdown();
        }

        let grace_over = asked + self.inner.config.shutdown_grace;
        while self.outstanding() > 0 && tokio::time::Instant::now() < grace_over {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let outstanding = self.outstanding();
        if outstanding > 0 {
            tracing::warn!(
                outstanding,
                "Shutdown grace over with work unfinished; handing back what can be"
            );
        }
        if let Some(workflows) = &self.inner.workflows {
            workflows.release_held().await;
        }
        if let Some(tasks) = &self.inner.tasks {
            tasks.abandon_running();
        }
        if let Some(actors) = &self.inner.actors {
            actors.abandon_held();
        }

        let stop_by = grace_over + config::RELEASE_MARGIN + DRIVER_STOP_TIMEOUT;
        for (name, _, running) in stops.drivers {
            let abort = running.abort_handle();
            if tokio::time::timeout_at(stop_by, running).await.is_err() {
                tracing::warn!(driver = name, "Driver did not stop in time; aborting it");
                abort.abort();
            }
        }
        if let Some(tasks) = &self.inner.tasks {
            tasks.sessions().close_all();
        }
        if let Some(re_registration) = stops.re_registration {
            re_registration.abort();
        }
        if let Some((stop, running)) = stops.registration {
            stop.shutdown();
            // Deregistration is bounded by the driver; this bounds the wait
            // for the task as well, so shutdown always ends.
            if tokio::time::timeout(DEREGISTER_TIMEOUT + Duration::from_secs(1), running)
                .await
                .is_err()
            {
                tracing::warn!(
                    identity = %self.inner.config.identity,
                    "Worker registration did not stop in time"
                );
            }
        }
    }
}
