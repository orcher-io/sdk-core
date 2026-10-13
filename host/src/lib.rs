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
//! Actor operations follow.

pub mod config;
mod sessions;
mod tasks;
mod workflows;

use std::sync::{Arc, Mutex};
use std::time::Duration;

use orcher_sdk_core::proto::host::v1::{HeartbeatAnswer, TaskAssignment, TaskOutcome};
use orcher_sdk_core::proto::kernel::v1::{Payload, RunActivation, RunActivationResult};
use orcher_sdk_core::worker::drivers::registration::DEREGISTER_TIMEOUT;
use orcher_sdk_core::worker::{NeededDrivers, ShutdownHandle, TaskDriver, WorkflowDriver};
use orcher_sdk_core::{Error, Result, WorkerRegistrationDriver};
use tokio::task::JoinHandle;

pub use config::HostConfig;

/// How long a driver has to stop once asked, before it is aborted.
pub const DRIVER_STOP_TIMEOUT: Duration = Duration::from_secs(10);

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
        }
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
    needed: NeededDrivers,
    tasks: Option<tasks::Tasks>,
    workflows: Option<workflows::Workflows>,
    stops: Mutex<Stops>,
}

/// What stops the worker's drivers, taken once by [`Host::shutdown`].
#[derive(Default)]
struct Stops {
    drivers: Vec<(&'static str, ShutdownHandle, JoinHandle<()>)>,
    registration: Option<(ShutdownHandle, JoinHandle<()>)>,
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
    /// it could run: it would poll for nothing.
    pub async fn start(config: HostConfig, registrations: Registrations) -> Result<Self> {
        let needed = registrations.needed_drivers();
        if needed.is_empty() {
            return Err(Error::Configuration(
                "the worker registered no workflow, task or actor to run".to_string(),
            ));
        }
        let mut stops = Stops::default();

        let mut eager_tasks = None;
        let tasks = if needed.tasks {
            let (mut driver, work, results, queue_changes) =
                TaskDriver::new(config.task_driver()).await?;
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
            let (driver, work, results) = WorkflowDriver::new(config.workflow_driver()).await?;
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

        let mut registration =
            WorkerRegistrationDriver::new(config.registration(&registrations)).await?;
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
                needed,
                tasks,
                workflows,
                stops: Mutex::new(stops),
            }),
        })
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

    /// Stops the worker: stops its drivers (each within
    /// [`DRIVER_STOP_TIMEOUT`], then aborted), then deregisters it, within a
    /// bounded time. A second call does nothing.
    pub async fn shutdown(&self) {
        let stops = std::mem::take(
            &mut *self
                .inner
                .stops
                .lock()
                .unwrap_or_else(|poisoned| poisoned.into_inner()),
        );
        for (name, stop, running) in stops.drivers {
            stop.shutdown();
            let abort = running.abort_handle();
            if tokio::time::timeout(DRIVER_STOP_TIMEOUT, running)
                .await
                .is_err()
            {
                tracing::warn!(driver = name, "Driver did not stop in time; aborting it");
                abort.abort();
            }
        }
        if let Some(tasks) = &self.inner.tasks {
            tasks.sessions().close_all();
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
