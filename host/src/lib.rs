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
//! of work when it has room to run one. That API arrives in later releases
//! alongside each kind of work; this release covers configuration and the
//! worker's registration lifecycle.

pub mod config;

use std::time::Duration;

use orcher_sdk_core::worker::drivers::registration::DEREGISTER_TIMEOUT;
use orcher_sdk_core::worker::{NeededDrivers, ShutdownHandle};
use orcher_sdk_core::{Error, Result, WorkerRegistrationDriver};
use tokio::task::JoinHandle;

pub use config::HostConfig;

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

/// A running worker host.
#[derive(Debug)]
pub struct Host {
    config: HostConfig,
    needed: NeededDrivers,
    registration: Option<(ShutdownHandle, JoinHandle<()>)>,
}

impl Host {
    /// Starts a worker: registers it with the engine and keeps it registered
    /// until [`shutdown`](Self::shutdown).
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
        let mut driver = WorkerRegistrationDriver::new(config.registration(&registrations)).await?;
        let stop = driver.shutdown_handle();
        let running = tokio::spawn(async move {
            if let Err(error) = driver.run().await {
                tracing::warn!(%error, "Worker registration ended with an error");
            }
        });
        tracing::info!(
            identity = %config.identity,
            workflows = needed.workflows,
            tasks = needed.tasks,
            actors = needed.actors,
            "Worker host started"
        );
        Ok(Self {
            config,
            needed,
            registration: Some((stop, running)),
        })
    }

    /// The worker's configuration.
    pub fn config(&self) -> &HostConfig {
        &self.config
    }

    /// The drivers the worker runs.
    pub fn needed_drivers(&self) -> NeededDrivers {
        self.needed
    }

    /// Stops the worker and deregisters it, within a bounded time: a server
    /// that never answers deregistration does not hold up shutdown.
    pub async fn shutdown(mut self) {
        self.stop_registration().await;
    }

    async fn stop_registration(&mut self) {
        let Some((stop, running)) = self.registration.take() else {
            return;
        };
        stop.shutdown();
        // Deregistration is bounded by the driver; this bounds the wait for
        // the task as well, so shutdown always ends.
        if tokio::time::timeout(DEREGISTER_TIMEOUT + Duration::from_secs(1), running)
            .await
            .is_err()
        {
            tracing::warn!(
                identity = %self.config.identity,
                "Worker registration did not stop in time"
            );
        }
    }
}
