//! Driver mode for workflow, task, and actor execution.
//!
//! In driver mode, sdk-core handles polling and gRPC communication, and the
//! language SDK runs the user's handlers.
//!
//! ## Architecture
//!
//! Each unit of work goes through the same sequence:
//! 1. sdk-core polls for work (`WorkflowExecutionTask`, `TaskExecutionTask`).
//! 2. sdk-core sends the work to the language SDK over a channel.
//! 3. The language SDK runs its handler.
//! 4. The language SDK sends the result back to sdk-core over a channel.
//! 5. sdk-core reports the result to the server over gRPC.
//!
//! - [`workflow`]: the workflow driver.
//! - [`task`]: the task driver, and eager tasks.
//! - [`actor`]: the actor driver.
//! - [`registration`]: registers the worker and heartbeats it; polls nothing.
//!
//! What the work drivers share, stopping and the reports still in flight,
//! lives here.

use std::future::Future;
use std::sync::Arc;

use crate::worker::lifecycle::{self};
use crate::worker::poll::completion::Completer;

pub mod actor;
pub mod registration;
pub mod task;
pub mod workflow;

pub use actor::{ActorDriver, ActorDriverConfig, ActorDriverEvent, ActorWork, ActorWorkResult};
pub use registration::{WorkerRegistrationConfig, WorkerRegistrationDriver};
pub(crate) use task::proto_task_to_task_work;
pub use task::{
    EagerTaskInjector, SessionQueueChange, TaskDriver, TaskDriverConfig, TaskWork, TaskWorkResult,
};
pub use workflow::{WorkflowDriver, WorkflowDriverConfig, WorkflowWork, WorkflowWorkResult};

/// A handle that stops a running [`WorkflowDriver`], [`TaskDriver`] or
/// [`WorkerRegistrationDriver`].
///
/// Get one from [`WorkflowDriver::shutdown_handle`],
/// [`TaskDriver::shutdown_handle`] or
/// [`WorkerRegistrationDriver::shutdown_handle`].
/// For the registration driver, [`shutdown`](Self::shutdown) ends the
/// heartbeat loop and deregisters the worker; the rest of its description is
/// about the work drivers.
#[derive(Clone, Debug)]
pub struct ShutdownHandle(tokio::sync::watch::Sender<bool>);

impl ShutdownHandle {
    pub(crate) fn from_sender(sender: tokio::sync::watch::Sender<bool>) -> Self {
        Self(sender)
    }

    /// Ask the driver to stop.
    ///
    /// The text below says "activations". For a task driver, read "tasks",
    /// and read the task's start-to-close timeout for the claim timeout.
    ///
    /// The driver's pollers start no new polls. A poll already out is left to
    /// finish, because the engine may have claimed the activation it brings
    /// back and cancelling the call would not release it; that activation is
    /// handed to the language SDK. While the SDK still takes work, the driver also
    /// waits for the results of activations it has handed over. All of this,
    /// and sending the completions that follow, happens within one shutdown
    /// grace ([`CompletionRetryConfig::shutdown_grace`](crate::worker::CompletionRetryConfig::shutdown_grace), five seconds by
    /// default) from this call: `run` returns by then.
    ///
    /// The driver also tells the engine, once, that this process is shutting
    /// down (`ShutdownWorker`, naming the queues the driver polls). An engine
    /// from 0.5.0 on answers the driver's open long polls at once with
    /// nothing and hands its later polls nothing, so an idle worker stops
    /// within moments. An older engine does not know the call; there an idle
    /// worker takes the whole grace to stop, as its pollers sit in long polls
    /// that answer only when work arrives and are abandoned only once the
    /// grace is over. A worker whose SDK has already stopped taking work
    /// stops at once either way.
    ///
    /// An activation that a poll brings back but that can no longer be handed
    /// to the SDK is handed back to the engine (`ReleaseWorkflowExecution`),
    /// within what is left of the grace, for the next poll to take. An engine
    /// older than 0.5.0 cannot take it back, so it waits out its claim timeout. Tasks
    /// have no such call: one not handed over waits out its start-to-close
    /// timeout.
    ///
    /// Call this at the first sign of shutdown, and keep taking work and
    /// returning results until `run` returns; only then stop the language
    /// side's loops. Stopping them first strands whatever polls bring back
    /// in the meantime: the engine has it claimed, and it waits out the
    /// engine's claim timeout.
    pub fn shutdown(&self) {
        let _ = self.0.send(true);
    }
}

/// The reports being sent, each from its own task.
struct InFlight {
    set: tokio::task::JoinSet<()>,
    /// Which driver sends them, capitalized to start a log sentence:
    /// "Workflow driver".
    driver: &'static str,
    /// What they are, in lowercase for the middle of a log sentence:
    /// "workflow completions".
    what: &'static str,
}

impl InFlight {
    fn new(driver: &'static str, what: &'static str) -> Self {
        Self {
            set: tokio::task::JoinSet::new(),
            driver,
            what,
        }
    }

    /// Run `send` in a task of its own, holding `slot` until it has sent its
    /// report or given up on it.
    fn spawn(
        &mut self,
        send: impl Future<Output = ()> + Send + 'static,
        slot: Option<tokio::sync::OwnedSemaphorePermit>,
    ) {
        self.set.spawn(async move {
            send.await;
            drop(slot);
        });
    }

    fn reaped(&self, finished: std::result::Result<(), tokio::task::JoinError>) {
        if let Err(e) = finished {
            if e.is_panic() {
                tracing::error!(error = %e, "Sending {} panicked", self.what);
            }
        }
    }

    /// Give reports still being sent until `stop_by` to finish, then abort the rest.
    ///
    /// Each report stops retrying once its own grace is up. This deadline also
    /// bounds the wait for a report stuck inside a call, and for one started
    /// late in the shutdown.
    async fn finish(mut self, stop_by: tokio::time::Instant) {
        if self.set.is_empty() {
            return;
        }
        tracing::info!(
            pending = self.set.len(),
            grace_ms = stop_by
                .saturating_duration_since(tokio::time::Instant::now())
                .as_millis() as u64,
            "Waiting for {} still being sent",
            self.what
        );
        let what = self.what;
        let set = &mut self.set;
        let drained = tokio::time::timeout_at(stop_by, async {
            while let Some(finished) = set.join_next().await {
                if let Err(e) = finished {
                    if e.is_panic() {
                        tracing::error!(error = %e, "Sending {what} panicked");
                    }
                }
            }
        })
        .await;
        if drained.is_err() {
            tracing::warn!(
                abandoned = self.set.len(),
                "Shutdown grace over; abandoning {} still being sent",
                self.what
            );
            self.set.abort_all();
            self.set.detach_all();
        }
    }
}

impl Drop for InFlight {
    fn drop(&mut self) {
        // Reports are still outstanding here only when the driver's run was
        // dropped rather than shut down: its task was aborted, or the runtime
        // stopped. Dropping the set aborts them, so log it; otherwise they
        // would vanish silently.
        if !self.set.is_empty() {
            tracing::warn!(
                abandoned = self.set.len(),
                "{} stopped without a shutdown; abandoning {} still being sent. Stop it \
                 through its shutdown handle to let them finish",
                self.driver,
                self.what
            );
        }
    }
}

/// Tell the engine this driver is shutting down.
///
/// The notice is sent from a task of its own so the run loop keeps draining
/// meanwhile. Because it is held in `in_flight`, it gets no more than the
/// shutdown grace, whatever its own timeout.
fn announce_shutdown(in_flight: &mut InFlight, completer: &Completer, task_queues: Vec<String>) {
    let caller = Arc::clone(&completer.caller);
    let channel_manager = Arc::clone(&completer.channel_manager);
    let timeout = lifecycle::SHUTDOWN_NOTICE_TIMEOUT.min(completer.retry.shutdown_grace);
    in_flight.spawn(
        async move {
            lifecycle::announce_shutdown(
                &caller,
                &channel_manager,
                lifecycle::shutdown_queues(task_queues),
                timeout,
            )
            .await;
        },
        None,
    );
}
