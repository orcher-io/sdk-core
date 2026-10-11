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
//! - [`needed`]: which of the work drivers a worker starts.
//!
//! What the work drivers share for stopping lives here. Their slots and the
//! reports they still have in flight live in the crate-internal
//! `worker::governor`.

use std::sync::Arc;

use crate::worker::governor::InFlight;
use crate::worker::lifecycle::{self};
use crate::worker::poll::completion::Completer;

pub mod actor;
pub mod needed;
pub mod registration;
pub mod task;
pub mod workflow;

pub use actor::{ActorDriver, ActorDriverConfig, ActorDriverEvent, ActorWork, ActorWorkResult};
pub use needed::{NeededDrivers, BUILT_IN_TASK_TYPES, SESSION_COMPLETE_TASK, SESSION_CREATE_TASK};
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
