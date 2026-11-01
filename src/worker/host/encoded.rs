//! The host's work, as encoded protocol messages: the contract every language
//! SDK is written against, Rust's included.
//!
//! Every operation takes and returns the encoded form of an
//! `orcher.kernel.v1` or `orcher.host.v1` message, the same bytes a sidecar
//! or any other runtime exchanges. An SDK decodes and encodes them with
//! generated code of its own, at the protocol version it implements; this
//! crate's public API names no generated type, so a new orcher-proto here
//! changes nothing an SDK compiles against.

use crate::error::Result;
use crate::wire::host as wire;

use super::Host;

/// The [`Host`]'s work as encoded protocol messages. Get one with
/// [`Host::encoded`]; cloning it is cheap and shares the host.
#[derive(Clone, Debug)]
pub struct EncodedHost {
    host: Host,
}

impl Host {
    /// This host's work as encoded protocol messages: the way a language SDK
    /// pulls work and hands back its results.
    pub fn encoded(&self) -> EncodedHost {
        EncodedHost { host: self.clone() }
    }
}

impl EncodedHost {
    /// The next workflow activation to run, once there is one: an encoded
    /// `orcher.kernel.v1.RunActivation`, the run's history resolved into the
    /// outcomes its code observes. `None` once the worker has stopped and
    /// every activation it took has been handed over, and at once for a
    /// worker that runs no workflows.
    pub async fn next_activation(&self) -> Option<Vec<u8>> {
        let activation = self.host.next_activation().await?;
        Some(wire::encode(&activation))
    }

    /// Answers an activation with an encoded
    /// `orcher.kernel.v1.RunActivationResult`: every durable call the code
    /// made, or a workflow failure, or a protocol error. An answer the kernel
    /// cannot carry hands the activation back to the engine rather than
    /// failing the run.
    ///
    /// # Errors
    ///
    /// [`Error::Protocol`](crate::Error::Protocol) for bytes that are not a
    /// `RunActivationResult`, or for an answer to an activation the host does
    /// not hold (already answered, or the worker stopped), and an error once
    /// the worker has stopped or when it runs no workflows.
    pub async fn complete_activation(&self, answer: &[u8]) -> Result<()> {
        let answer = wire::activation_result(answer)?;
        self.host.complete_activation(answer).await
    }

    /// The next task to run, once there is one: an encoded
    /// `orcher.host.v1.TaskAssignment`. `None` once the worker has stopped
    /// and every task it took has been handed over, and at once for a worker
    /// that runs no tasks.
    pub async fn next_task(&self) -> Option<Vec<u8>> {
        let task = self.host.next_task().await?;
        Some(wire::encode(&task))
    }

    /// Records a heartbeat for a running task, an encoded
    /// `orcher.host.v1.TaskHeartbeat` carrying the task's token and optional
    /// progress details, and answers with an encoded
    /// `orcher.host.v1.HeartbeatAnswer` saying whether the task has been
    /// asked to cancel. Heartbeats are batched to the engine; this does not
    /// wait for one to be sent.
    ///
    /// # Errors
    ///
    /// [`Error::Protocol`](crate::Error::Protocol) for bytes that are not a
    /// `TaskHeartbeat`.
    pub fn heartbeat(&self, heartbeat: &[u8]) -> Result<Vec<u8>> {
        let heartbeat = wire::task_heartbeat(heartbeat)?;
        let answer = self.host.heartbeat(&heartbeat.token, heartbeat.details);
        Ok(wire::encode(&answer))
    }

    /// Waits until the task with `token` is asked to cancel (`true`), or its
    /// outcome is reported (`false`).
    pub async fn task_cancelled(&self, token: &[u8]) -> bool {
        self.host.task_cancelled(token).await
    }

    /// Reports how a task attempt ended, an encoded
    /// `orcher.host.v1.TaskOutcome`: its value, or its failure.
    ///
    /// # Errors
    ///
    /// [`Error::Protocol`](crate::Error::Protocol) for bytes that are not a
    /// `TaskOutcome`, an error for an outcome with neither a value nor a
    /// failure, and an error once the worker has stopped or when it runs no
    /// tasks.
    pub async fn complete_task(&self, outcome: &[u8]) -> Result<()> {
        let outcome = wire::task_outcome(outcome)?;
        self.host.complete_task(outcome).await
    }

    /// The next actor operation to run, once there is one: an encoded
    /// `orcher.host.v1.ActorOperation`. `None` once the worker has stopped,
    /// and at once for a worker that runs no actors.
    pub async fn next_actor_operation(&self) -> Option<Vec<u8>> {
        let operation = self.host.next_actor_operation().await?;
        Some(wire::encode(&operation))
    }

    /// Reports how an actor operation ended, an encoded
    /// `orcher.host.v1.ActorResult`: its value, or its failure with the type
    /// the language gave it.
    ///
    /// # Errors
    ///
    /// [`Error::Protocol`](crate::Error::Protocol) for bytes that are not an
    /// `ActorResult` or for a result of an operation the host does not hold,
    /// an error for a result with neither a value nor a failure, and an error
    /// once the worker has stopped or when it runs no actors.
    pub async fn complete_actor_operation(&self, result: &[u8]) -> Result<()> {
        let result = wire::actor_result(result)?;
        self.host.complete_actor_operation(result).await
    }

    /// The host itself, for what does not cross as a protocol message: its
    /// configuration, actor state, metrics and shutdown.
    pub fn host(&self) -> &Host {
        &self.host
    }
}
