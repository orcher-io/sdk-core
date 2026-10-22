//! The host's work, as encoded protocol messages: the contract a language
//! binding is written against.
//!
//! Every operation takes and returns the encoded form of an
//! `orcher.kernel.v1` or `orcher.host.v1` message, the same bytes a sidecar
//! or any other runtime exchanges. A binding passes them across its language
//! boundary as they are; it needs no generated Rust code and no protobuf
//! library of its own, and nothing here changes when the host's typed API
//! does.

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
    /// This host's work as encoded protocol messages, for a language binding.
    pub fn encoded(&self) -> EncodedHost {
        EncodedHost { host: self.clone() }
    }
}

impl EncodedHost {
    /// The next workflow activation, an encoded `orcher.kernel.v1.RunActivation`;
    /// as [`Host::next_activation`].
    pub async fn next_activation(&self) -> Option<Vec<u8>> {
        let activation = self.host.next_activation().await?;
        Some(wire::encode(&activation))
    }

    /// Answers an activation with an encoded
    /// `orcher.kernel.v1.RunActivationResult`; as [`Host::complete_activation`].
    ///
    /// # Errors
    ///
    /// [`Error::Protocol`](crate::Error::Protocol) for bytes that are not
    /// one, and the errors of [`Host::complete_activation`].
    pub async fn complete_activation(&self, answer: &[u8]) -> Result<()> {
        let answer = wire::activation_result(answer)?;
        self.host.complete_activation(answer).await
    }

    /// The next task, an encoded `orcher.host.v1.TaskAssignment`; as
    /// [`Host::next_task`].
    pub async fn next_task(&self) -> Option<Vec<u8>> {
        let task = self.host.next_task().await?;
        Some(wire::encode(&task))
    }

    /// Records a heartbeat, an encoded `orcher.host.v1.TaskHeartbeat`, and
    /// answers with an encoded `orcher.host.v1.HeartbeatAnswer`; as
    /// [`Host::heartbeat`].
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
    /// outcome is reported (`false`); as [`Host::task_cancelled`].
    pub async fn task_cancelled(&self, token: &[u8]) -> bool {
        self.host.task_cancelled(token).await
    }

    /// Reports a task's outcome, an encoded `orcher.host.v1.TaskOutcome`; as
    /// [`Host::complete_task`].
    ///
    /// # Errors
    ///
    /// [`Error::Protocol`](crate::Error::Protocol) for bytes that are not
    /// one, and the errors of [`Host::complete_task`].
    pub async fn complete_task(&self, outcome: &[u8]) -> Result<()> {
        let outcome = wire::task_outcome(outcome)?;
        self.host.complete_task(outcome).await
    }

    /// The next actor operation, an encoded `orcher.host.v1.ActorOperation`;
    /// as [`Host::next_actor_operation`].
    pub async fn next_actor_operation(&self) -> Option<Vec<u8>> {
        let operation = self.host.next_actor_operation().await?;
        Some(wire::encode(&operation))
    }

    /// Reports an actor operation's result, an encoded
    /// `orcher.host.v1.ActorResult`; as [`Host::complete_actor_operation`].
    ///
    /// # Errors
    ///
    /// [`Error::Protocol`](crate::Error::Protocol) for bytes that are not
    /// one, and the errors of [`Host::complete_actor_operation`].
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
