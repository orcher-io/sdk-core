//! The kernel and host protocol messages in their encoded form: what a
//! language binding exchanges with the host.

use crate::error::{Error, Result};
use crate::proto::host::v1::{ActorResult, TaskHeartbeat, TaskOutcome};
use crate::proto::kernel::v1::RunActivationResult;
use crate::proto::prost::Message;
use orcher_proto::orcher::v1::PollWorkflowExecutionResponse;

/// A message, encoded.
pub(crate) fn encode(message: &impl Message) -> Vec<u8> {
    message.encode_to_vec()
}

fn decode<M: Message + Default>(bytes: &[u8], what: &str) -> Result<M> {
    M::decode(bytes).map_err(|e| Error::Protocol(format!("not an encoded {what}: {e}")))
}

/// An encoded `orcher.kernel.v1.RunActivationResult`.
pub(crate) fn activation_result(bytes: &[u8]) -> Result<RunActivationResult> {
    decode(bytes, "RunActivationResult")
}

/// An encoded `orcher.host.v1.TaskOutcome`.
pub(crate) fn task_outcome(bytes: &[u8]) -> Result<TaskOutcome> {
    decode(bytes, "TaskOutcome")
}

/// An encoded `orcher.host.v1.TaskHeartbeat`.
pub(crate) fn task_heartbeat(bytes: &[u8]) -> Result<TaskHeartbeat> {
    decode(bytes, "TaskHeartbeat")
}

/// An encoded `orcher.host.v1.ActorResult`.
pub(crate) fn actor_result(bytes: &[u8]) -> Result<ActorResult> {
    decode(bytes, "ActorResult")
}

/// A recorded activation: an encoded `orcher.v1.PollWorkflowExecutionResponse`.
pub(crate) fn recorded_activation(bytes: &[u8]) -> Result<PollWorkflowExecutionResponse> {
    decode(bytes, "PollWorkflowExecutionResponse")
}
