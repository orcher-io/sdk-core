//! The kernel's replay tools as encoded protocol messages, for a language
//! binding: resolving a recorded activation, and turning a language's answer
//! into what the worker would send, without an engine.

use crate::error::Result;
use crate::protocol::ExecutionResult;
use crate::wire::host as wire;
use crate::worker::WorkflowExecutionTask;

/// Resolves a recorded activation (an encoded
/// `orcher.v1.PollWorkflowExecutionResponse`, as `worker::record` reads them)
/// into the encoded `orcher.kernel.v1.RunActivation` the host would hand the
/// language; as [`resolve`](super::resolve).
///
/// # Errors
///
/// [`Error::Protocol`](crate::Error::Protocol) for bytes that are not a
/// recorded activation.
pub fn resolve_recorded(recorded: &[u8], namespace: &str) -> Result<Vec<u8>> {
    let task = WorkflowExecutionTask::from(wire::recorded_activation(recorded)?);
    Ok(wire::encode(&super::resolve(&task, namespace)))
}

/// What the worker would send for the language's answer to an activation,
/// an encoded `orcher.kernel.v1.RunActivationResult`; as
/// [`execution_result`](super::execution_result).
///
/// # Errors
///
/// [`Error::Protocol`](crate::Error::Protocol) for bytes that are not a
/// `RunActivationResult`, and the errors of
/// [`execution_result`](super::execution_result).
pub fn execution_result(run_id: &str, answer: &[u8]) -> Result<ExecutionResult> {
    super::execution_result(run_id, wire::activation_result(answer)?)
}
