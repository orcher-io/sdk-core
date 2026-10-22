//! The kernel's replay tools, on encoded protocol messages: resolving a
//! recorded activation, turning a language's answer into what the worker
//! would send, and checking that answer against the history, without an
//! engine. Every language SDK, Rust's included, replays recorded runs
//! ([`crate::worker::record`]) with these, so a replay test or a conformance
//! kit is the same calls in every language.

use crate::error::Result;
use crate::protocol::ExecutionResult;
use crate::wire::host as wire;
use crate::worker::WorkflowExecutionTask;
use crate::DeterminismViolation;

/// Resolves a recorded activation (an encoded
/// `orcher.v1.PollWorkflowExecutionResponse`, as `worker::record` reads them)
/// into the encoded `orcher.kernel.v1.RunActivation` the host would hand the
/// language.
///
/// # Errors
///
/// [`Error::Protocol`](crate::Error::Protocol) for bytes that are not a
/// recorded activation.
pub fn resolve_recorded(recorded: &[u8], namespace: &str) -> Result<Vec<u8>> {
    let task = WorkflowExecutionTask::from(wire::recorded_activation(recorded)?);
    Ok(wire::encode(&super::resolve(&task, namespace)))
}

/// Checks the language's answer to a recorded activation (an encoded
/// `orcher.kernel.v1.RunActivationResult`) against the history the
/// activation carried: the checks the worker runs before it sends an answer
/// to the engine. Empty when the answer replays its history.
///
/// The answer's commands are compared with the steps the history recorded.
/// When the answer also reports the steps the code reached, a recorded step
/// it did not reach, in an answer that issues new work or ends the workflow,
/// is reported too: the code no longer does what it did when the history was
/// written.
///
/// # Errors
///
/// [`Error::Protocol`](crate::Error::Protocol) for bytes that are not the
/// messages, and the errors of [`execution_result`] and of an answer the
/// worker would refuse to send.
pub fn check_recorded(recorded: &[u8], answer: &[u8]) -> Result<Vec<DeterminismViolation>> {
    let task = WorkflowExecutionTask::from(wire::recorded_activation(recorded)?);
    let result = super::execution_result(&task.execution.run_id, wire::activation_result(answer)?)?;
    crate::protocol::validate_execution_result(&result)?;
    Ok(super::check_activation(&task, &result))
}

/// What the worker would send for the language's answer to an activation,
/// an encoded `orcher.kernel.v1.RunActivationResult`.
///
/// # Errors
///
/// [`Error::Protocol`](crate::Error::Protocol) for bytes that are not a
/// `RunActivationResult`, and an answer the kernel cannot carry: one with
/// no outcome, or a command it cannot express.
pub fn execution_result(run_id: &str, answer: &[u8]) -> Result<ExecutionResult> {
    super::execution_result(run_id, wire::activation_result(answer)?)
}
