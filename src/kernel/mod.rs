//! The kernel: what a run's history means, resolved once for every language.
//!
//! The kernel turns the history the engine sends with an activation into a
//! [`RunActivation`](crate::proto::kernel::v1::RunActivation): the outcomes the
//! run observes, in the order it observes them, each with the run's time when
//! it happened and one decoding for every SDK. A language SDK runs the
//! workflow's code against those outcomes and never reads the journal.
//!
//! The protocol is `orcher.kernel.v1` (orcher-proto); the design is
//! `docs/kernel-protocol-v1.md`.
//!
//! - `activation`: an activation's history, resolved.
//! - `completion`: the language's answer to an activation, back to commands
//!   for the engine.
//! - [`step_ids`]: step ids under the kernel's own scheme, `KERNEL_V1`.
//! - [`random`]: the deterministic random source for runs under that scheme.
//! - [`encoded`]: the replay tools, on encoded protocol messages.

pub(crate) mod activation;
pub(crate) mod completion;
pub mod encoded;
pub mod random;
pub mod step_ids;

pub(crate) use activation::resolve;
pub(crate) use completion::execution_result;

use crate::protocol::ExecutionResult;
use crate::worker::WorkflowExecutionTask;
use crate::DeterminismViolation;

/// Checks the answer to an activation, as the kernel turned it into an
/// [`ExecutionResult`], against the history the activation was resolved
/// from: the checks the worker runs before it sends an answer to the engine.
///
/// The answer's commands are compared with the steps the history recorded.
/// When the answer also reports the steps the code reached, a recorded step
/// it did not reach, in an answer that issues new work or ends the workflow,
/// is reported too: the code no longer does what it did when the history was
/// written. Empty when the answer replays its history.
pub(crate) fn check_activation(
    task: &WorkflowExecutionTask,
    result: &ExecutionResult,
) -> Vec<DeterminismViolation> {
    crate::state::replayer::Replayer::check_activation(&task.journal, result)
}
