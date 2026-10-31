//! Whether a runtime's answer to a vector is the recorded one.
//!
//! An answer passes when the kernel finds it replays the recorded history
//! (`orcher_sdk_core::kernel::encoded::check_recorded`, the checks the worker
//! runs before it sends an answer), and the worker would send the engine for
//! it what it sent when the run was recorded
//! (`orcher_sdk_core::kernel::encoded::recorded_answer`): the same commands,
//! in the same order and with the same payloads, the same query and update
//! answers, or the same failure.
//!
//! The answer itself is not compared byte for byte with another
//! `RunActivationResult`: what the engine receives is what counts, and two
//! answers that send it the same thing are the same answer. Calls answered
//! from history (`satisfied`) send nothing, so they are judged only by the
//! history check, which needs each one's step id.

use std::fmt::Debug;

use orcher_proto::kernel::v1::{run_activation_result, RunActivationResult};
use orcher_proto::prost::Message;
use orcher_proto::{CompleteWorkflowExecutionRequest, FailWorkflowExecutionRequest};
use orcher_sdk_core::kernel::encoded;
use orcher_sdk_core::worker::record::Recorded;

use crate::vectors::Vector;

/// What is wrong with `answer`, an encoded `RunActivationResult`, as the
/// answer to `vector`, judged in `namespace`; empty when it is the recorded
/// answer.
pub fn judge(vector: &Vector, answer: &[u8], namespace: &str) -> Vec<String> {
    let result = match RunActivationResult::decode(answer) {
        Ok(result) => result,
        Err(e) => {
            return vec![format!(
                "the answer is not an encoded RunActivationResult: {e}"
            )]
        }
    };
    let mut problems = Vec::new();
    if result.activation_id != vector.activation_id {
        problems.push(format!(
            "the answer names activation {:?}, not the one it answers, {:?}",
            String::from_utf8_lossy(&result.activation_id),
            String::from_utf8_lossy(&vector.activation_id),
        ));
    }
    if let Some(run_activation_result::Outcome::ProtocolError(error)) = &result.outcome {
        problems.push(format!(
            "the runtime could not process the activation: {}",
            error.message
        ));
        return problems;
    }

    match encoded::check_recorded(&vector.recorded_activation, answer) {
        Err(e) => {
            problems.push(format!("the kernel refuses the answer: {e}"));
            return problems;
        }
        Ok(violations) if !violations.is_empty() => {
            problems.extend(
                violations.iter().map(|violation| {
                    format!("the answer does not replay the history: {violation}")
                }),
            );
            return problems;
        }
        Ok(_) => {}
    }

    match encoded::recorded_answer(&vector.recorded_activation, answer, namespace) {
        Err(e) => problems.push(format!("the worker would send nothing for the answer: {e}")),
        Ok(sent) => problems.extend(compare(&vector.expected, &sent)),
    }
    problems
}

/// How what the worker would send differs from what it sent.
fn compare(recorded: &Recorded, sent: &Recorded) -> Vec<String> {
    match (recorded, sent) {
        (Recorded::Completion(recorded), Recorded::Completion(sent)) => {
            let (Ok(recorded), Ok(sent)) = (
                CompleteWorkflowExecutionRequest::decode(recorded.as_slice()),
                CompleteWorkflowExecutionRequest::decode(sent.as_slice()),
            ) else {
                return vec!["a completion does not decode".to_string()];
            };
            let mut problems = differences("command", &recorded.commands, &sent.commands);
            problems.extend(differences(
                "query answer",
                &recorded.query_results,
                &sent.query_results,
            ));
            problems.extend(differences(
                "update answer",
                &recorded.update_results,
                &sent.update_results,
            ));
            problems
        }
        (Recorded::Failure(recorded), Recorded::Failure(sent)) => {
            let (Ok(recorded), Ok(sent)) = (
                FailWorkflowExecutionRequest::decode(recorded.as_slice()),
                FailWorkflowExecutionRequest::decode(sent.as_slice()),
            ) else {
                return vec!["a failure report does not decode".to_string()];
            };
            let what = |request: &FailWorkflowExecutionRequest| {
                request.failure.as_ref().map(|failure| {
                    (
                        failure.message.clone(),
                        failure.failure_type.clone(),
                        failure.non_retryable,
                    )
                })
            };
            if what(&recorded) == what(&sent) {
                Vec::new()
            } else {
                vec![format!(
                    "the workflow fails differently\n  recorded: {:?}\n  answered: {:?}",
                    recorded.failure, sent.failure
                )]
            }
        }
        _ => vec![format!(
            "the worker would send {}, but {} was recorded",
            kind(sent),
            kind(recorded)
        )],
    }
}

fn kind(message: &Recorded) -> &'static str {
    match message {
        Recorded::Completion(_) => "a completion",
        Recorded::Failure(_) => "a workflow failure",
        _ => "an activation",
    }
}

/// How the `sent` list differs from the `recorded` one, item by item.
fn differences<T: PartialEq + Debug>(what: &str, recorded: &[T], sent: &[T]) -> Vec<String> {
    if recorded == sent {
        return Vec::new();
    }
    let mut problems = Vec::new();
    if recorded.len() != sent.len() {
        problems.push(format!(
            "{} {what}s recorded, {} answered",
            recorded.len(),
            sent.len()
        ));
    }
    for n in 0..recorded.len().max(sent.len()) {
        match (recorded.get(n), sent.get(n)) {
            (Some(recorded), Some(sent)) if recorded == sent => {}
            (recorded, sent) => problems.push(format!(
                "{what} {n} differs\n  recorded: {}\n  answered: {}",
                shown(recorded),
                shown(sent)
            )),
        }
    }
    problems
}

fn shown<T: Debug>(item: Option<&T>) -> String {
    item.map_or_else(|| "nothing".to_string(), |item| format!("{item:?}"))
}
