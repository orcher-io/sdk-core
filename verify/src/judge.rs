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
//! history check, which needs each one's step id. Payload bytes are compared
//! exactly by default; an explicit portable-runtime mode ignores only those
//! serialized input and result bytes.

use std::fmt::Debug;

use orcher_proto::kernel::v1::{run_activation_result, RunActivationResult};
use orcher_proto::prost::Message;
use orcher_proto::{
    command::Attributes, query_result, update_result, Command, CompleteWorkflowExecutionRequest,
    FailWorkflowExecutionRequest, QueryResult, UpdateResult,
};
#[cfg(test)]
use orcher_proto::{CommandType, ScheduleTaskCommandAttributes};
use orcher_sdk_core::kernel::encoded;
use orcher_sdk_core::worker::record::Recorded;

use crate::vectors::Vector;

/// Whether to compare serialized payload bytes in the worker's report.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum PayloadComparison {
    /// Require payload bytes to match the recording exactly.
    #[default]
    Exact,
    /// Ignore serialized input and result bytes, but compare command structure,
    /// headers, and every other non-payload field.
    IgnoreBytes,
}

/// What is wrong with `answer`, an encoded `RunActivationResult`, as the
/// answer to `vector`, judged in `namespace`; empty when it is the recorded
/// answer.
pub fn judge(vector: &Vector, answer: &[u8], namespace: &str) -> Vec<String> {
    judge_with_payload_comparison(vector, answer, namespace, PayloadComparison::Exact)
}

/// What is wrong with `answer`, using the requested payload-byte comparison.
pub fn judge_with_payload_comparison(
    vector: &Vector,
    answer: &[u8],
    namespace: &str,
    payload_comparison: PayloadComparison,
) -> Vec<String> {
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
        Ok(sent) => problems.extend(compare(&vector.expected, &sent, payload_comparison)),
    }
    problems
}

/// How what the worker would send differs from what it sent.
fn compare(
    recorded: &Recorded,
    sent: &Recorded,
    payload_comparison: PayloadComparison,
) -> Vec<String> {
    match (recorded, sent) {
        (Recorded::Completion(recorded), Recorded::Completion(sent)) => {
            let (Ok(recorded), Ok(sent)) = (
                CompleteWorkflowExecutionRequest::decode(recorded.as_slice()),
                CompleteWorkflowExecutionRequest::decode(sent.as_slice()),
            ) else {
                return vec!["a completion does not decode".to_string()];
            };
            let mut problems = differences_with_payloads(
                "command",
                &recorded.commands,
                &sent.commands,
                payload_comparison,
                strip_command_payload_bytes,
            );
            problems.extend(differences_with_payloads(
                "query answer",
                &recorded.query_results,
                &sent.query_results,
                payload_comparison,
                strip_query_payload_bytes,
            ));
            problems.extend(differences_with_payloads(
                "update answer",
                &recorded.update_results,
                &sent.update_results,
                payload_comparison,
                strip_update_payload_bytes,
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

/// Compare typed messages after optionally clearing their serialized payload
/// bytes. Message counts and all non-payload fields remain significant.
fn differences_with_payloads<T>(
    what: &str,
    recorded: &[T],
    sent: &[T],
    comparison: PayloadComparison,
    strip_payload_bytes: impl Fn(&mut T),
) -> Vec<String>
where
    T: Clone + PartialEq + Debug,
{
    if comparison == PayloadComparison::Exact {
        return differences(what, recorded, sent);
    }

    let mut recorded = recorded.to_vec();
    let mut sent = sent.to_vec();
    for message in &mut recorded {
        strip_payload_bytes(message);
    }
    for message in &mut sent {
        strip_payload_bytes(message);
    }
    differences(what, &recorded, &sent)
}

/// Clear serialized inputs/results while retaining the command shape and
/// comparing all non-payload fields.
fn strip_command_payload_bytes(command: &mut Command) {
    match command.attributes.as_mut() {
        Some(Attributes::ScheduleTask(attributes)) => {
            attributes.input.clear();
        }
        Some(Attributes::CompleteWorkflow(attributes)) => {
            attributes.result.clear();
        }
        Some(Attributes::CancelWorkflow(attributes)) => {
            attributes.details.clear();
        }
        Some(Attributes::FailWorkflow(attributes)) => {
            if let Some(failure) = attributes.failure.as_mut() {
                failure.details.clear();
            }
        }
        Some(Attributes::RestartFresh(attributes)) => {
            attributes.input.clear();
        }
        Some(Attributes::StartChildWorkflow(attributes)) => {
            attributes.input.clear();
        }
        Some(Attributes::SendEvent(attributes)) => {
            attributes.payload.clear();
        }
        Some(Attributes::QueryChildWorkflow(attributes)) => {
            attributes.query_args.clear();
        }
        Some(Attributes::RecordStepResult(attributes)) => {
            attributes.result.clear();
            if let Some(failure) = attributes.failure.as_mut() {
                failure.details.clear();
            }
        }
        _ => {}
    }
}

fn strip_query_payload_bytes(result: &mut QueryResult) {
    if let Some(query_result::Result::Answer(answer)) = result.result.as_mut() {
        answer.clear();
    }
}

fn strip_update_payload_bytes(result: &mut UpdateResult) {
    if let Some(update_result::Result::Answer(answer)) = result.result.as_mut() {
        answer.clear();
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

#[cfg(test)]
mod tests {
    use super::*;

    fn completion(task_type: &str, input: &[u8], query_answer: &[u8], header: &[u8]) -> Recorded {
        Recorded::Completion(
            CompleteWorkflowExecutionRequest {
                commands: vec![Command {
                    command_type: CommandType::ScheduleTask as i32,
                    attributes: Some(Attributes::ScheduleTask(ScheduleTaskCommandAttributes {
                        task_type: task_type.to_string(),
                        input: input.to_vec(),
                        headers: [("trace".to_string(), header.to_vec())].into(),
                        ..Default::default()
                    })),
                }],
                query_results: vec![QueryResult {
                    query_id: "query-1".to_string(),
                    result: Some(query_result::Result::Answer(query_answer.to_vec())),
                    ..Default::default()
                }],
                ..Default::default()
            }
            .encode_to_vec(),
        )
    }

    #[test]
    fn ignore_payload_bytes_keeps_structure_and_non_payload_fields_strict() {
        let recorded = completion("charge", br#"{"amount":1}"#, br#"{"ok":true}"#, b"header");
        let different_bytes = completion(
            "charge",
            br#"{"amount":1.0}"#,
            br#"{ "ok": true }"#,
            b"header",
        );
        let different_task = completion(
            "refund",
            br#"{"amount":1.0}"#,
            br#"{ "ok": true }"#,
            b"header",
        );
        let different_header = completion(
            "charge",
            br#"{"amount":1.0}"#,
            br#"{ "ok": true }"#,
            b"other-header",
        );

        assert!(!compare(&recorded, &different_bytes, PayloadComparison::Exact).is_empty());
        assert!(compare(&recorded, &different_bytes, PayloadComparison::IgnoreBytes).is_empty());
        assert!(!compare(&recorded, &different_task, PayloadComparison::IgnoreBytes).is_empty());
        assert!(!compare(&recorded, &different_header, PayloadComparison::IgnoreBytes).is_empty());
    }
}
