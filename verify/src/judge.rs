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
//! exactly by default; `json` compares them as JSON values instead, for a
//! runtime whose serializer spells the same values differently, and
//! `ignore-bytes` does not compare them at all.

use std::collections::BTreeMap;
use std::fmt::Debug;

use orcher_proto::kernel::v1::{run_activation_result, RunActivationResult};
use orcher_proto::prost::Message;
use orcher_proto::{
    command::Attributes, query_result, update_result, Command, CompleteWorkflowExecutionRequest,
    FailWorkflowExecutionRequest, Failure, QueryResult, UpdateResult,
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
    /// Compare payloads as JSON values when both decode as JSON; compare bytes
    /// exactly when either side is not JSON.
    Json,
    /// Ignore serialized input and result bytes, but compare command structure,
    /// headers, and every other non-payload field.
    IgnoreBytes,
}

/// What is wrong with `answer`, an encoded `RunActivationResult`, as the
/// answer to `vector`, judged in `namespace`; empty when it is the recorded
/// answer.
pub fn judge(
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
                |command| strip_command_payload_bytes(command, payload_comparison),
            );
            problems.extend(differences_with_payloads(
                "query answer",
                &recorded.query_results,
                &sent.query_results,
                payload_comparison,
                |result| strip_query_payload_bytes(result, payload_comparison),
            ));
            problems.extend(differences_with_payloads(
                "update answer",
                &recorded.update_results,
                &sent.update_results,
                payload_comparison,
                |result| strip_update_payload_bytes(result, payload_comparison),
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

/// Compare typed messages after normalizing their serialized payload bytes.
/// Message counts and all non-payload fields remain significant.
fn differences_with_payloads<T>(
    what: &str,
    recorded: &[T],
    sent: &[T],
    comparison: PayloadComparison,
    normalize_payloads: impl Fn(&mut T),
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
        normalize_payloads(message);
    }
    for message in &mut sent {
        normalize_payloads(message);
    }
    differences(what, &recorded, &sent)
}

/// Normalize serialized inputs/results while retaining command structure and
/// comparing all non-payload fields.
fn strip_command_payload_bytes(command: &mut Command, comparison: PayloadComparison) {
    match command.attributes.as_mut() {
        Some(Attributes::ScheduleTask(attributes)) => {
            normalize_payload_bytes(&mut attributes.input, comparison);
        }
        Some(Attributes::CompleteWorkflow(attributes)) => {
            normalize_payload_bytes(&mut attributes.result, comparison);
        }
        Some(Attributes::CancelWorkflow(attributes)) => {
            normalize_payload_bytes(&mut attributes.details, comparison);
        }
        Some(Attributes::FailWorkflow(attributes)) => {
            if let Some(failure) = attributes.failure.as_mut() {
                normalize_failure_payload_bytes(failure, comparison);
            }
        }
        Some(Attributes::RestartFresh(attributes)) => {
            normalize_payload_bytes(&mut attributes.input, comparison);
        }
        Some(Attributes::StartChildWorkflow(attributes)) => {
            normalize_payload_bytes(&mut attributes.input, comparison);
        }
        Some(Attributes::SendEvent(attributes)) => {
            normalize_payload_bytes(&mut attributes.payload, comparison);
        }
        Some(Attributes::QueryChildWorkflow(attributes)) => {
            normalize_payload_bytes(&mut attributes.query_args, comparison);
        }
        Some(Attributes::RecordStepResult(attributes)) => {
            normalize_payload_bytes(&mut attributes.result, comparison);
            if let Some(failure) = attributes.failure.as_mut() {
                normalize_failure_payload_bytes(failure, comparison);
            }
        }
        Some(Attributes::StartTimer(_))
        | Some(Attributes::CancelTimer(_))
        | Some(Attributes::RequestCancelTask(_))
        | Some(Attributes::CancelChildWorkflow(_))
        | Some(Attributes::RequestCancellation(_))
        | Some(Attributes::UpsertLabels(_))
        | None => {}
    }
}

fn normalize_payload_bytes(bytes: &mut Vec<u8>, comparison: PayloadComparison) {
    match comparison {
        PayloadComparison::Exact => {}
        PayloadComparison::Json => {
            if let Some(canonical) = canonical_json_bytes(bytes) {
                *bytes = canonical;
            }
        }
        PayloadComparison::IgnoreBytes => bytes.clear(),
    }
}

fn canonical_json_bytes(bytes: &[u8]) -> Option<Vec<u8>> {
    let value = serde_json::from_slice(bytes).ok()?;
    serde_json::to_vec(&CanonicalJson::from_value(value)?).ok()
}

/// A JSON value with one spelling for each value: object keys in order, and
/// a number that is a whole number written as an integer (`1.0` is `1`),
/// kept exact whatever its size, so two different large integers never
/// compare equal by rounding to the same float.
#[derive(serde::Serialize)]
#[serde(untagged)]
enum CanonicalJson {
    Null,
    Boolean(bool),
    Integer(i128),
    Number(f64),
    String(String),
    Array(Vec<CanonicalJson>),
    Object(BTreeMap<String, CanonicalJson>),
}

impl CanonicalJson {
    fn from_value(value: serde_json::Value) -> Option<Self> {
        match value {
            serde_json::Value::Null => Some(Self::Null),
            serde_json::Value::Bool(value) => Some(Self::Boolean(value)),
            serde_json::Value::Number(value) => {
                if let Some(integer) = value.as_i64() {
                    return Some(Self::Integer(i128::from(integer)));
                }
                if let Some(integer) = value.as_u64() {
                    return Some(Self::Integer(i128::from(integer)));
                }
                let number = value.as_f64()?;
                // A whole number written with a fraction (`1.0`, `-0.0`) is
                // the integer it equals, as a runtime without an integer type
                // writes it.
                if number.fract() == 0.0 && number.abs() < 9.0e15 {
                    Some(Self::Integer(number as i128))
                } else {
                    Some(Self::Number(number))
                }
            }
            serde_json::Value::String(value) => Some(Self::String(value)),
            serde_json::Value::Array(values) => values
                .into_iter()
                .map(Self::from_value)
                .collect::<Option<Vec<_>>>()
                .map(Self::Array),
            serde_json::Value::Object(values) => values
                .into_iter()
                .map(|(key, value)| Some((key, Self::from_value(value)?)))
                .collect::<Option<BTreeMap<_, _>>>()
                .map(Self::Object),
        }
    }
}

fn normalize_failure_payload_bytes(failure: &mut Failure, comparison: PayloadComparison) {
    normalize_payload_bytes(&mut failure.details, comparison);
    if let Some(cause) = failure.cause.as_mut() {
        normalize_failure_payload_bytes(cause, comparison);
    }
}

fn strip_query_payload_bytes(result: &mut QueryResult, comparison: PayloadComparison) {
    match result.result.as_mut() {
        Some(query_result::Result::Answer(answer)) => normalize_payload_bytes(answer, comparison),
        Some(query_result::Result::ErrorMessage(_)) | None => {}
    }
}

fn strip_update_payload_bytes(result: &mut UpdateResult, comparison: PayloadComparison) {
    match result.result.as_mut() {
        Some(update_result::Result::Answer(answer)) => normalize_payload_bytes(answer, comparison),
        Some(update_result::Result::ErrorMessage(_)) | None => {}
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
                    ..Default::default()
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

    #[test]
    fn json_payloads_compare_decoded_values_and_fall_back_to_bytes() {
        let recorded = completion(
            "charge",
            br#"{"amount":1,"tags":["a","b"]}"#,
            br#"{"ok":true,"count":1}"#,
            b"header",
        );
        let equivalent_json = completion(
            "charge",
            br#"{ "tags": ["a", "b"], "amount": 1.0 }"#,
            br#"{"count":1.0,"ok":true}"#,
            b"header",
        );
        let invalid_json = completion("charge", b"\x00\xff", b"\x01\xfe", b"header");
        let invalid_json_changed = completion("charge", b"\x00\xfe", b"\x01\xfe", b"header");

        assert!(compare(&recorded, &equivalent_json, PayloadComparison::Json).is_empty());

        // Integers stay exact: two that differ only past a float's precision
        // are different values.
        let large = completion("charge", b"9007199254740993", b"1", b"header");
        let large_rounded = completion("charge", b"9007199254740992", b"1", b"header");
        assert!(!compare(&large, &large_rounded, PayloadComparison::Json).is_empty());
        assert!(!compare(
            &invalid_json,
            &invalid_json_changed,
            PayloadComparison::Json
        )
        .is_empty());
    }
}
