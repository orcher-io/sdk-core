//! The kernel's replay tools, through the encoded API every language SDK
//! uses: every recorded activation resolves as it is expected to, and an
//! answer is checked against the history it answers.
//!
//! `verify/vectors/replay/<sdk>/resolutions.json` holds, for each activation
//! recorded from that SDK's contract suite, the `RunActivation` the kernel
//! resolves it to, encoded. They were first written when the kernel was
//! proven to resolve each recording exactly as the SDKs did before it
//! existed, from the jobs core derived from the journal and the times read
//! beside them. A change to what the kernel resolves fails here; a change
//! meant to alter it rewrites them with `ORCHER_BLESS_RESOLUTIONS=1`.
//!
//! The expected messages are compared decoded, field by field, so the order a
//! map was encoded in does not count.

use std::collections::BTreeMap;
use std::path::Path;

use orcher_proto::kernel::v1::RunActivation;
use orcher_proto::prost::Message;
use orcher_sdk_core::kernel::encoded;
use orcher_sdk_core::worker::record::{read_runs, Recorded};

const NAMESPACE: &str = "contract";

#[test]
fn the_kernel_resolves_every_committed_recording_as_expected() {
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("verify/vectors/replay");
    let checked: usize = ["rust", "python", "typescript"]
        .iter()
        .map(|sdk| check(&fixtures.join(sdk)))
        .sum();
    assert!(checked > 0, "no activations checked");
}

/// Each recorded activation of the runs under `dir`, keyed by run and its
/// place among the run's activations, resolved by the kernel.
fn resolved(dir: &Path) -> BTreeMap<String, RunActivation> {
    let mut out = BTreeMap::new();
    for run in read_runs(dir).expect("read the recorded runs") {
        let activations = run.messages.iter().filter_map(|message| match message {
            Recorded::Activation(recorded) => Some(recorded),
            _ => None,
        });
        for (n, recorded) in activations.enumerate() {
            let resolved = encoded::resolve_recorded(recorded, NAMESPACE)
                .expect("a recorded activation resolves");
            out.insert(
                format!("{}/{}/{n}", run.workflow_id, run.run_id),
                RunActivation::decode(resolved.as_slice()).expect("a RunActivation"),
            );
        }
    }
    out
}

/// Checks every activation recorded under `dir` against its expected
/// resolution, or writes the expected resolutions when blessing; returns how
/// many there are.
fn check(dir: &Path) -> usize {
    let resolved = resolved(dir);
    let expected_path = dir.join("resolutions.json");
    if std::env::var_os("ORCHER_BLESS_RESOLUTIONS").is_some_and(|v| !v.is_empty()) {
        let encoded: BTreeMap<&String, String> = resolved
            .iter()
            .map(|(key, activation)| (key, hex(&activation.encode_to_vec())))
            .collect();
        std::fs::write(
            &expected_path,
            serde_json::to_string_pretty(&encoded).expect("encode") + "\n",
        )
        .expect("write the expected resolutions");
        return resolved.len();
    }

    let expected: BTreeMap<String, String> = serde_json::from_str(
        &std::fs::read_to_string(&expected_path).expect("read the expected resolutions"),
    )
    .expect("parse the expected resolutions");
    let mut problems = Vec::new();
    for key in expected.keys().filter(|key| !resolved.contains_key(*key)) {
        problems.push(format!(
            "{key}: expected, but no such activation is recorded"
        ));
    }
    for (key, activation) in &resolved {
        match expected.get(key) {
            None => problems.push(format!("{key}: recorded, but has no expected resolution")),
            Some(encoded) => {
                let want = RunActivation::decode(unhex(encoded).as_slice())
                    .expect("decode an expected resolution");
                if &want != activation {
                    problems.push(format!(
                        "{key}: resolves differently\n  expected: {want:?}\n  now:      {activation:?}"
                    ));
                }
            }
        }
    }
    eprintln!(
        "{}: {} activations resolved as expected",
        dir.display(),
        resolved.len()
    );
    assert!(problems.is_empty(), "{}", problems.join("\n"));
    resolved.len()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn unhex(text: &str) -> Vec<u8> {
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(&text[i..i + 2], 16).expect("hex"))
        .collect()
}

/// Bytes that are not the messages are refused as protocol errors.
#[test]
fn the_replay_tools_refuse_bytes_that_are_not_the_message() {
    let garbage = [0xff, 0xff, 0xff];
    assert!(matches!(
        encoded::resolve_recorded(&garbage, NAMESPACE),
        Err(orcher_sdk_core::Error::Protocol(_))
    ));
    assert!(matches!(
        encoded::execution_result("run", &garbage),
        Err(orcher_sdk_core::Error::Protocol(_))
    ));
    assert!(matches!(
        encoded::check_recorded(&garbage, &answer(vec![]).encode_to_vec()),
        Err(orcher_sdk_core::Error::Protocol(_))
    ));
    assert!(matches!(
        encoded::check_recorded(&charged(), &garbage),
        Err(orcher_sdk_core::Error::Protocol(_))
    ));
}

/// An answer that makes the recorded call again and goes on replays its
/// history; one that makes another call at the recorded step does not, and
/// says where.
#[test]
fn an_answer_is_checked_against_the_recorded_history() {
    let replays = answer(vec![
        call(0, true, "charge_0", "charge"),
        call(1, false, "ship_1", "ship"),
    ]);
    let violations = encoded::check_recorded(&charged(), &replays.encode_to_vec()).unwrap();
    assert!(violations.is_empty(), "{violations:?}");

    let changed = answer(vec![call(0, false, "charge_0", "refund")]);
    let violations = encoded::check_recorded(&charged(), &changed.encode_to_vec()).unwrap();
    assert_eq!(violations.len(), 1, "{violations:?}");
    assert_eq!(violations[0].step_id.as_deref(), Some("charge_0"));
}

/// What the worker would send for an answer: its new calls as commands for
/// the engine, satisfied ones sending nothing; a failure report for a
/// workflow the code failed; and nothing for code that no longer replays.
#[test]
fn an_answer_becomes_what_the_worker_sends() {
    use orcher_proto::kernel::v1::{run_activation_result, Failure, FailureCode};
    use orcher_proto::orcher::v1::{
        command::Attributes, CompleteWorkflowExecutionRequest, FailWorkflowExecutionRequest,
    };

    let replays = answer(vec![
        call(0, true, "charge_0", "charge"),
        call(1, false, "ship_1", "ship"),
    ]);
    let sent = encoded::recorded_answer(&charged(), &replays.encode_to_vec(), NAMESPACE).unwrap();
    let Recorded::Completion(sent) = sent else {
        panic!("not a completion: {sent:?}");
    };
    let sent = CompleteWorkflowExecutionRequest::decode(sent.as_slice()).unwrap();
    assert_eq!(
        (sent.workflow_id.as_str(), sent.execution_id.as_str()),
        ("order-7", "run-1")
    );
    assert_eq!(sent.namespace, NAMESPACE);
    assert_eq!(sent.commands.len(), 1, "{sent:?}");
    let Some(Attributes::ScheduleTask(task)) = &sent.commands[0].attributes else {
        panic!("not a task: {sent:?}");
    };
    assert_eq!(
        (task.task_id.as_str(), task.task_type.as_str()),
        ("ship_1", "ship")
    );
    // A task given no queue runs on the run's.
    assert_eq!(task.task_queue, "orders");

    let failing = |code: FailureCode| {
        let mut failed = answer(vec![]);
        failed.outcome = Some(run_activation_result::Outcome::WorkflowFailed(Failure {
            message: "out of stock".into(),
            non_retryable: true,
            code: code as i32,
            ..Default::default()
        }));
        failed.encode_to_vec()
    };
    let sent = encoded::recorded_answer(&charged(), &failing(FailureCode::Application), NAMESPACE)
        .unwrap();
    let Recorded::Failure(sent) = sent else {
        panic!("not a failure: {sent:?}");
    };
    let failure = FailWorkflowExecutionRequest::decode(sent.as_slice())
        .unwrap()
        .failure
        .unwrap();
    assert_eq!(failure.message, "out of stock");
    assert!(failure.non_retryable);

    assert!(matches!(
        encoded::recorded_answer(&charged(), &failing(FailureCode::NonDeterminism), NAMESPACE),
        Err(orcher_sdk_core::Error::DeterminismViolation { .. })
    ));
}

/// A recorded activation of a run whose history scheduled task `charge_0`
/// and recorded its result.
fn charged() -> Vec<u8> {
    use orcher_proto::orcher::v1::{
        journal_entry::Attributes, EntryType, JournalEntry, PollWorkflowExecutionResponse,
        StepCompletedEventAttributes, TaskScheduledEventAttributes,
    };
    let entry = |entry_id, entry_type: EntryType, attributes| JournalEntry {
        entry_id,
        entry_type: entry_type as i32,
        attributes: Some(attributes),
        timestamp: Some(orcher_proto::prost_types::Timestamp {
            seconds: entry_id,
            nanos: 0,
        }),
        ..Default::default()
    };
    PollWorkflowExecutionResponse {
        workflow_id: "order-7".into(),
        execution_id: "run-1".into(),
        workflow_type: "checkout".into(),
        task_queue: "orders".into(),
        stream_entry_id: "act-1".into(),
        journal: vec![
            entry(
                1,
                EntryType::WorkflowExecutionStarted,
                Attributes::WorkflowExecutionStarted(Default::default()),
            ),
            entry(
                2,
                EntryType::TaskScheduled,
                Attributes::TaskScheduled(TaskScheduledEventAttributes {
                    task_id: "charge_0".into(),
                    task_type: "charge".into(),
                    ..Default::default()
                }),
            ),
            entry(
                3,
                EntryType::StepCompleted,
                Attributes::StepCompleted(StepCompletedEventAttributes {
                    step_name: "charge_0".into(),
                    result: br#""ok""#.to_vec(),
                    execution_attempt: 1,
                    ..Default::default()
                }),
            ),
        ],
        ..Default::default()
    }
    .encode_to_vec()
}

fn call(
    seq: u64,
    satisfied: bool,
    step_id: &str,
    task_type: &str,
) -> orcher_proto::kernel::v1::Command {
    use orcher_proto::kernel::v1::{command, Command, RunTask};
    Command {
        seq,
        satisfied,
        step_id: step_id.into(),
        what: Some(command::What::RunTask(RunTask {
            task_type: task_type.into(),
            ..Default::default()
        })),
        ..Default::default()
    }
}

fn answer(
    commands: Vec<orcher_proto::kernel::v1::Command>,
) -> orcher_proto::kernel::v1::RunActivationResult {
    use orcher_proto::kernel::v1::{run_activation_result, Commands, RunActivationResult};
    RunActivationResult {
        activation_id: b"act-1".to_vec(),
        outcome: Some(run_activation_result::Outcome::Commands(Commands {
            commands,
            ..Default::default()
        })),
        ..Default::default()
    }
}
