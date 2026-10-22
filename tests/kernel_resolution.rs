//! The kernel resolves every recorded activation as it is expected to.
//!
//! `tests/fixtures/replay/<sdk>/resolutions.json` holds, for each activation
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

use orcher_sdk_core::kernel::resolve;
use orcher_sdk_core::proto::kernel::v1::RunActivation;
use orcher_sdk_core::proto::prost::Message;
use orcher_sdk_core::worker::record::{read_runs, Recorded};
use orcher_sdk_core::WorkflowExecutionTask;

const NAMESPACE: &str = "contract";

#[test]
fn the_kernel_resolves_every_committed_recording_as_expected() {
    let fixtures = Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/replay");
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
            Recorded::Activation(response) => Some(response),
            _ => None,
        });
        for (n, response) in activations.enumerate() {
            let task = WorkflowExecutionTask::from(response.clone());
            out.insert(
                format!("{}/{}/{n}", run.workflow_id, run.run_id),
                resolve(&task, NAMESPACE),
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
