//! Replays recorded runs (see `crate::worker::record`).
//!
//! `tests/fixtures/replay` holds runs recorded from each SDK's contract
//! suite (worker identities replaced), and is always replayed. Set
//! `ORCHER_REPLAY_CORPUS` to a directory recorded with
//! `ORCHER_RECORD_ACTIVATIONS` to replay a larger corpus as well.
//!
//! Each recorded activation decodes and replays through the replayer that
//! checks every activation before the language runs it, without a critical
//! violation. What the kernel resolves them to is checked by
//! `tests/kernel_resolution.rs`.

use std::collections::BTreeMap;

use crate::proto::orcher::v1::EntryType;
use crate::state::replayer::{ReplayConfig, Replayer};
use crate::worker::record::{read_runs, Recorded};
use crate::worker::WorkflowExecutionTask;

/// The committed runs, one directory per SDK.
#[test]
fn every_committed_recording_replays() {
    let fixtures = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/fixtures/replay");
    for sdk in ["rust", "python", "typescript"] {
        replay_all(&fixtures.join(sdk));
    }
}

/// A corpus outside the repository, when one is given.
#[test]
fn every_recording_in_the_given_corpus_replays() {
    let Some(dir) = std::env::var_os("ORCHER_REPLAY_CORPUS").filter(|dir| !dir.is_empty()) else {
        eprintln!("ORCHER_REPLAY_CORPUS is not set: no outside corpus replayed");
        return;
    };
    for sdk in ["rust", "python", "typescript"] {
        let dir = std::path::Path::new(&dir).join(sdk);
        if dir.is_dir() {
            replay_all(&dir);
        }
    }
}

fn replay_all(dir: &std::path::Path) {
    let runs = read_runs(dir).expect("read the recorded runs");
    assert!(!runs.is_empty(), "{} holds no runs", dir.display());

    let mut activations = 0usize;
    let mut answers = 0usize;
    let mut entry_types: BTreeMap<String, usize> = BTreeMap::new();
    let mut workflow_types: BTreeMap<String, usize> = BTreeMap::new();
    let mut problems = Vec::new();

    for run in &runs {
        for message in &run.messages {
            match message {
                Recorded::Activation(response) => {
                    activations += 1;
                    *workflow_types
                        .entry(response.workflow_type.clone())
                        .or_default() += 1;
                    for entry in &response.journal {
                        let name = EntryType::try_from(entry.entry_type)
                            .map(|kind| kind.as_str_name().to_string())
                            .unwrap_or_else(|_| format!("unknown({})", entry.entry_type));
                        *entry_types.entry(name).or_default() += 1;
                    }
                    let task = WorkflowExecutionTask::from(response.clone());
                    let mut replayer = Replayer::new(ReplayConfig::default());
                    match replayer.replay(task.execution.clone(), task.journal.clone()) {
                        Ok(result) if result.has_critical_violations() => problems.push(format!(
                            "{}/{}: critical violations: {:?}",
                            run.workflow_id, run.run_id, result.violations
                        )),
                        Ok(_) => {}
                        Err(error) => problems.push(format!(
                            "{}/{}: does not replay: {error}",
                            run.workflow_id, run.run_id
                        )),
                    }
                }
                Recorded::Completion(_) | Recorded::Failure(_) => answers += 1,
            }
        }
    }

    eprintln!(
        "{}: replayed {} runs: {activations} activations, {answers} answers",
        dir.display(),
        runs.len()
    );
    eprintln!("workflow types: {workflow_types:?}");
    eprintln!("journal entry types: {entry_types:?}");
    assert!(
        problems.is_empty(),
        "{} of {activations} recorded activations failed:\n{}",
        problems.len(),
        problems.join("\n")
    );
}
