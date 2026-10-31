//! The vectors are current, and the runner tells a runtime that answers
//! right from one that does not: it passes the reference runtime answering
//! with the recorded answers, and fails it answering wrong on purpose.
//!
//! The runner and the runtime are this crate's binary, run as a user would
//! run them: `orcher-verify vectors --runtime orcher-verify
//! reference-runtime`.

use std::path::Path;
use std::process::Command;

use orcher_verify::vectors::{self, Files};

const BIN: &str = env!("CARGO_BIN_EXE_orcher-verify");

fn vectors_dir() -> &'static Path {
    Path::new(concat!(env!("CARGO_MANIFEST_DIR"), "/vectors"))
}

/// `vectors/index.json` lists every answered activation recorded under
/// `vectors/replay/`; `ORCHER_BLESS_VECTORS=1` writes it again.
#[test]
fn the_index_lists_every_recorded_activation() {
    let index = vectors::index_of(vectors_dir()).expect("index the recordings");
    let path = vectors_dir().join("index.json");
    let written = serde_json::to_string_pretty(&index).expect("encode the index") + "\n";
    if std::env::var_os("ORCHER_BLESS_VECTORS").is_some_and(|v| !v.is_empty()) {
        std::fs::write(&path, written).expect("write the index");
        return;
    }
    let committed = std::fs::read_to_string(&path).expect("read vectors/index.json");
    assert!(
        committed == written,
        "vectors/index.json is stale: run `ORCHER_BLESS_VECTORS=1 cargo test -p orcher-verify` \
         and commit it"
    );
    for sdk in vectors::SDKS {
        assert!(
            index.vectors.iter().any(|vector| vector.sdk == sdk),
            "no vectors recorded from {sdk}"
        );
    }
}

/// What the binary has built in is what is on disk, and every vector reads.
#[test]
fn the_built_in_vectors_are_the_ones_on_disk() {
    let (embedded, from_embedded) = vectors::load(&Files::Embedded).expect("built-in vectors");
    let (on_disk, from_disk) =
        vectors::load(&Files::Dir(vectors_dir().to_path_buf())).expect("vectors on disk");
    assert_eq!(embedded, on_disk);
    assert_eq!(from_embedded.len(), from_disk.len());
    assert!(!from_disk.is_empty());
}

/// Runs the runner against the reference runtime answering with `answers`;
/// the exit code's success and the runner's output.
fn verify(answers: &str) -> (bool, String) {
    let output = Command::new(BIN)
        .args([
            "vectors",
            "--runtime",
            BIN,
            "reference-runtime",
            "--answers",
        ])
        .arg(answers)
        .output()
        .expect("run orcher-verify");
    let stdout = String::from_utf8_lossy(&output.stdout).into_owned();
    eprintln!("{stdout}{}", String::from_utf8_lossy(&output.stderr));
    (output.status.success(), stdout)
}

fn count(output: &str, prefix: &str) -> usize {
    output
        .lines()
        .filter(|line| line.starts_with(prefix))
        .count()
}

#[test]
fn a_runtime_answering_with_the_recorded_answers_passes() {
    let (_, vectors) = vectors::load(&Files::Embedded).expect("built-in vectors");
    let (ok, output) = verify("recorded");
    assert!(ok, "the runner failed the reference runtime:\n{output}");
    assert_eq!(count(&output, "ok    "), vectors.len());
    assert_eq!(count(&output, "FAIL"), 0);
    assert!(output.ends_with(&format!(
        "{n} vectors: {n} passed, 0 failed\n",
        n = vectors.len()
    )));
}

/// Every recorded answer issues a command, so dropping the last one fails
/// every vector: an answer left with none is refused, and one with fewer
/// commands than were recorded differs.
#[test]
fn a_runtime_that_drops_a_command_fails() {
    let (ok, output) = verify("drop-command");
    assert!(
        !ok,
        "the runner passed a runtime that drops commands:\n{output}"
    );
    assert_eq!(count(&output, "ok    "), 0, "{output}");
    assert!(output.contains("commands recorded,"), "{output}");
}

/// A task called by another type fails every vector whose answer schedules
/// a task or whose history holds one, and only those.
#[test]
fn a_runtime_that_calls_the_wrong_task_fails() {
    let (ok, output) = verify("change-task-type");
    assert!(
        !ok,
        "the runner passed a runtime calling the wrong tasks:\n{output}"
    );
    assert!(count(&output, "FAIL") > 0, "{output}");
    assert!(output.contains("-changed"), "{output}");
}

/// Code that no longer reaches the steps its history recorded fails the
/// history check before anything is compared.
#[test]
fn a_runtime_that_leaves_its_history_behind_fails() {
    let (ok, output) = verify("forget-history");
    assert!(
        !ok,
        "the runner passed a runtime that forgets its history:\n{output}"
    );
    assert!(
        output.contains("the answer does not replay the history"),
        "{output}"
    );
}

/// A runtime that is not one fails every vector rather than hanging or
/// passing.
#[test]
fn a_runtime_that_answers_nothing_fails() {
    let output = Command::new(BIN)
        .args(["vectors", "--timeout", "5", "--runtime", "true"])
        .output()
        .expect("run orcher-verify");
    let stdout = String::from_utf8_lossy(&output.stdout);
    let (_, vectors) = vectors::load(&Files::Embedded).expect("built-in vectors");
    assert!(!output.status.success(), "{stdout}");
    assert_eq!(count(&stdout, "ok    "), 0, "{stdout}");
    assert_eq!(count(&stdout, "FAIL  "), vectors.len(), "{stdout}");
}
