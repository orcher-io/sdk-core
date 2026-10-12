//! Recording workflow activations, to replay a run without an engine.
//!
//! With [`RECORD_ACTIVATIONS_ENV`] set to a directory, a worker writes every
//! workflow activation the engine sends it, and every answer it gives, as the
//! protobuf messages that crossed the wire. Nothing else changes. A recorded
//! run can then be fed back to the kernel, which is how replay and
//! determinism are checked against real histories (see
//! `docs/kernel-protocol-v1.md`, "Conformance").
//!
//! Recordings hold the runs' inputs and results. Turn recording on only where
//! that data may be written to disk, and only for as long as it is needed:
//! each activation costs a file write.
//!
//! Layout: `<dir>/<workflow id>/<run id>/<order>-<kind>.pb`, where `<order>`
//! sorts the run's messages in the order they happened and `<kind>` is
//! `activation`, `completion` or `failure`.

use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::OnceLock;

use prost::Message;

use crate::proto::orcher::v1::{
    CompleteWorkflowExecutionRequest, FailWorkflowExecutionRequest, PollWorkflowExecutionResponse,
};

/// The environment variable that turns recording on: the directory to
/// record into. Unset or empty: nothing is recorded.
pub const RECORD_ACTIVATIONS_ENV: &str = "ORCHER_RECORD_ACTIVATIONS";

/// One message of a recorded run.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub enum Recorded {
    /// An activation the engine sent.
    Activation(PollWorkflowExecutionResponse),
    /// The commands the worker answered an activation with.
    Completion(CompleteWorkflowExecutionRequest),
    /// The worker reporting it could not run an activation.
    Failure(FailWorkflowExecutionRequest),
}

/// A recorded run: its messages, in the order they happened.
#[derive(Debug, Clone, PartialEq)]
#[non_exhaustive]
pub struct RecordedRun {
    /// The run's workflow id.
    pub workflow_id: String,
    /// The run's id.
    pub run_id: String,
    /// The run's messages, oldest first.
    pub messages: Vec<Recorded>,
}

/// Reads every run recorded under `dir`, ordered by workflow and run id.
///
/// # Errors
///
/// Returns the I/O error if the directory cannot be read, or an
/// [`std::io::ErrorKind::InvalidData`] error naming a file that is not a
/// recorded message.
pub fn read_runs(dir: impl AsRef<Path>) -> std::io::Result<Vec<RecordedRun>> {
    let mut runs = Vec::new();
    for workflow in sorted_dirs(dir.as_ref())? {
        for run in sorted_dirs(&workflow)? {
            let mut files: Vec<PathBuf> = std::fs::read_dir(&run)?
                .filter_map(|entry| entry.ok().map(|entry| entry.path()))
                .filter(|path| path.extension().is_some_and(|ext| ext == "pb"))
                .collect();
            files.sort();
            let messages = files
                .iter()
                .map(|file| read_message(file))
                .collect::<std::io::Result<Vec<_>>>()?;
            runs.push(RecordedRun {
                workflow_id: name_of(&workflow),
                run_id: name_of(&run),
                messages,
            });
        }
    }
    Ok(runs)
}

fn sorted_dirs(dir: &Path) -> std::io::Result<Vec<PathBuf>> {
    let mut dirs: Vec<PathBuf> = std::fs::read_dir(dir)?
        .filter_map(|entry| entry.ok().map(|entry| entry.path()))
        .filter(|path| path.is_dir())
        .collect();
    dirs.sort();
    Ok(dirs)
}

fn name_of(path: &Path) -> String {
    path.file_name()
        .map(|name| name.to_string_lossy().into_owned())
        .unwrap_or_default()
}

fn read_message(file: &Path) -> std::io::Result<Recorded> {
    let invalid = |why: String| {
        std::io::Error::new(
            std::io::ErrorKind::InvalidData,
            format!("{}: {why}", file.display()),
        )
    };
    let bytes = std::fs::read(file)?;
    let stem = file
        .file_stem()
        .map(|stem| stem.to_string_lossy().into_owned())
        .unwrap_or_default();
    let kind = stem.rsplit_once('-').map(|(_, kind)| kind).unwrap_or("");
    let decoded = match kind {
        "activation" => {
            PollWorkflowExecutionResponse::decode(bytes.as_slice()).map(Recorded::Activation)
        }
        "completion" => {
            CompleteWorkflowExecutionRequest::decode(bytes.as_slice()).map(Recorded::Completion)
        }
        "failure" => FailWorkflowExecutionRequest::decode(bytes.as_slice()).map(Recorded::Failure),
        other => return Err(invalid(format!("not a recorded message kind: {other:?}"))),
    };
    decoded.map_err(|e| invalid(e.to_string()))
}

/// Where recordings go, read once from the environment.
fn recording_dir() -> Option<&'static Path> {
    static DIR: OnceLock<Option<PathBuf>> = OnceLock::new();
    DIR.get_or_init(|| {
        let dir = std::env::var_os(RECORD_ACTIVATIONS_ENV)?;
        if dir.is_empty() {
            return None;
        }
        let dir = PathBuf::from(dir);
        tracing::warn!(
            dir = %dir.display(),
            "Recording every workflow activation and answer, with their payloads ({RECORD_ACTIVATIONS_ENV})"
        );
        Some(dir)
    })
    .as_deref()
}

/// Records an activation the engine sent, when recording is on.
pub(crate) fn activation(response: &PollWorkflowExecutionResponse) {
    write(
        &response.workflow_id,
        &response.execution_id,
        "activation",
        response,
    );
}

/// Records the commands the worker answers an activation with, when
/// recording is on.
pub(crate) fn completion(request: &CompleteWorkflowExecutionRequest) {
    write(
        &request.workflow_id,
        &request.execution_id,
        "completion",
        request,
    );
}

/// Records the worker reporting it could not run an activation, when
/// recording is on.
pub(crate) fn failure(request: &FailWorkflowExecutionRequest) {
    write(
        &request.workflow_id,
        &request.execution_id,
        "failure",
        request,
    );
}

fn write(workflow_id: &str, run_id: &str, kind: &str, message: &impl Message) {
    let Some(dir) = recording_dir() else {
        return;
    };
    // Sorts the run's messages in the order they were written, across
    // threads and restarts of the process.
    static SEQUENCE: AtomicU64 = AtomicU64::new(0);
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|since| since.as_nanos())
        .unwrap_or_default();
    let sequence = SEQUENCE.fetch_add(1, Ordering::Relaxed);
    let run_dir = dir.join(path_safe(workflow_id)).join(path_safe(run_id));
    let file = run_dir.join(format!("{nanos:020}{sequence:08}-{kind}.pb"));
    let written = std::fs::create_dir_all(&run_dir)
        .and_then(|()| std::fs::write(&file, message.encode_to_vec()));
    if let Err(error) = written {
        tracing::warn!(file = %file.display(), %error, "Could not record a workflow activation message");
    }
}

/// An id as a single path component: anything but letters, digits, `-`, `_`
/// and `.` becomes `_`, so an id can never name a path outside the
/// recording directory.
fn path_safe(id: &str) -> String {
    let safe: String = id
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() || matches!(c, '-' | '_' | '.') {
                c
            } else {
                '_'
            }
        })
        .collect();
    match safe.as_str() {
        "" | "." | ".." => format!("_{safe}"),
        _ => safe,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_id_never_escapes_the_recording_directory() {
        assert_eq!(path_safe("order-7"), "order-7");
        assert_eq!(path_safe("../../etc/passwd"), ".._.._etc_passwd");
        assert_eq!(path_safe(".."), "_..");
        assert_eq!(path_safe(""), "_");
        assert_eq!(path_safe("a/b\\c"), "a_b_c");
    }

    /// What `write` puts on disk, `read_runs` reads back, in order.
    #[test]
    fn a_recorded_run_reads_back_in_order() {
        let dir = std::env::temp_dir().join(format!("orcher-record-{}", uuid::Uuid::new_v4()));
        let run_dir = dir.join("order-7").join("run-1");
        std::fs::create_dir_all(&run_dir).unwrap();
        let activation = PollWorkflowExecutionResponse {
            workflow_id: "order-7".into(),
            execution_id: "run-1".into(),
            workflow_type: "checkout".into(),
            ..Default::default()
        };
        let completion = CompleteWorkflowExecutionRequest {
            workflow_id: "order-7".into(),
            execution_id: "run-1".into(),
            ..Default::default()
        };
        std::fs::write(
            run_dir.join("0001-activation.pb"),
            activation.encode_to_vec(),
        )
        .unwrap();
        std::fs::write(
            run_dir.join("0002-completion.pb"),
            completion.encode_to_vec(),
        )
        .unwrap();

        let runs = read_runs(&dir).unwrap();
        std::fs::remove_dir_all(&dir).unwrap();

        assert_eq!(runs.len(), 1);
        assert_eq!(runs[0].workflow_id, "order-7");
        assert_eq!(runs[0].run_id, "run-1");
        assert_eq!(
            runs[0].messages,
            vec![
                Recorded::Activation(activation),
                Recorded::Completion(completion)
            ]
        );
    }

    #[test]
    fn a_file_of_another_kind_is_an_error_not_skipped() {
        let dir = std::env::temp_dir().join(format!("orcher-record-{}", uuid::Uuid::new_v4()));
        let run_dir = dir.join("wf").join("run");
        std::fs::create_dir_all(&run_dir).unwrap();
        std::fs::write(run_dir.join("0001-mystery.pb"), b"").unwrap();
        let error = read_runs(&dir).unwrap_err();
        std::fs::remove_dir_all(&dir).unwrap();
        assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
    }
}
