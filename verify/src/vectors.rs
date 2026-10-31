//! The replay vectors.
//!
//! `vectors/replay/<sdk>/` holds runs recorded from that SDK's contract
//! suite, as `orcher_sdk_core::worker::record` writes them:
//! `<workflow id>/<run id>/<order>-<kind>.pb`, each file one engine message
//! (an activation, or the completion or failure the worker answered it
//! with). Beside them, `resolutions.json` holds the `RunActivation` the
//! kernel resolves each recorded activation to, hex-encoded, keyed
//! `<workflow id>/<run id>/<n>` for the run's `n`th activation; sdk-core's
//! `tests/kernel_resolution.rs` fails when the kernel resolves one
//! differently.
//!
//! `vectors/index.json` ([`Index`]) lists every vector, one per recorded
//! activation the worker answered, with the files it is made of, so a reader
//! in any language needs no rule but the index. It is generated from the
//! recordings ([`index_of`]); this crate's tests fail when it is stale, and
//! `ORCHER_BLESS_VECTORS=1 cargo test -p orcher-verify` writes it again.

use std::collections::BTreeMap;
use std::io;
use std::path::{Path, PathBuf};

use orcher_proto::kernel::v1::RunActivation;
use orcher_proto::prost::Message;
use orcher_proto::PollWorkflowExecutionResponse;
use orcher_sdk_core::worker::record::Recorded;
use serde::{Deserialize, Serialize};

/// The index's format. A change a reader must know of is a new number.
pub const FORMAT: u32 = 1;

/// The namespace the runs were recorded in, and the one the answers are
/// judged in.
pub const NAMESPACE: &str = "contract";

/// The SDKs whose contract suites were recorded, each a directory under
/// `vectors/replay/`, in the order the index lists them.
pub const SDKS: [&str; 3] = ["rust", "python", "typescript"];

/// `vectors/index.json`: every vector.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Index {
    /// [`FORMAT`].
    pub format: u32,
    /// [`NAMESPACE`].
    pub namespace: String,
    /// The vectors, by SDK, then workflow id, run id and activation.
    pub vectors: Vec<Entry>,
}

/// One vector in the index. Every path is relative to `vectors/`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Entry {
    /// `<sdk>/<workflow id>/<run id>/<n>`: unique, and stable while the
    /// recordings are.
    pub name: String,
    /// The SDK whose contract suite recorded it.
    pub sdk: String,
    /// The workflow the activation runs: the contract suite's name for it.
    pub workflow_type: String,
    /// The run's workflow id.
    pub workflow_id: String,
    /// The run's id.
    pub run_id: String,
    /// The activation's place among the run's activations, from 0.
    pub activation: usize,
    /// Where the `RunActivation` the runtime is handed is.
    pub run_activation: ResolutionRef,
    /// The recorded activation: an encoded
    /// `orcher.v1.PollWorkflowExecutionResponse`, the history the answer is
    /// checked against.
    pub recorded_activation: String,
    /// The answer the worker sent the engine after the activation.
    pub expected: ExpectedRef,
}

/// A `RunActivation` in a `resolutions.json`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ResolutionRef {
    /// The `resolutions.json`.
    pub file: String,
    /// Its key there, whose value is the encoded `RunActivation` in hex.
    pub key: String,
}

/// The recorded answer to an activation.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ExpectedRef {
    /// What the worker sent.
    pub kind: AnswerKind,
    /// The encoded message.
    pub file: String,
}

/// What the worker sent the engine for an activation.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "lowercase")]
pub enum AnswerKind {
    /// The commands it answered with: an encoded
    /// `orcher.v1.CompleteWorkflowExecutionRequest`.
    Completion,
    /// The workflow's code failed it: an encoded
    /// `orcher.v1.FailWorkflowExecutionRequest`.
    Failure,
}

/// A vector, read.
#[derive(Debug, Clone)]
pub struct Vector {
    /// [`Entry::name`].
    pub name: String,
    /// [`Entry::sdk`].
    pub sdk: String,
    /// [`Entry::workflow_type`].
    pub workflow_type: String,
    /// The encoded `orcher.kernel.v1.RunActivation` the runtime is handed.
    pub run_activation: Vec<u8>,
    /// Its `activation_id`, which the answer echoes.
    pub activation_id: Vec<u8>,
    /// The recorded activation, an encoded
    /// `orcher.v1.PollWorkflowExecutionResponse`.
    pub recorded_activation: Vec<u8>,
    /// The answer the worker sent after it.
    pub expected: Recorded,
}

/// Where the vectors are read from.
#[derive(Debug, Clone)]
pub enum Files {
    /// The vectors built into this binary.
    Embedded,
    /// A `vectors/` directory.
    Dir(PathBuf),
}

impl Files {
    fn read(&self, name: &str) -> io::Result<Vec<u8>> {
        match self {
            Files::Embedded => crate::embedded::FILES
                .iter()
                .find(|(path, _)| *path == name)
                .map(|(_, bytes)| bytes.to_vec())
                .ok_or_else(|| invalid(format!("{name}: no such vector file"))),
            Files::Dir(dir) => std::fs::read(dir.join(name)),
        }
    }
}

/// Reads the index and every vector it lists.
///
/// # Errors
///
/// The error of a file that cannot be read, and
/// [`io::ErrorKind::InvalidData`] for an index of another format or a file
/// that is not what the index says it is.
pub fn load(files: &Files) -> io::Result<(Index, Vec<Vector>)> {
    let index: Index = serde_json::from_slice(&files.read("index.json")?)
        .map_err(|e| invalid(format!("index.json: {e}")))?;
    if index.format != FORMAT {
        return Err(invalid(format!(
            "index.json is format {}; this orcher-verify reads format {FORMAT}",
            index.format
        )));
    }
    let mut resolutions: BTreeMap<String, BTreeMap<String, String>> = BTreeMap::new();
    let mut vectors = Vec::with_capacity(index.vectors.len());
    for entry in &index.vectors {
        let file = &entry.run_activation.file;
        if !resolutions.contains_key(file) {
            let parsed = serde_json::from_slice(&files.read(file)?)
                .map_err(|e| invalid(format!("{file}: {e}")))?;
            resolutions.insert(file.clone(), parsed);
        }
        let key = &entry.run_activation.key;
        let run_activation = resolutions[file]
            .get(key)
            .ok_or_else(|| invalid(format!("{file}: no resolution {key:?}")))
            .and_then(|hex| unhex(hex).ok_or_else(|| invalid(format!("{file}: {key}: not hex"))))?;
        let activation_id = RunActivation::decode(run_activation.as_slice())
            .map_err(|e| invalid(format!("{file}: {key}: not a RunActivation: {e}")))?
            .activation_id;
        let expected = files.read(&entry.expected.file)?;
        vectors.push(Vector {
            name: entry.name.clone(),
            sdk: entry.sdk.clone(),
            workflow_type: entry.workflow_type.clone(),
            run_activation,
            activation_id,
            recorded_activation: files.read(&entry.recorded_activation)?,
            expected: match entry.expected.kind {
                AnswerKind::Completion => Recorded::Completion(expected),
                AnswerKind::Failure => Recorded::Failure(expected),
            },
        });
    }
    Ok((index, vectors))
}

/// The index of the recordings under `dir` (a `vectors/` directory): every
/// recorded activation the worker answered, in the order [`Index::vectors`]
/// keeps.
///
/// # Errors
///
/// The error of a directory or file that cannot be read, and
/// [`io::ErrorKind::InvalidData`] for a file that is not a recorded message.
pub fn index_of(dir: &Path) -> io::Result<Index> {
    let mut vectors = Vec::new();
    for sdk in SDKS {
        let sdk_dir = dir.join("replay").join(sdk);
        let resolutions = format!("replay/{sdk}/resolutions.json");
        for workflow in sorted_dirs(&sdk_dir)? {
            for run in sorted_dirs(&workflow)? {
                let (workflow_id, run_id) = (name_of(&workflow), name_of(&run));
                let mut files: Vec<String> = std::fs::read_dir(&run)?
                    .filter_map(|entry| entry.ok().map(|entry| name_of(&entry.path())))
                    .filter(|name| name.ends_with(".pb"))
                    .collect();
                files.sort();
                let path = |file: &str| format!("replay/{sdk}/{workflow_id}/{run_id}/{file}");
                let mut activation = 0;
                for (n, file) in files.iter().enumerate() {
                    if kind_of(file)? != "activation" {
                        continue;
                    }
                    // An activation the worker did not answer (it handed it
                    // back) has no answer to compare with.
                    let answer = match files.get(n + 1) {
                        Some(next) => match kind_of(next)? {
                            "completion" => Some((next, AnswerKind::Completion)),
                            "failure" => Some((next, AnswerKind::Failure)),
                            _ => None,
                        },
                        None => None,
                    };
                    if let Some((answer, kind)) = answer {
                        let recorded = std::fs::read(run.join(file))?;
                        let workflow_type =
                            PollWorkflowExecutionResponse::decode(recorded.as_slice())
                                .map_err(|e| invalid(format!("{}: {e}", path(file))))?
                                .workflow_type;
                        vectors.push(Entry {
                            name: format!("{sdk}/{workflow_id}/{run_id}/{activation}"),
                            sdk: sdk.to_string(),
                            workflow_type,
                            workflow_id: workflow_id.clone(),
                            run_id: run_id.clone(),
                            activation,
                            run_activation: ResolutionRef {
                                file: resolutions.clone(),
                                key: format!("{workflow_id}/{run_id}/{activation}"),
                            },
                            recorded_activation: path(file),
                            expected: ExpectedRef {
                                kind,
                                file: path(answer),
                            },
                        });
                    }
                    activation += 1;
                }
            }
        }
    }
    Ok(Index {
        format: FORMAT,
        namespace: NAMESPACE.to_string(),
        vectors,
    })
}

/// The kind of a recorded message's file, `<order>-<kind>.pb`.
fn kind_of(file: &str) -> io::Result<&str> {
    match file.trim_end_matches(".pb").rsplit_once('-') {
        Some((_, kind @ ("activation" | "completion" | "failure"))) => Ok(kind),
        _ => Err(invalid(format!("{file}: not a recorded message"))),
    }
}

fn sorted_dirs(dir: &Path) -> io::Result<Vec<PathBuf>> {
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

/// `None` for text that is not hex, an odd number of digits included.
fn unhex(text: &str) -> Option<Vec<u8>> {
    (0..text.len())
        .step_by(2)
        .map(|i| u8::from_str_radix(text.get(i..i + 2)?, 16).ok())
        .collect()
}

fn invalid(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidData, message)
}
