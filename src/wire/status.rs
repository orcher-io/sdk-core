//! A refused call's status and a failed connection, as the crate's errors.

use std::sync::Arc;

use crate::error::{Code, Error, Status, EXECUTION_ID_METADATA_KEY, WORKFLOW_ID_METADATA_KEY};

/// The error for a call the engine refused, its status restated by what
/// actually happened: a message over a size limit, or an answer from
/// something other than the engine.
pub(crate) fn error(status: tonic::Status) -> Error {
    if is_own_deadline(&status) {
        return Error::timeout("request");
    }
    Error::Status(self::status(crate::transport::clarify(status)))
}

/// Whether the call ran out of the deadline the client gave it: tonic ends
/// such a call itself, as CANCELLED "Timeout expired", whatever the engine
/// was doing.
fn is_own_deadline(status: &tonic::Status) -> bool {
    status.code() == tonic::Code::Cancelled && status.message().starts_with("Timeout expired")
}

/// The status as the crate's own.
pub(crate) fn status(status: tonic::Status) -> Status {
    let source = Cause::chain(std::error::Error::source(&status))
        .map(|cause| Arc::new(cause) as Arc<dyn std::error::Error + Send + Sync>);
    Status::new(code(status.code()), status.message()).with_source(source)
}

/// An error under a status, kept as the text of each error in its chain:
/// the originals belong to the transport and cannot be owned past it.
#[derive(Debug)]
struct Cause {
    text: String,
    next: Option<Box<Cause>>,
}

impl Cause {
    fn chain(error: Option<&(dyn std::error::Error + 'static)>) -> Option<Self> {
        let error = error?;
        Some(Self {
            text: error.to_string(),
            next: Self::chain(error.source()).map(Box::new),
        })
    }
}

impl std::fmt::Display for Cause {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(&self.text)
    }
}

impl std::error::Error for Cause {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.next.as_deref().map(|e| e as _)
    }
}

fn code(code: tonic::Code) -> Code {
    Code::from_i32(code as i32)
}

/// Maps a refused call about one workflow to a semantic error.
///
/// NOT_FOUND becomes [`Error::WorkflowNotFound`], ALREADY_EXISTS becomes
/// [`Error::WorkflowAlreadyExists`], and DEADLINE_EXCEEDED becomes
/// [`Error::Timeout`], so no SDK interprets an opaque status on its own.
///
/// The workflow id is a parameter rather than something parsed out of the
/// message. A caller asking about a specific workflow already knows which
/// one, and recovering it from prose would break as soon as the server
/// rewords the error.
///
/// Codes with no semantic equivalent stay a [`Error::Status`], so nothing is
/// lost or misfiled by guessing.
pub(crate) fn for_workflow(status: tonic::Status, workflow_id: impl Into<String>) -> Error {
    let message = status.message().to_string();
    match status.code() {
        tonic::Code::NotFound => Error::WorkflowNotFound {
            workflow_id: workflow_id.into(),
            run_id: None,
        },
        tonic::Code::AlreadyExists => {
            let named = |key: &str| {
                status
                    .metadata()
                    .get(key)
                    .and_then(|v| v.to_str().ok())
                    .filter(|v| !v.is_empty())
                    .map(str::to_string)
            };
            Error::WorkflowAlreadyExists {
                // The server's id takes precedence: a start that left the id
                // to the server has none of its own to report.
                workflow_id: named(WORKFLOW_ID_METADATA_KEY).unwrap_or_else(|| workflow_id.into()),
                run_id: named(EXECUTION_ID_METADATA_KEY),
            }
        }
        tonic::Code::DeadlineExceeded => Error::Timeout {
            operation: if message.is_empty() {
                "request".to_string()
            } else {
                message
            },
        },
        _ => error(status),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// Every code keeps its number across the conversion.
    #[test]
    fn every_code_keeps_its_number() {
        for n in 0..=16 {
            let tonic = tonic::Code::from_i32(n);
            assert_eq!(code(tonic).as_i32(), n, "{tonic:?}");
        }
        assert_eq!(Code::from_i32(99), Code::Unknown);
    }

    /// A status the client produced from a broken connection keeps the
    /// error underneath, for code that walks the chain.
    #[test]
    fn a_status_keeps_its_cause() {
        let cause = std::io::Error::new(std::io::ErrorKind::ConnectionReset, "reset by peer");
        let converted = status(tonic::Status::from_error(Box::new(cause)));
        assert_eq!(converted.code(), Code::Unknown);
        assert!(std::error::Error::source(&converted).is_some());
        assert!(
            crate::error::with_causes(&converted).contains("reset by peer"),
            "{converted}"
        );
    }

    #[test]
    fn not_found_becomes_a_workflow_not_found() {
        let err = for_workflow(
            tonic::Status::new(tonic::Code::NotFound, "Workflow execution not found: wf-1"),
            "wf-1",
        );
        assert!(
            matches!(err, Error::WorkflowNotFound { ref workflow_id, .. } if workflow_id == "wf-1")
        );
    }

    #[test]
    fn already_exists_becomes_a_workflow_already_exists() {
        let err = for_workflow(
            tonic::Status::new(tonic::Code::AlreadyExists, "duplicate"),
            "wf-2",
        );
        assert!(
            matches!(err, Error::WorkflowAlreadyExists { ref workflow_id, run_id: None } if workflow_id == "wf-2")
        );
    }

    /// The execution holding the id travels in trailing metadata. Dropping it
    /// leaves a caller that started a duplicate unable to reach the run that
    /// is already doing the work.
    #[test]
    fn already_exists_names_the_execution_in_the_way() {
        let mut metadata = tonic::metadata::MetadataMap::new();
        metadata.insert(EXECUTION_ID_METADATA_KEY, "run-7".parse().unwrap());
        metadata.insert(WORKFLOW_ID_METADATA_KEY, "order-7".parse().unwrap());
        let err = for_workflow(
            tonic::Status::with_metadata(tonic::Code::AlreadyExists, "running", metadata),
            "",
        );
        match err {
            Error::WorkflowAlreadyExists {
                workflow_id,
                run_id,
            } => {
                assert_eq!(workflow_id, "order-7");
                assert_eq!(run_id.as_deref(), Some("run-7"));
            }
            other => panic!("expected WorkflowAlreadyExists, got {other:?}"),
        }
    }

    #[test]
    fn deadline_exceeded_becomes_a_timeout() {
        let err = for_workflow(
            tonic::Status::new(tonic::Code::DeadlineExceeded, "took too long"),
            "wf-3",
        );
        assert!(matches!(err, Error::Timeout { .. }));
    }

    /// Codes with no semantic equivalent must stay opaque. Guessing would be
    /// worse than opacity: a misfiled error is harder to diagnose than an
    /// unclassified one.
    #[test]
    fn codes_without_a_meaning_stay_a_status() {
        for code in [
            tonic::Code::PermissionDenied,
            tonic::Code::InvalidArgument,
            tonic::Code::Internal,
            tonic::Code::Unavailable,
        ] {
            let err = for_workflow(tonic::Status::new(code, "x"), "wf-4");
            assert!(
                matches!(err, Error::Status(ref s) if s.code().as_i32() == code as i32),
                "{code:?} should not have been given a semantic meaning"
            );
        }
    }

    /// The id comes from the caller, never from the message. A reworded server
    /// error must not change which error type a caller sees.
    #[test]
    fn the_workflow_id_is_not_parsed_out_of_the_message() {
        let err = for_workflow(
            tonic::Status::new(tonic::Code::NotFound, "some entirely different wording"),
            "wf-5",
        );
        assert!(
            matches!(err, Error::WorkflowNotFound { ref workflow_id, .. } if workflow_id == "wf-5")
        );
    }

    /// Retryability follows the mapped meaning, not the status underneath.
    #[test]
    fn a_missing_workflow_is_not_retryable_after_mapping() {
        let err = for_workflow(tonic::Status::new(tonic::Code::NotFound, "gone"), "wf-6");
        assert!(!err.is_retryable());
    }
}
