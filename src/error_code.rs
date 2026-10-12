//! Machine-readable codes for sdk-core's errors.
//!
//! Every language SDK used to work out a code from a core error itself, each
//! its own way (the TypeScript bridge tagged messages with a `[CODE]` prefix
//! its JavaScript side then parsed back out). [`Error::code`] is that mapping,
//! once. A code is derived from the error's variant, never from its text, and
//! an error with no distinct meaning is [`ErrorCode::Unknown`] rather than a
//! code that would mislead a caller matching on it.
//!
//! [`ErrorCode::as_str`] spells each code as the TypeScript SDK's error codes
//! already do. Against the TypeScript bridge's mapping, two codes are more
//! precise and none is otherwise changed: an error with added context reports
//! the code of the error it wraps, and [`Error::ResourceExhausted`] reports
//! [`ErrorCode::ResourceExhausted`]; both were `UNKNOWN`.

use crate::error::Error;
use crate::types::WorkflowStatus;

/// What an error means, for a program to act on.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ErrorCode {
    /// The workflow does not exist.
    WorkflowNotFound,
    /// A workflow with this id is already open.
    WorkflowAlreadyExists,
    /// The workflow failed.
    WorkflowExecutionFailed,
    /// The workflow was cancelled.
    WorkflowCanceled,
    /// The workflow was terminated.
    WorkflowTerminated,
    /// The workflow timed out.
    WorkflowTimeout,
    /// An operation ran out of time.
    Timeout,
    /// The engine could not be reached.
    ConnectionFailed,
    /// The caller is not authenticated.
    Unauthenticated,
    /// The caller may not do this.
    PermissionDenied,
    /// The request or configuration is not valid.
    InvalidArgument,
    /// The engine has no such thing.
    NotFound,
    /// The thing already exists.
    AlreadyExists,
    /// The engine is unavailable for now; trying again may succeed.
    Unavailable,
    /// A rate limit or quota was hit.
    ResourceExhausted,
    /// The system is not in a state that allows this.
    FailedPrecondition,
    /// The operation was aborted, typically by a conflict.
    Aborted,
    /// A value is outside its allowed range (a message over a size limit, say).
    OutOfRange,
    /// The engine does not implement this.
    Unimplemented,
    /// Data was lost or corrupted.
    DataLoss,
    /// An error inside the SDK or the engine.
    Internal,
    /// No more specific code applies.
    Unknown,
}

impl ErrorCode {
    /// The code as SDKs spell it, for example `WORKFLOW_NOT_FOUND`.
    pub fn as_str(self) -> &'static str {
        match self {
            Self::WorkflowNotFound => "WORKFLOW_NOT_FOUND",
            Self::WorkflowAlreadyExists => "WORKFLOW_ALREADY_EXISTS",
            Self::WorkflowExecutionFailed => "WORKFLOW_EXECUTION_FAILED",
            Self::WorkflowCanceled => "WORKFLOW_CANCELED",
            Self::WorkflowTerminated => "WORKFLOW_TERMINATED",
            Self::WorkflowTimeout => "WORKFLOW_TIMEOUT",
            Self::Timeout => "TIMEOUT",
            Self::ConnectionFailed => "CONNECTION_FAILED",
            Self::Unauthenticated => "UNAUTHENTICATED",
            Self::PermissionDenied => "PERMISSION_DENIED",
            Self::InvalidArgument => "INVALID_ARGUMENT",
            Self::NotFound => "NOT_FOUND",
            Self::AlreadyExists => "ALREADY_EXISTS",
            Self::Unavailable => "UNAVAILABLE",
            Self::ResourceExhausted => "RESOURCE_EXHAUSTED",
            Self::FailedPrecondition => "FAILED_PRECONDITION",
            Self::Aborted => "ABORTED",
            Self::OutOfRange => "OUT_OF_RANGE",
            Self::Unimplemented => "UNIMPLEMENTED",
            Self::DataLoss => "DATA_LOSS",
            Self::Internal => "INTERNAL",
            Self::Unknown => "UNKNOWN",
        }
    }
}

impl std::fmt::Display for ErrorCode {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

impl Error {
    /// What this error means, from its variant.
    ///
    /// A [`Error::WorkflowExecutionFailed`] is [`ErrorCode::Unknown`] here,
    /// because the same variant reports an actor operation's failure; use
    /// [`Error::result_code`] for an error from waiting on a workflow's result.
    pub fn code(&self) -> ErrorCode {
        match self {
            Error::WorkflowNotFound { .. } => ErrorCode::WorkflowNotFound,
            Error::WorkflowAlreadyExists { .. } => ErrorCode::WorkflowAlreadyExists,
            Error::WorkflowCancelled { .. } => ErrorCode::WorkflowCanceled,
            Error::WorkflowTerminated { .. } => ErrorCode::WorkflowTerminated,
            Error::Timeout { .. } => ErrorCode::Timeout,
            Error::Transport(_) | Error::Connection(_) => ErrorCode::ConnectionFailed,
            Error::Authentication(_) => ErrorCode::Unauthenticated,
            Error::Authorization(_) => ErrorCode::PermissionDenied,
            Error::Configuration(_) | Error::InvalidPayload { .. } => ErrorCode::InvalidArgument,
            Error::Serialization(_) | Error::Deserialization(_) | Error::Codec(_) => {
                ErrorCode::Internal
            }
            Error::ResourceExhausted { .. } => ErrorCode::ResourceExhausted,
            // The context is for people; the code is the wrapped error's.
            Error::WithContext { source, .. } => source.code(),
            Error::GrpcStatus(status) => match status.code() {
                tonic::Code::NotFound => ErrorCode::NotFound,
                tonic::Code::AlreadyExists => ErrorCode::AlreadyExists,
                tonic::Code::DeadlineExceeded => ErrorCode::Timeout,
                tonic::Code::Unavailable => ErrorCode::Unavailable,
                tonic::Code::PermissionDenied => ErrorCode::PermissionDenied,
                tonic::Code::Unauthenticated => ErrorCode::Unauthenticated,
                tonic::Code::InvalidArgument => ErrorCode::InvalidArgument,
                tonic::Code::ResourceExhausted => ErrorCode::ResourceExhausted,
                tonic::Code::FailedPrecondition => ErrorCode::FailedPrecondition,
                tonic::Code::Aborted => ErrorCode::Aborted,
                tonic::Code::OutOfRange => ErrorCode::OutOfRange,
                tonic::Code::Unimplemented => ErrorCode::Unimplemented,
                tonic::Code::DataLoss => ErrorCode::DataLoss,
                _ => ErrorCode::Internal,
            },
            _ => ErrorCode::Unknown,
        }
    }

    /// What this error means, for an error from waiting on a workflow's
    /// result: how the run ended, when it ended without a result.
    pub fn result_code(&self) -> ErrorCode {
        match self {
            Error::WorkflowExecutionFailed { status, .. } => match status {
                WorkflowStatus::Cancelled => ErrorCode::WorkflowCanceled,
                WorkflowStatus::Terminated => ErrorCode::WorkflowTerminated,
                WorkflowStatus::TimedOut => ErrorCode::WorkflowTimeout,
                _ => ErrorCode::WorkflowExecutionFailed,
            },
            Error::WithContext { source, .. } => source.result_code(),
            other => other.code(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The codes the TypeScript bridge tagged errors with before this
    /// mapping moved here, which switching to it keeps.
    #[test]
    fn the_codes_are_the_ones_the_typescript_sdk_already_reads() {
        let cases: Vec<(Error, &str)> = vec![
            (Error::workflow_not_found("wf"), "WORKFLOW_NOT_FOUND"),
            (
                Error::Timeout {
                    operation: "x".into(),
                },
                "TIMEOUT",
            ),
            (Error::Connection("down".into()), "CONNECTION_FAILED"),
            (Error::Authentication("no".into()), "UNAUTHENTICATED"),
            (Error::Authorization("no".into()), "PERMISSION_DENIED"),
            (Error::Configuration("bad".into()), "INVALID_ARGUMENT"),
            (Error::Serialization("bad".into()), "INTERNAL"),
            (
                Error::GrpcStatus(tonic::Status::not_found("x")),
                "NOT_FOUND",
            ),
            (
                Error::GrpcStatus(tonic::Status::unavailable("x")),
                "UNAVAILABLE",
            ),
            (Error::GrpcStatus(tonic::Status::cancelled("x")), "INTERNAL"),
            (Error::Internal("x".into()), "UNKNOWN"),
            (Error::workflow_execution_failed("x"), "UNKNOWN"),
        ];
        for (error, code) in cases {
            assert_eq!(error.code().as_str(), code, "{error:?}");
        }
    }

    #[test]
    fn a_result_error_says_how_the_run_ended() {
        for (status, code) in [
            (WorkflowStatus::Failed, ErrorCode::WorkflowExecutionFailed),
            (WorkflowStatus::Cancelled, ErrorCode::WorkflowCanceled),
            (WorkflowStatus::Terminated, ErrorCode::WorkflowTerminated),
            (WorkflowStatus::TimedOut, ErrorCode::WorkflowTimeout),
        ] {
            assert_eq!(Error::workflow_ended(status, "x").result_code(), code);
        }
        assert_eq!(
            Error::workflow_not_found("wf").result_code(),
            ErrorCode::WorkflowNotFound
        );
    }

    /// Context added for people does not hide what the error is.
    #[test]
    fn context_keeps_the_wrapped_errors_code() {
        let wrapped = Error::WithContext {
            context: "starting checkout".into(),
            source: Box::new(Error::workflow_not_found("wf")),
        };
        assert_eq!(wrapped.code(), ErrorCode::WorkflowNotFound);
    }
}
