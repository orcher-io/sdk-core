//! Error types for every SDK operation: calls to the engine, workflow and
//! task execution, and replay.

use std::fmt;
use std::sync::Arc;

/// Metadata key the server sets on an ALREADY_EXISTS from StartWorkflow, naming
/// the workflow whose id is in use.
pub const WORKFLOW_ID_METADATA_KEY: &str = "x-orcher-workflow-id";

/// Metadata key the server sets on an ALREADY_EXISTS from StartWorkflow, naming
/// the execution holding the id: the open one, or the closed one the reuse
/// policy refused to reuse.
pub const EXECUTION_ID_METADATA_KEY: &str = "x-orcher-execution-id";

/// Result type for SDK operations.
pub type Result<T> = std::result::Result<T, Error>;

/// Error type for every SDK operation.
///
/// Both the enum and its variants with named fields are `#[non_exhaustive]`,
/// so adding a kind of error, or a detail to an existing one, is not a
/// breaking change. Match with a wildcard arm and `..` in struct patterns,
/// and build errors with the constructor functions.
#[derive(Debug, thiserror::Error)]
#[non_exhaustive]
pub enum Error {
    /// The engine answered a call with a status other than OK.
    ///
    /// A status that reports a message over a size limit arrives restated as
    /// OUT_OF_RANGE, saying which limit to raise or that the data belongs
    /// elsewhere.
    #[error("gRPC status error: {}", with_causes(.0))]
    Status(#[source] Status),

    /// Workflow not found.
    #[error("Workflow not found: workflow_id={workflow_id}, run_id={run_id:?}")]
    #[non_exhaustive]
    WorkflowNotFound {
        workflow_id: String,
        run_id: Option<String>,
    },

    /// The workflow id is already in use.
    ///
    /// Either an execution with this id is still open, or a closed one exists
    /// that the start's reuse policy does not allow reusing. `run_id` names
    /// that execution when the server reports it.
    #[error("Workflow already exists: workflow_id={workflow_id}, run_id={run_id:?}")]
    #[non_exhaustive]
    WorkflowAlreadyExists {
        workflow_id: String,
        run_id: Option<String>,
    },

    /// A workflow (or actor operation) ended without a result.
    ///
    /// `status` says how it ended: [`WorkflowStatus::Failed`],
    /// [`WorkflowStatus::Cancelled`], [`WorkflowStatus::Terminated`] or
    /// [`WorkflowStatus::TimedOut`], so a caller can tell a cancellation from
    /// a failure without asking for the status separately.
    ///
    /// [`WorkflowStatus::Failed`]: crate::types::WorkflowStatus::Failed
    /// [`WorkflowStatus::Cancelled`]: crate::types::WorkflowStatus::Cancelled
    /// [`WorkflowStatus::Terminated`]: crate::types::WorkflowStatus::Terminated
    /// [`WorkflowStatus::TimedOut`]: crate::types::WorkflowStatus::TimedOut
    #[error("Workflow execution failed: {message}")]
    #[non_exhaustive]
    WorkflowExecutionFailed {
        message: String,
        status: crate::types::WorkflowStatus,
    },

    /// Task execution failed.
    #[error("Task execution failed: task_id={task_id}, reason={reason}")]
    #[non_exhaustive]
    TaskExecutionFailed { task_id: String, reason: String },

    /// Invalid workflow state.
    #[error("Invalid workflow state: expected={expected}, actual={actual}")]
    #[non_exhaustive]
    InvalidWorkflowState { expected: String, actual: String },

    /// Determinism violation during replay.
    #[error("Determinism violation: {reason}")]
    #[non_exhaustive]
    DeterminismViolation { reason: String },

    /// Invalid execution journal.
    #[error("Invalid execution journal: {reason}")]
    #[non_exhaustive]
    InvalidExecutionJournal { reason: String },

    /// Serialization error.
    #[error("Serialization error: {0}")]
    Serialization(String),

    /// Deserialization error.
    #[error("Deserialization error: {0}")]
    Deserialization(String),

    /// Invalid payload format.
    #[error("Invalid payload: {reason}")]
    #[non_exhaustive]
    InvalidPayload { reason: String },

    /// Codec error (compression, encryption, and so on).
    #[error("Codec error: {0}")]
    Codec(String),

    /// Configuration error.
    #[error("Configuration error: {0}")]
    Configuration(String),

    /// Worker error.
    #[error("Worker error: {0}")]
    WorkerError(String),

    /// Poller error.
    #[error("Poller error: {0}")]
    PollerError(String),

    /// Timeout error.
    #[error("Operation timed out: {operation}")]
    #[non_exhaustive]
    Timeout { operation: String },

    /// Workflow cancelled.
    #[error("Workflow cancelled: workflow_id={workflow_id}")]
    #[non_exhaustive]
    WorkflowCancelled { workflow_id: String },

    /// Workflow terminated.
    #[error("Workflow terminated: workflow_id={workflow_id}, reason={reason:?}")]
    #[non_exhaustive]
    WorkflowTerminated {
        workflow_id: String,
        reason: Option<String>,
    },

    /// Task cancelled.
    #[error("Task cancelled: task_id={task_id}")]
    #[non_exhaustive]
    TaskCancelled { task_id: String },

    /// Invalid command.
    #[error("Invalid command: {reason}")]
    #[non_exhaustive]
    InvalidCommand { reason: String },

    /// Invalid event.
    #[error("Invalid event: {reason}")]
    #[non_exhaustive]
    InvalidEvent { reason: String },

    /// Cache error.
    #[error("Cache error: {0}")]
    CacheError(String),

    /// State machine error.
    #[error("State machine error: {0}")]
    StateMachineError(String),

    /// Replay error.
    #[error("Replay error: {0}")]
    ReplayError(String),

    /// Connection error.
    #[error("Connection error: {0}")]
    Connection(String),

    /// Authentication error.
    #[error("Authentication error: {0}")]
    Authentication(String),

    /// Authorization error.
    #[error("Authorization error: {0}")]
    Authorization(String),

    /// Resource exhausted (rate limiting, quota).
    #[error("Resource exhausted: {resource}")]
    #[non_exhaustive]
    ResourceExhausted { resource: String },

    /// Internal SDK error.
    #[error("Internal SDK error: {0}")]
    Internal(String),

    /// The language SDK could not process an activation, or answered it with
    /// something the kernel cannot carry. Not the workflow's failure: the
    /// activation is handed back to the engine for another worker.
    #[error("Kernel protocol error: {0}")]
    Protocol(String),

    /// Another error with added context.
    #[error("{context}: {source}")]
    #[non_exhaustive]
    WithContext { context: String, source: Box<Error> },

    /// Any other error, including a [`TaskFailure`].
    #[error(transparent)]
    Other(#[from] anyhow::Error),
}

/// `error` followed by each error in its source chain, as `error: cause: cause`.
///
/// A connection that fails at the TLS handshake surfaces as a bare
/// "transport error"; the reason (an untrusted certificate, a client
/// certificate the server required) is only in the chain. A cause whose text
/// the message already contains is skipped.
pub(crate) fn with_causes(error: &(dyn std::error::Error + 'static)) -> String {
    let mut text = error.to_string();
    let mut source = error.source();
    while let Some(cause) = source {
        let cause_text = cause.to_string();
        if !cause_text.is_empty() && !text.contains(&cause_text) {
            text.push_str(": ");
            text.push_str(&cause_text);
        }
        source = cause.source();
    }
    text
}

/// The status code of a call the engine refused, as RPC status codes number
/// them.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum Code {
    /// The call succeeded.
    Ok = 0,
    /// The call was cancelled, usually by the caller.
    Cancelled = 1,
    /// An error with no better code, including one the client could not read.
    Unknown = 2,
    /// The request was invalid, whatever the system's state.
    InvalidArgument = 3,
    /// The deadline passed before the call completed.
    DeadlineExceeded = 4,
    /// Something the call named does not exist.
    NotFound = 5,
    /// What the call would create already exists.
    AlreadyExists = 6,
    /// The caller may not do this.
    PermissionDenied = 7,
    /// A quota or limit ran out.
    ResourceExhausted = 8,
    /// The system is not in the state the call needs.
    FailedPrecondition = 9,
    /// The call was aborted, usually by a conflict.
    Aborted = 10,
    /// A value was outside its valid range, a message size limit included.
    OutOfRange = 11,
    /// The engine does not offer the call.
    Unimplemented = 12,
    /// The engine broke one of its own invariants.
    Internal = 13,
    /// The engine cannot be reached for now.
    Unavailable = 14,
    /// Data was lost or corrupted.
    DataLoss = 15,
    /// The call carried no valid credentials.
    Unauthenticated = 16,
}

impl Code {
    /// The code numbered `value`; [`Code::Unknown`] for a number with no code.
    pub fn from_i32(value: i32) -> Self {
        match value {
            0 => Code::Ok,
            1 => Code::Cancelled,
            3 => Code::InvalidArgument,
            4 => Code::DeadlineExceeded,
            5 => Code::NotFound,
            6 => Code::AlreadyExists,
            7 => Code::PermissionDenied,
            8 => Code::ResourceExhausted,
            9 => Code::FailedPrecondition,
            10 => Code::Aborted,
            11 => Code::OutOfRange,
            12 => Code::Unimplemented,
            13 => Code::Internal,
            14 => Code::Unavailable,
            15 => Code::DataLoss,
            16 => Code::Unauthenticated,
            _ => Code::Unknown,
        }
    }

    /// The code's number.
    pub fn as_i32(self) -> i32 {
        self as i32
    }

    /// The code's name, as RPC status codes are written: `NOT_FOUND`.
    pub fn as_str(self) -> &'static str {
        match self {
            Code::Ok => "OK",
            Code::Cancelled => "CANCELLED",
            Code::Unknown => "UNKNOWN",
            Code::InvalidArgument => "INVALID_ARGUMENT",
            Code::DeadlineExceeded => "DEADLINE_EXCEEDED",
            Code::NotFound => "NOT_FOUND",
            Code::AlreadyExists => "ALREADY_EXISTS",
            Code::PermissionDenied => "PERMISSION_DENIED",
            Code::ResourceExhausted => "RESOURCE_EXHAUSTED",
            Code::FailedPrecondition => "FAILED_PRECONDITION",
            Code::Aborted => "ABORTED",
            Code::OutOfRange => "OUT_OF_RANGE",
            Code::Unimplemented => "UNIMPLEMENTED",
            Code::Internal => "INTERNAL",
            Code::Unavailable => "UNAVAILABLE",
            Code::DataLoss => "DATA_LOSS",
            Code::Unauthenticated => "UNAUTHENTICATED",
        }
    }
}

impl fmt::Display for Code {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

/// What the engine answered a call it did not complete with: a code and a
/// message, and the error underneath when the client produced the status
/// itself (a connection that broke mid-call).
#[derive(Debug, Clone)]
pub struct Status {
    code: Code,
    message: String,
    source: Option<Arc<dyn std::error::Error + Send + Sync + 'static>>,
}

impl Status {
    /// A status with `code` and `message`.
    pub fn new(code: Code, message: impl Into<String>) -> Self {
        Self {
            code,
            message: message.into(),
            source: None,
        }
    }

    /// Sets the error underneath the status.
    pub(crate) fn with_source(
        mut self,
        source: Option<Arc<dyn std::error::Error + Send + Sync + 'static>>,
    ) -> Self {
        self.source = source;
        self
    }

    /// The status code.
    pub fn code(&self) -> Code {
        self.code
    }

    /// The message the status came with.
    pub fn message(&self) -> &str {
        &self.message
    }
}

impl fmt::Display for Status {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.message.is_empty() {
            write!(f, "{}", self.code)
        } else {
            write!(f, "{}: {}", self.code, self.message)
        }
    }
}

impl std::error::Error for Status {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_deref().map(|e| e as _)
    }
}

/// A task's own description of its failure.
///
/// It says what went wrong, the type of error the task raised, and whether a
/// retry could ever succeed. The engine decides whether to retry a task from
/// the error type it is told, and a retry policy names the types it will not
/// retry. An error that is only a message gives the engine nothing to match,
/// so every failure looks alike and the policy's non-retryable list never
/// applies. A language SDK builds a `TaskFailure` from the exception or error
/// its task raised and hands it back as the task's result, converted into an
/// [`Error`].
///
/// It travels as [`Error::Other`] rather than as a variant of its own, so
/// bindings that match on every variant keep compiling.
#[derive(Debug, Clone, PartialEq, Eq)]
#[non_exhaustive]
pub struct TaskFailure {
    /// What went wrong, as the task's error described it.
    pub message: String,
    /// The type of the error, as a retry policy would name it. `None` reports
    /// the generic `TaskExecutionError`.
    pub error_type: Option<String>,
    /// The task's code says no retry can succeed, so the engine does not retry
    /// it, whatever the policy allows.
    pub non_retryable: bool,
}

impl TaskFailure {
    /// Creates a failure with a message and nothing else known about it.
    pub fn new(message: impl Into<String>) -> Self {
        Self {
            message: message.into(),
            error_type: None,
            non_retryable: false,
        }
    }

    /// Sets the type of the error, as a retry policy would list it.
    pub fn with_type(mut self, error_type: impl Into<String>) -> Self {
        self.error_type = Some(error_type.into());
        self
    }

    /// Marks the failure as one no retry can fix.
    pub fn with_non_retryable(mut self, non_retryable: bool) -> Self {
        self.non_retryable = non_retryable;
        self
    }
}

impl fmt::Display for TaskFailure {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(&self.message)
    }
}

impl std::error::Error for TaskFailure {}

impl From<TaskFailure> for Error {
    fn from(failure: TaskFailure) -> Self {
        Error::Other(anyhow::Error::new(failure))
    }
}

impl Error {
    /// Returns the [`TaskFailure`] this error carries, if a language SDK built
    /// it from one.
    ///
    /// Looks through any context added along the way.
    pub fn task_failure(&self) -> Option<&TaskFailure> {
        match self {
            Error::Other(e) => e.downcast_ref::<TaskFailure>(),
            Error::WithContext { source, .. } => source.task_failure(),
            _ => None,
        }
    }
}

impl Error {
    /// Creates a workflow not found error.
    pub fn workflow_not_found(workflow_id: impl Into<String>) -> Self {
        Self::WorkflowNotFound {
            workflow_id: workflow_id.into(),
            run_id: None,
        }
    }

    /// Creates a workflow not found error with run ID.
    pub fn workflow_not_found_with_run(
        workflow_id: impl Into<String>,
        run_id: impl Into<String>,
    ) -> Self {
        Self::WorkflowNotFound {
            workflow_id: workflow_id.into(),
            run_id: Some(run_id.into()),
        }
    }

    /// Creates a workflow already exists error.
    pub fn workflow_already_exists(workflow_id: impl Into<String>) -> Self {
        Self::WorkflowAlreadyExists {
            workflow_id: workflow_id.into(),
            run_id: None,
        }
    }

    /// Creates a workflow already exists error naming the run that holds the id.
    pub fn workflow_already_exists_with_run(
        workflow_id: impl Into<String>,
        run_id: impl Into<String>,
    ) -> Self {
        Self::WorkflowAlreadyExists {
            workflow_id: workflow_id.into(),
            run_id: Some(run_id.into()),
        }
    }

    /// Creates a workflow execution failed error.
    pub fn workflow_execution_failed(message: impl Into<String>) -> Self {
        Self::workflow_ended(crate::types::WorkflowStatus::Failed, message)
    }

    /// Creates the error for a workflow that ended without a result, with how
    /// it ended.
    pub fn workflow_ended(
        status: crate::types::WorkflowStatus,
        message: impl Into<String>,
    ) -> Self {
        Self::WorkflowExecutionFailed {
            message: message.into(),
            status,
        }
    }

    /// How the workflow ended, when this error reports one that ended without
    /// a result.
    pub fn workflow_status(&self) -> Option<crate::types::WorkflowStatus> {
        match self {
            Self::WorkflowExecutionFailed { status, .. } => Some(*status),
            _ => None,
        }
    }

    /// Creates a task execution failed error.
    pub fn task_execution_failed(task_id: impl Into<String>, reason: impl Into<String>) -> Self {
        Self::TaskExecutionFailed {
            task_id: task_id.into(),
            reason: reason.into(),
        }
    }

    /// Creates an invalid workflow state error.
    pub fn invalid_workflow_state(expected: impl Into<String>, actual: impl Into<String>) -> Self {
        Self::InvalidWorkflowState {
            expected: expected.into(),
            actual: actual.into(),
        }
    }

    /// Creates an invalid execution journal error.
    pub fn invalid_execution_journal(reason: impl Into<String>) -> Self {
        Self::InvalidExecutionJournal {
            reason: reason.into(),
        }
    }

    /// Creates a task cancelled error.
    pub fn task_cancelled(task_id: impl Into<String>) -> Self {
        Self::TaskCancelled {
            task_id: task_id.into(),
        }
    }

    /// Creates an invalid command error.
    pub fn invalid_command(reason: impl Into<String>) -> Self {
        Self::InvalidCommand {
            reason: reason.into(),
        }
    }

    /// Creates an invalid event error.
    pub fn invalid_event(reason: impl Into<String>) -> Self {
        Self::InvalidEvent {
            reason: reason.into(),
        }
    }

    /// Creates a resource exhausted error.
    pub fn resource_exhausted(resource: impl Into<String>) -> Self {
        Self::ResourceExhausted {
            resource: resource.into(),
        }
    }

    /// Creates a determinism violation error.
    pub fn determinism_violation(reason: impl Into<String>) -> Self {
        Self::DeterminismViolation {
            reason: reason.into(),
        }
    }

    /// Creates a serialization error.
    pub fn serialization(error: impl fmt::Display) -> Self {
        Self::Serialization(error.to_string())
    }

    /// Creates a deserialization error.
    pub fn deserialization(error: impl fmt::Display) -> Self {
        Self::Deserialization(error.to_string())
    }

    /// Creates an invalid payload error.
    pub fn invalid_payload(reason: impl Into<String>) -> Self {
        Self::InvalidPayload {
            reason: reason.into(),
        }
    }

    /// Creates a codec error.
    pub fn codec(error: impl fmt::Display) -> Self {
        Self::Codec(error.to_string())
    }

    /// Creates a configuration error.
    pub fn configuration(message: impl Into<String>) -> Self {
        Self::Configuration(message.into())
    }

    /// Creates a worker error.
    pub fn worker_error(message: impl Into<String>) -> Self {
        Self::WorkerError(message.into())
    }

    /// Deprecated alias for [`Error::worker_error`].
    #[deprecated(note = "Use worker_error() instead")]
    pub fn service_error(message: impl Into<String>) -> Self {
        Self::WorkerError(message.into())
    }

    /// Creates a timeout error.
    pub fn timeout(operation: impl Into<String>) -> Self {
        Self::Timeout {
            operation: operation.into(),
        }
    }

    /// Creates a workflow cancelled error.
    pub fn workflow_cancelled(workflow_id: impl Into<String>) -> Self {
        Self::WorkflowCancelled {
            workflow_id: workflow_id.into(),
        }
    }

    /// Creates a workflow terminated error.
    pub fn workflow_terminated(
        workflow_id: impl Into<String>,
        reason: Option<impl Into<String>>,
    ) -> Self {
        Self::WorkflowTerminated {
            workflow_id: workflow_id.into(),
            reason: reason.map(|r| r.into()),
        }
    }

    /// Creates a connection error.
    pub fn connection(message: impl Into<String>) -> Self {
        Self::Connection(message.into())
    }

    /// Creates an internal error.
    pub fn internal(message: impl Into<String>) -> Self {
        Self::Internal(message.into())
    }

    /// Wraps this error with a context message.
    pub fn context(self, context: impl Into<String>) -> Self {
        Self::WithContext {
            context: context.into(),
            source: Box::new(self),
        }
    }

    /// Returns whether this error is an ordinary long-poll timeout rather than
    /// a real failure.
    ///
    /// A long poll that ends with nothing to return is expected; the caller
    /// should simply poll again.
    pub fn is_normal_timeout(&self) -> bool {
        match self {
            Error::Timeout { .. } => true,

            Error::Status(status) if status.code() == Code::DeadlineExceeded => true,

            // Cancelled with "Timeout expired" is how the server ends a long poll
            Error::Status(status)
                if status.code() == Code::Cancelled && status.message() == "Timeout expired" =>
            {
                true
            }

            _ => false,
        }
    }

    /// Returns whether retrying the operation could succeed.
    ///
    /// True for connection failures, timeouts, resource
    /// exhaustion (after a backoff), and the status codes that signal a
    /// transient condition.
    pub fn is_retryable(&self) -> bool {
        match self {
            Error::Connection(_) => true,

            Error::Status(status) => {
                matches!(
                    status.code(),
                    Code::Unavailable
                        | Code::DeadlineExceeded
                        | Code::ResourceExhausted
                        | Code::Aborted
                        | Code::Cancelled
                        | Code::Unknown // How transport errors surface mid-long-poll
                )
            }

            Error::Timeout { .. } => true,

            Error::ResourceExhausted { .. } => true,

            _ => false,
        }
    }

    /// Returns whether this error is permanent, so retrying cannot help.
    ///
    /// True for authentication and authorization failures, workflow state and
    /// determinism errors, configuration errors, invalid data, and the status
    /// codes that describe a bad or impossible request. An error can be
    /// neither retryable nor permanent.
    pub fn is_permanent(&self) -> bool {
        match self {
            Error::Authentication(_) | Error::Authorization(_) => true,

            Error::WorkflowAlreadyExists { .. }
            | Error::DeterminismViolation { .. }
            | Error::InvalidWorkflowState { .. } => true,

            Error::Configuration(_) => true,

            Error::InvalidPayload { .. }
            | Error::InvalidCommand { .. }
            | Error::InvalidEvent { .. }
            | Error::InvalidExecutionJournal { .. } => true,

            Error::Status(status) => {
                matches!(
                    status.code(),
                    Code::InvalidArgument
                        | Code::NotFound
                        | Code::AlreadyExists
                        | Code::PermissionDenied
                        | Code::Unauthenticated
                        | Code::FailedPrecondition
                        | Code::Unimplemented
                )
            }

            _ => false,
        }
    }

    /// Converts a `serde_json` error.
    ///
    /// Syntax and data errors become [`Error::Deserialization`]; I/O and other
    /// errors become [`Error::Serialization`].
    pub fn from_serde_json(error: serde_json::Error) -> Self {
        if error.is_io() {
            Self::Serialization(format!("IO error during serialization: {}", error))
        } else if error.is_syntax() || error.is_data() {
            Self::Deserialization(error.to_string())
        } else {
            Self::Serialization(error.to_string())
        }
    }
}

impl From<serde_json::Error> for Error {
    fn from(error: serde_json::Error) -> Self {
        Self::from_serde_json(error)
    }
}

#[cfg(test)]
mod tests {

    /// An error with an optional cause, like the layers of a failed connect.
    #[derive(Debug)]
    struct Layer(&'static str, Option<Box<Layer>>);

    impl std::fmt::Display for Layer {
        fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
            f.write_str(self.0)
        }
    }

    impl std::error::Error for Layer {
        fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
            self.1.as_deref().map(|e| e as _)
        }
    }

    #[test]
    fn a_transport_status_names_the_handshake_failure() {
        // How tonic reports a connection the server refused at the handshake:
        // the status says "transport error", the reason is in the chain.
        let cause = Layer(
            "transport error",
            Some(Box::new(Layer(
                "client error (Connect)",
                Some(Box::new(Layer(
                    "received fatal alert: CertificateRequired",
                    None,
                ))),
            ))),
        );
        let err = crate::wire::status::error(tonic::Status::from_error(Box::new(cause)));
        let text = err.to_string();
        assert!(
            text.ends_with(": client error (Connect): received fatal alert: CertificateRequired"),
            "{text}"
        );
        // The chain is still there for code that walks it.
        assert!(std::error::Error::source(&err).is_some());
    }

    #[test]
    fn a_cause_already_in_the_message_is_not_repeated() {
        let err = Layer(
            "connect failed: refused",
            Some(Box::new(Layer("refused", None))),
        );
        assert_eq!(with_causes(&err), "connect failed: refused");
    }
    use super::*;

    #[test]
    fn test_error_creation() {
        let err = Error::workflow_not_found("test-workflow");
        assert!(err.to_string().contains("test-workflow"));

        let err = Error::determinism_violation("replay mismatch");
        assert!(err.to_string().contains("replay mismatch"));
    }

    #[test]
    fn test_retryable_errors() {
        let err = Error::Connection("connection failed".to_string());
        assert!(err.is_retryable());

        let err = Error::timeout("poll");
        assert!(err.is_retryable());

        let err = Error::workflow_not_found("test");
        assert!(!err.is_retryable());
    }

    #[test]
    fn test_permanent_errors() {
        let err = Error::configuration("invalid config");
        assert!(err.is_permanent());

        let err = Error::determinism_violation("mismatch");
        assert!(err.is_permanent());

        let err = Error::timeout("operation");
        assert!(!err.is_permanent());
    }

    #[test]
    fn test_error_context() {
        let err = Error::internal("something failed");
        let err_with_context = err.context("while processing workflow");
        assert!(err_with_context
            .to_string()
            .contains("while processing workflow"));
    }
}
