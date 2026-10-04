//! How large a gRPC message may be, and what is said when one is larger.
//!
//! tonic limits a message it receives to 4 MiB unless told otherwise, and a
//! workflow carrying a long prompt or a conversation history passes that
//! quickly: every activation carries every task result and event payload the
//! workflow has received. So every client and worker here sends and receives
//! up to [`DEFAULT_MAX_MESSAGE_BYTES`] instead, the engine's own default, and
//! tells the engine on its polls how much it can receive, so the engine fails
//! a workflow whose activation would not fit rather than handing it out.
//!
//! A message over the limit is reported as what it is. A raw tonic status
//! ("decoded message length too large") becomes a status that says which
//! limit was hit and what to do; a worker's own completion that is too large
//! to send becomes a failure of its task or workflow that says why, instead
//! of being sent, refused and sent again.

use tonic::{Code, Status};

const MIB: usize = 1024 * 1024;

/// The largest message sent or received unless configured: 32 MiB, the
/// engine's default.
pub const DEFAULT_MAX_MESSAGE_BYTES: usize = 32 * MIB;

/// The environment variable that sets the message limit for every client and
/// worker in the process that is not given one explicitly.
pub const MAX_MESSAGE_BYTES_ENV: &str = "ORCHER_MAX_MESSAGE_BYTES";

/// The header a worker states, on its polls and completions, the largest
/// message it can receive. An engine that reads it fails a workflow or task
/// too large for the worker instead of handing it out; one that does not
/// ignores it.
pub const MAX_RECEIVE_MESSAGE_BYTES_HEADER: &str = "x-orcher-max-receive-message-bytes";

/// The failure type a task or workflow is failed with when what it produced
/// is too large to send, so a retry policy can name it.
pub const PAYLOAD_TOO_LARGE: &str = "PayloadTooLarge";

/// The message limit for a client or worker that is given none:
/// [`MAX_MESSAGE_BYTES_ENV`] if set to a positive number, otherwise
/// [`DEFAULT_MAX_MESSAGE_BYTES`].
pub fn default_max_message_bytes() -> usize {
    std::env::var(MAX_MESSAGE_BYTES_ENV)
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|n| *n > 0)
        .unwrap_or(DEFAULT_MAX_MESSAGE_BYTES)
}

/// Whether `status` says a message was over a gRPC message limit, on either
/// side: tonic's "message length too large", or the "larger than max" that
/// other gRPC stacks and proxies answer with.
pub fn is_message_too_large(status: &Status) -> bool {
    if !matches!(
        status.code(),
        Code::OutOfRange | Code::ResourceExhausted | Code::Internal
    ) {
        return false;
    }
    let message = status.message().to_ascii_lowercase();
    message.contains("message length too large")
        || message.contains("larger than max")
        || message.contains("message too large")
}

/// `status`, or for a message over a size limit, a status that says so and
/// what to do about it. Always OUT_OF_RANGE then, which nothing retries:
/// sending the same message again cannot fit it.
pub fn clarify(status: Status) -> Status {
    if !is_message_too_large(&status) {
        return status;
    }
    Status::out_of_range(format!(
        "a gRPC message was larger than the limit ({}). Raise the client's or \
         worker's max_message_bytes ({MAX_MESSAGE_BYTES_ENV}, default {}) and the \
         engine's grpc_max_message_bytes, or store large data elsewhere and pass a \
         reference.",
        status.message(),
        format_bytes(DEFAULT_MAX_MESSAGE_BYTES),
    ))
}

/// The message for something a worker produced that is too large to send.
pub(crate) fn too_large_to_send(what: &str, size: usize, limit: usize) -> String {
    format!(
        "{what} is {}, more than the {} this worker sends in one message \
         (max_message_bytes). Store large data elsewhere and pass a reference.",
        format_bytes(size),
        format_bytes(limit),
    )
}

/// The message for something a worker produced that the engine refused as
/// too large.
pub(crate) fn refused_as_too_large(what: &str, size: usize) -> String {
    format!(
        "{what} is {}, more than the engine accepts in one message \
         (its grpc_max_message_bytes). Store large data elsewhere and pass a reference.",
        format_bytes(size),
    )
}

/// A byte count as a person reads it: `5.2 MiB`, `8 MiB`, `512 KiB`.
pub(crate) fn format_bytes(bytes: usize) -> String {
    const KIB: usize = 1024;
    if bytes >= MIB {
        if bytes.is_multiple_of(MIB) {
            format!("{} MiB", bytes / MIB)
        } else {
            format!("{:.1} MiB", bytes as f64 / MIB as f64)
        }
    } else if bytes >= KIB {
        if bytes.is_multiple_of(KIB) {
            format!("{} KiB", bytes / KIB)
        } else {
            format!("{:.1} KiB", bytes as f64 / KIB as f64)
        }
    } else {
        format!("{bytes} bytes")
    }
}

/// A generated gRPC client with its message limits set to `max` both ways.
macro_rules! sized {
    ($client:expr, $max:expr) => {{
        let max: usize = $max;
        $client
            .max_decoding_message_size(max)
            .max_encoding_message_size(max)
    }};
}
pub(crate) use sized;

/// Add [`MAX_RECEIVE_MESSAGE_BYTES_HEADER`] to `request`.
pub(crate) fn state_receive_limit<T>(request: &mut tonic::Request<T>, max: usize) {
    if let Ok(value) = max.to_string().parse() {
        request
            .metadata_mut()
            .insert(MAX_RECEIVE_MESSAGE_BYTES_HEADER, value);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn tonic_size_refusals_are_recognised_and_explained() {
        for status in [
            Status::out_of_range(
                "Error, decoded message length too large: found 6291918 bytes, \
                 the limit is: 4194304 bytes",
            ),
            Status::out_of_range(
                "Error, encoded message length too large: found 9437184 bytes, \
                 the limit is: 4194304 bytes",
            ),
            Status::resource_exhausted(
                "grpc: received message larger than max (6291918 vs. 4194304)",
            ),
        ] {
            assert!(is_message_too_large(&status), "{status:?}");
            let clear = clarify(status.clone());
            assert_eq!(clear.code(), Code::OutOfRange);
            assert!(clear.message().contains(status.message()), "{clear:?}");
            assert!(clear.message().contains("max_message_bytes"), "{clear:?}");
            assert!(
                clear.message().contains("ORCHER_MAX_MESSAGE_BYTES"),
                "{clear:?}"
            );
        }
    }

    #[test]
    fn other_statuses_are_left_alone() {
        for status in [
            Status::resource_exhausted("worker at capacity"),
            Status::out_of_range("page token out of range"),
            Status::unavailable("message length too large"),
        ] {
            assert!(!is_message_too_large(&status), "{status:?}");
            let same = clarify(status.clone());
            assert_eq!(same.code(), status.code());
            assert_eq!(same.message(), status.message());
        }
    }

    #[test]
    fn sizes_read_as_a_person_would_write_them() {
        assert_eq!(format_bytes(32 * MIB), "32 MiB");
        assert_eq!(format_bytes(5 * MIB + MIB / 5), "5.2 MiB");
        assert_eq!(format_bytes(512 * 1024), "512 KiB");
        assert_eq!(format_bytes(10), "10 bytes");
    }
}
