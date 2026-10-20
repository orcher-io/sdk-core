//! The connection to the engine: gRPC channels and TLS, the credentials every
//! worker call carries, and the message size limits every client and worker
//! applies.

pub mod channel;
pub mod credentials;
pub mod instrument;
pub mod limits;
pub mod non_grpc;

// Channel manager and TLS config.
pub use channel::{ChannelManager, TlsConfig};

// The credential helper every worker-originated call uses.
pub use credentials::credentialed_request;

/// Restates a failed call's status by what actually happened: a message over
/// a size limit, or an answer from something other than the engine. Every
/// path that turns a call's status into a decision goes through this.
pub fn clarify(status: tonic::Status) -> tonic::Status {
    limits::clarify(non_grpc::clarify(status))
}
