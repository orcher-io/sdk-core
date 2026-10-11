//! The connection to the engine: gRPC channels and TLS, the credentials every
//! worker call carries, and the message size limits every client and worker
//! applies.

pub mod channel;
pub mod credentials;
pub mod limits;

// Channel manager and TLS config.
pub use channel::{ChannelManager, TlsConfig};

// The credential helper every worker-originated call uses.
pub use credentials::credentialed_request;
