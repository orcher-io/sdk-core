//! The module paths from before the sources were organised by role.
//!
//! Each old module re-exports what moved out of it, so code written against
//! the old paths still builds unchanged. Each is marked deprecated, which
//! shows in the documentation with the new path; rustc does not warn on a
//! deprecated module, so their users see no warnings.

/// Moved to [`crate::protocol`].
#[deprecated(note = "moved to `orcher_sdk_core::protocol`")]
pub mod bridge {
    pub use crate::protocol::*;
}

/// Moved to [`crate::payload::codec`].
#[deprecated(note = "moved to `orcher_sdk_core::payload::codec`")]
pub mod codec {
    pub use crate::payload::codec::*;
}

/// Moved to [`crate::payload::converter`].
#[deprecated(note = "moved to `orcher_sdk_core::payload::converter`")]
pub mod converter {
    pub use crate::payload::converter::*;
}

/// Moved to [`crate::transport::limits`].
#[deprecated(note = "moved to `orcher_sdk_core::transport::limits`")]
pub mod limits {
    pub use crate::transport::limits::*;
}

/// Moved to [`crate::worker`] (drivers, polling, heartbeats, metrics) and
/// [`crate::transport`] (channels, TLS, credentials).
#[deprecated(note = "moved to `orcher_sdk_core::worker` and `orcher_sdk_core::transport`")]
pub mod poller {
    pub use crate::transport::{credentialed_request, ChannelManager, TlsConfig};
    pub use crate::worker::{
        worker_instance_id, ActorDriver, ActorDriverConfig, ActorDriverEvent, ActorOperationPoller,
        ActorPollerConfig, ActorWork, ActorWorkResult, CancellationToken, CompletionRetryConfig,
        EagerTaskInjector, PollerConfig, SessionQueueChange, ShutdownHandle, TaskDriver,
        TaskDriverConfig, TaskExecutionPoller, TaskExecutionTask, TaskHeartbeat, TaskWork,
        TaskWorkResult, WorkerMetrics, WorkerMetricsSnapshot, WorkerRegistrationConfig,
        WorkerRegistrationDriver, WorkflowDriver, WorkflowDriverConfig, WorkflowExecutionPoller,
        WorkflowExecutionTask, WorkflowWork, WorkflowWorkResult,
    };

    /// Moved to [`crate::transport::channel`].
    pub mod channel {
        pub use crate::transport::channel::*;
    }

    /// Moved to [`crate::transport::credentials`].
    pub mod credentials {
        pub use crate::transport::credentials::*;
    }

    /// Moved to [`crate::worker::drivers`], one file per driver.
    pub mod driver {
        pub use crate::worker::drivers::actor::*;
        pub use crate::worker::drivers::task::*;
        pub use crate::worker::drivers::workflow::*;
        pub use crate::worker::drivers::ShutdownHandle;
    }

    /// Moved to [`crate::worker::heartbeat`].
    pub mod heartbeat {
        pub use crate::worker::heartbeat::*;
    }

    /// Moved to [`crate::worker::metrics`].
    pub mod metrics {
        pub use crate::worker::metrics::*;
    }

    /// Moved to [`crate::worker::poll::polling`].
    pub mod polling {
        pub use crate::worker::poll::polling::*;
    }

    /// Moved to [`crate::worker::drivers::registration`].
    pub mod worker_registration {
        pub use crate::worker::drivers::registration::*;
    }
}
