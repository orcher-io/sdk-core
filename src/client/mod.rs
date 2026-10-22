//! Client API for starting, querying, and managing ORCHER workflows from
//! outside a worker.
//!
//! ## Terminology
//!
//! - **Workflow**: a durable execution unit.
//! - **Task**: a unit of work a workflow schedules.
//! - **Event**: an external message delivered to a running workflow.

pub mod actor;
pub mod connection;
pub mod namespace;
pub mod workflow;

pub use actor::ActorClient;
pub use connection::{ConnectOptions, Connection};
pub use namespace::NamespaceClient;
pub use workflow::{CancelWorkflowOpts, StartWorkflowOpts, WorkflowClient, WorkflowHandle};
