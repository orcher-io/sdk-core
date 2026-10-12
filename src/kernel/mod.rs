//! The kernel: what a run's history means, resolved once for every language.
//!
//! The kernel turns the history the engine sends with an activation into a
//! [`RunActivation`](crate::proto::kernel::v1::RunActivation): the outcomes the
//! run observes, in the order it observes them, each with the run's time when
//! it happened and one decoding for every SDK. A language SDK runs the
//! workflow's code against those outcomes and never reads the journal.
//!
//! The protocol is `orcher.kernel.v1` (orcher-proto); the design is
//! `docs/kernel-protocol-v1.md`.
//!
//! - [`activation`]: an activation's history, resolved.
//! - [`step_ids`]: step ids under the kernel's own scheme, `KERNEL_V1`.
//! - [`random`]: the deterministic random source for runs under that scheme.

pub mod activation;
pub mod random;
pub mod step_ids;

pub use activation::resolve;
