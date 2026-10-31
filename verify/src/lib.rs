//! `orcher-verify`: checks a workflow runtime in any language against the
//! kernel protocol (`docs/kernel-protocol-v1.md` in sdk-core), without an
//! engine and without Rust tests, and its worker against the contract
//! scenarios, with one.
//!
//! The replay vectors are activations recorded from each SDK's contract
//! suite: for each, the `RunActivation` the kernel resolves it to and the
//! answer the worker sent the engine after it. The runner starts the runtime
//! under test as a child process, hands it each vector's `RunActivation`
//! ([`framing`]), and judges the `RunActivationResult` it answers with the
//! kernel's own checks ([`judge`]): the answer must replay the recorded
//! history, and the worker must send for it what it sent when the run was
//! recorded.
//!
//! - [`vectors`]: the vectors, their index and how they are read.
//! - [`framing`]: the messages on the runtime's stdin and stdout.
//! - [`judge`]: whether an answer is the recorded one.
//! - [`runner`]: runs a runtime against the vectors.
//! - [`reference`](mod@reference): a runtime that answers with the recorded
//!   answers, right or wrong on purpose, which the runner's own tests run.
//!
//! With an engine, [`scenarios`] runs the contract scenarios, the catalog
//! every SDK's contract suite runs, against the runtime's own worker, and
//! judges each by what a client observes.
//!
//! The vectors, the framing and the scenarios are documented for runtime
//! authors in `verify/README.md`.

pub mod framing;
pub mod judge;
pub mod reference;
pub mod runner;
pub mod scenarios;
pub mod vectors;

/// The files under `vectors/`, built into the binary by `build.rs`.
mod embedded {
    include!(concat!(env!("OUT_DIR"), "/vectors.rs"));
}
