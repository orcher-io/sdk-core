//! A run's journal and the replayer that checks workflow code against it,
//! step by step, before the language runs anything.
//!
//! ## Terminology
//!
//! - **Execution Journal**: the event-sourced history of a workflow run.
//! - **Execution Step**: one point at which workflow code runs and decides what to do next.

pub(crate) mod journal;
pub(crate) mod replayer;

#[cfg(test)]
mod replay_corpus_tests;
