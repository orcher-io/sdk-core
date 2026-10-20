//! Wire-level measurements of a worker's calls to the engine.
//!
//! Core measures what crosses the wire, the same way for every language SDK:
//! each RPC's calls, latency, outcome and retries, by method. Application
//! hooks and tracing stay in each SDK (see [`crate::interceptor`]).
//!
//! One [`WireMetrics`] belongs to one worker, shared by every connection the
//! worker opens; [`WireMetrics::snapshot`] copies it out for an SDK or an
//! exporter to report. Nothing here depends on a metrics library: a snapshot
//! is plain data an SDK hands to whatever it reports with.

use std::collections::BTreeMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use serde::{Deserialize, Serialize};

/// Upper bounds, in milliseconds, of the latency histogram's buckets; the
/// last bucket holds everything slower than the last bound.
pub const LATENCY_BUCKETS_MS: [u64; 12] =
    [1, 5, 10, 25, 50, 100, 250, 500, 1_000, 2_500, 5_000, 10_000];

/// A worker's wire measurements, shared by all its connections.
#[derive(Debug, Default)]
pub struct WireMetrics {
    methods: parking_lot::Mutex<BTreeMap<String, MethodStats>>,
    work: WorkCounters,
}

/// The work that crossed the wire, counted as it is handed to the language
/// and as its outcome is sent.
#[derive(Debug, Default)]
struct WorkCounters {
    activations: AtomicU64,
    activation_journal_entries: AtomicU64,
    workflow_reports: AtomicU64,
    workflow_commands: AtomicU64,
    workflow_failures: AtomicU64,
    tasks: AtomicU64,
    task_input_bytes: AtomicU64,
    task_reports: AtomicU64,
    task_result_bytes: AtomicU64,
    task_failures: AtomicU64,
}

/// The work a worker has handed to its language and reported back, as
/// [`WireMetrics::snapshot`] reports it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct WorkStats {
    /// Workflow activations handed to the language.
    pub activations: u64,
    /// Journal entries those activations carried.
    pub activation_journal_entries: u64,
    /// Workflow activation outcomes sent: completions and failures.
    pub workflow_reports: u64,
    /// Commands those completions carried.
    pub workflow_commands: u64,
    /// Workflow activation outcomes sent as failures.
    pub workflow_failures: u64,
    /// Tasks handed to the language.
    pub tasks: u64,
    /// Bytes of those tasks' input.
    pub task_input_bytes: u64,
    /// Task outcomes sent: completions and failures.
    pub task_reports: u64,
    /// Bytes of the results those completions carried.
    pub task_result_bytes: u64,
    /// Task outcomes sent as failures.
    pub task_failures: u64,
}

/// What one RPC method has done, as [`WireMetrics::snapshot`] reports it.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct MethodStats {
    /// Calls sent.
    pub calls: u64,
    /// Calls the engine answered with a gRPC status other than OK, by code.
    pub status_errors: BTreeMap<i32, u64>,
    /// Calls that got no answer from the engine (connection or transport).
    pub transport_errors: u64,
    /// Calls sent again after an earlier attempt of the same call failed.
    pub retries: u64,
    /// Calls by latency, to the first response: one count per bucket of
    /// [`LATENCY_BUCKETS_MS`], and a last one for slower calls.
    pub latency_buckets: Vec<u64>,
    /// Sum of all calls' latencies, in microseconds.
    pub latency_total_us: u64,
}

/// A copy of a worker's wire measurements at one moment.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[non_exhaustive]
pub struct WireMetricsSnapshot {
    /// Each RPC method's measurements, by its full gRPC path
    /// (`/orcher.v1.ExecutionService/PollTaskExecution`).
    pub methods: BTreeMap<String, MethodStats>,
    /// The work the worker handed to its language and reported back.
    pub work: WorkStats,
}

/// How a call ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CallOutcome {
    /// The engine answered OK.
    Ok,
    /// The engine answered with this non-OK gRPC status code.
    Status(i32),
    /// No answer came back.
    Transport,
}

impl WireMetrics {
    /// A new, empty set of measurements for one worker.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Record one call of `method` that took `latency` and ended `outcome`.
    pub fn record_call(&self, method: &str, latency: Duration, outcome: CallOutcome) {
        let mut methods = self.methods.lock();
        // Looked up before inserting, so a call of a method already seen
        // allocates nothing.
        if !methods.contains_key(method) {
            methods.insert(method.to_string(), MethodStats::default());
        }
        let stats = methods.get_mut(method).expect("inserted above");
        if stats.latency_buckets.is_empty() {
            stats.latency_buckets = vec![0; LATENCY_BUCKETS_MS.len() + 1];
        }
        stats.calls += 1;
        let ms = latency.as_millis();
        let bucket = LATENCY_BUCKETS_MS
            .iter()
            .position(|bound| ms <= u128::from(*bound))
            .unwrap_or(LATENCY_BUCKETS_MS.len());
        stats.latency_buckets[bucket] += 1;
        stats.latency_total_us = stats
            .latency_total_us
            .saturating_add(u64::try_from(latency.as_micros()).unwrap_or(u64::MAX));
        match outcome {
            CallOutcome::Ok => {}
            CallOutcome::Status(code) => *stats.status_errors.entry(code).or_default() += 1,
            CallOutcome::Transport => stats.transport_errors += 1,
        }
    }

    /// Record that a call of `method` is being sent again after a failed
    /// attempt.
    pub fn record_retry(&self, method: &str) {
        let mut methods = self.methods.lock();
        methods.entry(method.to_string()).or_default().retries += 1;
    }

    /// Record a workflow activation with `journal_entries` entries handed
    /// to the language.
    pub fn record_activation(&self, journal_entries: usize) {
        let work = &self.work;
        work.activations.fetch_add(1, Ordering::Relaxed);
        work.activation_journal_entries
            .fetch_add(journal_entries as u64, Ordering::Relaxed);
    }

    /// Record a workflow activation's outcome sent: a completion carrying
    /// `commands` commands, or a failure.
    pub fn record_workflow_report(&self, commands: usize, failed: bool) {
        let work = &self.work;
        work.workflow_reports.fetch_add(1, Ordering::Relaxed);
        work.workflow_commands
            .fetch_add(commands as u64, Ordering::Relaxed);
        if failed {
            work.workflow_failures.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Record a task with `input_bytes` of input handed to the language.
    pub fn record_task(&self, input_bytes: usize) {
        let work = &self.work;
        work.tasks.fetch_add(1, Ordering::Relaxed);
        work.task_input_bytes
            .fetch_add(input_bytes as u64, Ordering::Relaxed);
    }

    /// Record a task's outcome sent: a completion with `result_bytes` of
    /// result, or a failure.
    pub fn record_task_report(&self, result_bytes: usize, failed: bool) {
        let work = &self.work;
        work.task_reports.fetch_add(1, Ordering::Relaxed);
        work.task_result_bytes
            .fetch_add(result_bytes as u64, Ordering::Relaxed);
        if failed {
            work.task_failures.fetch_add(1, Ordering::Relaxed);
        }
    }

    /// Copy the measurements out.
    pub fn snapshot(&self) -> WireMetricsSnapshot {
        let methods = self.methods.lock();
        WireMetricsSnapshot {
            methods: methods
                .iter()
                .map(|(method, stats)| {
                    let mut stats = stats.clone();
                    if stats.latency_buckets.is_empty() {
                        stats.latency_buckets = vec![0; LATENCY_BUCKETS_MS.len() + 1];
                    }
                    (method.clone(), stats)
                })
                .collect(),
            work: self.work_stats(),
        }
    }
}

impl WireMetrics {
    fn work_stats(&self) -> WorkStats {
        let w = &self.work;
        let load = |counter: &AtomicU64| counter.load(Ordering::Relaxed);
        WorkStats {
            activations: load(&w.activations),
            activation_journal_entries: load(&w.activation_journal_entries),
            workflow_reports: load(&w.workflow_reports),
            workflow_commands: load(&w.workflow_commands),
            workflow_failures: load(&w.workflow_failures),
            tasks: load(&w.tasks),
            task_input_bytes: load(&w.task_input_bytes),
            task_reports: load(&w.task_reports),
            task_result_bytes: load(&w.task_result_bytes),
            task_failures: load(&w.task_failures),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const POLL: &str = "/orcher.v1.ExecutionService/PollTaskExecution";

    #[test]
    fn a_call_is_counted_with_its_latency_and_outcome() {
        let wire = WireMetrics::new();
        wire.record_call(POLL, Duration::from_millis(3), CallOutcome::Ok);
        wire.record_call(POLL, Duration::from_millis(30), CallOutcome::Status(14));
        wire.record_call(POLL, Duration::from_secs(60), CallOutcome::Transport);

        let stats = &wire.snapshot().methods[POLL];
        assert_eq!(stats.calls, 3);
        assert_eq!(stats.status_errors, BTreeMap::from([(14, 1)]));
        assert_eq!(stats.transport_errors, 1);
        // 3 ms in the 5 ms bucket, 30 ms in the 50 ms bucket, 60 s past the last.
        assert_eq!(stats.latency_buckets[1], 1);
        assert_eq!(stats.latency_buckets[4], 1);
        assert_eq!(stats.latency_buckets[LATENCY_BUCKETS_MS.len()], 1);
        assert_eq!(stats.latency_total_us, 3_000 + 30_000 + 60_000_000);
    }

    #[test]
    fn a_retry_is_counted_against_its_method() {
        let wire = WireMetrics::new();
        wire.record_retry(POLL);
        let stats = &wire.snapshot().methods[POLL];
        assert_eq!(stats.retries, 1);
        assert_eq!(stats.calls, 0);
        assert_eq!(stats.latency_buckets.len(), LATENCY_BUCKETS_MS.len() + 1);
    }

    #[test]
    fn a_snapshot_is_plain_data() {
        let wire = WireMetrics::new();
        wire.record_call(POLL, Duration::from_millis(1), CallOutcome::Ok);
        let snapshot = wire.snapshot();
        let json = serde_json::to_string(&snapshot).unwrap();
        let back: WireMetricsSnapshot = serde_json::from_str(&json).unwrap();
        assert_eq!(back, snapshot);
    }

    #[test]
    fn work_is_counted_as_it_is_handed_over_and_reported() {
        let wire = WireMetrics::new();
        wire.record_activation(5);
        wire.record_workflow_report(3, false);
        wire.record_workflow_report(0, true);
        wire.record_task(120);
        wire.record_task_report(40, false);
        wire.record_task_report(0, true);
        assert_eq!(
            wire.snapshot().work,
            WorkStats {
                activations: 1,
                activation_journal_entries: 5,
                workflow_reports: 2,
                workflow_commands: 3,
                workflow_failures: 1,
                tasks: 1,
                task_input_bytes: 120,
                task_reports: 2,
                task_result_bytes: 40,
                task_failures: 1,
            }
        );
    }
}
