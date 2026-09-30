//! What a worker is currently doing, and how much it has done.
//!
//! The heartbeat carries these numbers, and the server writes them on the
//! worker's row, which is what a console shows as a worker's status and load.
//!
//! One instance is shared by the drivers that dispatch work and the driver
//! that heartbeats. The counters must be reachable from every dispatching
//! driver; if only the heartbeating driver held them, nothing would count and
//! a worker running a hundred tasks would look exactly like an idle one.

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Shared, cheap counters for one worker.
///
/// All counters use relaxed atomics: each is independent, and a heartbeat
/// only needs a recent value, not a consistent cut across counters.
#[derive(Debug, Default)]
pub struct WorkerMetrics {
    workflows_in_progress: AtomicU64,
    workflows_completed: AtomicU64,
    workflows_failed: AtomicU64,
    tasks_in_progress: AtomicU64,
    tasks_completed: AtomicU64,
    tasks_failed: AtomicU64,
}

/// A point-in-time copy of a worker's counters, to put in a heartbeat.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct WorkerMetricsSnapshot {
    /// Workflow activations handed to the language SDK and not yet returned.
    pub workflows_in_progress: u64,
    /// Workflow activations that returned successfully since the worker started.
    pub workflows_completed: u64,
    /// Workflow activations that returned a failure since the worker started.
    pub workflows_failed: u64,
    /// Tasks handed to the language SDK and not yet returned.
    pub tasks_in_progress: u64,
    /// Tasks that returned successfully since the worker started.
    pub tasks_completed: u64,
    /// Tasks that returned a failure since the worker started.
    pub tasks_failed: u64,
}

impl WorkerMetrics {
    /// Create a set of counters, all at zero.
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Record that a workflow activation was handed to the language SDK.
    pub fn workflow_started(&self) {
        self.workflows_in_progress.fetch_add(1, Ordering::Relaxed);
    }

    /// Record that a workflow activation came back.
    ///
    /// It leaves the in-progress count and is added to the completed total if
    /// `succeeded`, or to the failed total otherwise.
    pub fn workflow_finished(&self, succeeded: bool) {
        decrement(&self.workflows_in_progress);
        let total = if succeeded {
            &self.workflows_completed
        } else {
            &self.workflows_failed
        };
        total.fetch_add(1, Ordering::Relaxed);
    }

    /// Record that a task was handed to the language SDK.
    pub fn task_started(&self) {
        self.tasks_in_progress.fetch_add(1, Ordering::Relaxed);
    }

    /// Record that a task came back.
    ///
    /// It leaves the in-progress count and is added to the completed total if
    /// `succeeded`, or to the failed total otherwise.
    pub fn task_finished(&self, succeeded: bool) {
        decrement(&self.tasks_in_progress);
        let total = if succeeded {
            &self.tasks_completed
        } else {
            &self.tasks_failed
        };
        total.fetch_add(1, Ordering::Relaxed);
    }

    /// Read the current value of every counter.
    pub fn snapshot(&self) -> WorkerMetricsSnapshot {
        WorkerMetricsSnapshot {
            workflows_in_progress: self.workflows_in_progress.load(Ordering::Relaxed),
            workflows_completed: self.workflows_completed.load(Ordering::Relaxed),
            workflows_failed: self.workflows_failed.load(Ordering::Relaxed),
            tasks_in_progress: self.tasks_in_progress.load(Ordering::Relaxed),
            tasks_completed: self.tasks_completed.load(Ordering::Relaxed),
            tasks_failed: self.tasks_failed.load(Ordering::Relaxed),
        }
    }
}

/// Subtract one without wrapping past zero.
///
/// An in-progress count that wraps reads as eighteen quintillion, and the
/// console would show a worker that is doing nothing as saturated. A finish
/// without a matching start is a bug, but not one worth amplifying.
fn decrement(counter: &AtomicU64) {
    let _ = counter.fetch_update(Ordering::Relaxed, Ordering::Relaxed, |current| {
        Some(current.saturating_sub(1))
    });
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn counts_work_in_flight_and_totals() {
        let metrics = WorkerMetrics::new();
        metrics.task_started();
        metrics.task_started();
        metrics.workflow_started();
        assert_eq!(metrics.snapshot().tasks_in_progress, 2);
        assert_eq!(metrics.snapshot().workflows_in_progress, 1);

        metrics.task_finished(true);
        metrics.task_finished(false);
        metrics.workflow_finished(true);

        let snapshot = metrics.snapshot();
        assert_eq!(snapshot.tasks_in_progress, 0);
        assert_eq!(snapshot.tasks_completed, 1);
        assert_eq!(snapshot.tasks_failed, 1);
        assert_eq!(snapshot.workflows_in_progress, 0);
        assert_eq!(snapshot.workflows_completed, 1);
    }

    #[test]
    fn in_flight_never_wraps_below_zero() {
        let metrics = WorkerMetrics::new();
        metrics.task_finished(true);
        assert_eq!(metrics.snapshot().tasks_in_progress, 0);
    }
}
