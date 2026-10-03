//! Heartbeats for the tasks a worker is running.
//!
//! A task the engine has handed out stays `started` until its outcome is
//! reported. If the worker dies while it runs, or the poll response carrying
//! it never arrives, nothing reports it, and the engine has only the task's
//! timeouts to go on. A task with no timeouts would never be recovered, and
//! the workflow waiting on it would wait forever.
//!
//! So the task driver heartbeats every task it hands to the language SDK on
//! its own, from the moment it hands the task over until the result is sent
//! or the driver stops, at a third or less of the task's heartbeat timeout.
//! It tells the engine it does so ([`TaskCapabilities::auto_heartbeat`] on
//! each poll), and the engine then gives a task that sets no heartbeat
//! timeout a default one: a task that nothing heartbeats any longer is timed
//! out once that passes, and retried under its retry policy. A long task
//! whose code never heartbeats is kept alive by the driver's heartbeats,
//! however long it runs.
//!
//! The task's own code can still heartbeat, with details, through the
//! [`TaskHeartbeat`] handed over with it. Its heartbeats are folded into the
//! same stream: sent immediately if none went out for a period, otherwise with
//! the next one due, carrying the latest details, so a task that reports
//! progress in a tight loop sends no more than the timer would.
//!
//! A heartbeat's answer is how the engine asks a task to stop: when it says
//! so, or says the attempt is no longer running (timed out, cancelled, or
//! finished elsewhere), the handle's cancellation token is cancelled. The
//! task decides what to do with that; its heartbeats go on until it reports
//! an outcome, so one that is winding down is not timed out meanwhile —
//! except after the attempt is gone, when there is nothing left to keep
//! alive.
//!
//! [`TaskCapabilities::auto_heartbeat`]: crate::proto::orcher::v1::TaskCapabilities

use std::collections::HashMap;
use std::fmt;
use std::sync::{Arc, Weak};
use std::time::Duration;

use parking_lot::Mutex;
use tokio::sync::Notify;
use tokio::time::Instant;
use tonic::Code;

pub use tokio_util::sync::CancellationToken;

use crate::poller::channel::{Breaker, ChannelManager};
use crate::poller::completion::{note_connection_failure, Caller};
use crate::poller::polling::TaskExecutionTask;
use crate::proto::orcher::v1::{
    execution_service_client::ExecutionServiceClient, RecordTaskHeartbeatRequest,
};

/// The longest wait between two heartbeats of a task, whatever its heartbeat timeout.
///
/// This bounds how long the engine's request to cancel a task can take to
/// reach it, and how long progress a task reports can sit unsent.
pub const MAX_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(30);

/// The shortest wait between two heartbeats of a task, however short its
/// heartbeat timeout.
const MIN_HEARTBEAT_INTERVAL: Duration = Duration::from_millis(100);

/// The longest a single heartbeat call may take.
const MAX_CALL_TIMEOUT: Duration = Duration::from_secs(10);

/// How long a task goes on being heartbeated once the language SDK has let go
/// of every handle to it, while waiting for its result to reach the driver. A
/// language SDK hands the result back and then drops the handle, and the
/// driver may take the result off its channel a little later; a task let go
/// of with no result coming — its handler panicked, say — stops being
/// heartbeated after this, so the engine times it out and retries it.
const RELEASE_GRACE: Duration = Duration::from_secs(2);

/// Returns how often a task with the given heartbeat timeout is heartbeated.
///
/// The interval is a third of the timeout, so that one lost or late heartbeat
/// still leaves another inside the timeout, and never more than
/// [`MAX_HEARTBEAT_INTERVAL`]. Returns `None` when the task has no heartbeat
/// timeout (or a zero one), since the engine then has none to enforce.
pub fn heartbeat_interval(heartbeat_timeout: Option<Duration>) -> Option<Duration> {
    heartbeat_timeout
        .filter(|timeout| !timeout.is_zero())
        .map(|timeout| (timeout / 3).clamp(MIN_HEARTBEAT_INTERVAL, MAX_HEARTBEAT_INTERVAL))
}

/// The heartbeat of one task attempt this worker is running, handed to the
/// language SDK with the task.
///
/// The driver heartbeats the task on its own for as long as it runs; this
/// handle is for the task's code, to report progress
/// ([`record`](Self::record)) and to learn when the engine wants it to stop
/// ([`cancellation_token`](Self::cancellation_token)).
///
/// Keep it until the task's result is handed back. Heartbeating stops once
/// the result has reached the engine, or shortly after every clone of the
/// handle is dropped with no result handed back: a task that is in fact still
/// running is then timed out by the engine.
#[derive(Clone)]
pub struct TaskHeartbeat {
    guard: Arc<Guard>,
}

/// Tells the heartbeat when the last handle goes.
struct Guard {
    beat: Arc<Beat>,
}

impl Drop for Guard {
    fn drop(&mut self) {
        self.beat.released.cancel();
    }
}

struct Beat {
    task_id: String,
    workflow_id: String,
    token: Vec<u8>,
    heartbeat_timeout: Option<Duration>,
    /// Details the task's code recorded that have not been sent yet.
    pending: Mutex<Option<Vec<u8>>>,
    /// Woken when the task's code records a heartbeat.
    recorded: Notify,
    /// Cancelled when the engine asks the task to stop, or says the attempt
    /// is no longer running.
    cancel: CancellationToken,
    /// Cancelled when heartbeating stops: the result reached the engine, the
    /// driver stopped, or the last handle was dropped with no result.
    finished: CancellationToken,
    /// Cancelled when the language SDK drops the last handle.
    released: CancellationToken,
    /// Set once the driver has the task's result: from then on it is
    /// heartbeated until the result is delivered, handles or not.
    reporting: std::sync::atomic::AtomicBool,
}

impl TaskHeartbeat {
    /// Creates a handle that sends nothing.
    ///
    /// For a task that no driver heartbeats: one built by hand, or run where
    /// there is no engine. The handle reports itself finished from the start.
    pub fn detached() -> Self {
        let beat = Arc::new(Beat::new(&TaskExecutionTask::detached()));
        beat.finished.cancel();
        beat.released.cancel();
        Self {
            guard: Arc::new(Guard { beat }),
        }
    }

    /// Records a heartbeat from the task's code, meaning it is still making progress.
    ///
    /// `details`, when given, say how far it has got; `None` keeps the
    /// details last recorded.
    ///
    /// Never waits. The heartbeat goes out immediately if none has gone out for
    /// a heartbeat interval, and otherwise with the next one due, carrying
    /// the latest details: however often this is called, no more heartbeats
    /// are sent than the timer sends on its own.
    pub fn record(&self, details: Option<Vec<u8>>) {
        let beat = &self.guard.beat;
        if beat.finished.is_cancelled() {
            return;
        }
        if let Some(details) = details {
            *beat.pending.lock() = Some(details);
        }
        beat.recorded.notify_one();
    }

    /// Returns a token that is cancelled when the engine wants the task to stop.
    ///
    /// That happens when the engine asks the task to stop (its workflow was
    /// cancelled or terminated), or answers that the attempt is no longer
    /// running: it timed out, or was cancelled or finished elsewhere. The
    /// task should stop soon after and report the outcome it has.
    pub fn cancellation_token(&self) -> CancellationToken {
        self.guard.beat.cancel.clone()
    }

    /// Returns whether [`cancellation_token`](Self::cancellation_token) is cancelled.
    pub fn is_cancelled(&self) -> bool {
        self.guard.beat.cancel.is_cancelled()
    }

    /// Returns the heartbeat timeout the engine holds this attempt to.
    ///
    /// This is the task's own, or the engine's default for a worker that
    /// heartbeats on its own. `None` when there is none.
    pub fn heartbeat_timeout(&self) -> Option<Duration> {
        self.guard.beat.heartbeat_timeout
    }

    /// Returns whether this handle's heartbeats have stopped.
    pub fn is_finished(&self) -> bool {
        self.guard.beat.finished.is_cancelled()
    }
}

impl fmt::Debug for TaskHeartbeat {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        // Never the token: it is a credential.
        let beat = &self.guard.beat;
        f.debug_struct("TaskHeartbeat")
            .field("task_id", &beat.task_id)
            .field("workflow_id", &beat.workflow_id)
            .field("heartbeat_timeout", &beat.heartbeat_timeout)
            .field("cancelled", &beat.cancel.is_cancelled())
            .field("finished", &beat.finished.is_cancelled())
            .finish()
    }
}

impl Beat {
    fn new(task: &TaskExecutionTask) -> Self {
        Self {
            task_id: task.task_id.clone(),
            workflow_id: task.execution.workflow_id.clone(),
            token: task.task_token.clone(),
            heartbeat_timeout: task.heartbeat_timeout,
            pending: Mutex::new(None),
            recorded: Notify::new(),
            cancel: CancellationToken::new(),
            finished: CancellationToken::new(),
            released: CancellationToken::new(),
            reporting: std::sync::atomic::AtomicBool::new(false),
        }
    }
}

impl TaskExecutionTask {
    fn detached() -> Self {
        Self {
            execution: crate::types::WorkflowExecution {
                workflow_id: String::new(),
                run_id: String::new(),
            },
            task_id: String::new(),
            task_type: String::new(),
            task_queue: String::new(),
            input: vec![],
            task_token: vec![],
            attempt: 0,
            start_to_close_timeout: None,
            heartbeat_timeout: None,
        }
    }
}

/// The heartbeats of the tasks one task driver is running.
#[derive(Clone)]
pub(crate) struct TaskHeartbeats {
    inner: Arc<Registry>,
}

struct Registry {
    caller: Arc<Caller>,
    channel_manager: Arc<ChannelManager>,
    /// Heartbeat every task on a timer, not only when its code asks.
    auto: bool,
    /// The heartbeats running, by task token.
    running: Mutex<HashMap<Vec<u8>, Weak<Beat>>>,
    /// Cancelled when the driver stops: every heartbeat stops with it.
    stopped: CancellationToken,
}

impl fmt::Debug for TaskHeartbeats {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("TaskHeartbeats")
            .field("auto", &self.inner.auto)
            .field("running", &self.inner.running.lock().len())
            .finish()
    }
}

impl TaskHeartbeats {
    pub(crate) fn new(caller: Caller, channel_manager: Arc<ChannelManager>, auto: bool) -> Self {
        Self {
            inner: Arc::new(Registry {
                caller: Arc::new(caller),
                channel_manager,
                auto,
                running: Mutex::new(HashMap::new()),
                stopped: CancellationToken::new(),
            }),
        }
    }

    /// Whether every task is heartbeated on a timer, which is what the
    /// driver's polls tell the engine.
    pub(crate) fn auto(&self) -> bool {
        self.inner.auto
    }

    /// Start heartbeating `task`, which is being handed to the language SDK.
    pub(crate) fn start(&self, task: &TaskExecutionTask) -> TaskHeartbeat {
        let beat = Arc::new(Beat::new(task));
        if self.inner.stopped.is_cancelled() || task.task_token.is_empty() {
            beat.finished.cancel();
        } else {
            let previous = self
                .inner
                .running
                .lock()
                .insert(task.task_token.clone(), Arc::downgrade(&beat));
            // The same attempt handed over twice: the older stream stops, so
            // the attempt has only one.
            if let Some(previous) = previous.and_then(|p| p.upgrade()) {
                previous.finished.cancel();
            }
            tokio::spawn(beat_until_finished(
                Arc::clone(&self.inner),
                Arc::clone(&beat),
            ));
        }
        TaskHeartbeat {
            guard: Arc::new(Guard { beat }),
        }
    }

    /// The driver has the result of the attempt with `token` and is sending
    /// it: heartbeat it until [`finish`](Self::finish), whether or not the
    /// language SDK still holds a handle.
    pub(crate) fn reporting(&self, token: &[u8]) {
        let beat = self.inner.running.lock().get(token).and_then(Weak::upgrade);
        if let Some(beat) = beat {
            beat.reporting
                .store(true, std::sync::atomic::Ordering::SeqCst);
        }
    }

    /// Stop heartbeating the attempt with `token`: its result has reached the
    /// engine, or been given up on.
    pub(crate) fn finish(&self, token: &[u8]) {
        let beat = self.inner.running.lock().remove(token);
        if let Some(beat) = beat.and_then(|b| b.upgrade()) {
            beat.finished.cancel();
        }
    }

    /// Stop every heartbeat: the driver is stopping.
    pub(crate) fn stop_all(&self) {
        self.inner.stopped.cancel();
        for (_, beat) in self.inner.running.lock().drain() {
            if let Some(beat) = beat.upgrade() {
                beat.finished.cancel();
            }
        }
    }
}

/// Heartbeat `beat` until it is finished or the driver stops.
async fn beat_until_finished(registry: Arc<Registry>, beat: Arc<Beat>) {
    let interval = heartbeat_interval(beat.heartbeat_timeout);
    // Heartbeats the task's code records are sent no closer together than
    // this; with no timeout to pace them, at the longest interval.
    let period = interval.unwrap_or(MAX_HEARTBEAT_INTERVAL);
    let timer = if registry.auto { interval } else { None };

    let mut last_sent: Option<Instant> = None;
    // The details sent last, sent again with every heartbeat that has none of
    // its own: an engine records each heartbeat's details in place of the
    // last, and one carrying none would clear what the task reported.
    let mut last_details: Option<Vec<u8>> = None;
    // The first timed heartbeat is one interval in: a task that finishes
    // sooner sends none.
    let mut next: Option<Instant> = timer.map(|t| Instant::now() + t);
    // When the language SDK let go of the last handle, the time by which the
    // driver must have its result.
    let mut release_by: Option<Instant> = None;
    let mut released = false;

    loop {
        let due = async {
            match next {
                Some(at) => tokio::time::sleep_until(at).await,
                None => std::future::pending().await,
            }
        };
        let let_go = async {
            match release_by {
                Some(at) => tokio::time::sleep_until(at).await,
                None => std::future::pending().await,
            }
        };
        tokio::select! {
            biased;
            _ = beat.finished.cancelled() => break,
            _ = registry.stopped.cancelled() => break,
            _ = beat.released.cancelled(), if !released => {
                released = true;
                release_by = Some(Instant::now() + RELEASE_GRACE);
                continue;
            }
            _ = let_go => {
                // The driver has its result: keep heartbeating until it is delivered.
                if beat.reporting.load(std::sync::atomic::Ordering::SeqCst) {
                    release_by = None;
                    continue;
                }
                tracing::debug!(
                    task_id = %beat.task_id,
                    workflow_id = %beat.workflow_id,
                    "Task let go of with no result; no longer heartbeating it"
                );
                break;
            }
            _ = beat.recorded.notified() => {
                // Send immediately if nothing went out for a period, otherwise
                // when the period is up.
                let now = Instant::now();
                let earliest = last_sent.map_or(now, |sent| (sent + period).max(now));
                next = Some(next.map_or(earliest, |at| at.min(earliest)));
                continue;
            }
            _ = due => {}
        }

        let details = beat.pending.lock().take().or_else(|| last_details.clone());
        let sent = tokio::select! {
            biased;
            _ = beat.finished.cancelled() => break,
            _ = registry.stopped.cancelled() => break,
            sent = send(&registry, &beat, details.clone(), period) => sent,
        };
        let now = Instant::now();
        last_sent = Some(now);
        match sent {
            Ok(cancel_requested) => {
                last_details = details;
                if cancel_requested && !beat.cancel.is_cancelled() {
                    tracing::info!(
                        task_id = %beat.task_id,
                        workflow_id = %beat.workflow_id,
                        "The engine asked the task to stop; cancelling it"
                    );
                    beat.cancel.cancel();
                }
            }
            // The engine no longer runs this attempt: it timed out, was
            // cancelled, or finished elsewhere. Nothing is left to keep
            // alive, and the task should stop.
            Err(code) if matches!(code, Code::FailedPrecondition | Code::NotFound) => {
                tracing::info!(
                    task_id = %beat.task_id,
                    workflow_id = %beat.workflow_id,
                    code = ?code,
                    "The engine no longer runs this task attempt; cancelling it"
                );
                beat.cancel.cancel();
                break;
            }
            // Retry with the next heartbeat, carrying these details unless
            // newer ones were recorded meanwhile.
            Err(_) => {
                if let Some(details) = details {
                    beat.pending.lock().get_or_insert(details);
                }
            }
        }
        next = match timer {
            Some(t) => Some(now + t),
            None if beat.pending.lock().is_some() => Some(now + period),
            None => None,
        };
    }

    let mut running = registry.running.lock();
    if running
        .get(&beat.token)
        .is_some_and(|b| b.as_ptr() == Arc::as_ptr(&beat))
    {
        running.remove(&beat.token);
    }
}

/// Send one heartbeat. `Ok` carries whether the engine asked the task to
/// stop; `Err` the code of a call that failed.
async fn send(
    registry: &Registry,
    beat: &Beat,
    details: Option<Vec<u8>>,
    period: Duration,
) -> std::result::Result<bool, Code> {
    let (channel, generation) = match registry.channel_manager.connection(Breaker::Respect).await {
        Ok(connected) => connected,
        Err(_) => {
            tracing::debug!(
                task_id = %beat.task_id,
                workflow_id = %beat.workflow_id,
                "No connection for a task heartbeat; trying again with the next"
            );
            return Err(Code::Unavailable);
        }
    };
    let mut request = crate::poller::credentials::credentialed_request(
        RecordTaskHeartbeatRequest {
            task_token: beat.token.clone(),
            namespace: registry.caller.namespace.clone(),
            identity: registry.caller.identity.clone(),
            details: details.unwrap_or_default(),
        },
        registry.caller.api_key.as_deref(),
        registry.caller.organization_id.as_deref(),
    );
    request.set_timeout(period.min(MAX_CALL_TIMEOUT));
    match crate::limits::sized!(
        ExecutionServiceClient::new(channel),
        registry.channel_manager.max_message_bytes()
    )
    .record_task_heartbeat(request)
    .await
    {
        Ok(response) => Ok(response.into_inner().cancel_requested),
        Err(status) => {
            note_connection_failure(&registry.channel_manager, &status, generation);
            tracing::debug!(
                task_id = %beat.task_id,
                workflow_id = %beat.workflow_id,
                code = ?status.code(),
                error = %status.message(),
                "Task heartbeat failed"
            );
            Err(status.code())
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn a_task_is_heartbeated_at_a_third_of_its_timeout_within_bounds() {
        assert_eq!(heartbeat_interval(None), None);
        assert_eq!(heartbeat_interval(Some(Duration::ZERO)), None);
        assert_eq!(
            heartbeat_interval(Some(Duration::from_secs(60))),
            Some(Duration::from_secs(20))
        );
        assert_eq!(
            heartbeat_interval(Some(Duration::from_secs(3600))),
            Some(MAX_HEARTBEAT_INTERVAL)
        );
        assert_eq!(
            heartbeat_interval(Some(Duration::from_millis(30))),
            Some(MIN_HEARTBEAT_INTERVAL)
        );
    }

    #[test]
    fn a_detached_handle_sends_nothing_and_says_so() {
        let handle = TaskHeartbeat::detached();
        handle.record(Some(b"progress".to_vec()));
        assert!(handle.is_finished());
        assert!(!handle.is_cancelled());
        assert_eq!(handle.heartbeat_timeout(), None);
    }
}
