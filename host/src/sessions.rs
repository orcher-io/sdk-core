//! Worker sessions, run inside the host.
//!
//! A session pins a series of tasks to one worker: the workflow runs the
//! built-in session creation task, the worker that takes it starts polling a
//! queue only it polls and answers with that queue's name, and the
//! workflow sends the session's tasks there until it runs the built-in
//! completion task. Both built-in tasks are handled here and never reach the
//! language; a task on a session queue reaches it like any other task.
//!
//! The wire is what the three SDKs' workflow code already sends and reads:
//! the creation task's input names the session and its timeouts in
//! milliseconds, and its result is the session's `SessionInfo` as JSON.

use std::collections::HashMap;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use orcher_sdk_core::worker::{
    CancellationToken, SessionQueueChange, TaskWork, TaskWorkResult, SESSION_COMPLETE_TASK,
    SESSION_CREATE_TASK,
};
use orcher_sdk_core::TaskFailure;
use serde::{Deserialize, Serialize};
use tokio::sync::{mpsc, Semaphore};

/// How much earlier than its creation timeout a session creation gives up
/// waiting for a slot, so the workflow learns why before the engine times
/// the task out.
pub const SLOT_WAIT_MARGIN: Duration = Duration::from_millis(500);

/// The failure type a session creation that found no free slot reports.
pub const NO_SESSION_SLOT: &str = "ResourceExhausted";

/// What the workflow sends the creation task. Missing fields take the
/// defaults the SDKs document.
#[derive(Debug, Deserialize)]
#[serde(default)]
struct CreateSession {
    session_id: String,
    creation_timeout_ms: u64,
    execution_timeout_ms: u64,
}

impl Default for CreateSession {
    fn default() -> Self {
        Self {
            session_id: String::new(),
            creation_timeout_ms: 30_000,
            execution_timeout_ms: 600_000,
        }
    }
}

/// What the workflow sends the completion task.
#[derive(Debug, Default, Deserialize)]
#[serde(default)]
struct CompleteSession {
    session_id: String,
}

/// The creation task's result. `state` is spelled `Open`, which every SDK's
/// workflow code accepts: the Rust SDK decodes it strictly, the Python and
/// TypeScript SDKs ignore it.
#[derive(Debug, Serialize)]
struct SessionInfo<'a> {
    session_id: &'a str,
    session_queue: &'a str,
    worker_identity: &'a str,
    state: &'static str,
}

/// The sessions one worker hosts.
pub(crate) struct Sessions {
    /// Names this worker's session queue, `{task_queue}__session__{id}`;
    /// fresh for each host, so a restarted worker never polls the queue of
    /// sessions it no longer holds.
    resource_id: String,
    max_sessions: usize,
    slots: Arc<Semaphore>,
    /// Each open session, by id: which opening it was, and what ends it.
    open: Mutex<HashMap<String, (u64, CancellationToken)>>,
    openings: AtomicU64,
    queue_changes: mpsc::UnboundedSender<SessionQueueChange>,
    results: mpsc::Sender<TaskWorkResult>,
}

impl Sessions {
    pub(crate) fn new(
        max_sessions: usize,
        queue_changes: mpsc::UnboundedSender<SessionQueueChange>,
        results: mpsc::Sender<TaskWorkResult>,
    ) -> Self {
        Self {
            resource_id: uuid::Uuid::new_v4().to_string(),
            max_sessions,
            slots: Arc::new(Semaphore::new(max_sessions)),
            open: Mutex::new(HashMap::new()),
            openings: AtomicU64::new(0),
            queue_changes,
            results,
        }
    }

    /// Whether `task_type` is one of the built-in session tasks.
    pub(crate) fn handles(task_type: &str) -> bool {
        task_type == SESSION_CREATE_TASK || task_type == SESSION_COMPLETE_TASK
    }

    /// Runs a built-in session task and reports its outcome. Creation can
    /// wait for a slot, so this runs on its own task; the caller does not
    /// wait for it.
    pub(crate) fn run(self: &Arc<Self>, work: TaskWork) {
        let sessions = Arc::clone(self);
        tokio::spawn(async move {
            let task = &work.task;
            let result = if task.task_type == SESSION_CREATE_TASK {
                sessions.create(&task.input, &task.task_queue).await
            } else {
                sessions.complete(&task.input)
            };
            let _ = sessions
                .results
                .send(TaskWorkResult {
                    task_token: task.task_token.clone(),
                    result: result.map_err(Into::into),
                })
                .await;
            // Heartbeated until the outcome has been handed back.
            drop(work);
        });
    }

    async fn create(
        self: &Arc<Self>,
        input: &[u8],
        task_queue: &str,
    ) -> Result<Vec<u8>, TaskFailure> {
        let request: CreateSession = decode(input)?;
        let wait =
            Duration::from_millis(request.creation_timeout_ms).saturating_sub(SLOT_WAIT_MARGIN);
        let permit = match tokio::time::timeout(wait, self.slots.clone().acquire_owned()).await {
            Ok(Ok(permit)) => permit,
            _ => {
                return Err(TaskFailure::new(format!(
                    "no session slot came free in time ({} of {} in use)",
                    self.max_sessions - self.slots.available_permits(),
                    self.max_sessions
                ))
                .with_type(NO_SESSION_SLOT))
            }
        };

        let session_queue = format!("{task_queue}__session__{}", self.resource_id);
        let end = CancellationToken::new();
        let opening = self.openings.fetch_add(1, Ordering::Relaxed);
        let replaced = self
            .open_sessions()
            .insert(request.session_id.clone(), (opening, end.clone()));
        if let Some((_, replaced)) = replaced {
            // A retried creation of a session already open here: the retry
            // takes over, and the earlier one ends.
            replaced.cancel();
        }
        let _ = self
            .queue_changes
            .send(SessionQueueChange::Add(session_queue.clone()));
        tracing::info!(
            session_id = %request.session_id,
            %session_queue,
            "Session opened"
        );

        // The session holds its slot until it ends: completed, past its
        // execution timeout, or the worker stopping.
        let sessions = Arc::clone(self);
        let session_id = request.session_id.clone();
        let queue = session_queue.clone();
        let lifetime = Duration::from_millis(request.execution_timeout_ms);
        tokio::spawn(async move {
            let timed_out = tokio::select! {
                _ = end.cancelled() => false,
                _ = tokio::time::sleep(lifetime) => true,
            };
            if timed_out {
                let mut open = sessions.open_sessions();
                if open
                    .get(&session_id)
                    .is_some_and(|(open, _)| *open == opening)
                {
                    open.remove(&session_id);
                }
                tracing::info!(%session_id, "Session closed: past its execution timeout");
            }
            let _ = sessions
                .queue_changes
                .send(SessionQueueChange::Remove(queue));
            drop(permit);
        });

        serde_json::to_vec(&SessionInfo {
            session_id: &request.session_id,
            session_queue: &session_queue,
            worker_identity: &self.resource_id,
            state: "Open",
        })
        .map_err(|error| TaskFailure::new(format!("could not encode the session: {error}")))
    }

    /// Ends a session. Ending one this worker does not hold (already ended,
    /// or a retried completion) succeeds.
    fn complete(&self, input: &[u8]) -> Result<Vec<u8>, TaskFailure> {
        let request: CompleteSession = decode(input)?;
        match self.open_sessions().remove(&request.session_id) {
            Some((_, end)) => {
                end.cancel();
                tracing::info!(session_id = %request.session_id, "Session completed");
            }
            None => tracing::debug!(
                session_id = %request.session_id,
                "Completion of a session not open on this worker"
            ),
        }
        Ok(b"{}".to_vec())
    }

    /// Ends every session, when the worker stops.
    pub(crate) fn close_all(&self) {
        let open = std::mem::take(&mut *self.open_sessions());
        if !open.is_empty() {
            tracing::info!(count = open.len(), "Closing the worker's sessions");
        }
        for (_, end) in open.into_values() {
            end.cancel();
        }
    }

    fn open_sessions(
        &self,
    ) -> std::sync::MutexGuard<'_, HashMap<String, (u64, CancellationToken)>> {
        self.open
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}

fn decode<T: Default + for<'de> Deserialize<'de>>(input: &[u8]) -> Result<T, TaskFailure> {
    if input.is_empty() {
        return Ok(T::default());
    }
    serde_json::from_slice(input).map_err(|error| {
        TaskFailure::new(format!("the session task's input is not valid: {error}"))
            .with_non_retryable(true)
    })
}
