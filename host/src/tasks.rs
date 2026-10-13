//! Tasks, handed to the language by pull.
//!
//! The language asks for its next task when it has room to run one, reports
//! the task's heartbeats while it runs, and hands back its outcome. The host
//! keeps a registry of the tasks the language holds, so a heartbeat or a
//! cancellation wait finds its task, and a task the language reported stops
//! being heartbeated for. The built-in session tasks are run by the host
//! ([`crate::sessions`]) and never handed over.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use orcher_sdk_core::proto::host::v1::{
    task_outcome, HeartbeatAnswer, TaskAssignment, TaskOutcome,
};
use orcher_sdk_core::proto::kernel::v1::{Payload, RunRef};
use orcher_sdk_core::proto::prost_types;
use orcher_sdk_core::worker::poll::polling::TaskExecutionTask;
use orcher_sdk_core::worker::{CancellationToken, TaskHeartbeat, TaskWork, TaskWorkResult};
use orcher_sdk_core::{Error, Result, TaskFailure};
use tokio::sync::mpsc;

use crate::sessions::Sessions;

/// A task the language holds.
struct Running {
    /// The task's heartbeat: what the language reports through, and how a
    /// cancellation request reaches it.
    heartbeat: TaskHeartbeat,
    /// Cancelled when the task's outcome is reported.
    reported: CancellationToken,
}

/// The host's side of the task driver.
pub(crate) struct Tasks {
    namespace: String,
    work: tokio::sync::Mutex<mpsc::Receiver<TaskWork>>,
    results: mpsc::Sender<TaskWorkResult>,
    running: Mutex<HashMap<Vec<u8>, Running>>,
    sessions: Arc<Sessions>,
}

impl Tasks {
    pub(crate) fn new(
        namespace: String,
        work: mpsc::Receiver<TaskWork>,
        results: mpsc::Sender<TaskWorkResult>,
        sessions: Sessions,
    ) -> Self {
        Self {
            namespace,
            work: tokio::sync::Mutex::new(work),
            results,
            running: Mutex::new(HashMap::new()),
            sessions: Arc::new(sessions),
        }
    }

    /// The worker's sessions.
    pub(crate) fn sessions(&self) -> &Sessions {
        &self.sessions
    }

    /// The next task to run, once there is one; `None` once the driver has
    /// stopped and handed over everything it took.
    pub(crate) async fn next(&self) -> Option<TaskAssignment> {
        let work = loop {
            let work = self.work.lock().await.recv().await?;
            if !Sessions::handles(&work.task.task_type) {
                break work;
            }
            self.sessions.run(work);
        };
        let assignment = self.assignment(&work.task);
        self.running_tasks().insert(
            work.task.task_token.clone(),
            Running {
                heartbeat: work.heartbeat,
                reported: CancellationToken::new(),
            },
        );
        Some(assignment)
    }

    /// Records a heartbeat for the task, with progress details, and says
    /// whether it has been asked to cancel. A task the host does not hold (it
    /// was already reported) is not asked to cancel.
    pub(crate) fn heartbeat(&self, token: &[u8], details: Option<Payload>) -> HeartbeatAnswer {
        let running = self.running_tasks();
        let Some(task) = running.get(token) else {
            return HeartbeatAnswer::default();
        };
        task.heartbeat.record(details.map(|payload| payload.data));
        HeartbeatAnswer {
            cancel_requested: task.heartbeat.is_cancelled(),
        }
    }

    /// Waits until the task is asked to cancel (`true`) or its outcome is
    /// reported (`false`). A task the host does not hold returns `false` at
    /// once.
    pub(crate) async fn cancelled(&self, token: &[u8]) -> bool {
        let (cancel, reported) = {
            let running = self.running_tasks();
            let Some(task) = running.get(token) else {
                return false;
            };
            (task.heartbeat.cancellation_token(), task.reported.clone())
        };
        tokio::select! {
            biased;
            _ = cancel.cancelled() => true,
            _ = reported.cancelled() => false,
        }
    }

    /// Reports the task's outcome. A failure goes to the engine as a
    /// [`TaskFailure`] with the type and retryability the language gave: it
    /// is the task's own failure, never an error of the host's.
    pub(crate) async fn complete(&self, outcome: TaskOutcome) -> Result<()> {
        let result = match outcome.result {
            Some(task_outcome::Result::Value(value)) => Ok(value.data),
            Some(task_outcome::Result::Failure(failure)) => {
                let mut reported =
                    TaskFailure::new(failure.message).with_non_retryable(failure.non_retryable);
                if !failure.error_type.is_empty() {
                    reported = reported.with_type(failure.error_type);
                }
                Err(reported.into())
            }
            None => {
                return Err(Error::Configuration(
                    "a task outcome must carry a value or a failure".to_string(),
                ))
            }
        };
        if let Some(task) = self.running_tasks().remove(&outcome.token) {
            task.reported.cancel();
        }
        self.results
            .send(TaskWorkResult {
                task_token: outcome.token,
                result,
            })
            .await
            .map_err(|_| Error::WorkerError("the task driver has stopped".to_string()))
    }

    fn running_tasks(&self) -> std::sync::MutexGuard<'_, HashMap<Vec<u8>, Running>> {
        self.running
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }

    fn assignment(&self, task: &TaskExecutionTask) -> TaskAssignment {
        TaskAssignment {
            token: task.task_token.clone(),
            task_id: task.task_id.clone(),
            task_type: task.task_type.clone(),
            run: Some(RunRef {
                workflow_id: task.execution.workflow_id.clone(),
                run_id: task.execution.run_id.clone(),
                task_queue: task.task_queue.clone(),
                namespace: self.namespace.clone(),
                ..Default::default()
            }),
            input: if task.input.is_empty() {
                vec![]
            } else {
                vec![Payload {
                    data: task.input.clone(),
                    metadata: [("encoding".to_string(), b"json".to_vec())].into(),
                }]
            },
            attempt: u32::try_from(task.attempt).unwrap_or(0),
            attempt_timeout: task.start_to_close_timeout.map(duration),
            heartbeat_timeout: task.heartbeat_timeout.map(duration),
            ..Default::default()
        }
    }
}

fn duration(d: std::time::Duration) -> prost_types::Duration {
    prost_types::Duration {
        seconds: d.as_secs() as i64,
        nanos: d.subsec_nanos() as i32,
    }
}
