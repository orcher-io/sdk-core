//! Workflow activations, handed to the language by pull, as the kernel
//! protocol's messages.
//!
//! The language asks for its next activation, runs the workflow's code
//! against the resolved history it carries, and answers with every durable
//! call the code made. The kernel resolves the history on the way out
//! ([`orcher_sdk_core::kernel::resolve`]) and turns the answer back into
//! commands on the way in ([`orcher_sdk_core::kernel::execution_result`]).
//! The language never sees the journal.

use std::collections::HashMap;
use std::sync::Mutex;

use orcher_sdk_core::kernel;
use orcher_sdk_core::proto::kernel::v1::{RunActivation, RunActivationResult};
use orcher_sdk_core::worker::{WorkflowWork, WorkflowWorkResult};
use orcher_sdk_core::{Error, Result};
use tokio::sync::mpsc;

/// An activation the language holds, and what its answer is sent with.
struct Held {
    workflow_id: String,
    run_id: String,
    task_token: Vec<u8>,
    stream_entry_id: Option<String>,
}

/// The host's side of the workflow driver.
pub(crate) struct Workflows {
    namespace: String,
    work: tokio::sync::Mutex<mpsc::Receiver<WorkflowWork>>,
    results: mpsc::Sender<WorkflowWorkResult>,
    held: Mutex<HashMap<Vec<u8>, Held>>,
}

impl Workflows {
    pub(crate) fn new(
        namespace: String,
        work: mpsc::Receiver<WorkflowWork>,
        results: mpsc::Sender<WorkflowWorkResult>,
    ) -> Self {
        Self {
            namespace,
            work: tokio::sync::Mutex::new(work),
            results,
            held: Mutex::new(HashMap::new()),
        }
    }

    /// The next activation, once there is one; `None` once the driver has
    /// stopped and handed over everything it took.
    pub(crate) async fn next(&self) -> Option<RunActivation> {
        let work = self.work.lock().await.recv().await?;
        let activation = kernel::resolve(&work.task, &self.namespace);
        self.held_activations().insert(
            activation.activation_id.clone(),
            Held {
                workflow_id: work.task.execution.workflow_id.clone(),
                run_id: work.task.execution.run_id.clone(),
                task_token: work.task.task_token.clone(),
                stream_entry_id: work.stream_entry_id.clone(),
            },
        );
        Some(activation)
    }

    /// Sends the language's answer to an activation it holds.
    ///
    /// An answer the kernel cannot carry is not the run's failure: the
    /// activation is handed back to the engine, for this or another worker.
    pub(crate) async fn complete(&self, answer: RunActivationResult) -> Result<()> {
        let Some(held) = self.held_activations().remove(&answer.activation_id) else {
            return Err(Error::Protocol(
                "no activation with this id is held: it was already answered, or the worker \
                 stopped"
                    .to_string(),
            ));
        };
        let result = kernel::execution_result(&held.run_id, answer);
        self.results
            .send(WorkflowWorkResult {
                workflow_id: held.workflow_id,
                run_id: held.run_id,
                task_token: held.task_token,
                stream_entry_id: held.stream_entry_id,
                result,
            })
            .await
            .map_err(|_| Error::WorkerError("the workflow driver has stopped".to_string()))
    }

    fn held_activations(&self) -> std::sync::MutexGuard<'_, HashMap<Vec<u8>, Held>> {
        self.held
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
    }
}
