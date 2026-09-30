//! Telling the engine a worker is going away, and handing back what it will
//! not run.
//!
//! Two calls, both available from engine 0.5.0 and both best-effort:
//!
//! - `ShutdownWorker`, sent once by each driver at the first sign of
//!   shutdown. The engine answers the driver's open long polls at once with
//!   nothing, on every replica, and hands its later polls nothing. Without
//!   it an idle driver sits in polls that answer only when work arrives, and
//!   takes its whole shutdown grace to stop.
//! - `ReleaseWorkflowExecution`, for an activation a poll brought back that
//!   the driver could not hand to the language SDK. The engine offers it to
//!   the next poll at once instead of after its claim times out. It names the
//!   activation by its token, so a release that arrives after the activation
//!   went to another worker takes nothing from that one.
//!
//! Neither may make a shutdown slower or fail it. Each is one attempt,
//! bounded by a short timeout; whatever goes wrong is logged and the driver
//! carries on as if it had never made the call. An engine older than 0.5.0
//! answers UNIMPLEMENTED, which is logged once, at debug, and is otherwise the
//! same as not having asked.
//!
//! # The process's instance id
//!
//! A shutdown names the worker by its identity, which a later process often
//! reuses: a fixed `service@hostname`, restarted in the same pod. So it also
//! names the process, by the id every poll from this process carries in
//! `worker_instance_id`, and the engine refuses work only to polls from this
//! process; a restarted one, with an id of its own, is served at once. A poll
//! naming no process is refused for up to five minutes after a shutdown of
//! its identity, since the engine cannot tell it is not the one stopping.
//!
//! The id is one per process rather than one per driver because that is what
//! the engine counts: it holds at most sixteen shutdowns of one identity at
//! once, which is sixteen processes this way and fewer if every driver in a
//! process took one. The cost is that drivers in one process sharing an
//! identity and a task queue stop together: the first to shut down ends the
//! others' polls on that queue too. That is acceptable because the drivers of
//! one worker stop together anyway. `RegisterWorkerRequest.service_id` is not
//! reused as the instance id: the language SDKs send their identity there,
//! which is not per process.

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::OnceLock;
use std::time::Duration;

use tonic::transport::Channel;
use tonic::{Code, Status};

use crate::poller::channel::{Breaker, ChannelManager};
use crate::poller::completion::Caller;
use crate::proto::orcher::v1::{
    execution_service_client::ExecutionServiceClient, ReleaseWorkflowExecutionRequest,
    ShutdownWorkerRequest,
};

/// The most task queues one shutdown may name. A driver polling more than
/// this names none, which the engine reads as every queue its credential may
/// poll: still only this identity's polls from this process.
const MAX_SHUTDOWN_QUEUES: usize = 16;

/// The longest a shutdown notice is waited for. It is one small write on the
/// engine; an engine that takes longer is struggling, and the driver stops
/// within its grace regardless.
pub(crate) const SHUTDOWN_NOTICE_TIMEOUT: Duration = Duration::from_secs(2);

/// Whether an engine without each call has already been logged, so only the
/// first finding is logged.
static SHUTDOWN_UNSUPPORTED_SAID: AtomicBool = AtomicBool::new(false);
static RELEASE_UNSUPPORTED_SAID: AtomicBool = AtomicBool::new(false);

/// Return this process's id, sent on every poll and with the shutdown notice.
///
/// The id is a random UUID generated on first call. Every later call, from
/// any driver in the process, returns the same value.
pub fn worker_instance_id() -> &'static str {
    static ID: OnceLock<String> = OnceLock::new();
    ID.get_or_init(|| uuid::Uuid::new_v4().to_string())
}

/// The queues to name in a driver's shutdown: the ones it polls, or none,
/// meaning all of them, when there are more than the engine takes.
pub(crate) fn shutdown_queues(mut task_queues: Vec<String>) -> Vec<String> {
    task_queues.sort();
    task_queues.dedup();
    if task_queues.len() > MAX_SHUTDOWN_QUEUES {
        task_queues.clear();
    }
    task_queues
}

/// Tell the engine this driver is shutting down, so it ends the driver's open
/// polls and hands it nothing more. One attempt, within `timeout`; never an
/// error.
pub(crate) async fn announce_shutdown(
    caller: &Caller,
    channel_manager: &ChannelManager,
    task_queues: Vec<String>,
    timeout: Duration,
) {
    let body = ShutdownWorkerRequest {
        namespace: caller.namespace.clone(),
        identity: caller.identity.clone(),
        worker_instance_id: worker_instance_id().to_string(),
        task_queues,
    };
    let sent = tokio::time::timeout(timeout, async {
        // Respect the breaker: a connection known to be down is not worth
        // waiting on for a notice the driver can do without.
        let (channel, _) = channel_manager
            .connection(Breaker::Respect)
            .await
            .map_err(|e| Status::unavailable(format!("{e:?}")))?;
        let mut request = crate::poller::credentials::credentialed_request(
            body,
            caller.api_key.as_deref(),
            caller.organization_id.as_deref(),
        );
        request.set_timeout(timeout);
        ExecutionServiceClient::new(channel)
            .shutdown_worker(request)
            .await
    })
    .await;
    match sent {
        Ok(Ok(_)) => tracing::debug!(
            identity = %caller.identity,
            "Told the engine this worker is shutting down; its open polls end now"
        ),
        Ok(Err(status)) if unsupported(&status) => {
            if !SHUTDOWN_UNSUPPORTED_SAID.swap(true, Ordering::Relaxed) {
                tracing::debug!(
                    "The engine predates ShutdownWorker (added in 0.5.0); idle pollers stop \
                     when their polls end or the shutdown grace runs out"
                );
            }
        }
        Ok(Err(status)) => tracing::warn!(
            identity = %caller.identity,
            code = ?status.code(),
            error = %status.message(),
            "Could not tell the engine this worker is shutting down; idle pollers stop when \
             their polls end or the shutdown grace runs out"
        ),
        Err(_) => tracing::warn!(
            identity = %caller.identity,
            timeout_ms = timeout.as_millis() as u64,
            "The engine did not acknowledge this worker's shutdown in time; idle pollers stop \
             when their polls end or the shutdown grace runs out"
        ),
    }
}

/// An activation received but not run, to hand back.
#[derive(Debug, Clone)]
pub(crate) struct Leftover {
    pub(crate) workflow_id: String,
    pub(crate) run_id: String,
    /// The activation token, as the poll carried it in `stream_entry_id`.
    pub(crate) token: Vec<u8>,
}

/// Hand an activation back to the engine over the driver's connection. One
/// attempt, within `timeout`; never an error.
pub(crate) async fn release(
    caller: &Caller,
    channel_manager: &ChannelManager,
    leftover: Leftover,
    timeout: Duration,
) {
    let channel =
        match tokio::time::timeout(timeout, channel_manager.connection(Breaker::Respect)).await {
            Ok(Ok((channel, _))) => channel,
            Ok(Err(e)) => {
                not_released(&leftover, &format!("no connection to the engine: {e:?}"));
                return;
            }
            Err(_) => {
                not_released(&leftover, "no connection to the engine in time");
                return;
            }
        };
    release_on(
        ExecutionServiceClient::new(channel),
        &caller.namespace,
        &caller.identity,
        caller.api_key.as_deref(),
        caller.organization_id.as_deref(),
        leftover,
        timeout,
    )
    .await;
}

/// Hand an activation back through `client`: for a poller, which has a
/// connection of its own and no driver to go through.
pub(crate) async fn release_on(
    mut client: ExecutionServiceClient<Channel>,
    namespace: &str,
    identity: &str,
    api_key: Option<&str>,
    organization_id: Option<&str>,
    leftover: Leftover,
    timeout: Duration,
) {
    let body = ReleaseWorkflowExecutionRequest {
        workflow_id: leftover.workflow_id.clone(),
        execution_id: leftover.run_id.clone(),
        namespace: namespace.to_string(),
        identity: identity.to_string(),
        task_token: leftover.token.clone(),
    };
    let mut request =
        crate::poller::credentials::credentialed_request(body, api_key, organization_id);
    request.set_timeout(timeout);
    let sent = tokio::time::timeout(timeout, client.release_workflow_execution(request)).await;
    match sent {
        Ok(Ok(_)) => tracing::info!(
            workflow_id = %leftover.workflow_id,
            run_id = %leftover.run_id,
            "Workflow activation received but not run; handed back to the engine"
        ),
        Ok(Err(status)) if unsupported(&status) => {
            if !RELEASE_UNSUPPORTED_SAID.swap(true, Ordering::Relaxed) {
                tracing::debug!(
                    "The engine predates ReleaseWorkflowExecution (added in 0.5.0); activations \
                     not run wait out their claim"
                );
            }
            not_released(&leftover, "the engine cannot take it back");
        }
        Ok(Err(status)) => not_released(&leftover, status.message()),
        Err(_) => not_released(&leftover, "the engine did not answer in time"),
    }
}

/// Log, at error, that an activation could not be handed back: it stays stuck
/// until the engine's claim timeout.
fn not_released(leftover: &Leftover, why: &str) {
    tracing::error!(
        workflow_id = %leftover.workflow_id,
        run_id = %leftover.run_id,
        reason = %why,
        "Workflow activation received but never handed over; it stays claimed until the \
         engine's claim timeout"
    );
}

/// Whether the engine does not have the call at all. A proxy in front of an
/// older engine answers an unknown path with 404, which tonic reads the same.
fn unsupported(status: &Status) -> bool {
    status.code() == Code::Unimplemented
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_instance_id_is_one_per_process() {
        let id = worker_instance_id();
        assert!(uuid::Uuid::parse_str(id).is_ok());
        assert_eq!(id, worker_instance_id());
    }

    #[test]
    fn a_shutdown_names_each_queue_once_and_all_of_them_past_the_cap() {
        let queues = |n: usize| (0..n).map(|i| format!("q{i}")).collect::<Vec<_>>();
        assert_eq!(
            shutdown_queues(vec!["b".into(), "a".into(), "b".into()]),
            vec!["a".to_string(), "b".to_string()]
        );
        assert_eq!(shutdown_queues(queues(16)).len(), 16);
        assert!(shutdown_queues(queues(17)).is_empty());
    }
}
