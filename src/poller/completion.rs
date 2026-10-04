//! Reporting the outcome of work to the engine, and retrying it.
//!
//! Workflow activations, tasks and actor operations all end in one call that
//! tells the engine how the work went. They share the sender here, which
//! resends the same body when an answer may change and stops when it cannot.
//! What follows was written for workflow completions; task and actor reports
//! follow the same table, with the differences noted at the end.
//!
//! A completion call can fail for reasons that pass: the engine restarting,
//! the node that received it unable to reach the node that owns the
//! workflow, a connection reset under load. Dropping the completion then
//! would leave the workflow claimed until the engine's claim timeout hands it
//! out again minutes later, or leave it unfinished until whoever is waiting
//! on it gives up.
//!
//! Sending a completion again is safe because of the activation token. The
//! engine numbers every activation it hands out, returns that number in a
//! token on the poll, and applies a completion carrying it at most once: a
//! repeat is acknowledged without being applied again, and one for an
//! activation that has since been handed out again is acknowledged and
//! changes nothing. A completion that named only its workflow could be
//! applied twice if sent twice. So every completion here carries the token,
//! and a failure that may be temporary is retried with the same one.
//!
//! What each answer means, and what is done about it:
//!
//! | Answer | Meaning | Here |
//! |---|---|---|
//! | OK | applied, already applied, or superseded | done |
//! | UNAVAILABLE | engine down, or refused: another node owns the workflow | retry |
//! | DEADLINE_EXCEEDED | the call or the engine's relay ran out of time | retry |
//! | CANCELLED, UNKNOWN | the connection broke under the call | retry |
//! | INTERNAL | the engine failed to apply it and gave the activation back | retry |
//! | RESOURCE_EXHAUSTED, ABORTED | busy, try again | retry |
//! | FAILED_PRECONDITION | the activation is no longer current | drop |
//! | INVALID_ARGUMENT, NOT_FOUND, PERMISSION_DENIED, anything else | sending it again cannot help | drop |
//!
//! Only a failure where no server answered at all counts against the
//! connection's circuit breaker. An engine that answers — even to refuse — is
//! reachable, and tripping the breaker for it would hold back every other
//! completion on a connection that works.
//!
//! A task's report carries the task token from its poll, which names one
//! attempt of the task. The engine applies a completion for it once and
//! acknowledges a repeat. What it answers to a report it cannot apply
//! depends on its version:
//!
//! - Engines later than 0.4.2 answer a report from an attempt that is no
//!   longer running (timed out, cancelled, or already failed) with
//!   FAILED_PRECONDITION, and a token they do not know with NOT_FOUND. Both
//!   are final and dropped immediately.
//! - Engines up to 0.4.2 answer the first with INTERNAL, which reads as a
//!   failure that may pass, so it is retried until the budget runs out,
//!   holding one of the driver's in-flight slots meanwhile. They answer the
//!   second with OK, and nothing shows that it was not applied.
//!
//! An actor operation's report names the operation and execution it answers.
//! The engine acknowledges one it cannot apply — unknown, or already
//! completed — with OK and `success: false`; that answer is final too.

use std::future::Future;
use std::sync::Arc;
use std::time::{Duration, Instant};

use rand::Rng;
use tokio::sync::{mpsc, watch};
use tonic::transport::Channel;
use tonic::{Code, Status};

use crate::limits::{refused_as_too_large, sized, too_large_to_send, PAYLOAD_TOO_LARGE};
use crate::poller::channel::{Breaker, ChannelManager, Unconnected};
use crate::poller::driver::{
    proto_task_to_task_work, ActorDriverConfig, TaskDriverConfig, TaskWork, WorkflowDriverConfig,
};
use crate::poller::heartbeat::TaskHeartbeats;
use crate::proto::orcher::v1::{
    actor_service_client::ActorServiceClient, execution_service_client::ExecutionServiceClient,
    Command, CompleteActorOperationRequest, CompleteTaskExecutionRequest,
    CompleteWorkflowExecutionRequest, FailTaskExecutionRequest, FailWorkflowExecutionRequest,
    Failure, QueryResult, TaskCapabilities, UpdateResult,
};
use prost::Message as _;

/// How hard to try to deliver one completion or failure report — for a
/// workflow activation, a task, or an actor operation.
///
/// The whole budget has to fit well inside the engine's claim timeout (five
/// minutes by default): once the claim lapses the activation is handed out
/// again, and a completion that arrives after that is acknowledged and
/// discarded, so trying for longer only delays the worker.
///
/// It also has to outlast a failover. When an engine node dies, the node
/// that takes over its workflows waits until the dead one has missed several
/// heartbeats — fifteen seconds by default — and until then answers every
/// completion for those workflows with UNAVAILABLE. So by default the budget,
/// not the attempt count, is what ends the retrying: twenty attempts at up to
/// three seconds apart reach past thirty seconds.
///
/// Built from [`Default`] and adjusted with the `with_` setters; more
/// settings may be added, so it cannot be built field by field outside this
/// crate.
#[derive(Debug, Clone)]
#[non_exhaustive]
pub struct CompletionRetryConfig {
    /// Attempts in all, the first one included. One means no retry.
    pub max_attempts: u32,
    /// Wait before the second attempt; each later wait doubles it.
    pub initial_backoff: Duration,
    /// The longest single wait between attempts.
    pub max_backoff: Duration,
    /// No attempt starts once this much time has passed since the first, and
    /// no attempt is given longer than what is left of it.
    pub budget: Duration,
    /// How long the whole driver shutdown may take, from the moment it
    /// starts: running work a poll brought back after shutdown, waiting for
    /// its results, and sending or retrying completions all end by then.
    pub shutdown_grace: Duration,
    /// How many completions may be being sent at once. `None` means the
    /// driver's `max_concurrent_executions`. With this many outstanding the
    /// driver stops taking results and new work, so a backlog of completions
    /// holds the pollers back instead of growing without bound.
    pub max_in_flight: Option<usize>,
}

impl Default for CompletionRetryConfig {
    fn default() -> Self {
        Self {
            max_attempts: 20,
            initial_backoff: Duration::from_millis(200),
            max_backoff: Duration::from_secs(3),
            budget: Duration::from_secs(30),
            shutdown_grace: Duration::from_secs(5),
            max_in_flight: None,
        }
    }
}

impl CompletionRetryConfig {
    /// Set [`max_attempts`](Self::max_attempts).
    pub fn with_max_attempts(mut self, max_attempts: u32) -> Self {
        self.max_attempts = max_attempts;
        self
    }

    /// Set [`initial_backoff`](Self::initial_backoff) and
    /// [`max_backoff`](Self::max_backoff).
    pub fn with_backoff(mut self, initial: Duration, max: Duration) -> Self {
        self.initial_backoff = initial;
        self.max_backoff = max;
        self
    }

    /// Set [`budget`](Self::budget).
    pub fn with_budget(mut self, budget: Duration) -> Self {
        self.budget = budget;
        self
    }

    /// Set [`shutdown_grace`](Self::shutdown_grace).
    pub fn with_shutdown_grace(mut self, grace: Duration) -> Self {
        self.shutdown_grace = grace;
        self
    }

    /// Set [`max_in_flight`](Self::max_in_flight).
    pub fn with_max_in_flight(mut self, max_in_flight: usize) -> Self {
        self.max_in_flight = Some(max_in_flight);
        self
    }
}

impl CompletionRetryConfig {
    /// The wait before attempt `attempt + 1`: doubling from the initial
    /// backoff up to the cap, then jittered down by up to half, so workers
    /// that failed together do not all come back at the same instant.
    fn backoff(&self, attempt: u32) -> Duration {
        let doubled = self
            .initial_backoff
            .saturating_mul(1u32 << attempt.saturating_sub(1).min(16));
        let capped = doubled.min(self.max_backoff);
        let half = capped / 2;
        let jitter_ms = half.as_millis() as u64;
        let jitter = if jitter_ms == 0 {
            0
        } else {
            rand::thread_rng().gen_range(0..=jitter_ms)
        };
        half + Duration::from_millis(jitter)
    }
}

/// The activation token to send with a completion or failure report.
///
/// The engine returns it in the poll's `stream_entry_id`, which is preferred
/// here because it cannot have been rewritten on the way through a language
/// SDK. The task token is the fallback, since older engines carry the token
/// for failure reports only there. Both are empty on an engine without
/// activation tokens, and such an engine accepts an empty token.
pub(crate) fn activation_token(task_token: Vec<u8>, stream_entry_id: Option<&str>) -> Vec<u8> {
    match stream_entry_id {
        Some(id) if !id.is_empty() => id.as_bytes().to_vec(),
        _ => task_token,
    }
}

/// Whether no server answered: the status was made up by the client from a
/// failure of its own — a connect that failed, a connection that broke, a
/// stream the other end reset, a local timeout.
///
/// tonic keeps the error it made such a status from as its source; a status
/// the server sent, whether in trailers or inferred from an HTTP status, has
/// none. The response's headers are no guide: a server that sends its
/// headers and then fails the call puts the status in trailers, which carry
/// no content type.
fn no_server_answered(status: &Status) -> bool {
    std::error::Error::source(status).is_some()
}

/// What a failed attempt says about the connection it was made on.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Blame {
    /// Nothing: the server answered, or is plainly there and turned this call
    /// away. The connection is kept and the breaker left alone.
    None,
    /// The connection: drop it and reconnect on the next attempt, counting
    /// the failure against the breaker when `count`.
    Connection { count: bool },
}

fn blame(status: &Status) -> Blame {
    if !no_server_answered(status) {
        return Blame::None;
    }
    let mut source = std::error::Error::source(status);
    while let Some(error) = source {
        // The client's own deadline ran out: the server is slow, not gone.
        if error.is::<tonic::TimeoutExpired>() {
            return Blame::None;
        }
        // A live server declining the stream — REFUSED_STREAM, or a GOAWAY
        // without an error as it drains — guarantees it did not process it,
        // and says nothing against the connection. tonic's channel moves to
        // a new connection by itself after a GOAWAY.
        if let Some(h2) = error.downcast_ref::<h2::Error>() {
            let refused = h2.reason() == Some(h2::Reason::REFUSED_STREAM);
            let draining = h2.is_go_away() && h2.reason() == Some(h2::Reason::NO_ERROR);
            if refused || draining {
                return Blame::None;
            }
        }
        source = error.source();
    }
    // A stream reset or broken HTTP/2 connection: reconnect on the next
    // attempt, without a cool-off. Could not reach the server at all: back
    // off before reconnecting.
    Blame::Connection {
        count: !matches!(
            status.code(),
            Code::Cancelled | Code::Unknown | Code::Internal
        ),
    }
}

/// What to do after a failed attempt.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Next {
    /// Send it again with the same token.
    Retry,
    /// Sending it again cannot change the answer.
    Drop,
}

fn next_after(code: Code) -> Next {
    match code {
        Code::Unavailable
        | Code::DeadlineExceeded
        | Code::Cancelled
        | Code::Unknown
        | Code::Internal
        | Code::ResourceExhausted
        | Code::Aborted => Next::Retry,
        _ => Next::Drop,
    }
}

/// How one delivery ended.
#[derive(Debug)]
pub(crate) enum Delivery<T> {
    /// The engine accepted it — applied, or acknowledged as already applied
    /// or superseded.
    Delivered(T),
    /// The engine answered with something another attempt cannot change.
    /// `Code::Ok` when it answered OK but said it did not apply the report,
    /// as an actor operation's completion can.
    Declined(Code),
    /// Every attempt the budget allowed failed, or the worker shut down.
    Abandoned,
}

/// Who a report comes from: what every report carries besides its own body.
///
/// The same for every report a driver sends, so each driver builds one and
/// shares it between the reports it has in flight.
#[derive(Debug, Clone)]
pub(crate) struct Caller {
    pub(crate) namespace: String,
    pub(crate) identity: String,
    pub(crate) api_key: Option<String>,
    pub(crate) organization_id: Option<String>,
}

impl From<&WorkflowDriverConfig> for Caller {
    fn from(config: &WorkflowDriverConfig) -> Self {
        Self {
            namespace: config.namespace.clone(),
            identity: config.identity.clone(),
            api_key: config.api_key.clone(),
            organization_id: config.organization_id.clone(),
        }
    }
}

impl From<&TaskDriverConfig> for Caller {
    fn from(config: &TaskDriverConfig) -> Self {
        Self {
            namespace: config.namespace.clone(),
            identity: config.identity.clone(),
            api_key: config.api_key.clone(),
            organization_id: config.organization_id.clone(),
        }
    }
}

impl From<&ActorDriverConfig> for Caller {
    fn from(config: &ActorDriverConfig) -> Self {
        Self {
            namespace: config.namespace.clone(),
            identity: config.identity.clone(),
            api_key: config.api_key.clone(),
            organization_id: config.organization_id.clone(),
        }
    }
}

/// Where tasks the engine hands back eagerly on a workflow completion go.
#[derive(Debug, Clone)]
pub(crate) struct EagerTasks {
    /// The task driver's work channel.
    pub(crate) sender: mpsc::Sender<TaskWork>,
    /// The task driver's heartbeats, which the tasks join; none for a bare
    /// work channel, whose tasks nothing heartbeats.
    pub(crate) heartbeats: Option<TaskHeartbeats>,
    /// The queue for an eager task that names none: the one the workflow was
    /// polled from.
    pub(crate) default_queue: String,
}

/// What kind of work a report answers. Only the log lines differ.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum Kind {
    Workflow,
    Task,
    ActorOperation,
}

impl Kind {
    /// As it starts a sentence.
    fn title(self) -> &'static str {
        match self {
            Kind::Workflow => "Workflow",
            Kind::Task => "Task",
            Kind::ActorOperation => "Actor operation",
        }
    }

    /// As it reads mid-sentence.
    fn noun(self) -> &'static str {
        match self {
            Kind::Workflow => "workflow",
            Kind::Task => "task",
            Kind::ActorOperation => "actor operation",
        }
    }

    /// What becomes of the work when its report is given up on.
    fn left_to(self) -> &'static str {
        match self {
            Kind::Workflow => "the engine hands the activation out again when its claim times out",
            Kind::Task => "the task waits out its start-to-close timeout",
            Kind::ActorOperation => "the operation is left to the engine's own timeout",
        }
    }
}

/// What a report answers, logged as fields of their own so that queries on
/// them match every line about it. Never the token: that is a credential.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Ids<'a> {
    Workflow {
        workflow_id: &'a str,
        run_id: &'a str,
    },
    /// Both empty for a task the workflow driver handed over eagerly, which
    /// the task driver never saw polled.
    Task {
        task_id: &'a str,
        workflow_id: &'a str,
    },
    ActorOperation {
        operation_id: &'a str,
        execution_id: &'a str,
    },
}

impl Ids<'_> {
    fn kind(self) -> Kind {
        match self {
            Ids::Workflow { .. } => Kind::Workflow,
            Ids::Task { .. } => Kind::Task,
            Ids::ActorOperation { .. } => Kind::ActorOperation,
        }
    }
}

/// Log an event about one report, with the ids it answers as fields.
macro_rules! report_event {
    ($level:ident, $ids:expr, $($rest:tt)+) => {
        match $ids {
            Ids::Workflow { workflow_id, run_id } => {
                tracing::$level!(workflow_id = %workflow_id, run_id = %run_id, $($rest)+)
            }
            Ids::Task { task_id, workflow_id } => {
                tracing::$level!(task_id = %task_id, workflow_id = %workflow_id, $($rest)+)
            }
            Ids::ActorOperation { operation_id, execution_id } => {
                tracing::$level!(
                    operation_id = %operation_id,
                    execution_id = %execution_id,
                    $($rest)+
                )
            }
        }
    };
}

/// Which report a delivery is sending, for its log lines.
#[derive(Debug, Clone, Copy)]
pub(crate) struct Subject<'a> {
    /// "completion" or "failure report".
    pub(crate) what: &'static str,
    pub(crate) ids: Ids<'a>,
}

impl Subject<'_> {
    fn kind(&self) -> Kind {
        self.ids.kind()
    }
}

/// One workflow activation's outcome, ready to send.
#[derive(Debug)]
pub(crate) enum Report {
    Complete {
        workflow_id: String,
        run_id: String,
        token: Vec<u8>,
        stream_entry_id: String,
        commands: Vec<Command>,
        query_results: Vec<QueryResult>,
        update_results: Vec<UpdateResult>,
    },
    Fail {
        workflow_id: String,
        run_id: String,
        token: Vec<u8>,
        message: String,
        /// `WorkflowExecutionError`, or [`NON_DETERMINISM_FAILURE_TYPE`] for
        /// an activation whose commands contradict its journal.
        ///
        /// [`NON_DETERMINISM_FAILURE_TYPE`]: crate::state::NON_DETERMINISM_FAILURE_TYPE
        failure_type: String,
        /// Running the same code again cannot succeed.
        non_retryable: bool,
    },
    /// Not run at all: handed back for the next poll to take.
    Release(crate::poller::lifecycle::Leftover),
    /// Run, and not to be applied: held for `after`, or until the worker
    /// shuts down, then handed back to be tried again. Nothing is recorded
    /// against the workflow, so the run stays open with its journal as it
    /// was.
    Retry {
        leftover: crate::poller::lifecycle::Leftover,
        after: Duration,
    },
}

impl Report {
    /// Whether sending it waits on purpose, and so should not hold a slot
    /// that new work needs.
    pub(crate) fn waits(&self) -> bool {
        matches!(self, Report::Retry { .. })
    }
}

/// One task attempt's outcome, ready to send.
///
/// `token` is the task token from the poll, unchanged: it names the attempt,
/// and is what lets the engine recognize a repeat. `task` names it in logs;
/// the token is a credential and is never logged.
#[derive(Debug)]
pub(crate) enum TaskReport {
    Complete {
        token: Vec<u8>,
        result: Vec<u8>,
        task: TaskIds,
    },
    Fail {
        token: Vec<u8>,
        message: String,
        /// The type the task's error was raised as, which a retry policy
        /// names; `TaskExecutionError` when the SDK gave none.
        failure_type: String,
        /// The task's code said no retry can succeed.
        non_retryable: bool,
        task: TaskIds,
    },
}

impl TaskReport {
    /// The task token of the attempt it reports.
    pub(crate) fn token(&self) -> &[u8] {
        match self {
            TaskReport::Complete { token, .. } | TaskReport::Fail { token, .. } => token,
        }
    }
}

/// Which task a report answers, for its log lines. Empty for a task handed
/// over eagerly, which the task driver never saw polled.
#[derive(Debug, Clone, Default)]
pub(crate) struct TaskIds {
    pub(crate) task_id: String,
    pub(crate) workflow_id: String,
}

impl TaskIds {
    fn ids(&self) -> Ids<'_> {
        Ids::Task {
            task_id: &self.task_id,
            workflow_id: &self.workflow_id,
        }
    }
}

/// Sends completions and failure reports, retrying what may succeed when sent
/// again.
///
/// Cheap to clone: each report is sent from its own task, so one being
/// retried does not hold up the others or the hand-off of new work.
#[derive(Clone)]
pub(crate) struct Completer {
    pub(crate) caller: Arc<Caller>,
    pub(crate) channel_manager: Arc<ChannelManager>,
    pub(crate) retry: CompletionRetryConfig,
    pub(crate) shutdown: watch::Receiver<bool>,
    pub(crate) eager_task_injector: Option<EagerTasks>,
    /// Heartbeats for the tasks a task completion's answer hands over; set
    /// for the task driver's reports.
    pub(crate) heartbeats: Option<TaskHeartbeats>,
}

impl Completer {
    fn credentialed<T>(&self, body: T) -> tonic::Request<T> {
        let mut request = crate::poller::credentials::credentialed_request(
            body,
            self.caller.api_key.as_deref(),
            self.caller.organization_id.as_deref(),
        );
        // What this worker can receive, which bounds the tasks the engine
        // hands back with its answer.
        crate::limits::state_receive_limit(&mut request, self.max_message_bytes());
        request
    }

    /// The largest message this worker sends or receives.
    fn max_message_bytes(&self) -> usize {
        self.channel_manager.max_message_bytes()
    }

    /// Send a report and log how it ended.
    pub(crate) async fn send(&self, report: Report) {
        match report {
            Report::Complete {
                workflow_id,
                run_id,
                token,
                stream_entry_id,
                commands,
                query_results,
                update_results,
            } => {
                let request = CompleteWorkflowExecutionRequest {
                    workflow_id: workflow_id.clone(),
                    execution_id: run_id.clone(),
                    namespace: self.caller.namespace.clone(),
                    identity: self.caller.identity.clone(),
                    task_token: token,
                    commands,
                    query_results,
                    update_results,
                    force_new_execution_step: false,
                    binary_checksum: vec![],
                    // Also sent here, for engines that read the activation
                    // token only from this field.
                    stream_entry_id,
                    // The tasks the engine hands back with its answer are
                    // heartbeated like polled ones only when they join the
                    // task driver's heartbeats.
                    eager_task_capabilities: self
                        .eager_task_injector
                        .as_ref()
                        .and_then(|eager| eager.heartbeats.as_ref())
                        .filter(|heartbeats| heartbeats.auto())
                        .map(|_| TaskCapabilities {
                            auto_heartbeat: true,
                        }),
                };
                self.complete_or_fail_too_large(&workflow_id, &run_id, request)
                    .await;
            }
            Report::Fail {
                workflow_id,
                run_id,
                token,
                message,
                failure_type,
                non_retryable,
            } => {
                let request = FailWorkflowExecutionRequest {
                    workflow_id: workflow_id.clone(),
                    execution_id: run_id.clone(),
                    namespace: self.caller.namespace.clone(),
                    identity: self.caller.identity.clone(),
                    task_token: token,
                    failure: Some(Failure {
                        message,
                        source: "WorkflowDriver".to_string(),
                        stack_trace: String::new(),
                        cause: None,
                        failure_type,
                        details: vec![],
                        non_retryable,
                    }),
                    binary_checksum: vec![],
                };
                self.fail(&workflow_id, &run_id, request).await;
            }
            // Once, not retried: a release that does not land leaves the
            // activation to its claim timeout, which is where it was anyway.
            // Bounded by the grace, since it is mostly sent during shutdown.
            Report::Release(leftover) => {
                crate::poller::lifecycle::release(
                    &self.caller,
                    &self.channel_manager,
                    leftover,
                    self.retry.shutdown_grace,
                )
                .await;
            }
            // Held here rather than released at once: the engine offers a
            // released activation to the next poll straight away, and the
            // same code would fail it the same way, as fast as it could poll.
            // A worker shutting down hands it back at once, so the code that
            // replaces it can take the run.
            Report::Retry { leftover, after } => {
                let mut shutdown = self.shutdown.clone();
                tokio::select! {
                    _ = tokio::time::sleep(after) => {}
                    _ = shutdown.wait_for(|stopping| *stopping) => {}
                }
                crate::poller::lifecycle::release(
                    &self.caller,
                    &self.channel_manager,
                    leftover,
                    self.retry.shutdown_grace,
                )
                .await;
            }
        }
    }

    /// Send a workflow completion, or, when it is too large to send or the
    /// engine refuses it as too large, a completion that fails the workflow
    /// with the reason.
    ///
    /// Sent anyway, a completion over the limit is refused by the transport,
    /// on one side or the other, every time it is sent: the activation is
    /// handed out again at its claim timeout, run again, and refused again,
    /// and the workflow never ends. Replay produces the same completion, so
    /// failing the workflow is the only outcome that ends it, and it says why.
    pub(crate) async fn complete_or_fail_too_large(
        &self,
        workflow_id: &str,
        run_id: &str,
        request: CompleteWorkflowExecutionRequest,
    ) -> Delivery<()> {
        let size = request.encoded_len();
        let what = workflow_completion_named(&request);
        let limit = self.max_message_bytes();
        let template = without_payloads(&request);
        if size > limit {
            let reason = too_large_to_send(&what, size, limit);
            tracing::warn!(
                workflow_id = %workflow_id,
                run_id = %run_id,
                reason = %reason,
                "Workflow completion too large to send; failing the workflow"
            );
            return self
                .complete(workflow_id, run_id, fail_workflow_instead(template, reason))
                .await;
        }
        match self.complete(workflow_id, run_id, request).await {
            Delivery::Declined(code) if declined_as_too_large(code) => {
                let reason = refused_as_too_large(&what, size);
                tracing::warn!(
                    workflow_id = %workflow_id,
                    run_id = %run_id,
                    reason = %reason,
                    "Workflow completion refused as too large; failing the workflow"
                );
                self.complete(workflow_id, run_id, fail_workflow_instead(template, reason))
                    .await
            }
            other => other,
        }
    }

    pub(crate) async fn complete(
        &self,
        workflow_id: &str,
        run_id: &str,
        request: CompleteWorkflowExecutionRequest,
    ) -> Delivery<()> {
        let max = self.max_message_bytes();
        let delivery = self
            .deliver(
                Subject {
                    what: "completion",
                    ids: Ids::Workflow {
                        workflow_id,
                        run_id,
                    },
                },
                request,
                move |channel, request| async move {
                    sized!(ExecutionServiceClient::new(channel), max)
                        .complete_workflow_execution(request)
                        .await
                },
            )
            .await;
        match delivery {
            Delivery::Delivered(response) => {
                tracing::info!(
                    workflow_id = %workflow_id,
                    run_id = %run_id,
                    "Workflow execution completed"
                );
                // Forward any eagerly-returned tasks to the task driver's work channel.
                if let Some(ref eager) = self.eager_task_injector {
                    let tasks = response.eager_tasks;
                    if !tasks.is_empty() {
                        tracing::debug!(
                            count = tasks.len(),
                            "Forwarding eager tasks from workflow completion"
                        );
                        for proto_task in tasks {
                            let work = proto_task_to_task_work(
                                proto_task,
                                &eager.default_queue,
                                eager.heartbeats.as_ref(),
                            );
                            if eager.sender.try_send(work).is_err() {
                                // Channel full or closed: the poller picks the task up normally.
                                break;
                            }
                        }
                    }
                }
                Delivery::Delivered(())
            }
            Delivery::Declined(code) => Delivery::Declined(code),
            Delivery::Abandoned => Delivery::Abandoned,
        }
    }

    pub(crate) async fn fail(
        &self,
        workflow_id: &str,
        run_id: &str,
        request: FailWorkflowExecutionRequest,
    ) -> Delivery<()> {
        let max = self.max_message_bytes();
        let delivery = self
            .deliver(
                Subject {
                    what: "failure report",
                    ids: Ids::Workflow {
                        workflow_id,
                        run_id,
                    },
                },
                request,
                move |channel, request| async move {
                    sized!(ExecutionServiceClient::new(channel), max)
                        .fail_workflow_execution(request)
                        .await
                },
            )
            .await;
        match delivery {
            Delivery::Delivered(_) => {
                tracing::info!(
                    workflow_id = %workflow_id,
                    run_id = %run_id,
                    "Workflow execution failure reported"
                );
                Delivery::Delivered(())
            }
            Delivery::Declined(code) => Delivery::Declined(code),
            Delivery::Abandoned => Delivery::Abandoned,
        }
    }

    /// Send a task attempt's outcome, retrying it with the same token.
    ///
    /// A completion's answer may carry tasks the engine hands to this worker
    /// at once; they go to `work`, the language SDK's work channel. The
    /// engine has claimed them for this worker by then, so one that finds no
    /// room is named: it waits out its start-to-close timeout.
    pub(crate) async fn send_task(
        &self,
        report: TaskReport,
        work: Option<&mpsc::Sender<TaskWork>>,
        default_queue: &str,
    ) -> Delivery<()> {
        match report {
            TaskReport::Complete {
                token,
                result,
                task,
            } => {
                let max = self.max_message_bytes();
                let request = CompleteTaskExecutionRequest {
                    task_token: token,
                    namespace: self.caller.namespace.clone(),
                    identity: self.caller.identity.clone(),
                    result,
                };
                // A result too large to send fails the task instead, saying
                // why. Sent anyway, it is refused on every attempt, and the
                // task waits out its timeout to be run, and refused, again.
                let what = match task.task_id.as_str() {
                    "" => "the task result".to_string(),
                    id => format!("the result of task {id:?}"),
                };
                let size = request.result.len();
                if request.encoded_len() > max {
                    let message = too_large_to_send(&what, size, max);
                    report_event!(warn, task.ids(), reason = %message,
                        "Task result too large to send; failing the task");
                    return self
                        .send_task_failure(request.task_token, message, task)
                        .await;
                }
                let token = request.task_token.clone();
                let delivery = self
                    .deliver(
                        Subject {
                            what: "completion",
                            ids: task.ids(),
                        },
                        request,
                        move |channel, request| async move {
                            sized!(ExecutionServiceClient::new(channel), max)
                                .complete_task_execution(request)
                                .await
                        },
                    )
                    .await;
                match delivery {
                    Delivery::Delivered(response) => {
                        report_event!(debug, task.ids(), "Task execution completed");
                        for proto_task in response.eager_tasks {
                            let eager = proto_task_to_task_work(
                                proto_task,
                                default_queue,
                                self.heartbeats.as_ref(),
                            );
                            let handed = match work {
                                Some(work) => work.try_send(eager).map_err(|e| e.into_inner()),
                                None => Err(eager),
                            };
                            if let Err(eager) = handed {
                                tracing::error!(
                                    task_id = %eager.task.task_id,
                                    workflow_id = %eager.task.execution.workflow_id,
                                    "No room for a task the engine handed over with a \
                                     completion; it waits out its start-to-close timeout"
                                );
                            }
                        }
                        Delivery::Delivered(())
                    }
                    // An engine whose own limit is lower than this worker's
                    // refused the message whole: fail the task, saying why.
                    Delivery::Declined(code) if declined_as_too_large(code) => {
                        let message = refused_as_too_large(&what, size);
                        report_event!(warn, task.ids(), reason = %message,
                            "Task result refused as too large; failing the task");
                        self.send_task_failure(token, message, task).await
                    }
                    Delivery::Declined(code) => Delivery::Declined(code),
                    Delivery::Abandoned => Delivery::Abandoned,
                }
            }
            TaskReport::Fail {
                token,
                message,
                failure_type,
                non_retryable,
                task,
            } => {
                self.send_task_fail(token, message, failure_type, non_retryable, task)
                    .await
            }
        }
    }

    /// Fail the task whose result is too large to send: final, since the same
    /// code produces the same result.
    async fn send_task_failure(
        &self,
        token: Vec<u8>,
        message: String,
        task: TaskIds,
    ) -> Delivery<()> {
        self.send_task_fail(token, message, PAYLOAD_TOO_LARGE.to_string(), true, task)
            .await
    }

    async fn send_task_fail(
        &self,
        token: Vec<u8>,
        message: String,
        failure_type: String,
        non_retryable: bool,
        task: TaskIds,
    ) -> Delivery<()> {
        let max = self.max_message_bytes();
        let request = FailTaskExecutionRequest {
            task_token: token,
            namespace: self.caller.namespace.clone(),
            identity: self.caller.identity.clone(),
            failure: Some(Failure {
                message,
                source: "TaskDriver".to_string(),
                stack_trace: String::new(),
                cause: None,
                failure_type,
                details: vec![],
                non_retryable,
            }),
        };
        let delivery = self
            .deliver(
                Subject {
                    what: "failure report",
                    ids: task.ids(),
                },
                request,
                move |channel, request| async move {
                    sized!(ExecutionServiceClient::new(channel), max)
                        .fail_task_execution(request)
                        .await
                },
            )
            .await;
        match delivery {
            Delivery::Delivered(_) => {
                report_event!(debug, task.ids(), "Task execution failure reported");
                Delivery::Delivered(())
            }
            Delivery::Declined(code) => Delivery::Declined(code),
            Delivery::Abandoned => Delivery::Abandoned,
        }
    }

    /// Send an actor operation's outcome, retrying it with the same body.
    ///
    /// An answer of `success: false` is the engine saying it cannot apply
    /// this report — the operation is unknown, or already completed — and
    /// sending it again would get the same answer, so it counts as declined.
    pub(crate) async fn send_actor(&self, request: CompleteActorOperationRequest) -> Delivery<()> {
        let max = self.max_message_bytes();
        let operation_id = request.operation_id.clone();
        let execution_id = request.execution_id.clone();
        let ids = Ids::ActorOperation {
            operation_id: &operation_id,
            execution_id: &execution_id,
        };
        let what = if request.status == crate::proto::orcher::v1::ExecutionStatus::Success as i32 {
            "completion"
        } else {
            "failure report"
        };
        let delivery = self
            .deliver(
                Subject { what, ids },
                request,
                move |channel, request| async move {
                    sized!(ActorServiceClient::new(channel), max)
                        .complete_actor_operation(request)
                        .await
                },
            )
            .await;
        match delivery {
            Delivery::Delivered(response) if response.success => {
                report_event!(debug, ids, "Actor operation {what} delivered");
                Delivery::Delivered(())
            }
            Delivery::Delivered(response) => {
                report_event!(
                    warn,
                    ids,
                    error = %response.error_message,
                    "Actor operation {what} not applied: the engine does not know the \
                     operation or has already completed it"
                );
                Delivery::Declined(Code::Ok)
            }
            Delivery::Declined(code) => Delivery::Declined(code),
            Delivery::Abandoned => Delivery::Abandoned,
        }
    }

    /// Send `body` until the engine takes it, refuses it for good, or the
    /// budget runs out.
    ///
    /// Every attempt sends the same body, and so the same token: that is what
    /// lets the engine recognize a repeat of a report it already applied.
    async fn deliver<Req, Resp, F, Fut>(
        &self,
        subject: Subject<'_>,
        body: Req,
        call: F,
    ) -> Delivery<Resp>
    where
        Req: Clone,
        F: Fn(Channel, tonic::Request<Req>) -> Fut,
        Fut: Future<Output = std::result::Result<tonic::Response<Resp>, Status>>,
    {
        let started = Instant::now();
        let mut shutdown = self.shutdown.clone();
        let mut shutdown_seen: Option<Instant> = None;
        let max_attempts = self.retry.max_attempts.max(1);
        let mut attempt = 0u32;
        // Set once the breaker would keep this delivery waiting past its
        // deadline: the next attempt connects regardless.
        let mut last_chance = false;

        loop {
            if shutdown_seen.is_none() && *shutdown.borrow() {
                shutdown_seen = Some(Instant::now());
            }
            let deadline = self.deadline(started, shutdown_seen);
            let remaining = deadline.saturating_duration_since(Instant::now());
            if remaining.is_zero() {
                break;
            }

            // While the breaker is open, one sender at a time probes the
            // connection at most every `max_backoff`, however far the
            // breaker's own cool-off has doubled: a completion should land
            // within a backoff of the engine coming back, not a cool-off.
            let breaker = if last_chance || attempt + 1 >= max_attempts {
                Breaker::Ignore
            } else {
                Breaker::CapAt(self.retry.max_backoff)
            };
            let (channel, generation) = match self.channel_manager.connection(breaker).await {
                Ok(connected) => connected,
                Err(Unconnected::CoolingOff(cool_off)) => {
                    // Nothing was sent, so no attempt is used. The breaker
                    // guards the connection for everyone; it must not make
                    // this completion give up untried, so if it stays open
                    // past the point where one last attempt still fits, wait
                    // for that point and connect regardless.
                    let final_at = deadline - final_attempt_reserve(remaining);
                    let wake = Instant::now() + cool_off;
                    let wait_until = if wake < final_at {
                        wake
                    } else {
                        last_chance = true;
                        final_at
                    };
                    report_event!(
                        debug,
                        subject.ids,
                        wait_ms = wait_until
                            .saturating_duration_since(Instant::now())
                            .as_millis() as u64,
                        last_chance,
                        "Connection cooling off; waiting to send the {} {}",
                        subject.kind().noun(),
                        subject.what
                    );
                    if !self
                        .sleep_until(wait_until, started, &mut shutdown, &mut shutdown_seen)
                        .await
                    {
                        break;
                    }
                    continue;
                }
                Err(Unconnected::Failed(e)) => {
                    attempt += 1;
                    self.log_failed_attempt(subject, attempt, None, &e);
                    if attempt >= max_attempts || last_chance {
                        break;
                    }
                    let wake = Instant::now() + self.retry.backoff(attempt);
                    if !self
                        .sleep_until(wake, started, &mut shutdown, &mut shutdown_seen)
                        .await
                    {
                        break;
                    }
                    continue;
                }
            };

            attempt += 1;
            let mut request = self.credentialed(body.clone());
            // Bounded by the budget, so an engine that stops answering
            // mid-call cannot hold a completion past it. The engine also
            // reads this as the time it has to relay the call to the
            // workflow's owner.
            request.set_timeout(remaining);
            match call(channel, request).await {
                Ok(response) => {
                    if attempt > 1 {
                        report_event!(
                            info,
                            subject.ids,
                            attempt,
                            "{} {} delivered after retrying",
                            subject.kind().title(),
                            subject.what
                        );
                    }
                    return Delivery::Delivered(response.into_inner());
                }
                Err(status) => {
                    // A message over a size limit is refused the same way on
                    // every attempt, whatever its code says about trying again.
                    let status = crate::limits::clarify(status);
                    let code = status.code();
                    if next_after(code) == Next::Drop {
                        self.log_declined(subject, &status);
                        return Delivery::Declined(code);
                    }
                    self.note_connection_failure(&status, generation);
                    self.log_failed_attempt(subject, attempt, Some(code), &status.message());
                }
            }

            if attempt >= max_attempts || last_chance {
                break;
            }
            let wake = Instant::now() + self.retry.backoff(attempt);
            if !self
                .sleep_until(wake, started, &mut shutdown, &mut shutdown_seen)
                .await
            {
                break;
            }
        }

        report_event!(
            warn,
            subject.ids,
            attempts = attempt,
            elapsed_ms = started.elapsed().as_millis() as u64,
            shutting_down = shutdown_seen.is_some(),
            "Giving up on the {} {}; {}",
            subject.kind().noun(),
            subject.what,
            subject.kind().left_to()
        );
        Delivery::Abandoned
    }

    /// The first failure is worth a warning; the ones after it, while the
    /// retrying runs its course, are detail. Giving up is warned about too.
    fn log_failed_attempt(
        &self,
        subject: Subject<'_>,
        attempt: u32,
        code: Option<Code>,
        error: &dyn std::fmt::Display,
    ) {
        if attempt == 1 {
            report_event!(
                warn,
                subject.ids,
                code = ?code,
                error = %error,
                "{} {} failed; sending it again",
                subject.kind().title(),
                subject.what
            );
        } else {
            report_event!(
                debug,
                subject.ids,
                attempt,
                code = ?code,
                error = %error,
                "{} {} failed again",
                subject.kind().title(),
                subject.what
            );
        }
    }

    /// When the current delivery must stop trying: the budget from its first
    /// attempt, or the shutdown grace from when shutdown was noticed,
    /// whichever comes first.
    fn deadline(&self, started: Instant, shutdown_seen: Option<Instant>) -> Instant {
        let budget = started + self.retry.budget;
        match shutdown_seen {
            Some(at) => budget.min(at + self.retry.shutdown_grace),
            None => budget,
        }
    }

    /// Waits until `wake`. Returns false when that is at or past the deadline,
    /// including a deadline brought forward by a shutdown that arrives
    /// during the wait.
    async fn sleep_until(
        &self,
        wake: Instant,
        started: Instant,
        shutdown: &mut watch::Receiver<bool>,
        shutdown_seen: &mut Option<Instant>,
    ) -> bool {
        loop {
            if shutdown_seen.is_none() && *shutdown.borrow_and_update() {
                *shutdown_seen = Some(Instant::now());
            }
            if wake >= self.deadline(started, *shutdown_seen) {
                return false;
            }
            let sleep = tokio::time::sleep_until(wake.into());
            if shutdown_seen.is_some() {
                sleep.await;
                return true;
            }
            tokio::select! {
                _ = sleep => return true,
                changed = shutdown.changed() => {
                    if changed.is_err() {
                        // The driver is gone; treat that as shutting down.
                        *shutdown_seen = Some(Instant::now());
                    }
                }
            }
        }
    }

    /// Tell the channel manager about a failed attempt, if the connection is
    /// to blame.
    fn note_connection_failure(&self, status: &Status, generation: u64) {
        note_connection_failure(&self.channel_manager, status, generation);
    }

    fn log_declined(&self, subject: Subject<'_>, status: &Status) {
        if status.code() == Code::FailedPrecondition {
            report_event!(
                info,
                subject.ids,
                error = %status.message(),
                "{} {} declined: the {} it answers is no longer current",
                subject.kind().title(),
                subject.what,
                match subject.kind() {
                    Kind::Workflow => "activation",
                    Kind::Task => "attempt",
                    Kind::ActorOperation => "operation",
                }
            );
        } else {
            report_event!(
                warn,
                subject.ids,
                code = ?status.code(),
                error = %status.message(),
                "{} {} refused; sending it again would not change the answer",
                subject.kind().title(),
                subject.what
            );
        }
    }
}

/// Whether a report declined with `code` was refused for its size: what
/// [`crate::limits::clarify`] makes of every size refusal before the decision
/// to retry, and an answer the engine gives for nothing else.
fn declined_as_too_large(code: Code) -> bool {
    code == Code::OutOfRange
}

/// The payload a workflow completion is made large by, named for the reason
/// the workflow fails with: its largest one, and what that is.
fn workflow_completion_named(request: &CompleteWorkflowExecutionRequest) -> String {
    use crate::proto::orcher::v1::command::Attributes;
    let mut largest: Option<(String, usize)> = None;
    let mut consider = |what: String, size: usize| {
        if largest.as_ref().is_none_or(|(_, most)| size > *most) {
            largest = Some((what, size));
        }
    };
    for command in &request.commands {
        match command.attributes.as_ref() {
            Some(Attributes::CompleteWorkflow(a)) => {
                consider("the workflow result".to_string(), a.result.len())
            }
            Some(Attributes::ScheduleTask(a)) => {
                consider(format!("the input of task {:?}", a.task_id), a.input.len())
            }
            Some(Attributes::StartChildWorkflow(a)) => consider(
                format!("the input of child workflow {:?}", a.workflow_id),
                a.input.len(),
            ),
            Some(Attributes::SendEvent(a)) => consider(
                format!("the payload of event {:?}", a.event_name),
                a.payload.len(),
            ),
            Some(Attributes::RecordStepResult(a)) => consider(
                format!("the result of step {:?}", a.step_name),
                a.result.len(),
            ),
            Some(Attributes::RestartFresh(a)) => {
                consider("the input of the fresh run".to_string(), a.input.len())
            }
            _ => {}
        }
    }
    for result in &request.query_results {
        consider(
            format!("the answer to query {:?}", result.query_id),
            result.encoded_len(),
        );
    }
    for result in &request.update_results {
        consider(
            format!("the result of update {:?}", result.update_id),
            result.encoded_len(),
        );
    }
    match largest {
        Some((what, size)) => format!(
            "the workflow's completion (largest: {what}, {})",
            crate::limits::format_bytes(size)
        ),
        None => "the workflow's completion".to_string(),
    }
}

/// `request` with nothing in it but who and what it answers.
fn without_payloads(
    request: &CompleteWorkflowExecutionRequest,
) -> CompleteWorkflowExecutionRequest {
    CompleteWorkflowExecutionRequest {
        workflow_id: request.workflow_id.clone(),
        execution_id: request.execution_id.clone(),
        namespace: request.namespace.clone(),
        identity: request.identity.clone(),
        task_token: request.task_token.clone(),
        stream_entry_id: request.stream_entry_id.clone(),
        eager_task_capabilities: request.eager_task_capabilities,
        ..Default::default()
    }
}

/// `template`, completing the activation it answers by failing the workflow
/// with `reason`, final whatever the retry policy: replay produces the same
/// completion.
fn fail_workflow_instead(
    template: CompleteWorkflowExecutionRequest,
    reason: String,
) -> CompleteWorkflowExecutionRequest {
    use crate::proto::orcher::v1::{
        command::Attributes, CommandType, FailWorkflowCommandAttributes,
    };
    CompleteWorkflowExecutionRequest {
        commands: vec![Command {
            command_type: CommandType::FailWorkflow as i32,
            attributes: Some(Attributes::FailWorkflow(FailWorkflowCommandAttributes {
                failure: Some(Failure {
                    message: reason,
                    source: "WorkflowDriver".to_string(),
                    stack_trace: String::new(),
                    cause: None,
                    failure_type: PAYLOAD_TOO_LARGE.to_string(),
                    details: vec![],
                    non_retryable: true,
                }),
            })),
        }],
        ..template
    }
}

/// Tell `channel_manager` about a failed call on connection `generation`, if
/// the connection is to blame: a call the server answered, even with an
/// error, says nothing against it.
pub(crate) fn note_connection_failure(
    channel_manager: &ChannelManager,
    status: &Status,
    generation: u64,
) {
    // Once per connection, not once per call that was on it.
    if let Blame::Connection { count } = blame(status) {
        channel_manager.connection_failed(generation, count);
    }
}

/// How long before its deadline a delivery the breaker is holding back makes
/// its last attempt: time for a connect and a call, but never more than half
/// of what is left.
fn final_attempt_reserve(remaining: Duration) -> Duration {
    Duration::from_secs(2).min(remaining / 2)
}

#[cfg(test)]
#[path = "completion_tests.rs"]
mod engine_tests;

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_poll_token_is_preferred_and_nothing_is_made_up() {
        assert_eq!(
            activation_token(b"act1.x".to_vec(), Some("act1.y")),
            b"act1.y".to_vec()
        );
        assert_eq!(
            activation_token(b"act1.x".to_vec(), None),
            b"act1.x".to_vec()
        );
        assert_eq!(activation_token(b"act1.x".to_vec(), Some("")), b"act1.x");
        assert!(activation_token(vec![], None).is_empty());
    }

    #[test]
    fn what_is_retried_and_what_is_dropped() {
        for code in [
            Code::Unavailable,
            Code::DeadlineExceeded,
            Code::Cancelled,
            Code::Unknown,
            Code::Internal,
            Code::ResourceExhausted,
            Code::Aborted,
        ] {
            assert_eq!(next_after(code), Next::Retry, "{code:?}");
        }
        for code in [
            Code::FailedPrecondition,
            Code::InvalidArgument,
            Code::NotFound,
            Code::PermissionDenied,
            Code::Unauthenticated,
            Code::Unimplemented,
            Code::AlreadyExists,
        ] {
            assert_eq!(next_after(code), Next::Drop, "{code:?}");
        }
    }

    #[test]
    fn backoff_doubles_up_to_the_cap_with_jitter_below_it() {
        let retry = CompletionRetryConfig::default();
        for _ in 0..100 {
            let first = retry.backoff(1);
            assert!(first >= Duration::from_millis(100) && first <= Duration::from_millis(200));
            let third = retry.backoff(3);
            assert!(third >= Duration::from_millis(400) && third <= Duration::from_millis(800));
            let late = retry.backoff(30);
            assert!(late >= Duration::from_millis(1500) && late <= Duration::from_secs(3));
        }
    }

    #[test]
    fn the_default_budget_fits_inside_the_claim_timeout() {
        let retry = CompletionRetryConfig::default();
        assert!(retry.budget + retry.shutdown_grace < Duration::from_secs(300) / 4);
    }

    /// Even with every wait jittered to its shortest, the default attempts
    /// last longer than a failover takes, so they do not run out first.
    #[test]
    fn the_default_attempts_outlast_a_failover() {
        let retry = CompletionRetryConfig::default();
        let shortest: Duration = (1..retry.max_attempts)
            .map(|attempt| {
                let full = retry
                    .initial_backoff
                    .saturating_mul(1u32 << (attempt - 1).min(16))
                    .min(retry.max_backoff);
                full / 2
            })
            .sum();
        assert!(shortest > Duration::from_secs(20), "{shortest:?}");
    }

    #[test]
    fn only_a_status_made_from_a_local_error_means_no_answer() {
        assert!(!no_server_answered(&Status::unavailable("from trailers")));
        let made_up = Status::from_error(Box::new(std::io::Error::new(
            std::io::ErrorKind::ConnectionReset,
            "reset",
        )));
        assert!(no_server_answered(&made_up));
    }
}
