//! Completions against a scripted engine.
//!
//! Each test stands up a real gRPC server that answers completions and
//! failure reports the way the test says, and records what arrived. What the
//! worker sends, how often, and what it does to its connection are only
//! visible from the other end of a real call.

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use tokio::sync::watch;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::{Code, Request, Response, Status};

use super::*;
use crate::bridge::{Command as BridgeCommand, CompleteWorkflowCommand, ExecutionResult};
use crate::poller::driver::{WorkflowDriver, WorkflowWorkResult};
use crate::proto::orcher::v1::execution_service_server::{
    ExecutionService, ExecutionServiceServer,
};
use crate::proto::orcher::v1::*;

/// One call as it arrived: the token it carried, and `stream_entry_id` for a
/// completion.
#[derive(Debug, Clone, PartialEq, Eq)]
struct Seen {
    task_token: Vec<u8>,
    stream_entry_id: Option<String>,
}

#[derive(Default)]
struct Script {
    /// Activations handed out by successive polls; an empty poll after.
    polls: VecDeque<PollWorkflowExecutionResponse>,
    /// Answers to successive completions and failure reports; OK after.
    answers: VecDeque<Code>,
    /// Answer every completion and failure report with this, ignoring
    /// `answers`.
    always: Option<Code>,
    completes: Vec<Seen>,
    fails: Vec<Seen>,
    /// How long each completion takes to be answered.
    delay: Duration,
    /// When set, each completion waits for a permit before it is answered.
    gate: Option<Arc<tokio::sync::Semaphore>>,
    /// When set, each poll waits for a permit before it is answered.
    poll_gate: Option<Arc<tokio::sync::Semaphore>>,
    /// Polls that have reached the engine.
    polls_started: usize,
    /// Polls the caller hung up on before they were answered.
    polls_abandoned: usize,
    /// Workflows whose activation a poll handed out: from then on the engine
    /// holds them claimed by the worker that polled.
    handed_out: Vec<String>,
    /// Answer ShutdownWorker and ReleaseWorkflowExecution UNIMPLEMENTED, as
    /// an engine before 0.5.0 does, and go on handing out work after a
    /// shutdown.
    old_engine: bool,
    /// ShutdownWorker calls that arrived. From the first, open polls are
    /// answered at once with nothing and later ones hand nothing out, as the
    /// engine does.
    shutdowns: Vec<ShutdownWorkerRequest>,
    /// ReleaseWorkflowExecution calls that arrived.
    releases: Vec<ReleaseWorkflowExecutionRequest>,
    /// The instance id each poll carried.
    poll_instance_ids: Vec<String>,
    /// The failures the failure reports carried, in order.
    failures: Vec<Failure>,
}

#[derive(Clone, Default)]
struct Engine(Arc<StdMutex<Script>>);

impl Engine {
    fn answer(&self) -> Result<(), Status> {
        let mut script = self.0.lock().unwrap();
        let code = match script.always {
            Some(code) => code,
            None => script.answers.pop_front().unwrap_or(Code::Ok),
        };
        match code {
            Code::Ok => Ok(()),
            code => Err(Status::new(code, "scripted")),
        }
    }
    fn completes(&self) -> Vec<Seen> {
        self.0.lock().unwrap().completes.clone()
    }
    /// Whether a worker has announced its shutdown to an engine that knows
    /// the call.
    fn shut_down(&self) -> bool {
        self.shut_down_locked(&self.0.lock().unwrap())
    }
    fn shut_down_locked(&self, script: &Script) -> bool {
        !script.old_engine && !script.shutdowns.is_empty()
    }
    /// The workflow and token of each release that arrived.
    fn releases(&self) -> Vec<(String, Vec<u8>)> {
        self.0
            .lock()
            .unwrap()
            .releases
            .iter()
            .map(|r| (r.workflow_id.clone(), r.task_token.clone()))
            .collect()
    }
    fn fails(&self) -> Vec<Seen> {
        self.0.lock().unwrap().fails.clone()
    }
    fn handed_out(&self) -> Vec<String> {
        self.0.lock().unwrap().handed_out.clone()
    }
    fn calls(&self) -> usize {
        let script = self.0.lock().unwrap();
        script.completes.len() + script.fails.len()
    }
}

/// Counts a poll as abandoned when dropped still armed.
struct Abandoned(Option<Arc<StdMutex<Script>>>);

impl Drop for Abandoned {
    fn drop(&mut self) {
        if let Some(script) = self.0.take() {
            if let Ok(mut script) = script.lock() {
                script.polls_abandoned += 1;
            }
        }
    }
}

#[tonic::async_trait]
impl ExecutionService for Engine {
    async fn poll_workflow_execution(
        &self,
        request: Request<PollWorkflowExecutionRequest>,
    ) -> Result<Response<PollWorkflowExecutionResponse>, Status> {
        let gate = {
            let mut script = self.0.lock().unwrap();
            script.polls_started += 1;
            script
                .poll_instance_ids
                .push(request.into_inner().worker_instance_id);
            script.poll_gate.clone()
        };
        if let Some(gate) = gate {
            // Counts the poll as abandoned if the caller hangs up while it
            // waits, which drops this future.
            let mut waiting = Abandoned(Some(Arc::clone(&self.0)));
            loop {
                if self.shut_down() {
                    waiting.0 = None;
                    return Ok(Response::new(PollWorkflowExecutionResponse::default()));
                }
                if let Ok(permit) = gate.try_acquire() {
                    permit.forget();
                    break;
                }
                tokio::time::sleep(Duration::from_millis(5)).await;
            }
            waiting.0 = None;
        }
        let next = {
            let mut script = self.0.lock().unwrap();
            let next = if self.shut_down_locked(&script) {
                None
            } else {
                script.polls.pop_front()
            };
            if let Some(activation) = &next {
                script.handed_out.push(activation.workflow_id.clone());
            }
            next
        };
        match next {
            Some(activation) => Ok(Response::new(activation)),
            None => {
                tokio::time::sleep(Duration::from_millis(50)).await;
                Ok(Response::new(PollWorkflowExecutionResponse::default()))
            }
        }
    }
    async fn poll_task_execution(
        &self,
        _: Request<PollTaskExecutionRequest>,
    ) -> Result<Response<PollTaskExecutionResponse>, Status> {
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(Response::new(PollTaskExecutionResponse::default()))
    }
    async fn complete_workflow_execution(
        &self,
        request: Request<CompleteWorkflowExecutionRequest>,
    ) -> Result<Response<CompleteWorkflowExecutionResponse>, Status> {
        let request = request.into_inner();
        let (delay, gate) = {
            let mut script = self.0.lock().unwrap();
            script.completes.push(Seen {
                task_token: request.task_token,
                stream_entry_id: Some(request.stream_entry_id),
            });
            (script.delay, script.gate.clone())
        };
        if let Some(gate) = gate {
            gate.acquire().await.unwrap().forget();
        }
        tokio::time::sleep(delay).await;
        self.answer()?;
        Ok(Response::new(CompleteWorkflowExecutionResponse::default()))
    }
    async fn fail_workflow_execution(
        &self,
        request: Request<FailWorkflowExecutionRequest>,
    ) -> Result<Response<FailWorkflowExecutionResponse>, Status> {
        let request = request.into_inner();
        let mut script = self.0.lock().unwrap();
        script.fails.push(Seen {
            task_token: request.task_token,
            stream_entry_id: None,
        });
        script.failures.extend(request.failure);
        drop(script);
        self.answer()?;
        Ok(Response::new(FailWorkflowExecutionResponse::default()))
    }
    async fn complete_task_execution(
        &self,
        _: Request<CompleteTaskExecutionRequest>,
    ) -> Result<Response<CompleteTaskExecutionResponse>, Status> {
        Err(Status::unimplemented("scripted engine"))
    }
    async fn fail_task_execution(
        &self,
        _: Request<FailTaskExecutionRequest>,
    ) -> Result<Response<FailTaskExecutionResponse>, Status> {
        Err(Status::unimplemented("scripted engine"))
    }
    async fn cancel_task_execution(
        &self,
        _: Request<CancelTaskExecutionRequest>,
    ) -> Result<Response<CancelTaskExecutionResponse>, Status> {
        Err(Status::unimplemented("scripted engine"))
    }
    async fn record_task_heartbeat(
        &self,
        _: Request<RecordTaskHeartbeatRequest>,
    ) -> Result<Response<RecordTaskHeartbeatResponse>, Status> {
        Err(Status::unimplemented("scripted engine"))
    }
    async fn respond_query_task(
        &self,
        _: Request<RespondQueryTaskRequest>,
    ) -> Result<Response<RespondQueryTaskResponse>, Status> {
        Err(Status::unimplemented("scripted engine"))
    }
    async fn respond_update_task(
        &self,
        _: Request<RespondUpdateTaskRequest>,
    ) -> Result<Response<RespondUpdateTaskResponse>, Status> {
        Err(Status::unimplemented("scripted engine"))
    }
    async fn release_workflow_execution(
        &self,
        request: Request<ReleaseWorkflowExecutionRequest>,
    ) -> Result<Response<ReleaseWorkflowExecutionResponse>, Status> {
        let mut script = self.0.lock().unwrap();
        script.releases.push(request.into_inner());
        if script.old_engine {
            return Err(Status::unimplemented("unknown method"));
        }
        Ok(Response::new(ReleaseWorkflowExecutionResponse {}))
    }
    async fn shutdown_worker(
        &self,
        request: Request<ShutdownWorkerRequest>,
    ) -> Result<Response<ShutdownWorkerResponse>, Status> {
        let mut script = self.0.lock().unwrap();
        script.shutdowns.push(request.into_inner());
        if script.old_engine {
            return Err(Status::unimplemented("unknown method"));
        }
        Ok(Response::new(ShutdownWorkerResponse {}))
    }
}

/// Poll until `done` holds, for up to ten seconds.
async fn wait_for(done: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !done() && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

async fn serve(engine: Engine) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(
        Server::builder()
            .add_service(ExecutionServiceServer::new(engine))
            .serve_with_incoming(TcpListenerStream::new(listener)),
    );
    addr
}

/// Retries quick enough for a test to watch several of them.
fn quick() -> CompletionRetryConfig {
    CompletionRetryConfig::default()
        .with_max_attempts(5)
        .with_backoff(Duration::from_millis(10), Duration::from_millis(40))
        .with_budget(Duration::from_secs(10))
        .with_shutdown_grace(Duration::from_millis(100))
}

struct Harness {
    completer: Completer,
    shutdown: watch::Sender<bool>,
}

fn completer_for(url: String, retry: CompletionRetryConfig) -> Harness {
    let (shutdown, shutdown_rx) = watch::channel(false);
    let config = WorkflowDriverConfig {
        server_url: url.clone(),
        ..WorkflowDriverConfig::default()
    };
    Harness {
        completer: Completer {
            caller: Arc::new(Caller::from(&config)),
            channel_manager: Arc::new(ChannelManager::new(url)),
            retry,
            shutdown: shutdown_rx,
            eager_task_injector: None,
            heartbeats: None,
        },
        shutdown,
    }
}

fn completion(token: &str) -> CompleteWorkflowExecutionRequest {
    CompleteWorkflowExecutionRequest {
        workflow_id: "wf-1".into(),
        execution_id: "exec-1".into(),
        namespace: "default".into(),
        task_token: token.as_bytes().to_vec(),
        stream_entry_id: token.into(),
        ..Default::default()
    }
}

fn failure_report(token: &str) -> FailWorkflowExecutionRequest {
    FailWorkflowExecutionRequest {
        workflow_id: "wf-1".into(),
        execution_id: "exec-1".into(),
        namespace: "default".into(),
        task_token: token.as_bytes().to_vec(),
        ..Default::default()
    }
}

async fn breaker_open(harness: &Harness) -> bool {
    harness.completer.channel_manager.is_cooling_off()
}

fn activation(workflow_id: &str, token: &str) -> PollWorkflowExecutionResponse {
    PollWorkflowExecutionResponse {
        workflow_id: workflow_id.into(),
        execution_id: format!("{workflow_id}-exec"),
        workflow_type: "wf".into(),
        task_queue: "default".into(),
        stream_entry_id: token.into(),
        ..Default::default()
    }
}

/// The token the engine returned on the poll is the one it gets back, on a
/// completion and on a failure report — even from a language SDK that hands
/// back only the task token, as some failure paths do.
#[tokio::test]
async fn the_poll_token_comes_back_on_completion_and_failure() {
    let engine = Engine::default();
    {
        let mut script = engine.0.lock().unwrap();
        script.polls.push_back(activation("wf-ok", "act1.b2s"));
        script.polls.push_back(activation("wf-bad", "act1.YmFk"));
    }
    let addr = serve(engine.clone()).await;
    let (driver, mut work_rx, result_tx) = WorkflowDriver::new(WorkflowDriverConfig {
        server_url: format!("http://{addr}"),
        poller_count: 1,
        poll_timeout: Duration::from_secs(1),
        ..WorkflowDriverConfig::default()
    })
    .await
    .unwrap();
    let mut driver = driver.with_completion_retry(quick());
    let shutdown = driver.shutdown_handle();
    let running = tokio::spawn(async move { driver.run().await });

    for _ in 0..2 {
        let work = tokio::time::timeout(Duration::from_secs(5), work_rx.recv())
            .await
            .expect("an activation")
            .expect("the driver is running");
        let task = work.task;
        assert_eq!(
            task.task_token,
            work.stream_entry_id.clone().unwrap().into_bytes(),
            "the task token is the activation token, not one made up from the ids"
        );
        let result = if task.execution.workflow_id == "wf-ok" {
            WorkflowWorkResult {
                workflow_id: task.execution.workflow_id.clone(),
                run_id: task.execution.run_id.clone(),
                task_token: task.task_token.clone(),
                stream_entry_id: work.stream_entry_id,
                result: Ok(ExecutionResult::success(
                    task.execution.run_id.clone(),
                    vec![BridgeCommand::CompleteWorkflow(CompleteWorkflowCommand {
                        result: crate::Payload::json(b"{}".to_vec()),
                    })],
                )),
            }
        } else {
            WorkflowWorkResult {
                workflow_id: task.execution.workflow_id.clone(),
                run_id: task.execution.run_id.clone(),
                task_token: task.task_token.clone(),
                stream_entry_id: None,
                result: Err(crate::Error::internal("the handler failed")),
            }
        };
        result_tx.send(result).await.unwrap();
    }

    let deadline = Instant::now() + Duration::from_secs(5);
    while engine.calls() < 2 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    shutdown.shutdown();
    let _ = running.await;

    assert_eq!(
        engine.completes(),
        vec![Seen {
            task_token: b"act1.b2s".to_vec(),
            stream_entry_id: Some("act1.b2s".into()),
        }]
    );
    assert_eq!(
        engine.fails(),
        vec![Seen {
            task_token: b"act1.YmFk".to_vec(),
            stream_entry_id: None,
        }]
    );
}

/// An engine that returns no token (one from before activation tokens) gets
/// an empty token back, not one made up from the ids.
#[tokio::test]
async fn no_token_from_the_poll_means_an_empty_one_back() {
    let engine = Engine::default();
    engine
        .0
        .lock()
        .unwrap()
        .polls
        .push_back(activation("wf-old", ""));
    let addr = serve(engine.clone()).await;
    let (driver, mut work_rx, result_tx) = WorkflowDriver::new(WorkflowDriverConfig {
        server_url: format!("http://{addr}"),
        poller_count: 1,
        poll_timeout: Duration::from_secs(1),
        ..WorkflowDriverConfig::default()
    })
    .await
    .unwrap();
    let mut driver = driver.with_completion_retry(quick());
    let shutdown = driver.shutdown_handle();
    let running = tokio::spawn(async move { driver.run().await });

    let work = tokio::time::timeout(Duration::from_secs(5), work_rx.recv())
        .await
        .unwrap()
        .unwrap();
    assert!(work.task.task_token.is_empty());
    assert_eq!(work.stream_entry_id, None);
    result_tx
        .send(WorkflowWorkResult {
            workflow_id: work.task.execution.workflow_id.clone(),
            run_id: work.task.execution.run_id.clone(),
            task_token: work.task.task_token.clone(),
            stream_entry_id: None,
            result: Err(crate::Error::internal("the handler failed")),
        })
        .await
        .unwrap();

    let deadline = Instant::now() + Duration::from_secs(5);
    while engine.calls() < 1 && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
    shutdown.shutdown();
    let _ = running.await;
    assert_eq!(engine.fails().len(), 1);
    assert!(engine.fails()[0].task_token.is_empty());
}

/// A completion refused as unavailable — the engine restarting, or the node
/// that took the call unable to reach the workflow's owner — is sent again
/// with the same token until it lands.
#[tokio::test]
async fn unavailable_is_retried_with_the_same_token_until_it_lands() {
    let engine = Engine::default();
    engine.0.lock().unwrap().answers =
        VecDeque::from([Code::Unavailable, Code::Unavailable, Code::Ok]);
    let addr = serve(engine.clone()).await;
    let harness = completer_for(format!("http://{addr}"), quick());

    let delivery = harness
        .completer
        .complete("wf-1", "exec-1", completion("act1.dG9r"))
        .await;

    assert!(matches!(delivery, Delivery::Delivered(())), "{delivery:?}");
    let completes = engine.completes();
    assert_eq!(completes.len(), 3);
    assert!(completes.iter().all(|seen| seen.task_token == b"act1.dG9r"
        && seen.stream_entry_id.as_deref() == Some("act1.dG9r")));
    assert!(
        !breaker_open(&harness).await,
        "an engine that answers is reachable; the breaker stays closed"
    );
}

/// The same holds for a failure report, which carries its token only in the
/// task token.
#[tokio::test]
async fn a_failure_report_is_retried_the_same_way() {
    let engine = Engine::default();
    engine.0.lock().unwrap().answers = VecDeque::from([Code::Internal, Code::DeadlineExceeded]);
    let addr = serve(engine.clone()).await;
    let harness = completer_for(format!("http://{addr}"), quick());

    let delivery = harness
        .completer
        .fail("wf-1", "exec-1", failure_report("act1.ZmFpbA"))
        .await;

    assert!(matches!(delivery, Delivery::Delivered(())), "{delivery:?}");
    let fails = engine.fails();
    assert_eq!(fails.len(), 3);
    assert!(fails.iter().all(|seen| seen.task_token == b"act1.ZmFpbA"));
}

/// An answer another attempt cannot change is taken at once, and does not
/// count against a connection that works.
#[tokio::test]
async fn a_final_refusal_is_not_retried_and_does_not_trip_the_breaker() {
    for code in [
        Code::FailedPrecondition,
        Code::InvalidArgument,
        Code::NotFound,
        Code::PermissionDenied,
    ] {
        let engine = Engine::default();
        engine.0.lock().unwrap().always = Some(code);
        let addr = serve(engine.clone()).await;
        let harness = completer_for(format!("http://{addr}"), quick());

        let delivery = harness
            .completer
            .complete("wf-1", "exec-1", completion("act1.eA"))
            .await;
        assert!(
            matches!(delivery, Delivery::Declined(c) if c == code),
            "{code:?}: {delivery:?}"
        );
        let delivery = harness
            .completer
            .fail("wf-1", "exec-1", failure_report("act1.eA"))
            .await;
        assert!(
            matches!(delivery, Delivery::Declined(c) if c == code),
            "{code:?}: {delivery:?}"
        );

        assert_eq!(engine.calls(), 2, "{code:?}: one call each, no retry");
        assert!(!breaker_open(&harness).await, "{code:?}");
    }
}

/// An engine that keeps refusing is given up on after the configured number
/// of attempts, and its refusals leave the breaker closed.
#[tokio::test]
async fn attempts_are_bounded() {
    let engine = Engine::default();
    engine.0.lock().unwrap().always = Some(Code::Unavailable);
    let addr = serve(engine.clone()).await;
    let harness = completer_for(format!("http://{addr}"), quick().with_max_attempts(3));

    let delivery = harness
        .completer
        .complete("wf-1", "exec-1", completion("act1.eA"))
        .await;

    assert!(matches!(delivery, Delivery::Abandoned), "{delivery:?}");
    assert_eq!(engine.calls(), 3);
    assert!(!breaker_open(&harness).await);
}

/// However many attempts are allowed, none starts once the time budget is
/// spent.
#[tokio::test]
async fn the_time_budget_is_bounded() {
    let engine = Engine::default();
    engine.0.lock().unwrap().always = Some(Code::Unavailable);
    let addr = serve(engine.clone()).await;
    let harness = completer_for(
        format!("http://{addr}"),
        quick()
            .with_max_attempts(1000)
            .with_backoff(Duration::from_millis(50), Duration::from_millis(50))
            .with_budget(Duration::from_millis(400)),
    );

    let started = Instant::now();
    let delivery = harness
        .completer
        .complete("wf-1", "exec-1", completion("act1.eA"))
        .await;

    assert!(matches!(delivery, Delivery::Abandoned), "{delivery:?}");
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "{:?}",
        started.elapsed()
    );
    let calls = engine.calls();
    assert!((2..=20).contains(&calls), "{calls} attempts in 400ms");
}

/// A worker shutting down stops retrying once the shutdown grace is up,
/// rather than holding the shutdown for the whole budget.
#[tokio::test]
async fn shutdown_stops_the_retrying() {
    let engine = Engine::default();
    engine.0.lock().unwrap().always = Some(Code::Unavailable);
    let addr = serve(engine.clone()).await;
    let harness = completer_for(
        format!("http://{addr}"),
        quick()
            .with_max_attempts(1000)
            .with_backoff(Duration::from_millis(30), Duration::from_millis(30))
            .with_budget(Duration::from_secs(60))
            .with_shutdown_grace(Duration::from_millis(200)),
    );

    let completer = harness.completer.clone();
    let sending = tokio::spawn(async move {
        completer
            .complete("wf-1", "exec-1", completion("act1.eA"))
            .await
    });
    wait_for(|| engine.calls() >= 2).await;
    let asked = Instant::now();
    harness.shutdown.send(true).unwrap();

    let delivery = tokio::time::timeout(Duration::from_secs(5), sending)
        .await
        .expect("retrying stops after the grace, not after the budget")
        .unwrap();
    assert!(matches!(delivery, Delivery::Abandoned), "{delivery:?}");
    // The budget is a minute; stopping within a few seconds can only be the
    // grace. Loose, so a loaded machine does not fail it.
    assert!(
        asked.elapsed() < Duration::from_secs(5),
        "{:?}",
        asked.elapsed()
    );
}

/// Nothing listening: every attempt fails to connect, the manager's own
/// breaker opens, and the completion is given up on within its budget.
#[tokio::test]
async fn no_engine_at_all_is_given_up_on_within_the_budget() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let harness = completer_for(
        format!("http://127.0.0.1:{port}"),
        quick()
            .with_max_attempts(3)
            .with_budget(Duration::from_secs(3)),
    );

    let started = Instant::now();
    let delivery = harness
        .completer
        .complete("wf-1", "exec-1", completion("act1.eA"))
        .await;

    assert!(matches!(delivery, Delivery::Abandoned), "{delivery:?}");
    assert!(breaker_open(&harness).await);
    assert!(
        started.elapsed() < Duration::from_secs(8),
        "{:?}",
        started.elapsed()
    );
}

/// A result the language SDK handed back just before shutdown is still sent.
#[tokio::test]
async fn a_result_queued_at_shutdown_is_still_sent() {
    let engine = Engine::default();
    let addr = serve(engine.clone()).await;
    let (driver, _work_rx, result_tx) = WorkflowDriver::new(WorkflowDriverConfig {
        server_url: format!("http://{addr}"),
        poller_count: 1,
        poll_timeout: Duration::from_secs(1),
        ..WorkflowDriverConfig::default()
    })
    .await
    .unwrap();
    let mut driver = driver.with_completion_retry(quick());
    let shutdown = driver.shutdown_handle();

    result_tx
        .send(WorkflowWorkResult {
            workflow_id: "wf-1".into(),
            run_id: "exec-1".into(),
            task_token: b"act1.cQ".to_vec(),
            stream_entry_id: Some("act1.cQ".into()),
            result: Err(crate::Error::internal("the handler failed")),
        })
        .await
        .unwrap();
    shutdown.shutdown();
    tokio::time::timeout(Duration::from_secs(5), driver.run())
        .await
        .expect("the driver stops")
        .unwrap();

    assert_eq!(engine.fails().len(), 1);
    assert_eq!(engine.fails()[0].task_token, b"act1.cQ");
}

/// A status the server sent is told apart from one tonic made up; the breaker
/// decision rests on it.
#[tokio::test]
async fn a_status_from_the_server_counts_as_an_answer() {
    let engine = Engine::default();
    engine.0.lock().unwrap().always = Some(Code::Unavailable);
    let addr = serve(engine).await;
    let mut client = ExecutionServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    let status = client
        .complete_workflow_execution(completion("act1.eA"))
        .await
        .unwrap_err();
    assert_eq!(status.code(), Code::Unavailable);
    assert!(!no_server_answered(&status));
}

// ── A connection that breaks under many completions at once ────────────────

/// A TCP proxy that can be killed — its listener and every connection through
/// it closed — and started again on the same port.
struct Proxy {
    addr: SocketAddr,
    upstream: SocketAddr,
    running: Option<tokio::task::JoinHandle<()>>,
    /// Connections accepted, over every run.
    accepted: Arc<std::sync::atomic::AtomicUsize>,
}

impl Proxy {
    async fn start(upstream: SocketAddr) -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let mut proxy = Proxy {
            addr,
            upstream,
            running: None,
            accepted: Arc::default(),
        };
        proxy.run(listener);
        proxy
    }

    fn run(&mut self, listener: tokio::net::TcpListener) {
        let upstream = self.upstream;
        let accepted = Arc::clone(&self.accepted);
        self.running = Some(tokio::spawn(async move {
            // Dropped with this task when the proxy is killed, which aborts
            // every connection through it.
            let mut connections = tokio::task::JoinSet::new();
            loop {
                let Ok((mut inbound, _)) = listener.accept().await else {
                    return;
                };
                accepted.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                connections.spawn(async move {
                    if let Ok(mut outbound) = tokio::net::TcpStream::connect(upstream).await {
                        let _ = tokio::io::copy_bidirectional(&mut inbound, &mut outbound).await;
                    }
                });
            }
        }));
    }

    async fn kill(&mut self) {
        if let Some(running) = self.running.take() {
            running.abort();
            let _ = running.await;
        }
    }

    async fn restart(&mut self) {
        let listener = tokio::net::TcpListener::bind(self.addr).await.unwrap();
        self.run(listener);
    }
}

/// Ten completions sent into a connection that has just broken, with the
/// engine back a moment later: every one is delivered.
///
/// All ten are handed the connection that was working a moment ago, and all
/// ten fail on it at once. That one breakage must count once: counted ten
/// times, it would open the breaker for its full thirty seconds, longer than
/// any completion's budget, and all ten would give up within a fraction of a
/// second without trying again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_connection_lost_under_many_completions_costs_none_of_them() {
    let engine = Engine::default();
    let engine_addr = serve(engine.clone()).await;
    let mut proxy = Proxy::start(engine_addr).await;
    let harness = completer_for(
        format!("http://{}", proxy.addr),
        CompletionRetryConfig::default(),
    );

    // A connection that works, then breaks.
    let warm = harness
        .completer
        .complete("wf-1", "exec-1", completion("act1.first"))
        .await;
    assert!(matches!(warm, Delivery::Delivered(())), "{warm:?}");
    proxy.kill().await;

    let mut sending = tokio::task::JoinSet::new();
    for i in 0..10 {
        let completer = harness.completer.clone();
        sending.spawn(async move {
            completer
                .complete("wf-1", "exec-1", completion(&format!("act1.{i}")))
                .await
        });
    }
    tokio::time::sleep(Duration::from_millis(1500)).await;
    proxy.restart().await;

    let mut delivered = 0;
    while let Some(delivery) = sending.join_next().await {
        if matches!(delivery.unwrap(), Delivery::Delivered(())) {
            delivered += 1;
        }
    }
    assert_eq!(delivered, 10, "delivered after the outage");
}

/// A breaker that stays open past a completion's deadline does not make it
/// give up untried: it makes one last attempt before the deadline.
#[tokio::test]
async fn a_breaker_open_past_the_deadline_still_leaves_a_last_attempt() {
    let engine = Engine::default();
    let addr = serve(engine.clone()).await;
    let url = format!("http://{addr}");
    let mut manager = ChannelManager::new(url.clone());
    for _ in 0..8 {
        manager.record_failure();
    }
    assert!(manager.cooling_off_duration() > Duration::from_secs(20));
    let mut harness = completer_for(url, quick().with_budget(Duration::from_secs(3)));
    harness.completer.channel_manager = Arc::new(manager);

    let delivery = harness
        .completer
        .complete("wf-1", "exec-1", completion("act1.eA"))
        .await;

    assert!(matches!(delivery, Delivery::Delivered(())), "{delivery:?}");
    assert_eq!(engine.completes().len(), 1);
}

/// Callers that find no connection share one connect rather than each making
/// their own, and a connect in progress does not hold up anyone asking about
/// the breaker.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn callers_without_a_connection_share_one_connect() {
    let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    drop(listener);
    let manager = Arc::new(ChannelManager::new(format!("http://127.0.0.1:{port}")));

    let mut asking = tokio::task::JoinSet::new();
    for _ in 0..10 {
        let manager = Arc::clone(&manager);
        asking.spawn(async move {
            manager
                .connection(crate::poller::channel::Breaker::Respect)
                .await
                .map(|_| ())
        });
    }
    while let Some(asked) = asking.join_next().await {
        assert!(asked.unwrap().is_err());
    }
    // One failed connect counted: a one-second cool-off, not ten doublings.
    let cool_off = manager.cooling_off_duration();
    assert!(cool_off <= Duration::from_millis(1300), "{cool_off:?}");
}

// ── Shutdown ────────────────────────────────────────────────────────────────

fn failed_result(token: &str) -> WorkflowWorkResult {
    WorkflowWorkResult {
        workflow_id: "wf-1".into(),
        run_id: "exec-1".into(),
        task_token: token.as_bytes().to_vec(),
        stream_entry_id: Some(token.into()),
        result: Err(crate::Error::internal("the handler failed")),
    }
}

/// One poller, and polls short enough that an idle one ends soon; override
/// what a test needs with `..test_config()`.
fn test_config() -> WorkflowDriverConfig {
    WorkflowDriverConfig {
        poller_count: 1,
        poll_timeout: Duration::from_secs(1),
        ..WorkflowDriverConfig::default()
    }
}

/// A driver for `engine`, built from `config` with only the address filled
/// in, so every setting the test chose applies.
async fn driver_for(engine: &Engine, config: WorkflowDriverConfig) -> WorkflowDriverParts {
    let addr = serve(engine.clone()).await;
    let (driver, work_rx, result_tx) = WorkflowDriver::new(WorkflowDriverConfig {
        server_url: format!("http://{addr}"),
        ..config
    })
    .await
    .unwrap();
    (driver, work_rx, result_tx)
}

type WorkflowDriverParts = (
    WorkflowDriver,
    tokio::sync::mpsc::Receiver<crate::poller::driver::WorkflowWork>,
    tokio::sync::mpsc::Sender<WorkflowWorkResult>,
);

/// A driver running in a task of its own is stopped through a handle taken
/// beforehand, and a completion it is still retrying gets the grace to land.
#[tokio::test]
async fn a_running_driver_is_stopped_through_its_handle_and_finishes_its_retries() {
    let engine = Engine::default();
    engine.0.lock().unwrap().answers =
        VecDeque::from([Code::Unavailable, Code::Unavailable, Code::Unavailable]);
    let (driver, _work_rx, result_tx) = driver_for(&engine, test_config()).await;
    let mut driver = driver.with_completion_retry(
        quick()
            .with_backoff(Duration::from_millis(100), Duration::from_millis(100))
            .with_shutdown_grace(Duration::from_secs(5)),
    );
    let handle = driver.shutdown_handle();
    let running = tokio::spawn(async move { driver.run().await });

    result_tx.send(failed_result("act1.aA")).await.unwrap();
    wait_for(|| engine.calls() >= 1).await;
    handle.shutdown();

    tokio::time::timeout(Duration::from_secs(10), running)
        .await
        .expect("the driver stops")
        .unwrap()
        .unwrap();
    assert_eq!(
        engine.fails().len(),
        4,
        "retried to the end within the grace"
    );
}

/// A driver whose run is dropped rather than shut down says how many
/// completions it abandoned, instead of letting them vanish.
#[tokio::test]
async fn a_dropped_driver_says_what_it_abandoned() {
    let logs = Arc::new(StdMutex::new(Vec::<u8>::new()));
    let writer = {
        let logs = Arc::clone(&logs);
        move || LogWriter(Arc::clone(&logs))
    };
    let subscriber = tracing_subscriber::fmt()
        .with_writer(writer)
        .with_ansi(false)
        .finish();
    let _logging = tracing::subscriber::set_default(subscriber);

    let engine = Engine::default();
    engine.0.lock().unwrap().always = Some(Code::Unavailable);
    let (driver, _work_rx, result_tx) = driver_for(&engine, test_config()).await;
    let mut driver = driver.with_completion_retry(
        quick()
            .with_max_attempts(1000)
            .with_budget(Duration::from_secs(60)),
    );
    result_tx.send(failed_result("act1.aA")).await.unwrap();
    let _ = tokio::time::timeout(Duration::from_millis(500), driver.run()).await;

    let logs = String::from_utf8(logs.lock().unwrap().clone()).unwrap();
    assert!(
        logs.contains("abandoning workflow completions") && logs.contains("abandoned=1"),
        "{logs}"
    );
}

struct LogWriter(Arc<StdMutex<Vec<u8>>>);

impl std::io::Write for LogWriter {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.0.lock().unwrap().extend_from_slice(buf);
        Ok(buf.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}

// ── Backpressure ────────────────────────────────────────────────────────────

/// With every slot taken by a completion still being sent, the driver takes
/// no further result, and an activation counts as finished only once its
/// completion has landed.
#[tokio::test]
async fn completions_in_flight_are_capped_and_counted_when_they_land() {
    let engine = Engine::default();
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    engine.0.lock().unwrap().gate = Some(Arc::clone(&gate));
    let (driver, _work_rx, result_tx) = driver_for(
        &engine,
        WorkflowDriverConfig {
            max_concurrent_executions: 1,
            ..test_config()
        },
    )
    .await;
    let metrics = crate::poller::metrics::WorkerMetrics::new();
    let mut driver = driver
        .with_completion_retry(quick())
        .with_metrics(Arc::clone(&metrics));
    let handle = driver.shutdown_handle();
    let running = tokio::spawn(async move { driver.run().await });

    let succeeded = |token: &str| WorkflowWorkResult {
        workflow_id: "wf-1".into(),
        run_id: "exec-1".into(),
        task_token: token.as_bytes().to_vec(),
        stream_entry_id: Some(token.into()),
        result: Ok(ExecutionResult::success(
            "exec-1".into(),
            vec![BridgeCommand::CompleteWorkflow(CompleteWorkflowCommand {
                result: crate::Payload::json(b"{}".to_vec()),
            })],
        )),
    };
    result_tx.send(succeeded("act1.one")).await.unwrap();
    wait_for(|| engine.completes().len() == 1).await;
    result_tx.send(succeeded("act1.two")).await.unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert_eq!(engine.completes().len(), 1, "a second completion was taken");
    assert_eq!(
        metrics.snapshot().workflows_completed,
        0,
        "counted before its completion landed"
    );

    gate.add_permits(2);
    wait_for(|| metrics.snapshot().workflows_completed == 2).await;
    assert_eq!(engine.completes().len(), 2);
    handle.shutdown();
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
}

// ── Answers that are not the connection's fault ─────────────────────────────

#[derive(Clone, Copy)]
enum Raw {
    /// Headers with the gRPC content type, then an error in the trailers.
    ErrorInTrailers,
    /// HTTP 502 and nothing else, as a proxy with no upstream sends.
    BadGateway,
    /// The stream refused: a live server declining it, unprocessed.
    Refused,
    /// The stream reset for an internal error, with no status.
    ResetInternal,
    /// A GOAWAY without an error before any stream is read: the server
    /// draining, every stream refused.
    Draining,
}

/// A bare HTTP/2 server answering every call the same way.
async fn serve_raw(answer: Raw, calls: Arc<std::sync::atomic::AtomicUsize>) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        loop {
            let Ok((socket, _)) = listener.accept().await else {
                return;
            };
            let calls = Arc::clone(&calls);
            tokio::spawn(async move {
                let Ok(mut connection) = h2::server::handshake(socket).await else {
                    return;
                };
                if let Raw::Draining = answer {
                    calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    connection.abrupt_shutdown(h2::Reason::NO_ERROR);
                    while connection.accept().await.is_some() {}
                    return;
                }
                while let Some(Ok((_request, mut respond))) = connection.accept().await {
                    calls.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    match answer {
                        Raw::ErrorInTrailers => {
                            let response = http::Response::builder()
                                .status(200)
                                .header("content-type", "application/grpc")
                                .body(())
                                .unwrap();
                            let Ok(mut body) = respond.send_response(response, false) else {
                                continue;
                            };
                            let mut trailers = http::HeaderMap::new();
                            trailers.insert("grpc-status", "14".parse().unwrap());
                            trailers.insert("grpc-message", "busy".parse().unwrap());
                            let _ = body.send_trailers(trailers);
                        }
                        Raw::BadGateway => {
                            let response = http::Response::builder().status(502).body(()).unwrap();
                            let _ = respond.send_response(response, true);
                        }
                        Raw::Refused => respond.send_reset(h2::Reason::REFUSED_STREAM),
                        Raw::ResetInternal => respond.send_reset(h2::Reason::INTERNAL_ERROR),
                        Raw::Draining => {}
                    }
                }
            });
        }
    });
    addr
}

/// A server that answers — with an error in trailers after its headers, or a
/// proxy's bare 502 — is reachable. Those are retried, and the connection's
/// breaker stays closed.
#[tokio::test]
async fn an_answer_without_grpc_headers_is_still_an_answer() {
    for answer in [Raw::ErrorInTrailers, Raw::BadGateway] {
        let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let addr = serve_raw(answer, Arc::clone(&calls)).await;
        let harness = completer_for(format!("http://{addr}"), quick().with_max_attempts(2));

        let delivery = harness
            .completer
            .complete("wf-1", "exec-1", completion("act1.eA"))
            .await;

        assert!(matches!(delivery, Delivery::Abandoned), "{delivery:?}");
        assert_eq!(
            calls.load(std::sync::atomic::Ordering::SeqCst),
            2,
            "retried"
        );
        assert!(!breaker_open(&harness).await, "breaker opened on an answer");
    }
}

/// A stream reset for an error, with no status, is no answer: the
/// connection is dropped, to be made again on the next attempt.
#[tokio::test]
async fn a_reset_stream_is_not_an_answer() {
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let addr = serve_raw(Raw::ResetInternal, Arc::clone(&calls)).await;
    let harness = completer_for(format!("http://{addr}"), quick().with_max_attempts(1));

    let delivery = harness
        .completer
        .complete("wf-1", "exec-1", completion("act1.eA"))
        .await;

    assert!(matches!(delivery, Delivery::Abandoned), "{delivery:?}");
    assert_eq!(calls.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(!harness.completer.channel_manager.is_connected());
}

/// A live server refusing a stream it did not process is still there: the
/// completion is retried on the same connection, and the breaker stays shut.
#[tokio::test]
async fn a_refused_stream_leaves_the_connection_alone() {
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let addr = serve_raw(Raw::Refused, Arc::clone(&calls)).await;
    let harness = completer_for(format!("http://{addr}"), quick().with_max_attempts(2));

    let delivery = harness
        .completer
        .complete("wf-1", "exec-1", completion("act1.eA"))
        .await;

    assert!(matches!(delivery, Delivery::Abandoned), "{delivery:?}");
    assert_eq!(
        calls.load(std::sync::atomic::Ordering::SeqCst),
        2,
        "retried"
    );
    assert!(
        !breaker_open(&harness).await,
        "breaker opened on a live server"
    );
}

/// A GOAWAY without an error — a server draining — refuses the stream and
/// says nothing against the connection.
#[tokio::test]
async fn a_draining_server_is_not_blamed() {
    let calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let addr = serve_raw(Raw::Draining, Arc::clone(&calls)).await;
    let mut client = ExecutionServiceClient::connect(format!("http://{addr}"))
        .await
        .unwrap();
    let status = client
        .complete_workflow_execution(completion("act1.eA"))
        .await
        .unwrap_err();
    assert_eq!(blame(&status), Blame::None, "{status:?}");
}

/// A call that runs out its own deadline on a slow engine keeps the
/// connection: the engine is slow, not gone, and the retry goes out on the
/// same connection.
#[tokio::test]
async fn a_client_side_timeout_keeps_the_connection() {
    let engine = Engine::default();
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    engine.0.lock().unwrap().gate = Some(Arc::clone(&gate));
    let proxy = Proxy::start(serve(engine.clone()).await).await;
    let harness = completer_for(
        format!("http://{}", proxy.addr),
        quick()
            .with_max_attempts(1)
            .with_budget(Duration::from_millis(500)),
    );

    let delivery = harness
        .completer
        .complete("wf-1", "exec-1", completion("act1.eA"))
        .await;
    assert!(matches!(delivery, Delivery::Abandoned), "{delivery:?}");
    assert!(!breaker_open(&harness).await);

    gate.add_permits(10);
    let delivery = harness
        .completer
        .complete("wf-1", "exec-1", completion("act1.eA"))
        .await;
    assert!(matches!(delivery, Delivery::Delivered(())), "{delivery:?}");
    assert_eq!(
        proxy.accepted.load(std::sync::atomic::Ordering::SeqCst),
        1,
        "reconnected after a timeout"
    );
}

// ── Recovery ────────────────────────────────────────────────────────────────

/// After a long outage the breaker's cool-off has doubled well past the
/// retry backoff; a completion still lands within a backoff of the engine
/// coming back, not a cool-off.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_completion_lands_promptly_after_a_long_outage() {
    let engine = Engine::default();
    let mut proxy = Proxy::start(serve(engine.clone()).await).await;
    let harness = completer_for(
        format!("http://{}", proxy.addr),
        CompletionRetryConfig::default(),
    );
    let warm = harness
        .completer
        .complete("wf-1", "exec-1", completion("act1.first"))
        .await;
    assert!(matches!(warm, Delivery::Delivered(())), "{warm:?}");
    proxy.kill().await;

    let completer = harness.completer.clone();
    let sending = tokio::spawn(async move {
        let delivery = completer
            .complete("wf-1", "exec-1", completion("act1.late"))
            .await;
        (delivery, Instant::now())
    });
    tokio::time::sleep(Duration::from_secs(16)).await;
    proxy.restart().await;
    let back = Instant::now();

    let (delivery, landed) = sending.await.unwrap();
    assert!(matches!(delivery, Delivery::Delivered(())), "{delivery:?}");
    let after = landed.saturating_duration_since(back);
    assert!(
        after < Duration::from_secs(5),
        "landed {after:?} after the engine was back"
    );
}

// ── The driver never stops reading results ──────────────────────────────────

/// A language SDK that runs at most as many handlers as the driver's
/// capacity, each holding its place until its result is taken. With
/// completions slow enough to fill every slot, the result channel fills, the
/// SDK stops reading work. A driver that waited on handing over new work
/// would then stop reading results, and nothing would move again.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn slow_completions_and_a_capped_sdk_do_not_lock_the_driver() {
    const WORKFLOWS: usize = 120;
    let engine = Engine::default();
    {
        let mut script = engine.0.lock().unwrap();
        script.delay = Duration::from_millis(500);
        for i in 0..WORKFLOWS {
            script
                .polls
                .push_back(activation(&format!("wf-{i}"), &format!("act1.{i}")));
        }
    }
    let (driver, mut work_rx, result_tx) = driver_for(
        &engine,
        WorkflowDriverConfig {
            max_concurrent_executions: 10,
            poller_count: 4,
            ..test_config()
        },
    )
    .await;
    let mut driver = driver.with_completion_retry(quick());
    let handle = driver.shutdown_handle();
    let running = tokio::spawn(async move { driver.run().await });

    let handlers = Arc::new(tokio::sync::Semaphore::new(10));
    let sdk = tokio::spawn(async move {
        loop {
            let place = Arc::clone(&handlers).acquire_owned().await.unwrap();
            let Some(work) = work_rx.recv().await else {
                return;
            };
            let result_tx = result_tx.clone();
            tokio::spawn(async move {
                let task = work.task;
                let result = WorkflowWorkResult {
                    workflow_id: task.execution.workflow_id.clone(),
                    run_id: task.execution.run_id.clone(),
                    task_token: task.task_token.clone(),
                    stream_entry_id: work.stream_entry_id,
                    result: Ok(ExecutionResult::success(
                        task.execution.run_id.clone(),
                        vec![BridgeCommand::CompleteWorkflow(CompleteWorkflowCommand {
                            result: crate::Payload::json(b"{}".to_vec()),
                        })],
                    )),
                };
                let _ = result_tx.send(result).await;
                drop(place);
            });
        }
    });

    let deadline = Instant::now() + Duration::from_secs(20);
    while engine.completes().len() < WORKFLOWS && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    let done = engine.completes().len();
    handle.shutdown();
    sdk.abort();
    let _ = tokio::time::timeout(Duration::from_secs(10), running).await;
    assert_eq!(done, WORKFLOWS, "stuck at {done}/{WORKFLOWS}");
}

/// A driver whose language SDK has dropped both its result sender and its
/// work receiver stops by itself, as its documentation says.
#[tokio::test]
async fn a_driver_left_with_no_sdk_stops() {
    let engine = Engine::default();
    let (driver, work_rx, result_tx) = driver_for(&engine, test_config()).await;
    let mut driver = driver.with_completion_retry(quick());
    let pollers_told = driver.shutdown_sender.subscribe();
    drop(work_rx);
    drop(result_tx);
    tokio::time::timeout(Duration::from_secs(5), driver.run())
        .await
        .expect("the driver stops once its SDK is gone")
        .unwrap();
    // Its pollers are told to stop with it, instead of claiming activations
    // nobody is left to run.
    assert!(*pollers_told.borrow(), "the pollers were not told to stop");
}

// ── Shutdown with a poll in flight ──────────────────────────────────────────

/// A language SDK that runs every activation it is given and hands back a
/// completion for it, recording which workflows it ran.
fn completing_sdk(
    mut work_rx: tokio::sync::mpsc::Receiver<crate::poller::driver::WorkflowWork>,
    result_tx: tokio::sync::mpsc::Sender<WorkflowWorkResult>,
    delay: Duration,
) -> Arc<StdMutex<Vec<String>>> {
    let ran = Arc::new(StdMutex::new(Vec::new()));
    let seen = Arc::clone(&ran);
    tokio::spawn(async move {
        while let Some(work) = work_rx.recv().await {
            let task = work.task;
            seen.lock()
                .unwrap()
                .push(task.execution.workflow_id.clone());
            let result_tx = result_tx.clone();
            tokio::spawn(async move {
                tokio::time::sleep(delay).await;
                let _ = result_tx
                    .send(WorkflowWorkResult {
                        workflow_id: task.execution.workflow_id.clone(),
                        run_id: task.execution.run_id.clone(),
                        task_token: task.task_token.clone(),
                        stream_entry_id: work.stream_entry_id,
                        result: Ok(ExecutionResult::success(
                            task.execution.run_id.clone(),
                            vec![BridgeCommand::CompleteWorkflow(CompleteWorkflowCommand {
                                result: crate::Payload::json(b"{}".to_vec()),
                            })],
                        )),
                    })
                    .await;
            });
        }
    });
    ran
}

/// A poll already out when shutdown arrives, which then brings back an
/// activation: the engine has claimed it for this worker, so it must reach
/// the language SDK and be completed, not be dropped on the way. Dropped, it
/// would sit claimed until the engine's claim timeout.
#[tokio::test]
async fn an_activation_polled_during_shutdown_is_run_and_completed() {
    let engine = Engine::default();
    let gate = Arc::new(tokio::sync::Semaphore::new(0));
    {
        let mut script = engine.0.lock().unwrap();
        script.poll_gate = Some(Arc::clone(&gate));
        script.polls.push_back(activation("wf-late", "act1.bGF0ZQ"));
        // An engine that would end the poll at the shutdown never lets it
        // bring anything back; one before 0.5.0 does.
        script.old_engine = true;
    }
    let (driver, work_rx, result_tx) = driver_for(
        &engine,
        WorkflowDriverConfig {
            poll_timeout: Duration::from_secs(30),
            ..test_config()
        },
    )
    .await;
    let mut driver =
        driver.with_completion_retry(quick().with_shutdown_grace(Duration::from_secs(3)));
    let handle = driver.shutdown_handle();
    let running = tokio::spawn(async move { driver.run().await });
    let ran = completing_sdk(work_rx, result_tx, Duration::ZERO);

    wait_for(|| engine.0.lock().unwrap().polls_started >= 1).await;
    handle.shutdown();
    // The shutdown has been seen before the poll answers.
    tokio::time::sleep(Duration::from_millis(200)).await;
    gate.add_permits(1);

    tokio::time::timeout(Duration::from_secs(10), running)
        .await
        .expect("the driver stops")
        .unwrap()
        .unwrap();

    assert_eq!(engine.handed_out(), vec!["wf-late".to_string()]);
    assert_eq!(
        *ran.lock().unwrap(),
        vec!["wf-late".to_string()],
        "the claimed activation never reached the language SDK"
    );
    assert_eq!(
        engine.completes(),
        vec![Seen {
            task_token: b"act1.bGF0ZQ".to_vec(),
            stream_entry_id: Some("act1.bGF0ZQ".into()),
        }],
        "the claimed activation was never completed"
    );
}

/// An activation handed over before shutdown whose result comes back after
/// it is still completed, within the grace.
#[tokio::test]
async fn a_result_that_comes_back_after_shutdown_is_still_sent() {
    let engine = Engine::default();
    engine
        .0
        .lock()
        .unwrap()
        .polls
        .push_back(activation("wf-slow", "act1.c2xvdw"));
    let (driver, work_rx, result_tx) = driver_for(&engine, test_config()).await;
    let mut driver =
        driver.with_completion_retry(quick().with_shutdown_grace(Duration::from_secs(3)));
    let handle = driver.shutdown_handle();
    let running = tokio::spawn(async move { driver.run().await });
    let ran = completing_sdk(work_rx, result_tx, Duration::from_millis(300));

    wait_for(|| !ran.lock().unwrap().is_empty()).await;
    handle.shutdown();

    tokio::time::timeout(Duration::from_secs(10), running)
        .await
        .expect("the driver stops")
        .unwrap()
        .unwrap();
    assert_eq!(
        engine.completes().len(),
        1,
        "the result handed back during shutdown was dropped"
    );
}

/// A long poll with nothing to bring back does not hold shutdown up past the
/// grace: once the driver stops taking activations, the poll is abandoned.
#[tokio::test]
async fn an_idle_poll_does_not_hold_shutdown_past_the_grace() {
    let engine = Engine::default();
    {
        let mut script = engine.0.lock().unwrap();
        script.poll_gate = Some(Arc::new(tokio::sync::Semaphore::new(0)));
        // An engine before 0.5.0, which cannot be told to end the poll.
        script.old_engine = true;
    }
    let (driver, work_rx, result_tx) = driver_for(
        &engine,
        WorkflowDriverConfig {
            poll_timeout: Duration::from_secs(30),
            ..test_config()
        },
    )
    .await;
    let mut driver =
        driver.with_completion_retry(quick().with_shutdown_grace(Duration::from_millis(300)));
    let handle = driver.shutdown_handle();
    let running = tokio::spawn(async move { driver.run().await });
    let _ran = completing_sdk(work_rx, result_tx, Duration::ZERO);

    wait_for(|| engine.0.lock().unwrap().polls_started >= 1).await;
    let asked = Instant::now();
    handle.shutdown();
    tokio::time::timeout(Duration::from_secs(5), running)
        .await
        .expect("the driver stops")
        .unwrap()
        .unwrap();
    assert!(
        asked.elapsed() < Duration::from_secs(2),
        "stopping took {:?}",
        asked.elapsed()
    );
    wait_for(|| engine.0.lock().unwrap().polls_abandoned >= 1).await;
    let abandoned = engine.0.lock().unwrap().polls_abandoned;
    assert_eq!(abandoned, 1, "the poll in flight was not abandoned");
    assert!(engine.handed_out().is_empty());
    // Asked, answered UNIMPLEMENTED, and carried on.
    assert_eq!(engine.0.lock().unwrap().shutdowns.len(), 1);
}

/// Shut down, a driver tells the engine once, naming its namespace,
/// identity, this process and its queue. The engine ends its open poll, so an
/// idle driver stops at once rather than after the grace. Every poll named
/// the process.
#[tokio::test]
async fn shutdown_tells_the_engine_and_an_idle_driver_stops_at_once() {
    let engine = Engine::default();
    engine.0.lock().unwrap().poll_gate = Some(Arc::new(tokio::sync::Semaphore::new(0)));
    let (driver, work_rx, result_tx) = driver_for(
        &engine,
        WorkflowDriverConfig {
            namespace: "ns-1".into(),
            identity: "wf-worker".into(),
            task_queue: "q-1".into(),
            poll_timeout: Duration::from_secs(30),
            ..test_config()
        },
    )
    .await;
    let mut driver =
        driver.with_completion_retry(quick().with_shutdown_grace(Duration::from_secs(5)));
    let handle = driver.shutdown_handle();
    let running = tokio::spawn(async move { driver.run().await });
    let _ran = completing_sdk(work_rx, result_tx, Duration::ZERO);

    wait_for(|| engine.0.lock().unwrap().polls_started >= 1).await;
    let asked = Instant::now();
    handle.shutdown();
    tokio::time::timeout(Duration::from_secs(10), running)
        .await
        .expect("the driver stops")
        .unwrap()
        .unwrap();
    assert!(
        asked.elapsed() < AT_ONCE,
        "an idle driver waited out the grace: {:?}",
        asked.elapsed()
    );

    let script = engine.0.lock().unwrap();
    assert_eq!(script.shutdowns.len(), 1, "told the engine once");
    let shutdown = &script.shutdowns[0];
    assert_eq!(shutdown.namespace, "ns-1");
    assert_eq!(shutdown.identity, "wf-worker");
    assert_eq!(
        shutdown.worker_instance_id,
        crate::poller::worker_instance_id()
    );
    assert_eq!(shutdown.task_queues, vec!["q-1"]);
    assert!(!script.poll_instance_ids.is_empty());
    assert!(
        script
            .poll_instance_ids
            .iter()
            .all(|id| id == crate::poller::worker_instance_id()),
        "a poll did not name the process: {:?}",
        script.poll_instance_ids
    );
    assert!(script.handed_out.is_empty());
}

/// Everything logged on this thread while the guard is held.
fn capture_logs() -> (Arc<StdMutex<Vec<u8>>>, tracing::subscriber::DefaultGuard) {
    let logs = Arc::new(StdMutex::new(Vec::<u8>::new()));
    let writer = {
        let logs = Arc::clone(&logs);
        move || LogWriter(Arc::clone(&logs))
    };
    let subscriber = tracing_subscriber::fmt()
        .with_writer(writer)
        .with_ansi(false)
        .finish();
    (logs, tracing::subscriber::set_default(subscriber))
}

fn error_names(logs: &StdMutex<Vec<u8>>, workflow_id: &str) -> bool {
    let logs = String::from_utf8(logs.lock().unwrap().clone()).unwrap();
    logs.lines()
        .any(|line| line.contains("ERROR") && line.contains(workflow_id))
}

/// Built with a long poll timeout and a long grace, so that a driver waiting
/// on either is plainly slower than one that stops at once.
async fn slow_to_stop(engine: &Engine, max_concurrent_executions: usize) -> WorkflowDriverParts {
    let (driver, work_rx, result_tx) = driver_for(
        engine,
        WorkflowDriverConfig {
            max_concurrent_executions,
            poll_timeout: Duration::from_secs(30),
            ..test_config()
        },
    )
    .await;
    let driver = driver.with_completion_retry(quick().with_shutdown_grace(Duration::from_secs(3)));
    (driver, work_rx, result_tx)
}

const AT_ONCE: Duration = Duration::from_millis(1000);

/// The order some bindings stop in: the language side stops taking work
/// first, and the driver is shut down afterwards. One activation was handed
/// over and left unread; another was polled but could not be handed over.
/// The driver stops at once, rather than waiting the grace for a result that
/// cannot come, and names the activation it could not hand over.
#[tokio::test]
async fn a_driver_shut_down_after_its_sdk_stopped_taking_work_stops_at_once() {
    let (logs, _logging) = capture_logs();
    let engine = Engine::default();
    {
        let mut script = engine.0.lock().unwrap();
        script.polls.push_back(activation("wf-unread", "act1.dQ"));
        script.polls.push_back(activation("wf-waiting", "act1.dw"));
        // An engine before 0.5.0, which cannot take the activation back.
        script.old_engine = true;
    }
    // One place in each channel: the first activation fills the SDK's, and
    // the second waits in the driver's.
    let (mut driver, work_rx, _result_tx) = slow_to_stop(&engine, 1).await;
    let handle = driver.shutdown_handle();
    let running = tokio::spawn(async move { driver.run().await });

    // A third poll has started only once the second activation was passed
    // to the driver.
    wait_for(|| engine.0.lock().unwrap().polls_started >= 3).await;
    assert_eq!(engine.handed_out(), vec!["wf-unread", "wf-waiting"]);
    drop(work_rx);
    tokio::task::yield_now().await;

    let asked = Instant::now();
    handle.shutdown();
    tokio::time::timeout(Duration::from_secs(10), running)
        .await
        .expect("the driver stops")
        .unwrap()
        .unwrap();
    assert!(
        asked.elapsed() < AT_ONCE,
        "stopping took {:?}",
        asked.elapsed()
    );
    assert!(
        error_names(&logs, "wf-waiting"),
        "the stranded activation was not named"
    );
    // Offered back, answered UNIMPLEMENTED, and left to its claim.
    assert_eq!(
        engine.releases(),
        vec![("wf-waiting".to_string(), b"act1.dw".to_vec())]
    );
}

/// Activations received but never handed to the SDK are handed back with the
/// token their poll carried: one waiting in the driver, and one the poller
/// that brought it could no longer pass on. Neither is left to its claim.
#[tokio::test]
async fn activations_never_handed_over_are_released_with_their_tokens() {
    let (logs, _logging) = capture_logs();
    let engine = Engine::default();
    {
        let mut script = engine.0.lock().unwrap();
        script.polls.push_back(activation("wf-unread", "act1.dQ"));
        script.polls.push_back(activation("wf-waiting", "act1.dw"));
        script.polls.push_back(activation("wf-poller", "act1.dA"));
    }
    // One place in each channel: the first activation fills the SDK's, the
    // second waits in the driver's, and the third in its poller.
    let (mut driver, work_rx, _result_tx) = slow_to_stop(&engine, 1).await;
    let handle = driver.shutdown_handle();
    let running = tokio::spawn(async move { driver.run().await });

    wait_for(|| engine.handed_out().len() >= 3).await;
    // Time for the third to reach its poller, which then waits on the
    // driver's full channel.
    tokio::time::sleep(Duration::from_millis(200)).await;
    drop(work_rx);
    tokio::task::yield_now().await;

    let asked = Instant::now();
    handle.shutdown();
    tokio::time::timeout(Duration::from_secs(10), running)
        .await
        .expect("the driver stops")
        .unwrap()
        .unwrap();
    assert!(
        asked.elapsed() < AT_ONCE,
        "stopping took {:?}",
        asked.elapsed()
    );
    wait_for(|| engine.releases().len() >= 2).await;
    let mut released = engine.releases();
    released.sort();
    assert_eq!(
        released,
        vec![
            ("wf-poller".to_string(), b"act1.dA".to_vec()),
            ("wf-waiting".to_string(), b"act1.dw".to_vec()),
        ]
    );
    assert!(!error_names(&logs, "wf-waiting"));
    assert!(!error_names(&logs, "wf-poller"));
}

/// An idle driver has room in the SDK's channel reserved for the next
/// activation. That reservation must not hide that the SDK has stopped
/// taking work: shut down afterwards, the driver stops at once.
#[tokio::test]
async fn an_idle_driver_notices_its_sdk_stopped_taking_work() {
    let engine = Engine::default();
    engine.0.lock().unwrap().poll_gate = Some(Arc::new(tokio::sync::Semaphore::new(0)));
    let (mut driver, work_rx, _result_tx) = slow_to_stop(&engine, 10).await;
    let handle = driver.shutdown_handle();
    let running = tokio::spawn(async move { driver.run().await });

    wait_for(|| engine.0.lock().unwrap().polls_started >= 1).await;
    drop(work_rx);
    tokio::task::yield_now().await;

    let asked = Instant::now();
    handle.shutdown();
    tokio::time::timeout(Duration::from_secs(10), running)
        .await
        .expect("the driver stops")
        .unwrap()
        .unwrap();
    assert!(
        asked.elapsed() < AT_ONCE,
        "stopping took {:?}",
        asked.elapsed()
    );
}

/// An activation taken by the SDK whose result never comes — dropped by a
/// deduplication, a failed conversion, a panicking handler — does not hold
/// shutdown once the SDK has stopped taking work: nothing is running that
/// could still answer it.
#[tokio::test]
async fn a_result_that_never_comes_does_not_hold_shutdown() {
    let engine = Engine::default();
    engine
        .0
        .lock()
        .unwrap()
        .polls
        .push_back(activation("wf-lost", "act1.bA"));
    let (mut driver, mut work_rx, _result_tx) = slow_to_stop(&engine, 10).await;
    let handle = driver.shutdown_handle();
    let running = tokio::spawn(async move { driver.run().await });

    let work = tokio::time::timeout(Duration::from_secs(5), work_rx.recv())
        .await
        .expect("an activation")
        .expect("the driver is running");
    drop(work);
    drop(work_rx);
    tokio::task::yield_now().await;

    let asked = Instant::now();
    handle.shutdown();
    tokio::time::timeout(Duration::from_secs(10), running)
        .await
        .expect("the driver stops")
        .unwrap()
        .unwrap();
    assert!(
        asked.elapsed() < AT_ONCE,
        "stopping took {:?}",
        asked.elapsed()
    );
}

/// Draining and sending the completions that follow share one grace: a
/// result handed back just before it ends, whose completion then hangs, does
/// not stretch the stop to twice the grace.
#[tokio::test]
async fn the_whole_shutdown_fits_in_one_grace() {
    const GRACE: Duration = Duration::from_secs(1);
    let engine = Engine::default();
    {
        let mut script = engine.0.lock().unwrap();
        // The first poll hands out the activation; the next never answers.
        script.poll_gate = Some(Arc::new(tokio::sync::Semaphore::new(1)));
        script.polls.push_back(activation("wf-late", "act1.bGF0"));
        // Completions are never answered.
        script.gate = Some(Arc::new(tokio::sync::Semaphore::new(0)));
    }
    let (driver, mut work_rx, result_tx) = driver_for(
        &engine,
        WorkflowDriverConfig {
            poll_timeout: Duration::from_secs(30),
            ..test_config()
        },
    )
    .await;
    let mut driver = driver.with_completion_retry(quick().with_shutdown_grace(GRACE));
    let handle = driver.shutdown_handle();
    let running = tokio::spawn(async move { driver.run().await });

    let work = tokio::time::timeout(Duration::from_secs(5), work_rx.recv())
        .await
        .expect("an activation")
        .expect("the driver is running");
    let asked = Instant::now();
    handle.shutdown();
    tokio::time::sleep(GRACE - Duration::from_millis(150)).await;
    let task = work.task;
    result_tx
        .send(WorkflowWorkResult {
            workflow_id: task.execution.workflow_id.clone(),
            run_id: task.execution.run_id.clone(),
            task_token: task.task_token.clone(),
            stream_entry_id: work.stream_entry_id,
            result: Ok(ExecutionResult::success(
                task.execution.run_id.clone(),
                vec![BridgeCommand::CompleteWorkflow(CompleteWorkflowCommand {
                    result: crate::Payload::json(b"{}".to_vec()),
                })],
            )),
        })
        .await
        .unwrap();

    tokio::time::timeout(Duration::from_secs(10), running)
        .await
        .expect("the driver stops")
        .unwrap()
        .unwrap();
    assert!(
        asked.elapsed() < GRACE + Duration::from_millis(400),
        "stopping took {:?} with a grace of {GRACE:?}",
        asked.elapsed()
    );
    assert_eq!(engine.calls(), 1, "the result was never sent");
}

// ── Non-determinism ─────────────────────────────────────────────────────────

/// An activation of a run whose journal records `task_0` as a `Charge` task.
fn charged_activation(workflow_id: &str, token: &str) -> PollWorkflowExecutionResponse {
    use crate::proto::orcher::v1::journal_entry::Attributes;
    let entry = |entry_id: i64, entry_type: EntryType, attributes: Attributes| JournalEntry {
        entry_id,
        timestamp: None,
        entry_type: entry_type as i32,
        version: 1,
        task_id: 0,
        attributes: Some(attributes),
    };
    PollWorkflowExecutionResponse {
        journal: vec![
            entry(
                1,
                EntryType::WorkflowExecutionStarted,
                Attributes::WorkflowExecutionStarted(Default::default()),
            ),
            entry(
                2,
                EntryType::TaskScheduled,
                Attributes::TaskScheduled(TaskScheduledEventAttributes {
                    task_id: "task_0".into(),
                    task_type: "Charge".into(),
                    ..Default::default()
                }),
            ),
        ],
        ..activation(workflow_id, token)
    }
}

fn schedule(task_id: &str, task_type: &str) -> BridgeCommand {
    BridgeCommand::ScheduleTask(crate::bridge::ScheduleTaskCommand {
        sequence: 0,
        task_id: task_id.into(),
        task_type: task_type.into(),
        task_queue: String::new(),
        input: vec![],
        timeout: Duration::from_secs(10),
        queue_timeout: None,
        heartbeat_timeout: None,
        retry_policy: None,
        headers: vec![],
    })
}

/// A worker restarted on changed code reuses the step id `task_0` for
/// another task type. The engine matches the command to the step by id
/// alone, so sent on it would be served the `Charge` task's result as the
/// `Refund` one's. The activation is failed as non-deterministic instead,
/// naming the step, and the same run's activation from unchanged code — the
/// open step issued again beside a new one — completes as before.
#[tokio::test]
async fn an_activation_whose_commands_contradict_its_journal_is_failed_as_non_deterministic() {
    let engine = Engine::default();
    {
        let mut script = engine.0.lock().unwrap();
        script
            .polls
            .push_back(charged_activation("wf-changed", "act1.Y2hhbmdlZA"));
        script
            .polls
            .push_back(charged_activation("wf-same", "act1.c2FtZQ"));
        script
            .polls
            .push_back(charged_activation("wf-sdk-found", "act1.c2Rr"));
    }
    let (driver, mut work_rx, result_tx) = driver_for(&engine, test_config()).await;
    let mut driver = driver.with_completion_retry(quick());
    let shutdown = driver.shutdown_handle();
    let running = tokio::spawn(async move { driver.run().await });

    for _ in 0..3 {
        let work = tokio::time::timeout(Duration::from_secs(5), work_rx.recv())
            .await
            .expect("an activation")
            .expect("the driver is running");
        let task = work.task;
        let result = match task.execution.workflow_id.as_str() {
            "wf-changed" => ExecutionResult::success(
                task.execution.run_id.clone(),
                vec![schedule("task_0", "Refund")],
            ),
            "wf-same" => ExecutionResult::success(
                task.execution.run_id.clone(),
                vec![schedule("task_0", "Charge"), schedule("ship_1", "Ship")],
            ),
            _ => ExecutionResult::failed(
                task.execution.run_id.clone(),
                crate::bridge::ExecutionError::non_determinism("the SDK found it".into(), None),
            ),
        };
        result_tx
            .send(WorkflowWorkResult {
                workflow_id: task.execution.workflow_id.clone(),
                run_id: task.execution.run_id.clone(),
                task_token: task.task_token.clone(),
                stream_entry_id: work.stream_entry_id,
                result: Ok(result),
            })
            .await
            .unwrap();
    }

    wait_for(|| engine.calls() >= 3).await;
    shutdown.shutdown();
    let _ = running.await;

    assert_eq!(
        engine.completes(),
        vec![Seen {
            task_token: b"act1.c2FtZQ".to_vec(),
            stream_entry_id: Some("act1.c2FtZQ".into()),
        }],
        "only the unchanged code's activation completes"
    );
    let fails = engine.fails();
    let failures = engine.0.lock().unwrap().failures.clone();
    assert_eq!(fails.len(), 2, "{failures:?}");
    for (seen, failure) in fails.iter().zip(&failures) {
        assert_eq!(failure.failure_type, "NonDeterminismError", "{failure:?}");
        assert!(failure.non_retryable, "{failure:?}");
        if seen.task_token == b"act1.Y2hhbmdlZA" {
            for part in ["task_0", "Charge", "Refund"] {
                assert!(failure.message.contains(part), "{}", failure.message);
            }
        } else {
            assert_eq!(seen.task_token, b"act1.c2Rr".to_vec());
            assert!(
                failure.message.contains("the SDK found it"),
                "{}",
                failure.message
            );
        }
    }
}

/// What a worker does with a message too large to send or receive.
///
/// A task result or workflow completion over a gRPC message limit used to be
/// sent, refused by the transport, and either dropped or sent again until the
/// budget ran out; the engine then handed the work out again at its timeout,
/// and it was run and refused again, forever. An activation over tonic's
/// 4 MiB default could not be received at all.
mod size_limits {
    use super::*;
    use crate::limits::{MAX_RECEIVE_MESSAGE_BYTES_HEADER, PAYLOAD_TOO_LARGE};
    use crate::proto::orcher::v1::command::Attributes;

    const MIB: usize = 1024 * 1024;

    #[derive(Default)]
    struct Seen {
        task_results: Vec<usize>,
        task_failures: Vec<Failure>,
        completions: Vec<CompleteWorkflowExecutionRequest>,
        /// The receive limit each completion stated.
        stated: Vec<Option<String>>,
    }

    /// An engine that records completions, and refuses the first `refuse`
    /// of them the way tonic refuses a message over its limit.
    #[derive(Clone, Default)]
    struct SizeEngine {
        seen: Arc<StdMutex<Seen>>,
        refuse: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl SizeEngine {
        fn refusing(times: usize) -> Self {
            let engine = Self::default();
            engine
                .refuse
                .store(times, std::sync::atomic::Ordering::SeqCst);
            engine
        }
        fn refused(&self) -> Result<(), Status> {
            let left = self.refuse.load(std::sync::atomic::Ordering::SeqCst);
            if left == 0 {
                return Ok(());
            }
            self.refuse
                .store(left - 1, std::sync::atomic::Ordering::SeqCst);
            Err(Status::out_of_range(
                "Error, decoded message length too large: found 6291918 bytes, \
                 the limit is: 4194304 bytes",
            ))
        }
        fn stated<T>(&self, request: &Request<T>) {
            let stated = request
                .metadata()
                .get(MAX_RECEIVE_MESSAGE_BYTES_HEADER)
                .and_then(|v| v.to_str().ok())
                .map(str::to_string);
            self.seen.lock().unwrap().stated.push(stated);
        }
    }

    #[tonic::async_trait]
    impl ExecutionService for SizeEngine {
        async fn poll_workflow_execution(
            &self,
            _: Request<PollWorkflowExecutionRequest>,
        ) -> Result<Response<PollWorkflowExecutionResponse>, Status> {
            Err(Status::unimplemented("size engine"))
        }
        async fn poll_task_execution(
            &self,
            _: Request<PollTaskExecutionRequest>,
        ) -> Result<Response<PollTaskExecutionResponse>, Status> {
            Err(Status::unimplemented("size engine"))
        }
        async fn complete_workflow_execution(
            &self,
            request: Request<CompleteWorkflowExecutionRequest>,
        ) -> Result<Response<CompleteWorkflowExecutionResponse>, Status> {
            self.stated(&request);
            self.refused()?;
            self.seen
                .lock()
                .unwrap()
                .completions
                .push(request.into_inner());
            Ok(Response::new(CompleteWorkflowExecutionResponse::default()))
        }
        async fn fail_workflow_execution(
            &self,
            _: Request<FailWorkflowExecutionRequest>,
        ) -> Result<Response<FailWorkflowExecutionResponse>, Status> {
            Err(Status::unimplemented("size engine"))
        }
        async fn complete_task_execution(
            &self,
            request: Request<CompleteTaskExecutionRequest>,
        ) -> Result<Response<CompleteTaskExecutionResponse>, Status> {
            self.stated(&request);
            self.refused()?;
            self.seen
                .lock()
                .unwrap()
                .task_results
                .push(request.into_inner().result.len());
            Ok(Response::new(CompleteTaskExecutionResponse::default()))
        }
        async fn fail_task_execution(
            &self,
            request: Request<FailTaskExecutionRequest>,
        ) -> Result<Response<FailTaskExecutionResponse>, Status> {
            self.seen
                .lock()
                .unwrap()
                .task_failures
                .extend(request.into_inner().failure);
            Ok(Response::new(FailTaskExecutionResponse {}))
        }
        async fn cancel_task_execution(
            &self,
            _: Request<CancelTaskExecutionRequest>,
        ) -> Result<Response<CancelTaskExecutionResponse>, Status> {
            Err(Status::unimplemented("size engine"))
        }
        async fn record_task_heartbeat(
            &self,
            _: Request<RecordTaskHeartbeatRequest>,
        ) -> Result<Response<RecordTaskHeartbeatResponse>, Status> {
            Err(Status::unimplemented("size engine"))
        }
        async fn respond_query_task(
            &self,
            _: Request<RespondQueryTaskRequest>,
        ) -> Result<Response<RespondQueryTaskResponse>, Status> {
            Err(Status::unimplemented("size engine"))
        }
        async fn respond_update_task(
            &self,
            _: Request<RespondUpdateTaskRequest>,
        ) -> Result<Response<RespondUpdateTaskResponse>, Status> {
            Err(Status::unimplemented("size engine"))
        }
        async fn release_workflow_execution(
            &self,
            _: Request<ReleaseWorkflowExecutionRequest>,
        ) -> Result<Response<ReleaseWorkflowExecutionResponse>, Status> {
            Err(Status::unimplemented("size engine"))
        }
        async fn shutdown_worker(
            &self,
            _: Request<ShutdownWorkerRequest>,
        ) -> Result<Response<ShutdownWorkerResponse>, Status> {
            Err(Status::unimplemented("size engine"))
        }
    }

    /// The engine on a local port, accepting messages of any size, and a
    /// completer whose own limit is `limit`.
    async fn completer_with_limit(engine: &SizeEngine, limit: usize) -> Harness {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(
            Server::builder()
                .add_service(
                    ExecutionServiceServer::new(engine.clone())
                        .max_decoding_message_size(usize::MAX),
                )
                .serve_with_incoming(TcpListenerStream::new(listener)),
        );
        let url = format!("http://{addr}");
        let mut harness = completer_for(url.clone(), quick());
        harness.completer.channel_manager =
            Arc::new(ChannelManager::new(url).with_max_message_bytes(limit));
        harness
    }

    fn task_result(bytes: usize) -> TaskReport {
        TaskReport::Complete {
            token: b"task-token".to_vec(),
            result: vec![b'x'; bytes],
            task: TaskIds {
                task_id: "summarise_0".into(),
                workflow_id: "wf-1".into(),
            },
        }
    }

    fn completing_with(result: Vec<u8>) -> Report {
        Report::Complete {
            workflow_id: "wf-1".into(),
            run_id: "exec-1".into(),
            token: b"act1.tok".to_vec(),
            stream_entry_id: "act1.tok".into(),
            commands: vec![Command {
                command_type: CommandType::CompleteWorkflow as i32,
                attributes: Some(Attributes::CompleteWorkflow(
                    CompleteWorkflowCommandAttributes { result },
                )),
            }],
            query_results: vec![],
            update_results: vec![],
        }
    }

    /// The single command a completion carries, if it fails the workflow.
    fn failed_with(completion: &CompleteWorkflowExecutionRequest) -> Option<&Failure> {
        match completion.commands.as_slice() {
            [Command {
                attributes: Some(Attributes::FailWorkflow(attrs)),
                ..
            }] => attrs.failure.as_ref(),
            _ => None,
        }
    }

    /// A task result larger than the worker sends fails the task, final, and
    /// says why; the result itself is never sent.
    #[tokio::test]
    async fn a_task_result_too_large_to_send_fails_the_task() {
        let engine = SizeEngine::default();
        let harness = completer_with_limit(&engine, 64 * 1024).await;

        let delivery = harness
            .completer
            .send_task(task_result(100 * 1024), None, "default")
            .await;
        assert!(matches!(delivery, Delivery::Delivered(())), "{delivery:?}");

        let seen = engine.seen.lock().unwrap();
        assert!(
            seen.task_results.is_empty(),
            "the oversized result was sent"
        );
        let [failure] = seen.task_failures.as_slice() else {
            panic!("the task was not failed: {:?}", seen.task_failures);
        };
        assert!(failure.non_retryable);
        assert_eq!(failure.failure_type, PAYLOAD_TOO_LARGE);
        assert_eq!(
            failure.message,
            "the result of task \"summarise_0\" is 100 KiB, more than the 64 KiB this \
             worker sends in one message (max_message_bytes). Store large data \
             elsewhere and pass a reference."
        );
    }

    /// An engine whose limit is lower than the worker's refuses the result
    /// whole. It is not sent again; the task is failed, saying why.
    #[tokio::test]
    async fn a_task_result_the_engine_refuses_as_too_large_fails_the_task() {
        let engine = SizeEngine::refusing(usize::MAX);
        let harness = completer_with_limit(&engine, 32 * MIB).await;

        let delivery = harness
            .completer
            .send_task(task_result(5 * MIB), None, "default")
            .await;
        assert!(matches!(delivery, Delivery::Delivered(())), "{delivery:?}");

        let seen = engine.seen.lock().unwrap();
        assert_eq!(seen.stated.len(), 1, "the refused result was sent again");
        let [failure] = seen.task_failures.as_slice() else {
            panic!("the task was not failed: {:?}", seen.task_failures);
        };
        assert!(failure.non_retryable);
        assert_eq!(failure.failure_type, PAYLOAD_TOO_LARGE);
        assert!(
            failure.message.starts_with(
                "the result of task \"summarise_0\" is 5 MiB, more than the engine accepts"
            ),
            "{}",
            failure.message
        );
    }

    /// A result between tonic's 4 MiB default and the worker's limit is sent
    /// as it is, and the worker tells the engine what it can receive.
    #[tokio::test]
    async fn a_task_result_over_the_grpc_default_is_sent_whole() {
        let engine = SizeEngine::default();
        let harness = completer_with_limit(&engine, crate::limits::DEFAULT_MAX_MESSAGE_BYTES).await;

        let delivery = harness
            .completer
            .send_task(task_result(5 * MIB), None, "default")
            .await;
        assert!(matches!(delivery, Delivery::Delivered(())), "{delivery:?}");

        let seen = engine.seen.lock().unwrap();
        assert_eq!(seen.task_results, vec![5 * MIB]);
        assert!(seen.task_failures.is_empty());
        assert_eq!(
            seen.stated,
            vec![Some(crate::limits::DEFAULT_MAX_MESSAGE_BYTES.to_string())]
        );
    }

    /// A workflow completion larger than the worker sends completes the
    /// activation by failing the workflow, final, saying which payload made
    /// it too large.
    #[tokio::test]
    async fn a_workflow_completion_too_large_to_send_fails_the_workflow() {
        let engine = SizeEngine::default();
        let harness = completer_with_limit(&engine, 64 * 1024).await;

        harness
            .completer
            .send(completing_with(vec![b'x'; 100 * 1024]))
            .await;

        let seen = engine.seen.lock().unwrap();
        let [completion] = seen.completions.as_slice() else {
            panic!("expected one completion, got {}", seen.completions.len());
        };
        assert_eq!(
            completion.stream_entry_id, "act1.tok",
            "the activation it answers"
        );
        let failure = failed_with(completion).expect("the workflow was not failed");
        assert!(failure.non_retryable);
        assert_eq!(failure.failure_type, PAYLOAD_TOO_LARGE);
        assert!(
            failure.message.starts_with(
                "the workflow's completion (largest: the workflow result, 100 KiB) is"
            ) && failure
                .message
                .contains("more than the 64 KiB this worker sends"),
            "{}",
            failure.message
        );
    }

    /// A workflow completion the engine refuses as too large is followed by
    /// one that fails the workflow, instead of the activation being handed
    /// out again to be refused again.
    #[tokio::test]
    async fn a_workflow_completion_the_engine_refuses_as_too_large_fails_the_workflow() {
        let engine = SizeEngine::refusing(1);
        let harness = completer_with_limit(&engine, 32 * MIB).await;

        harness
            .completer
            .send(completing_with(vec![b'x'; 5 * MIB]))
            .await;

        let seen = engine.seen.lock().unwrap();
        assert_eq!(seen.stated.len(), 2, "refused once, then the failure");
        let [completion] = seen.completions.as_slice() else {
            panic!(
                "expected one accepted completion, got {}",
                seen.completions.len()
            );
        };
        let failure = failed_with(completion).expect("the workflow was not failed");
        assert_eq!(failure.failure_type, PAYLOAD_TOO_LARGE);
        assert!(
            failure.message.contains("more than the engine accepts"),
            "{}",
            failure.message
        );
    }

    /// An activation over tonic's 4 MiB default reaches the language SDK.
    /// It used to be refused on arrival by the worker's own gRPC stack.
    #[tokio::test]
    async fn an_activation_over_the_grpc_default_is_received() {
        let engine = Engine::default();
        let input = serde_json::to_vec(&"x".repeat(5 * MIB)).unwrap();
        engine
            .0
            .lock()
            .unwrap()
            .polls
            .push_back(PollWorkflowExecutionResponse {
                input: input.clone(),
                ..activation("wf-large", "act1.large")
            });
        let (driver, mut work_rx, _result_tx) = driver_for(&engine, test_config()).await;
        let shutdown = driver.shutdown_handle();
        let mut driver = driver;
        let running = tokio::spawn(async move { driver.run().await });

        let work = tokio::time::timeout(Duration::from_secs(5), work_rx.recv())
            .await
            .expect("the 5 MiB activation never arrived")
            .expect("the driver is running");
        assert_eq!(work.task.execution.workflow_id, "wf-large");
        assert_eq!(
            work.task.input,
            serde_json::Value::String("x".repeat(5 * MIB))
        );

        shutdown.shutdown();
        let _ = tokio::time::timeout(Duration::from_secs(5), running).await;
    }
}
