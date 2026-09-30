//! Task and actor reports against a scripted engine.
//!
//! As in the workflow completion tests, each test stands up a real gRPC
//! server that answers the way the test says and records what arrived: what
//! the driver sends, how often, and whether one slow report holds up the
//! rest are only visible from the other end of a real call.

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex as StdMutex};
use std::time::{Duration, Instant};

use tokio::sync::{mpsc, Semaphore};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::{Code, Request, Response, Status};

use super::*;
use crate::poller::metrics::WorkerMetrics as Metrics;
use crate::proto::orcher::v1::actor_service_server::{ActorService, ActorServiceServer};
use crate::proto::orcher::v1::execution_service_server::{
    ExecutionService, ExecutionServiceServer,
};
use crate::proto::orcher::v1::*;

#[derive(Default)]
struct Script {
    /// Tasks handed out by successive task polls; an empty poll after.
    tasks: VecDeque<PollTaskExecutionResponse>,
    /// When set, each task poll waits for a permit before it is answered.
    poll_gate: Option<Arc<Semaphore>>,
    /// Task polls that have reached the engine.
    polls_started: usize,
    /// Answers to successive task completions and failure reports; OK after.
    answers: VecDeque<Code>,
    /// Answer every task completion and failure report with this.
    always: Option<Code>,
    /// Tokens of the task completions that arrived, in order.
    completes: Vec<Vec<u8>>,
    /// Tokens of the task failure reports that arrived, in order.
    fails: Vec<Vec<u8>>,
    /// The failures those reports carried, in the same order.
    failures: Vec<Failure>,
    /// When set, every task completion waits for a permit before it is
    /// answered.
    gate: Option<Arc<Semaphore>>,
    /// A completion carrying this token waits for a permit from the
    /// semaphore beside it before it is answered; the others do not.
    slow: Option<(Vec<u8>, Arc<Semaphore>)>,
    /// Actor operations handed out by the first actor poll.
    operations: Vec<ActorOperation>,
    /// Answers to successive completions of each actor operation, by id:
    /// the success flag, or an error. `Ok(true)` after.
    actor_answers: HashMap<String, VecDeque<std::result::Result<bool, Code>>>,
    /// Operation ids of the actor completions that arrived, in order.
    actor_calls: Vec<String>,
    /// ShutdownWorker calls that arrived. From the first, open polls are
    /// answered at once with nothing, as the engine does.
    shutdowns: Vec<ShutdownWorkerRequest>,
    /// The instance id each task poll carried.
    poll_instance_ids: Vec<String>,
    /// Answer ShutdownWorker UNIMPLEMENTED and keep polls open, as an engine
    /// before 0.5.0 does.
    old_engine: bool,
    /// The capabilities each task poll declared.
    poll_capabilities: Vec<Option<TaskCapabilities>>,
    /// Task heartbeats that arrived: token, details, and when.
    heartbeats: Vec<(Vec<u8>, Vec<u8>, Instant)>,
    /// Answer every heartbeat with this code instead of OK.
    heartbeat_error: Option<Code>,
    /// Answer every heartbeat asking the task to stop.
    cancel_requested: bool,
    /// When set, accept workflow completions, handing these tasks back
    /// eagerly with the first; refuse them as unimplemented otherwise.
    workflow_eager: Option<Vec<PollTaskExecutionResponse>>,
    /// The eager-task capabilities each accepted workflow completion declared.
    eager_capabilities: Vec<Option<TaskCapabilities>>,
}

#[derive(Clone, Default)]
struct Engine(Arc<StdMutex<Script>>);

impl Engine {
    fn script(&self) -> std::sync::MutexGuard<'_, Script> {
        self.0.lock().unwrap()
    }
    fn answer(&self) -> std::result::Result<(), Status> {
        let mut script = self.script();
        let code = match script.always {
            Some(code) => code,
            None => script.answers.pop_front().unwrap_or(Code::Ok),
        };
        match code {
            Code::Ok => Ok(()),
            code => Err(Status::new(code, "scripted")),
        }
    }
    fn completes(&self) -> Vec<Vec<u8>> {
        self.script().completes.clone()
    }
    fn fails(&self) -> Vec<Vec<u8>> {
        self.script().fails.clone()
    }
    fn failures(&self) -> Vec<Failure> {
        self.script().failures.clone()
    }
    /// When each heartbeat for `token` arrived, and the details it carried.
    fn heartbeats(&self, token: &str) -> Vec<(Instant, Vec<u8>)> {
        self.script()
            .heartbeats
            .iter()
            .filter(|(t, _, _)| t == token.as_bytes())
            .map(|(_, details, at)| (*at, details.clone()))
            .collect()
    }
    fn actor_calls(&self, operation_id: &str) -> usize {
        self.script()
            .actor_calls
            .iter()
            .filter(|id| *id == operation_id)
            .count()
    }
}

#[tonic::async_trait]
impl ExecutionService for Engine {
    async fn poll_workflow_execution(
        &self,
        _: Request<PollWorkflowExecutionRequest>,
    ) -> std::result::Result<Response<PollWorkflowExecutionResponse>, Status> {
        tokio::time::sleep(Duration::from_millis(50)).await;
        Ok(Response::new(PollWorkflowExecutionResponse::default()))
    }
    async fn poll_task_execution(
        &self,
        request: Request<PollTaskExecutionRequest>,
    ) -> std::result::Result<Response<PollTaskExecutionResponse>, Status> {
        let gate = {
            let mut script = self.script();
            script.polls_started += 1;
            let request = request.into_inner();
            script.poll_instance_ids.push(request.worker_instance_id);
            script.poll_capabilities.push(request.task_capabilities);
            script.poll_gate.clone()
        };
        if let Some(gate) = gate {
            // A closed gate lets every poll through; a shutdown ends the
            // wait with nothing.
            tokio::select! {
                permit = gate.acquire() => {
                    if let Ok(permit) = permit {
                        permit.forget();
                    }
                }
                _ = async {
                    while self.script().shutdowns.is_empty() || self.script().old_engine {
                        tokio::time::sleep(Duration::from_millis(5)).await;
                    }
                } => return Ok(Response::new(PollTaskExecutionResponse::default())),
            }
        }
        let next = self.script().tasks.pop_front();
        match next {
            Some(task) => Ok(Response::new(task)),
            None => {
                tokio::time::sleep(Duration::from_millis(50)).await;
                Ok(Response::new(PollTaskExecutionResponse::default()))
            }
        }
    }
    async fn complete_workflow_execution(
        &self,
        request: Request<CompleteWorkflowExecutionRequest>,
    ) -> std::result::Result<Response<CompleteWorkflowExecutionResponse>, Status> {
        let mut script = self.script();
        let Some(eager) = script.workflow_eager.as_mut() else {
            return Err(Status::unimplemented("scripted engine"));
        };
        let eager_tasks = std::mem::take(eager);
        script
            .eager_capabilities
            .push(request.into_inner().eager_task_capabilities);
        Ok(Response::new(CompleteWorkflowExecutionResponse {
            eager_tasks,
        }))
    }
    async fn fail_workflow_execution(
        &self,
        _: Request<FailWorkflowExecutionRequest>,
    ) -> std::result::Result<Response<FailWorkflowExecutionResponse>, Status> {
        Err(Status::unimplemented("scripted engine"))
    }
    async fn complete_task_execution(
        &self,
        request: Request<CompleteTaskExecutionRequest>,
    ) -> std::result::Result<Response<CompleteTaskExecutionResponse>, Status> {
        let token = request.into_inner().task_token;
        let wait = {
            let mut script = self.script();
            script.completes.push(token.clone());
            match &script.slow {
                Some((slow, gate)) if *slow == token => Some(Arc::clone(gate)),
                _ => script.gate.clone(),
            }
        };
        if let Some(gate) = wait {
            // A closed gate lets every completion through.
            if let Ok(permit) = gate.acquire().await {
                permit.forget();
            }
        }
        self.answer()?;
        Ok(Response::new(CompleteTaskExecutionResponse::default()))
    }
    async fn fail_task_execution(
        &self,
        request: Request<FailTaskExecutionRequest>,
    ) -> std::result::Result<Response<FailTaskExecutionResponse>, Status> {
        let request = request.into_inner();
        let mut script = self.script();
        script.fails.push(request.task_token);
        script.failures.extend(request.failure);
        drop(script);
        self.answer()?;
        Ok(Response::new(FailTaskExecutionResponse::default()))
    }
    async fn cancel_task_execution(
        &self,
        _: Request<CancelTaskExecutionRequest>,
    ) -> std::result::Result<Response<CancelTaskExecutionResponse>, Status> {
        Err(Status::unimplemented("scripted engine"))
    }
    async fn record_task_heartbeat(
        &self,
        request: Request<RecordTaskHeartbeatRequest>,
    ) -> std::result::Result<Response<RecordTaskHeartbeatResponse>, Status> {
        let request = request.into_inner();
        let mut script = self.script();
        script
            .heartbeats
            .push((request.task_token, request.details, Instant::now()));
        if let Some(code) = script.heartbeat_error {
            return Err(Status::new(code, "scripted"));
        }
        Ok(Response::new(RecordTaskHeartbeatResponse {
            cancel_requested: script.cancel_requested,
        }))
    }
    async fn respond_query_task(
        &self,
        _: Request<RespondQueryTaskRequest>,
    ) -> std::result::Result<Response<RespondQueryTaskResponse>, Status> {
        Err(Status::unimplemented("scripted engine"))
    }
    async fn respond_update_task(
        &self,
        _: Request<RespondUpdateTaskRequest>,
    ) -> std::result::Result<Response<RespondUpdateTaskResponse>, Status> {
        Err(Status::unimplemented("scripted engine"))
    }
    async fn release_workflow_execution(
        &self,
        _: Request<ReleaseWorkflowExecutionRequest>,
    ) -> std::result::Result<Response<ReleaseWorkflowExecutionResponse>, Status> {
        Err(Status::unimplemented("scripted engine"))
    }
    async fn shutdown_worker(
        &self,
        request: Request<ShutdownWorkerRequest>,
    ) -> std::result::Result<Response<ShutdownWorkerResponse>, Status> {
        let mut script = self.script();
        script.shutdowns.push(request.into_inner());
        if script.old_engine {
            return Err(Status::unimplemented("unknown method"));
        }
        Ok(Response::new(ShutdownWorkerResponse {}))
    }
}

#[tonic::async_trait]
impl ActorService for Engine {
    async fn poll_actor_operation(
        &self,
        _: Request<PollActorOperationRequest>,
    ) -> std::result::Result<Response<PollActorOperationResponse>, Status> {
        let operations = std::mem::take(&mut self.script().operations);
        if operations.is_empty() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Ok(Response::new(PollActorOperationResponse { operations }))
    }
    async fn complete_actor_operation(
        &self,
        request: Request<CompleteActorOperationRequest>,
    ) -> std::result::Result<Response<CompleteActorOperationResponse>, Status> {
        let operation_id = request.into_inner().operation_id;
        let answer = {
            let mut script = self.script();
            script.actor_calls.push(operation_id.clone());
            script
                .actor_answers
                .get_mut(&operation_id)
                .and_then(|answers| answers.pop_front())
                .unwrap_or(Ok(true))
        };
        match answer {
            Ok(success) => Ok(Response::new(CompleteActorOperationResponse {
                success,
                error_message: if success {
                    String::new()
                } else {
                    "unknown operation".into()
                },
            })),
            Err(code) => Err(Status::new(code, "scripted")),
        }
    }
    async fn get_state(
        &self,
        _: Request<GetStateRequest>,
    ) -> std::result::Result<Response<GetStateResponse>, Status> {
        Err(Status::unimplemented("scripted engine"))
    }
    async fn set_state(
        &self,
        _: Request<SetStateRequest>,
    ) -> std::result::Result<Response<SetStateResponse>, Status> {
        Err(Status::unimplemented("scripted engine"))
    }
    async fn delete_state(
        &self,
        _: Request<DeleteStateRequest>,
    ) -> std::result::Result<Response<DeleteStateResponse>, Status> {
        Err(Status::unimplemented("scripted engine"))
    }
    async fn list_state_keys(
        &self,
        _: Request<ListStateKeysRequest>,
    ) -> std::result::Result<Response<ListStateKeysResponse>, Status> {
        Err(Status::unimplemented("scripted engine"))
    }
    async fn invoke_operation(
        &self,
        _: Request<InvokeOperationRequest>,
    ) -> std::result::Result<Response<InvokeOperationResponse>, Status> {
        Err(Status::unimplemented("scripted engine"))
    }
    async fn register_handlers(
        &self,
        _: Request<RegisterHandlersRequest>,
    ) -> std::result::Result<Response<RegisterHandlersResponse>, Status> {
        Err(Status::unimplemented("scripted engine"))
    }
    async fn heartbeat(
        &self,
        _: Request<HeartbeatRequest>,
    ) -> std::result::Result<Response<HeartbeatResponse>, Status> {
        Ok(Response::new(HeartbeatResponse::default()))
    }
}

async fn serve(engine: Engine) -> SocketAddr {
    serve_until_stopped(engine).await.0
}

/// Serve `engine` until the returned notify is notified, then stop taking
/// connections: after that nothing answers at the address.
async fn serve_until_stopped(engine: Engine) -> (SocketAddr, Arc<tokio::sync::Notify>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let stop = Arc::new(tokio::sync::Notify::new());
    let stopped = Arc::clone(&stop);
    tokio::spawn(
        Server::builder()
            .add_service(ExecutionServiceServer::new(engine.clone()))
            .add_service(ActorServiceServer::new(engine))
            .serve_with_incoming_shutdown(TcpListenerStream::new(listener), async move {
                stopped.notified().await
            }),
    );
    (addr, stop)
}

/// The grace every test driver gets, and the most `run` may take past it.
const GRACE: Duration = Duration::from_secs(5);
const MARGIN: Duration = Duration::from_secs(2);

/// Wait for a running driver to return, failing the test if it takes longer
/// than `limit`: a shutdown that hangs is a failure, not something to wait
/// out and ignore.
async fn stops_within(running: tokio::task::JoinHandle<Result<()>>, limit: Duration) {
    match tokio::time::timeout(limit, running).await {
        Ok(joined) => joined.expect("run panicked").expect("run failed"),
        Err(_) => panic!("run did not return within {limit:?}"),
    }
}

/// Poll until `done` holds, for up to ten seconds.
async fn wait_for(done: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(10);
    while !done() && Instant::now() < deadline {
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Retries quick enough for a test to watch several of them.
fn quick() -> CompletionRetryConfig {
    CompletionRetryConfig::default()
        .with_max_attempts(5)
        .with_backoff(Duration::from_millis(10), Duration::from_millis(40))
        .with_budget(Duration::from_secs(10))
        .with_shutdown_grace(GRACE)
}

fn task(task_id: &str, token: &str) -> PollTaskExecutionResponse {
    PollTaskExecutionResponse {
        workflow_id: "wf-1".into(),
        execution_id: "exec-1".into(),
        task_id: task_id.into(),
        task_type: "t".into(),
        task_queue: "default".into(),
        task_token: token.as_bytes().to_vec(),
        ..Default::default()
    }
}

fn succeeded(work: &TaskWork) -> TaskWorkResult {
    TaskWorkResult {
        task_token: work.task.task_token.clone(),
        result: Ok(b"{}".to_vec()),
    }
}

struct TaskParts {
    driver: TaskDriver,
    work_rx: mpsc::Receiver<TaskWork>,
    result_tx: mpsc::Sender<TaskWorkResult>,
    // Held so the driver does not see the binding go away.
    _sessions: mpsc::UnboundedSender<SessionQueueChange>,
}

/// A task driver for `engine` with one poller, so the order of polls is the
/// order the script gives.
async fn task_driver(engine: &Engine, config: TaskDriverConfig) -> TaskParts {
    let addr = serve(engine.clone()).await;
    task_driver_at(addr, config).await
}

async fn task_driver_at(addr: SocketAddr, config: TaskDriverConfig) -> TaskParts {
    let (driver, work_rx, result_tx, sessions) = TaskDriver::new(TaskDriverConfig {
        server_url: format!("http://{addr}"),
        poller_count: 1,
        poll_timeout: Duration::from_secs(1),
        ..config
    })
    .await
    .unwrap();
    TaskParts {
        driver: driver.with_completion_retry(quick()),
        work_rx,
        result_tx,
        _sessions: sessions,
    }
}

async fn next_work(work_rx: &mut mpsc::Receiver<TaskWork>) -> TaskWork {
    tokio::time::timeout(Duration::from_secs(5), work_rx.recv())
        .await
        .expect("a task handed over")
        .expect("the driver is running")
}

/// A completion the engine could not take is sent again, with the token from
/// the poll every time, until it lands.
#[tokio::test]
async fn a_task_completion_is_retried_on_unavailable_with_the_same_token() {
    let engine = Engine::default();
    {
        let mut script = engine.script();
        script.tasks.push_back(task("t-1", "tok-1"));
        script.answers = VecDeque::from([Code::Unavailable, Code::Unavailable]);
    }
    let mut parts = task_driver(&engine, TaskDriverConfig::default()).await;
    let handle = parts.driver.shutdown_handle();
    let mut driver = parts.driver;
    let running = tokio::spawn(async move { driver.run().await });

    let work = next_work(&mut parts.work_rx).await;
    parts.result_tx.send(succeeded(&work)).await.unwrap();
    wait_for(|| engine.completes().len() >= 3).await;

    assert_eq!(engine.completes(), vec![b"tok-1".to_vec(); 3]);
    handle.shutdown();
    stops_within(running, GRACE + MARGIN).await;
    assert_eq!(engine.completes().len(), 3, "sent again after it landed");
}

/// A report the engine refuses for good — from an attempt it no longer runs,
/// for a token it does not know, malformed, or not this worker's to send —
/// is sent once: another attempt would get the same answer. The engine
/// answered, so the connection is not blamed. Once nothing answers at all,
/// it is: the same breaker check then comes out the other way.
#[tokio::test]
async fn a_final_refusal_of_a_task_report_is_not_retried_and_does_not_trip_the_breaker() {
    let refusals = [
        Code::FailedPrecondition,
        Code::NotFound,
        Code::InvalidArgument,
        Code::PermissionDenied,
    ];
    let engine = Engine::default();
    {
        let mut script = engine.script();
        for i in 0..=refusals.len() {
            script
                .tasks
                .push_back(task(&format!("t-{i}"), &format!("tok-{i}")));
        }
        script.answers = VecDeque::from(refusals);
    }
    let (addr, stop) = serve_until_stopped(engine.clone()).await;
    let mut parts = task_driver_at(addr, TaskDriverConfig::default()).await;
    let handle = parts.driver.shutdown_handle();
    let channel_manager = Arc::clone(&parts.driver.channel_manager);
    let mut driver = parts
        .driver
        .with_completion_retry(quick().with_shutdown_grace(Duration::from_secs(1)));
    let running = tokio::spawn(async move { driver.run().await });

    // Completions and failure reports in turn, each answered with the next
    // refusal.
    for i in 0..refusals.len() {
        let work = next_work(&mut parts.work_rx).await;
        let result = if i % 2 == 0 {
            succeeded(&work)
        } else {
            TaskWorkResult {
                task_token: work.task.task_token.clone(),
                result: Err(crate::Error::internal("the handler failed")),
            }
        };
        parts.result_tx.send(result).await.unwrap();
        wait_for(|| engine.completes().len() + engine.fails().len() == i + 1).await;
    }
    let last = next_work(&mut parts.work_rx).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert_eq!(
        engine.completes(),
        vec![b"tok-0".to_vec(), b"tok-2".to_vec()]
    );
    assert_eq!(engine.fails(), vec![b"tok-1".to_vec(), b"tok-3".to_vec()]);
    assert!(
        !channel_manager.is_cooling_off(),
        "an engine that answered tripped the breaker"
    );

    // Once nothing answers at all, the connection is to blame.
    stop.notify_one();
    tokio::time::sleep(Duration::from_millis(200)).await;
    parts.result_tx.send(succeeded(&last)).await.unwrap();
    wait_for(|| channel_manager.is_cooling_off()).await;
    assert!(
        channel_manager.is_cooling_off(),
        "a server that was gone did not trip the breaker"
    );

    handle.shutdown();
    stops_within(running, Duration::from_secs(1) + MARGIN).await;
}

/// A completion the engine is slow to answer holds up neither the next
/// result nor the hand-off of a task polled meanwhile.
#[tokio::test]
async fn a_slow_task_completion_holds_up_neither_other_results_nor_new_tasks() {
    let engine = Engine::default();
    let slow = Arc::new(Semaphore::new(0));
    let polls = Arc::new(Semaphore::new(2));
    {
        let mut script = engine.script();
        script.tasks.push_back(task("t-slow", "tok-slow"));
        script.tasks.push_back(task("t-fast", "tok-fast"));
        script.tasks.push_back(task("t-next", "tok-next"));
        script.slow = Some((b"tok-slow".to_vec(), Arc::clone(&slow)));
        script.poll_gate = Some(Arc::clone(&polls));
    }
    let mut parts = task_driver(&engine, TaskDriverConfig::default()).await;
    let handle = parts.driver.shutdown_handle();
    let mut driver = parts.driver;
    let running = tokio::spawn(async move { driver.run().await });

    let slow_work = next_work(&mut parts.work_rx).await;
    let fast_work = next_work(&mut parts.work_rx).await;
    parts.result_tx.send(succeeded(&slow_work)).await.unwrap();
    wait_for(|| engine.completes().len() == 1).await;

    parts.result_tx.send(succeeded(&fast_work)).await.unwrap();
    wait_for(|| engine.completes().len() == 2).await;
    assert_eq!(
        engine.completes(),
        vec![b"tok-slow".to_vec(), b"tok-fast".to_vec()],
        "the second result waited on the first completion"
    );

    polls.add_permits(1);
    let next = tokio::time::timeout(Duration::from_secs(3), parts.work_rx.recv())
        .await
        .expect("a task polled meanwhile waited on the slow completion")
        .unwrap();
    assert_eq!(next.task.task_id, "t-next");

    // Let everything through and answer the last task, so the shutdown has
    // nothing to wait for and does not sit out the grace.
    slow.close();
    polls.close();
    parts.result_tx.send(succeeded(&next)).await.unwrap();
    wait_for(|| engine.completes().len() == 3).await;
    handle.shutdown();
    stops_within(running, Duration::from_secs(2)).await;
}

/// With every slot taken by a report still being sent, the driver takes no
/// further result and hands over no further task, and a task counts as
/// finished only once its report has landed.
#[tokio::test]
async fn task_completions_in_flight_are_capped_and_counted_when_they_land() {
    let engine = Engine::default();
    let gate = Arc::new(Semaphore::new(0));
    {
        let mut script = engine.script();
        script.tasks.push_back(task("t-1", "tok-1"));
        script.tasks.push_back(task("t-2", "tok-2"));
        script.gate = Some(Arc::clone(&gate));
    }
    let metrics = Metrics::new();
    let mut parts = task_driver(
        &engine,
        TaskDriverConfig {
            max_concurrent_executions: 1,
            ..TaskDriverConfig::default()
        },
    )
    .await;
    let handle = parts.driver.shutdown_handle();
    let mut driver = parts.driver.with_metrics(Arc::clone(&metrics));
    let running = tokio::spawn(async move { driver.run().await });

    let first = next_work(&mut parts.work_rx).await;
    parts.result_tx.send(succeeded(&first)).await.unwrap();
    wait_for(|| engine.completes().len() == 1).await;
    // With the slot taken, a result the SDK hands back waits in the channel,
    // as the next task waits in the pollers'.
    let early = TaskWorkResult {
        task_token: b"tok-early".to_vec(),
        result: Ok(b"{}".to_vec()),
    };
    parts.result_tx.send(early).await.unwrap();
    let held = tokio::time::timeout(Duration::from_millis(300), parts.work_rx.recv()).await;

    assert!(held.is_err(), "a task was handed over with no slot free");
    assert_eq!(engine.completes().len(), 1, "a second report was taken");
    assert_eq!(
        metrics.snapshot().tasks_completed,
        0,
        "counted before its report landed"
    );

    gate.add_permits(3);
    let second = next_work(&mut parts.work_rx).await;
    parts.result_tx.send(succeeded(&second)).await.unwrap();
    wait_for(|| metrics.snapshot().tasks_completed == 3).await;
    assert_eq!(
        engine.completes(),
        vec![b"tok-1".to_vec(), b"tok-early".to_vec(), b"tok-2".to_vec()]
    );
    handle.shutdown();
    stops_within(running, GRACE + MARGIN).await;
}

/// A task a poll brings back after shutdown was asked for is handed over,
/// and its completion sent, before the driver stops — well within the
/// grace, since nothing else is left to wait for.
#[tokio::test]
async fn a_task_polled_during_shutdown_is_run_and_completed() {
    let engine = Engine::default();
    let polls = Arc::new(Semaphore::new(0));
    {
        let mut script = engine.script();
        script.tasks.push_back(task("t-late", "tok-late"));
        script.poll_gate = Some(Arc::clone(&polls));
        // An engine that would end the poll at the shutdown never lets it
        // bring anything back; one before 0.5.0 does.
        script.old_engine = true;
    }
    let mut parts = task_driver(&engine, TaskDriverConfig::default()).await;
    let handle = parts.driver.shutdown_handle();
    let mut driver = parts.driver;
    let running = tokio::spawn(async move { driver.run().await });

    wait_for(|| engine.script().polls_started >= 1).await;
    handle.shutdown();
    let asked = Instant::now();
    tokio::time::sleep(Duration::from_millis(100)).await;
    polls.add_permits(1);

    let work = next_work(&mut parts.work_rx).await;
    parts.result_tx.send(succeeded(&work)).await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), running)
        .await
        .expect("the driver stops")
        .unwrap()
        .unwrap();

    assert_eq!(engine.completes(), vec![b"tok-late".to_vec()]);
    assert!(
        asked.elapsed() < Duration::from_secs(4),
        "waited out the grace: {:?}",
        asked.elapsed()
    );
    assert_eq!(
        engine.script().polls_started,
        1,
        "polled again after shutdown"
    );
}

/// An actor operation's completion the engine could not take is sent again
/// until it lands; one the engine answers with `success: false` is not.
#[tokio::test]
async fn an_actor_completion_is_retried_on_unavailable_and_success_false_is_final() {
    let engine = Engine::default();
    {
        let mut script = engine.script();
        let operation = |id: &str| ActorOperation {
            operation_id: id.into(),
            execution_id: format!("{id}-exec"),
            actor_name: "Cart".into(),
            key: "k".into(),
            operation: "add".into(),
            ..Default::default()
        };
        script.operations = vec![operation("op-retry"), operation("op-unknown")];
        script.actor_answers.insert(
            "op-retry".into(),
            VecDeque::from([Err(Code::Unavailable), Err(Code::Unavailable), Ok(true)]),
        );
        script
            .actor_answers
            .insert("op-unknown".into(), VecDeque::from([Ok(false)]));
    }
    let addr = serve(engine.clone()).await;
    let (driver, mut work_rx, result_tx, _events) = ActorDriver::new(ActorDriverConfig {
        server_url: format!("http://{addr}"),
        poller_count: 1,
        poll_timeout: Duration::from_secs(1),
        enable_heartbeat: false,
        ..ActorDriverConfig::default()
    })
    .await
    .unwrap();
    let mut driver = driver.with_completion_retry(quick());
    let running = tokio::spawn(async move { driver.run().await });

    for _ in 0..2 {
        let work = tokio::time::timeout(Duration::from_secs(5), work_rx.recv())
            .await
            .expect("an operation handed over")
            .unwrap();
        result_tx
            .send(ActorWorkResult {
                operation_id: work.operation.operation_id.clone(),
                execution_id: work.operation.execution_id.clone(),
                result: Ok(b"{}".to_vec()),
            })
            .await
            .unwrap();
    }
    wait_for(|| engine.actor_calls("op-retry") >= 3 && engine.actor_calls("op-unknown") >= 1).await;
    tokio::time::sleep(Duration::from_millis(300)).await;

    assert_eq!(engine.actor_calls("op-retry"), 3);
    assert_eq!(
        engine.actor_calls("op-unknown"),
        1,
        "success:false was retried"
    );
    // A binding that lets go of both channels has gone; the driver stops.
    drop(result_tx);
    drop(work_rx);
    stops_within(running, MARGIN).await;
}

/// A language SDK that lets go of both its channels without a shutdown has
/// gone: the driver stops promptly rather than running on for nobody.
#[tokio::test]
async fn a_task_driver_whose_sdk_is_gone_stops_promptly() {
    let engine = Engine::default();
    let parts = task_driver(&engine, TaskDriverConfig::default()).await;
    let mut driver = parts.driver;
    let running = tokio::spawn(async move { driver.run().await });
    tokio::time::sleep(Duration::from_millis(200)).await;

    drop(parts.work_rx);
    drop(parts.result_tx);
    stops_within(running, MARGIN).await;
}

/// The run loop keeps a place in the work channel reserved for the next
/// polled task. That must not leave the workflow driver's eager tasks, which
/// are offered without waiting, with no room: with one execution allowed,
/// an idle driver still takes an eager task at once.
#[tokio::test]
async fn an_idle_driver_with_one_execution_still_takes_an_eager_task() {
    let engine = Engine::default();
    let mut parts = task_driver(
        &engine,
        TaskDriverConfig {
            max_concurrent_executions: 1,
            ..TaskDriverConfig::default()
        },
    )
    .await;
    let handle = parts.driver.shutdown_handle();
    let injector = parts.driver.clone_work_sender();
    let mut driver = parts.driver;
    let running = tokio::spawn(async move { driver.run().await });
    // Long enough for the loop to have reserved its place.
    tokio::time::sleep(Duration::from_millis(200)).await;

    let eager = proto_task_to_task_work(task("t-eager", "tok-eager"), "default", None);
    assert!(
        injector.try_send(eager).is_ok(),
        "no room for an eager task in an idle driver"
    );
    let work = next_work(&mut parts.work_rx).await;
    assert_eq!(work.task.task_id, "t-eager");

    parts.result_tx.send(succeeded(&work)).await.unwrap();
    wait_for(|| engine.completes().len() == 1).await;
    assert_eq!(engine.completes(), vec![b"tok-eager".to_vec()]);
    handle.shutdown();
    stops_within(running, GRACE + MARGIN).await;
}

/// A report the engine keeps failing to apply is retried to the end of its
/// attempts and then given up on, and its slot is freed: with one slot, the
/// next task is still taken and reported.
#[tokio::test]
async fn a_task_report_that_keeps_failing_is_given_up_on_and_frees_its_slot() {
    let engine = Engine::default();
    {
        let mut script = engine.script();
        script.tasks.push_back(task("t-stuck", "tok-stuck"));
        script.tasks.push_back(task("t-after", "tok-after"));
        script.always = Some(Code::Internal);
    }
    let metrics = Metrics::new();
    let mut parts = task_driver(
        &engine,
        TaskDriverConfig {
            max_concurrent_executions: 1,
            ..TaskDriverConfig::default()
        },
    )
    .await;
    let handle = parts.driver.shutdown_handle();
    let mut driver = parts.driver.with_metrics(Arc::clone(&metrics));
    let running = tokio::spawn(async move { driver.run().await });

    let stuck = next_work(&mut parts.work_rx).await;
    parts.result_tx.send(succeeded(&stuck)).await.unwrap();
    wait_for(|| metrics.snapshot().tasks_completed == 1).await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    assert_eq!(
        engine.completes(),
        vec![b"tok-stuck".to_vec(); 5],
        "not retried to the end of its attempts, or retried past it"
    );

    engine.script().always = None;
    let after = next_work(&mut parts.work_rx).await;
    parts.result_tx.send(succeeded(&after)).await.unwrap();
    wait_for(|| engine.completes().len() == 6).await;
    assert_eq!(engine.completes()[5], b"tok-after".to_vec());
    handle.shutdown();
    stops_within(running, GRACE + MARGIN).await;
}

/// What each log line recorded: its message and its fields, by name.
#[derive(Clone, Default)]
struct Captured(Arc<StdMutex<Vec<HashMap<String, String>>>>);

impl<S: tracing::Subscriber> tracing_subscriber::Layer<S> for Captured {
    fn on_event(&self, event: &tracing::Event<'_>, _: tracing_subscriber::layer::Context<'_, S>) {
        struct Fields(HashMap<String, String>);
        impl tracing::field::Visit for Fields {
            fn record_debug(&mut self, field: &tracing::field::Field, value: &dyn std::fmt::Debug) {
                self.0
                    .insert(field.name().to_string(), format!("{value:?}"));
            }
            fn record_str(&mut self, field: &tracing::field::Field, value: &str) {
                self.0.insert(field.name().to_string(), value.to_string());
            }
        }
        let mut fields = Fields(HashMap::new());
        event.record(&mut fields);
        self.0.lock().unwrap().push(fields.0);
    }
}

impl Captured {
    /// The fields of the first line whose message starts with `message`.
    fn line(&self, message: &str) -> HashMap<String, String> {
        self.0
            .lock()
            .unwrap()
            .iter()
            .find(|fields| {
                fields
                    .get("message")
                    .is_some_and(|m| m.starts_with(message))
            })
            .cloned()
            .unwrap_or_else(|| panic!("no line starting {message:?}"))
    }
}

/// A report's log lines name what it answers in fields of their own —
/// workflow and run for a workflow, task and workflow for a task — so that
/// a query on one of them finds every line about it. The task token is a
/// credential and appears in none of them.
#[tokio::test]
async fn report_log_lines_carry_their_ids_as_fields_and_never_the_token() {
    use tracing_subscriber::layer::SubscriberExt;
    let captured = Captured::default();
    let _logging =
        tracing::subscriber::set_default(tracing_subscriber::registry().with(captured.clone()));

    let engine = Engine::default();
    {
        let mut script = engine.script();
        script.tasks.push_back(task("t-1", "tok-secret"));
        script.answers = VecDeque::from([Code::Unavailable]);
    }
    let (addr, _stop) = serve_until_stopped(engine.clone()).await;
    let mut parts = task_driver_at(addr, TaskDriverConfig::default()).await;
    let handle = parts.driver.shutdown_handle();
    let mut driver = parts.driver;
    let running = tokio::spawn(async move { driver.run().await });
    let work = next_work(&mut parts.work_rx).await;
    parts.result_tx.send(succeeded(&work)).await.unwrap();
    wait_for(|| engine.completes().len() == 2).await;
    handle.shutdown();
    stops_within(running, GRACE + MARGIN).await;

    let task_line = captured.line("Task completion failed; sending it again");
    assert_eq!(task_line.get("task_id").map(String::as_str), Some("t-1"));
    assert_eq!(
        task_line.get("workflow_id").map(String::as_str),
        Some("wf-1")
    );

    // The scripted engine refuses workflow completions as unimplemented,
    // which is final: one attempt, one warning.
    let (shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
    let completer = Completer {
        caller: Arc::new(Caller::from(&WorkflowDriverConfig::default())),
        channel_manager: Arc::new(ChannelManager::new(format!("http://{addr}"))),
        retry: quick(),
        shutdown: shutdown_rx,
        eager_task_injector: None,
        heartbeats: None,
    };
    completer
        .complete(
            "wf-9",
            "run-9",
            CompleteWorkflowExecutionRequest {
                task_token: b"tok-secret".to_vec(),
                ..Default::default()
            },
        )
        .await;
    drop(shutdown);
    let workflow_line = captured.line("Workflow completion refused");
    assert_eq!(
        workflow_line.get("workflow_id").map(String::as_str),
        Some("wf-9")
    );
    assert_eq!(
        workflow_line.get("run_id").map(String::as_str),
        Some("run-9")
    );

    for fields in captured.0.lock().unwrap().iter() {
        for (name, value) in fields {
            assert!(
                !value.contains("tok-secret"),
                "the token was logged, as {name}: {fields:?}"
            );
        }
    }
}

/// Shut down, a task driver tells the engine once, naming its namespace,
/// identity, this process, and its own queue with every session queue it
/// polls. The engine ends its open polls, so an idle driver stops at once
/// rather than after the grace. Every poll named the process.
#[tokio::test]
async fn a_task_driver_tells_the_engine_it_is_shutting_down_and_stops_at_once() {
    let engine = Engine::default();
    engine.script().poll_gate = Some(Arc::new(Semaphore::new(0)));
    let parts = task_driver(
        &engine,
        TaskDriverConfig {
            namespace: "ns-1".into(),
            identity: "task-worker".into(),
            task_queue: "q-1".into(),
            ..TaskDriverConfig::default()
        },
    )
    .await;
    parts
        .driver
        .add_session_queue("q-1__session__w".into())
        .await
        .unwrap();
    let handle = parts.driver.shutdown_handle();
    let mut driver = parts.driver;
    let running = tokio::spawn(async move { driver.run().await });

    wait_for(|| engine.script().polls_started >= 2).await;
    let asked = Instant::now();
    handle.shutdown();
    stops_within(running, GRACE + MARGIN).await;
    assert!(
        asked.elapsed() < Duration::from_secs(1),
        "an idle driver waited out the grace: {:?}",
        asked.elapsed()
    );

    let script = engine.script();
    assert_eq!(script.shutdowns.len(), 1, "told the engine once");
    let shutdown = &script.shutdowns[0];
    assert_eq!(shutdown.namespace, "ns-1");
    assert_eq!(shutdown.identity, "task-worker");
    assert_eq!(
        shutdown.worker_instance_id,
        crate::poller::worker_instance_id()
    );
    assert_eq!(shutdown.task_queues, vec!["q-1", "q-1__session__w"]);
    assert!(script.poll_instance_ids.len() >= 2);
    assert!(
        script
            .poll_instance_ids
            .iter()
            .all(|id| id == crate::poller::worker_instance_id()),
        "a poll did not name the process: {:?}",
        script.poll_instance_ids
    );
}

/// Against an engine before 0.5.0, which answers the shutdown UNIMPLEMENTED
/// and keeps the poll open, the driver waits out the grace and then stops
/// without an error.
#[tokio::test]
async fn a_task_driver_against_an_old_engine_stops_as_before() {
    let engine = Engine::default();
    {
        let mut script = engine.script();
        script.poll_gate = Some(Arc::new(Semaphore::new(0)));
        script.old_engine = true;
    }
    let mut parts = task_driver(&engine, TaskDriverConfig::default()).await;
    parts.driver = parts
        .driver
        .with_completion_retry(quick().with_shutdown_grace(Duration::from_millis(500)));
    let handle = parts.driver.shutdown_handle();
    let mut driver = parts.driver;
    let running = tokio::spawn(async move { driver.run().await });

    wait_for(|| engine.script().polls_started >= 1).await;
    let asked = Instant::now();
    handle.shutdown();
    stops_within(running, GRACE).await;
    assert!(asked.elapsed() >= Duration::from_millis(500));
    assert_eq!(engine.script().shutdowns.len(), 1);
}

/// A task failure is reported with the type its error was raised as and its
/// non-retryable mark, which the engine's retry decision reads. An error that
/// is only a message is still reported as a retryable `TaskExecutionError`.
#[tokio::test]
async fn a_task_failure_reports_its_type_and_non_retryable_mark() {
    let engine = Engine::default();
    {
        let mut script = engine.script();
        script.tasks.push_back(task("t-typed", "tok-typed"));
        script.tasks.push_back(task("t-final", "tok-final"));
        script.tasks.push_back(task("t-plain", "tok-plain"));
    }
    let mut parts = task_driver(&engine, TaskDriverConfig::default()).await;
    let handle = parts.driver.shutdown_handle();
    let mut driver = parts.driver;
    let running = tokio::spawn(async move { driver.run().await });

    let errors = [
        crate::Error::from(crate::TaskFailure::new("card declined").with_type("CardDeclined")),
        // Context added on the way does not hide what the task raised.
        crate::Error::WithContext {
            context: "charging".into(),
            source: Box::new(
                crate::TaskFailure::new("account closed")
                    .with_type("AccountClosed")
                    .with_non_retryable(true)
                    .into(),
            ),
        },
        crate::Error::internal("the handler failed"),
    ];
    for (i, error) in errors.into_iter().enumerate() {
        let work = next_work(&mut parts.work_rx).await;
        parts
            .result_tx
            .send(TaskWorkResult {
                task_token: work.task.task_token.clone(),
                result: Err(error),
            })
            .await
            .unwrap();
        wait_for(|| engine.fails().len() == i + 1).await;
    }

    let reported: Vec<_> = engine
        .failures()
        .into_iter()
        .map(|f| (f.message, f.failure_type, f.non_retryable))
        .collect();
    assert_eq!(
        reported,
        vec![
            ("card declined".into(), "CardDeclined".into(), false),
            (
                "charging: account closed".into(),
                "AccountClosed".into(),
                true
            ),
            (
                "Internal SDK error: the handler failed".into(),
                "TaskExecutionError".into(),
                false
            ),
        ]
    );

    handle.shutdown();
    stops_within(running, Duration::from_secs(1) + MARGIN).await;
}

/// A task with a heartbeat timeout of `timeout_ms`.
fn heartbeating_task(task_id: &str, token: &str, timeout_ms: i32) -> PollTaskExecutionResponse {
    PollTaskExecutionResponse {
        heartbeat_timeout: Some(prost_types::Duration {
            seconds: 0,
            nanos: timeout_ms * 1_000_000,
        }),
        ..task(task_id, token)
    }
}

/// A task handed to the language SDK is heartbeated on its own while it runs,
/// at a third of its heartbeat timeout or less, though its code never asks —
/// and not once its result is sent. Its polls tell the engine so.
#[tokio::test]
async fn a_running_task_is_heartbeated_until_its_result_is_sent() {
    let engine = Engine::default();
    engine
        .script()
        .tasks
        .push_back(heartbeating_task("t-1", "tok-1", 900));
    let mut parts = task_driver(&engine, TaskDriverConfig::default()).await;
    let handle = parts.driver.shutdown_handle();
    let mut driver = parts.driver;
    let running = tokio::spawn(async move { driver.run().await });

    let work = next_work(&mut parts.work_rx).await;
    let handed_at = Instant::now();
    // The task runs for three of its heartbeat timeouts.
    tokio::time::sleep(Duration::from_millis(2700)).await;
    let beats = engine.heartbeats("tok-1");
    assert!(
        beats.len() >= 7,
        "a running task was heartbeated {} times in three timeouts",
        beats.len()
    );
    let mut previous = handed_at;
    for (at, _) in &beats {
        assert!(
            at.duration_since(previous) < Duration::from_millis(600),
            "two heartbeats {:?} apart, with a 900ms heartbeat timeout",
            at.duration_since(previous)
        );
        previous = *at;
    }

    parts.result_tx.send(succeeded(&work)).await.unwrap();
    wait_for(|| engine.completes().len() == 1).await;
    // Heartbeats go on until the completion is answered, which is just
    // after it is recorded here.
    tokio::time::sleep(Duration::from_millis(100)).await;
    let after_result = engine.heartbeats("tok-1").len();
    tokio::time::sleep(Duration::from_millis(900)).await;
    assert_eq!(
        engine.heartbeats("tok-1").len(),
        after_result,
        "heartbeats went on after the result was sent"
    );
    assert!(
        engine
            .script()
            .poll_capabilities
            .iter()
            .all(|c| c.as_ref().is_some_and(|c| c.auto_heartbeat)),
        "a poll did not say its tasks are heartbeated"
    );

    handle.shutdown();
    stops_within(running, GRACE + MARGIN).await;
}

/// With auto-heartbeat off, the polls say nothing and a task is heartbeated
/// only when its code asks.
#[tokio::test]
async fn with_auto_heartbeat_off_only_the_tasks_own_heartbeats_are_sent() {
    let engine = Engine::default();
    engine
        .script()
        .tasks
        .push_back(heartbeating_task("t-1", "tok-1", 600));
    let mut parts = task_driver(
        &engine,
        TaskDriverConfig {
            auto_heartbeat: false,
            ..TaskDriverConfig::default()
        },
    )
    .await;
    let handle = parts.driver.shutdown_handle();
    let mut driver = parts.driver;
    let running = tokio::spawn(async move { driver.run().await });

    let work = next_work(&mut parts.work_rx).await;
    tokio::time::sleep(Duration::from_millis(900)).await;
    assert!(
        engine.heartbeats("tok-1").is_empty(),
        "heartbeated on a timer"
    );
    work.heartbeat.record(Some(b"half".to_vec()));
    wait_for(|| engine.heartbeats("tok-1").len() == 1).await;
    assert_eq!(engine.heartbeats("tok-1")[0].1, b"half".to_vec());
    assert!(engine
        .script()
        .poll_capabilities
        .iter()
        .all(Option::is_none));

    parts.result_tx.send(succeeded(&work)).await.unwrap();
    handle.shutdown();
    stops_within(running, GRACE + MARGIN).await;
}

/// Heartbeats the task's code records in a tight loop are folded into the
/// timed ones: one goes out at once, the rest wait for the next one due, which
/// carries the latest details; and a timed heartbeat after it keeps them.
#[tokio::test]
async fn a_tasks_own_heartbeats_are_coalesced_and_keep_their_details() {
    let engine = Engine::default();
    engine
        .script()
        .tasks
        .push_back(heartbeating_task("t-1", "tok-1", 1500));
    let mut parts = task_driver(&engine, TaskDriverConfig::default()).await;
    let handle = parts.driver.shutdown_handle();
    let mut driver = parts.driver;
    let running = tokio::spawn(async move { driver.run().await });

    let work = next_work(&mut parts.work_rx).await;
    for i in 0..1000 {
        work.heartbeat.record(Some(format!("{i}").into_bytes()));
        if i % 100 == 0 {
            tokio::task::yield_now().await;
        }
    }
    // A period is 500ms: the first recorded goes at once, the rest with the
    // next due, and one more on the timer after that.
    tokio::time::sleep(Duration::from_millis(1300)).await;
    let beats = engine.heartbeats("tok-1");
    assert!(
        (2..=4).contains(&beats.len()),
        "a thousand recorded heartbeats sent {} in two periods",
        beats.len()
    );
    assert_eq!(
        beats.last().map(|(_, d)| d.clone()),
        Some(b"999".to_vec()),
        "the latest details were not sent, or a timed heartbeat dropped them"
    );

    parts.result_tx.send(succeeded(&work)).await.unwrap();
    handle.shutdown();
    stops_within(running, GRACE + MARGIN).await;
}

/// The engine's answer asking a task to stop reaches the task's cancellation
/// token, and heartbeats go on while the task winds down.
#[tokio::test]
async fn a_cancel_request_in_a_heartbeat_answer_cancels_the_task() {
    let engine = Engine::default();
    {
        let mut script = engine.script();
        script
            .tasks
            .push_back(heartbeating_task("t-1", "tok-1", 600));
        script.cancel_requested = true;
    }
    let mut parts = task_driver(&engine, TaskDriverConfig::default()).await;
    let handle = parts.driver.shutdown_handle();
    let mut driver = parts.driver;
    let running = tokio::spawn(async move { driver.run().await });

    let work = next_work(&mut parts.work_rx).await;
    let token = work.heartbeat.cancellation_token();
    tokio::time::timeout(Duration::from_secs(2), token.cancelled())
        .await
        .expect("the task was not told to stop");
    let seen = engine.heartbeats("tok-1").len();
    wait_for(|| engine.heartbeats("tok-1").len() > seen).await;
    assert!(
        engine.heartbeats("tok-1").len() > seen,
        "heartbeats stopped while the cancelled task was still running"
    );

    parts.result_tx.send(succeeded(&work)).await.unwrap();
    handle.shutdown();
    stops_within(running, GRACE + MARGIN).await;
}

/// An engine that no longer runs the attempt — it timed out, or finished
/// elsewhere — cancels the task, and its heartbeats stop.
#[tokio::test]
async fn an_attempt_the_engine_no_longer_runs_is_cancelled_and_not_heartbeated() {
    for code in [Code::FailedPrecondition, Code::NotFound] {
        let engine = Engine::default();
        {
            let mut script = engine.script();
            script
                .tasks
                .push_back(heartbeating_task("t-1", "tok-1", 600));
            script.heartbeat_error = Some(code);
        }
        let mut parts = task_driver(&engine, TaskDriverConfig::default()).await;
        let handle = parts.driver.shutdown_handle();
        let mut driver = parts.driver;
        let running = tokio::spawn(async move { driver.run().await });

        let work = next_work(&mut parts.work_rx).await;
        tokio::time::timeout(
            Duration::from_secs(2),
            work.heartbeat.cancellation_token().cancelled(),
        )
        .await
        .expect("the task was not told its attempt is gone");
        tokio::time::sleep(Duration::from_millis(700)).await;
        assert_eq!(
            engine.heartbeats("tok-1").len(),
            1,
            "heartbeated an attempt the engine answered {code:?} for"
        );

        parts.result_tx.send(succeeded(&work)).await.unwrap();
        handle.shutdown();
        stops_within(running, GRACE + MARGIN).await;
    }
}

/// A heartbeat that fails for a reason that may pass is not the end: the next
/// is sent on time.
#[tokio::test]
async fn a_failed_heartbeat_is_followed_by_the_next() {
    let engine = Engine::default();
    {
        let mut script = engine.script();
        script
            .tasks
            .push_back(heartbeating_task("t-1", "tok-1", 600));
        script.heartbeat_error = Some(Code::Unavailable);
    }
    let mut parts = task_driver(&engine, TaskDriverConfig::default()).await;
    let handle = parts.driver.shutdown_handle();
    let mut driver = parts.driver;
    let running = tokio::spawn(async move { driver.run().await });

    let work = next_work(&mut parts.work_rx).await;
    tokio::time::sleep(Duration::from_millis(1000)).await;
    assert!(engine.heartbeats("tok-1").len() >= 3);
    assert!(!work.heartbeat.is_cancelled());

    parts.result_tx.send(succeeded(&work)).await.unwrap();
    handle.shutdown();
    stops_within(running, GRACE + MARGIN).await;
}

/// A task with no heartbeat timeout — one from an engine that gives none —
/// is not heartbeated on a timer: nothing would enforce it.
#[tokio::test]
async fn a_task_with_no_heartbeat_timeout_is_not_heartbeated_on_a_timer() {
    let engine = Engine::default();
    engine.script().tasks.push_back(task("t-1", "tok-1"));
    let mut parts = task_driver(&engine, TaskDriverConfig::default()).await;
    let handle = parts.driver.shutdown_handle();
    let mut driver = parts.driver;
    let running = tokio::spawn(async move { driver.run().await });

    let work = next_work(&mut parts.work_rx).await;
    assert_eq!(work.heartbeat.heartbeat_timeout(), None);
    tokio::time::sleep(Duration::from_millis(500)).await;
    assert!(engine.heartbeats("tok-1").is_empty());
    work.heartbeat.record(None);
    wait_for(|| engine.heartbeats("tok-1").len() == 1).await;
    assert_eq!(engine.heartbeats("tok-1").len(), 1);

    parts.result_tx.send(succeeded(&work)).await.unwrap();
    handle.shutdown();
    stops_within(running, GRACE + MARGIN).await;
}

/// Heartbeats stop when the driver stops, for a task whose result never came
/// back, and when the language SDK lets go of a task's work unanswered.
#[tokio::test]
async fn heartbeats_stop_with_the_driver_and_with_the_work() {
    let engine = Engine::default();
    {
        let mut script = engine.script();
        script
            .tasks
            .push_back(heartbeating_task("t-1", "tok-1", 600));
        script
            .tasks
            .push_back(heartbeating_task("t-2", "tok-2", 600));
    }
    let mut parts = task_driver(&engine, TaskDriverConfig::default()).await;
    let handle = parts.driver.shutdown_handle();
    let mut driver = parts
        .driver
        .with_completion_retry(quick().with_shutdown_grace(Duration::from_millis(300)));
    let running = tokio::spawn(async move { driver.run().await });

    let kept = next_work(&mut parts.work_rx).await;
    let dropped = next_work(&mut parts.work_rx).await;
    wait_for(|| !engine.heartbeats("tok-2").is_empty()).await;
    drop(dropped);
    // Let go of with no result, a task is heartbeated a short grace longer,
    // for a result that may still be on its way, and then not at all.
    tokio::time::sleep(Duration::from_millis(2300)).await;
    let after_drop = engine.heartbeats("tok-2").len();
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(
        engine.heartbeats("tok-2").len(),
        after_drop,
        "a task the language SDK let go of is still heartbeated"
    );
    assert!(engine.heartbeats("tok-1").len() >= 2);

    handle.shutdown();
    stops_within(running, Duration::from_millis(300) + MARGIN).await;
    assert!(kept.heartbeat.is_finished());
    let after_stop = engine.heartbeats("tok-1").len();
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(
        engine.heartbeats("tok-1").len(),
        after_stop,
        "heartbeats went on after the driver stopped"
    );
}

/// Tasks the engine hands back with a workflow completion join the task
/// driver's heartbeats when the workflow driver was given the task driver's
/// injector, and the completion tells the engine they will; given a bare work
/// channel, nothing heartbeats them and the completion says nothing.
#[tokio::test]
async fn eager_tasks_join_the_task_drivers_heartbeats_and_say_so() {
    let engine = Engine::default();
    engine.script().workflow_eager = Some(vec![heartbeating_task("t-e", "tok-e", 600)]);
    let (addr, _stop) = serve_until_stopped(engine.clone()).await;
    let mut parts = task_driver_at(addr, TaskDriverConfig::default()).await;
    let handle = parts.driver.shutdown_handle();
    let injectors = [
        parts.driver.eager_task_injector(),
        EagerTaskInjector::from(parts.driver.clone_work_sender()),
    ];
    let mut driver = parts.driver;
    let running = tokio::spawn(async move { driver.run().await });
    // Long enough for the loop to have reserved its place.
    tokio::time::sleep(Duration::from_millis(200)).await;

    for (i, injector) in injectors.into_iter().enumerate() {
        let (_shutdown, shutdown_rx) = tokio::sync::watch::channel(false);
        let completer = Completer {
            caller: Arc::new(Caller::from(&WorkflowDriverConfig::default())),
            channel_manager: Arc::new(ChannelManager::new(format!("http://{addr}"))),
            retry: quick(),
            shutdown: shutdown_rx,
            eager_task_injector: Some(EagerTasks {
                sender: injector.sender,
                heartbeats: injector.heartbeats,
                default_queue: "default".into(),
            }),
            heartbeats: None,
        };
        completer
            .send(Report::Complete {
                workflow_id: "wf-1".into(),
                run_id: "run-1".into(),
                token: format!("activation-{i}").into_bytes(),
                stream_entry_id: String::new(),
                commands: vec![],
                query_results: vec![],
                update_results: vec![],
            })
            .await;
        if i == 0 {
            let work = next_work(&mut parts.work_rx).await;
            assert_eq!(work.task.task_id, "t-e");
            wait_for(|| engine.heartbeats("tok-e").len() >= 2).await;
            assert!(
                engine.heartbeats("tok-e").len() >= 2,
                "an eager task was not heartbeated"
            );
            parts.result_tx.send(succeeded(&work)).await.unwrap();
            engine.script().workflow_eager = Some(vec![]);
        }
    }
    assert_eq!(
        engine.script().eager_capabilities,
        vec![
            Some(TaskCapabilities {
                auto_heartbeat: true
            }),
            None
        ]
    );

    handle.shutdown();
    stops_within(running, GRACE + MARGIN).await;
}

/// A task is heartbeated until its result has reached the engine, though the
/// language SDK let go of it as soon as it handed the result back: a
/// completion held up for longer than the task's heartbeat timeout does not
/// find its attempt timed out.
#[tokio::test]
async fn a_task_is_heartbeated_until_its_result_reaches_the_engine() {
    let engine = Engine::default();
    let gate = Arc::new(Semaphore::new(0));
    {
        let mut script = engine.script();
        script
            .tasks
            .push_back(heartbeating_task("t-1", "tok-1", 600));
        script.gate = Some(Arc::clone(&gate));
    }
    let mut parts = task_driver(&engine, TaskDriverConfig::default()).await;
    let handle = parts.driver.shutdown_handle();
    let mut driver = parts.driver;
    let running = tokio::spawn(async move { driver.run().await });

    let work = next_work(&mut parts.work_rx).await;
    parts.result_tx.send(succeeded(&work)).await.unwrap();
    drop(work);
    wait_for(|| engine.completes().len() == 1).await;
    let at_result = engine.heartbeats("tok-1").len();
    // Held up for four heartbeat timeouts, past the grace for a let-go task.
    tokio::time::sleep(Duration::from_millis(2400)).await;
    assert!(
        engine.heartbeats("tok-1").len() >= at_result + 8,
        "the task was not heartbeated while its completion was held up"
    );

    gate.close();
    tokio::time::sleep(Duration::from_millis(100)).await;
    let delivered = engine.heartbeats("tok-1").len();
    tokio::time::sleep(Duration::from_millis(600)).await;
    assert_eq!(
        engine.heartbeats("tok-1").len(),
        delivered,
        "heartbeated after its completion was answered"
    );

    handle.shutdown();
    stops_within(running, GRACE + MARGIN).await;
}
