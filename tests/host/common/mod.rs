//! A gRPC server standing in for the engine: it registers workers, hands out
//! scripted tasks, and records what the worker reports.

#![allow(dead_code)]

use std::collections::{HashMap, VecDeque};
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use orcher_proto::orcher::v1::actor_service_server::{ActorService, ActorServiceServer};
use orcher_proto::orcher::v1::execution_service_server::{
    ExecutionService, ExecutionServiceServer,
};
use orcher_proto::orcher::v1::worker_service_server::{WorkerService, WorkerServiceServer};
use orcher_proto::orcher::v1::*;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

#[derive(Default)]
pub struct Script {
    /// Tasks to hand out, one per poll of the worker's own queue.
    pub tasks: VecDeque<PollTaskExecutionResponse>,
    /// Tasks to hand out, one per poll of a session queue.
    pub session_tasks: VecDeque<PollTaskExecutionResponse>,
    /// The queue each task poll named.
    pub polled_queues: Vec<String>,
    /// Workflow activations to hand out, one per poll.
    pub activations: VecDeque<PollWorkflowExecutionResponse>,
    pub workflow_completions: Vec<CompleteWorkflowExecutionRequest>,
    pub workflow_failures: Vec<FailWorkflowExecutionRequest>,
    pub releases: Vec<ReleaseWorkflowExecutionRequest>,
    /// Whether heartbeats are answered with a cancellation request.
    pub cancel_requested: bool,
    pub completed: Vec<CompleteTaskExecutionRequest>,
    /// Task completions to refuse, one per completion, with these codes.
    pub refuse_completions: VecDeque<tonic::Code>,
    pub failed: Vec<FailTaskExecutionRequest>,
    pub heartbeats: Vec<RecordTaskHeartbeatRequest>,
    pub registrations: Vec<RegisterWorkerRequest>,
    /// Actor operations handed out by the next actor poll.
    pub actor_operations: Vec<ActorOperation>,
    pub actor_completions: Vec<CompleteActorOperationRequest>,
    pub handler_registrations: Vec<RegisterHandlersRequest>,
    /// Ask the next actor heartbeat for the handlers again.
    pub re_register: bool,
    /// The registration each actor heartbeat named.
    pub actor_heartbeats: Vec<String>,
    /// Actor state, by actor, key and state key: value and version.
    pub actor_state: HashMap<(String, String, String), (Vec<u8>, u64)>,
    /// The credentials each actor service call carried: the method, its
    /// authorization header and organization.
    pub actor_credentials: Vec<(&'static str, String, String)>,
    pub deregistrations: Vec<DeregisterWorkerRequest>,
    /// The worker heartbeats that arrived, with the load each reported.
    pub worker_heartbeats: Vec<WorkerHeartbeatRequest>,
}

#[derive(Clone, Default)]
pub struct Engine {
    pub script: Arc<Mutex<Script>>,
}

impl Engine {
    pub fn script(&self) -> std::sync::MutexGuard<'_, Script> {
        self.script.lock().unwrap()
    }

    /// Serves this engine on a free local port; returns its address.
    pub async fn serve(&self) -> SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let engine = self.clone();
        tokio::spawn(async move {
            Server::builder()
                .add_service(WorkerServiceServer::new(engine.clone()))
                .add_service(ActorServiceServer::new(engine.clone()))
                .add_service(ExecutionServiceServer::new(engine))
                .serve_with_incoming(TcpListenerStream::new(listener))
                .await
                .unwrap();
        });
        addr
    }
}

/// A task the engine hands out.
pub fn task(task_id: &str, token: &str) -> PollTaskExecutionResponse {
    PollTaskExecutionResponse {
        workflow_id: "order-7".into(),
        execution_id: "run-1".into(),
        task_id: task_id.into(),
        task_type: "charge".into(),
        task_queue: "orders".into(),
        task_token: token.as_bytes().to_vec(),
        input: br#"{"amount":7}"#.to_vec(),
        attempt: 2,
        start_to_close_timeout: Some(orcher_sdk_core::proto::prost_types::Duration {
            seconds: 30,
            nanos: 0,
        }),
        ..Default::default()
    }
}

#[tonic::async_trait]
impl WorkerService for Engine {
    async fn register_worker(
        &self,
        request: Request<RegisterWorkerRequest>,
    ) -> Result<Response<RegisterWorkerResponse>, Status> {
        self.script().registrations.push(request.into_inner());
        Ok(Response::new(RegisterWorkerResponse {
            success: true,
            registration_id: "registration-1".to_string(),
            ..Default::default()
        }))
    }

    async fn worker_heartbeat(
        &self,
        request: Request<WorkerHeartbeatRequest>,
    ) -> Result<Response<WorkerHeartbeatResponse>, Status> {
        self.script().worker_heartbeats.push(request.into_inner());
        Ok(Response::new(WorkerHeartbeatResponse {
            success: true,
            ..Default::default()
        }))
    }

    async fn deregister_worker(
        &self,
        request: Request<DeregisterWorkerRequest>,
    ) -> Result<Response<DeregisterWorkerResponse>, Status> {
        self.script().deregistrations.push(request.into_inner());
        Ok(Response::new(DeregisterWorkerResponse {
            success: true,
            ..Default::default()
        }))
    }
}

#[tonic::async_trait]
impl ExecutionService for Engine {
    async fn record_task_heartbeats(
        &self,
        _: Request<RecordTaskHeartbeatsRequest>,
    ) -> std::result::Result<Response<RecordTaskHeartbeatsResponse>, Status> {
        Err(Status::unimplemented(
            "an engine that takes one heartbeat per call",
        ))
    }
    async fn poll_workflow_executions(
        &self,
        _: Request<PollWorkflowExecutionsRequest>,
    ) -> std::result::Result<Response<PollWorkflowExecutionsResponse>, Status> {
        Err(Status::unimplemented(
            "an engine that hands out one item per poll",
        ))
    }
    async fn poll_task_executions(
        &self,
        _: Request<PollTaskExecutionsRequest>,
    ) -> std::result::Result<Response<PollTaskExecutionsResponse>, Status> {
        Err(Status::unimplemented(
            "an engine that hands out one item per poll",
        ))
    }
    async fn poll_workflow_execution(
        &self,
        _: Request<PollWorkflowExecutionRequest>,
    ) -> Result<Response<PollWorkflowExecutionResponse>, Status> {
        let next = self.script().activations.pop_front();
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
        request: Request<PollTaskExecutionRequest>,
    ) -> Result<Response<PollTaskExecutionResponse>, Status> {
        let queue = request.into_inner().task_queue;
        let next = {
            let mut script = self.script();
            script.polled_queues.push(queue.clone());
            if queue.contains("__session__") {
                script.session_tasks.pop_front()
            } else {
                script.tasks.pop_front()
            }
        };
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
    ) -> Result<Response<CompleteWorkflowExecutionResponse>, Status> {
        self.script()
            .workflow_completions
            .push(request.into_inner());
        Ok(Response::new(CompleteWorkflowExecutionResponse::default()))
    }

    async fn fail_workflow_execution(
        &self,
        request: Request<FailWorkflowExecutionRequest>,
    ) -> Result<Response<FailWorkflowExecutionResponse>, Status> {
        self.script().workflow_failures.push(request.into_inner());
        Ok(Response::new(FailWorkflowExecutionResponse::default()))
    }

    async fn complete_task_execution(
        &self,
        request: Request<CompleteTaskExecutionRequest>,
    ) -> Result<Response<CompleteTaskExecutionResponse>, Status> {
        if let Some(code) = self.script().refuse_completions.pop_front() {
            return Err(Status::new(code, "refused by the fake engine"));
        }
        self.script().completed.push(request.into_inner());
        Ok(Response::new(CompleteTaskExecutionResponse::default()))
    }

    async fn fail_task_execution(
        &self,
        request: Request<FailTaskExecutionRequest>,
    ) -> Result<Response<FailTaskExecutionResponse>, Status> {
        self.script().failed.push(request.into_inner());
        Ok(Response::new(FailTaskExecutionResponse::default()))
    }

    async fn cancel_task_execution(
        &self,
        _: Request<CancelTaskExecutionRequest>,
    ) -> Result<Response<CancelTaskExecutionResponse>, Status> {
        Err(Status::unimplemented("fake engine"))
    }

    async fn record_task_heartbeat(
        &self,
        request: Request<RecordTaskHeartbeatRequest>,
    ) -> Result<Response<RecordTaskHeartbeatResponse>, Status> {
        let mut script = self.script();
        script.heartbeats.push(request.into_inner());
        Ok(Response::new(RecordTaskHeartbeatResponse {
            cancel_requested: script.cancel_requested,
            ..Default::default()
        }))
    }

    async fn respond_query_task(
        &self,
        _: Request<RespondQueryTaskRequest>,
    ) -> Result<Response<RespondQueryTaskResponse>, Status> {
        Err(Status::unimplemented("fake engine"))
    }

    async fn respond_update_task(
        &self,
        _: Request<RespondUpdateTaskRequest>,
    ) -> Result<Response<RespondUpdateTaskResponse>, Status> {
        Err(Status::unimplemented("fake engine"))
    }

    async fn release_workflow_execution(
        &self,
        request: Request<ReleaseWorkflowExecutionRequest>,
    ) -> Result<Response<ReleaseWorkflowExecutionResponse>, Status> {
        self.script().releases.push(request.into_inner());
        Ok(Response::new(ReleaseWorkflowExecutionResponse::default()))
    }

    async fn shutdown_worker(
        &self,
        _: Request<ShutdownWorkerRequest>,
    ) -> Result<Response<ShutdownWorkerResponse>, Status> {
        Ok(Response::new(ShutdownWorkerResponse::default()))
    }
}

/// A journal entry recorded at `ms` milliseconds.
fn entry(
    id: i64,
    ms: i64,
    entry_type: EntryType,
    attributes: journal_entry::Attributes,
) -> JournalEntry {
    JournalEntry {
        entry_id: id,
        timestamp: Some(orcher_sdk_core::proto::prost_types::Timestamp {
            seconds: ms / 1000,
            nanos: ((ms % 1000) * 1_000_000) as i32,
        }),
        entry_type: entry_type as i32,
        version: 1,
        attributes: Some(attributes),
        ..Default::default()
    }
}

/// An activation of run `run-1` whose history records task `charge_0`
/// scheduled and completed with `"ok"`.
pub fn activation(token: &str) -> PollWorkflowExecutionResponse {
    PollWorkflowExecutionResponse {
        workflow_id: "order-7".into(),
        execution_id: "run-1".into(),
        workflow_type: "checkout".into(),
        task_queue: "orders".into(),
        input: br#"{"n":1}"#.to_vec(),
        stream_entry_id: token.into(),
        journal: vec![
            entry(
                1,
                1_000,
                EntryType::WorkflowExecutionStarted,
                journal_entry::Attributes::WorkflowExecutionStarted(Default::default()),
            ),
            entry(
                2,
                1_100,
                EntryType::TaskScheduled,
                journal_entry::Attributes::TaskScheduled(TaskScheduledEventAttributes {
                    task_id: "charge_0".into(),
                    task_type: "charge".into(),
                    ..Default::default()
                }),
            ),
            entry(
                3,
                1_200,
                EntryType::StepCompleted,
                journal_entry::Attributes::StepCompleted(StepCompletedEventAttributes {
                    step_name: "charge_0".into(),
                    result: br#""ok""#.to_vec(),
                    execution_attempt: 1,
                    ..Default::default()
                }),
            ),
        ],
        ..Default::default()
    }
}

impl Engine {
    fn credentials<T>(&self, method: &'static str, request: &Request<T>) {
        let header = |name: &str| {
            request
                .metadata()
                .get(name)
                .and_then(|value| value.to_str().ok())
                .unwrap_or_default()
                .to_string()
        };
        let carried = (method, header("authorization"), header("x-organization-id"));
        self.script().actor_credentials.push(carried);
    }
}

fn state_key(actor: &str, key: &str, state_key: &str) -> (String, String, String) {
    (actor.to_string(), key.to_string(), state_key.to_string())
}

#[tonic::async_trait]
impl ActorService for Engine {
    async fn poll_actor_operation(
        &self,
        request: Request<PollActorOperationRequest>,
    ) -> Result<Response<PollActorOperationResponse>, Status> {
        self.credentials("poll", &request);
        let operations = std::mem::take(&mut self.script().actor_operations);
        if operations.is_empty() {
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        Ok(Response::new(PollActorOperationResponse {
            operations,
            ..Default::default()
        }))
    }

    async fn complete_actor_operation(
        &self,
        request: Request<CompleteActorOperationRequest>,
    ) -> Result<Response<CompleteActorOperationResponse>, Status> {
        self.credentials("complete", &request);
        self.script().actor_completions.push(request.into_inner());
        Ok(Response::new(CompleteActorOperationResponse {
            success: true,
            ..Default::default()
        }))
    }

    async fn get_state(
        &self,
        request: Request<GetStateRequest>,
    ) -> Result<Response<GetStateResponse>, Status> {
        self.credentials("get_state", &request);
        let request = request.into_inner();
        let stored = self
            .script()
            .actor_state
            .get(&state_key(
                &request.actor_name,
                &request.key,
                &request.state_key,
            ))
            .cloned();
        Ok(Response::new(match stored {
            Some((value, version)) => GetStateResponse {
                value,
                exists: true,
                version: version.to_string(),
                ..Default::default()
            },
            None => GetStateResponse::default(),
        }))
    }

    async fn set_state(
        &self,
        request: Request<SetStateRequest>,
    ) -> Result<Response<SetStateResponse>, Status> {
        self.credentials("set_state", &request);
        let request = request.into_inner();
        let mut script = self.script();
        let slot = state_key(&request.actor_name, &request.key, &request.state_key);
        let current = script
            .actor_state
            .get(&slot)
            .map_or(0, |(_, version)| *version);
        if !request.expected_version.is_empty() && request.expected_version != current.to_string() {
            return Ok(Response::new(SetStateResponse {
                success: false,
                error_message: format!("version is {current}"),
                ..Default::default()
            }));
        }
        script
            .actor_state
            .insert(slot, (request.value, current + 1));
        Ok(Response::new(SetStateResponse {
            success: true,
            version: (current + 1).to_string(),
            ..Default::default()
        }))
    }

    async fn delete_state(
        &self,
        request: Request<DeleteStateRequest>,
    ) -> Result<Response<DeleteStateResponse>, Status> {
        self.credentials("delete_state", &request);
        let request = request.into_inner();
        let existed = self
            .script()
            .actor_state
            .remove(&state_key(
                &request.actor_name,
                &request.key,
                &request.state_key,
            ))
            .is_some();
        Ok(Response::new(DeleteStateResponse {
            success: true,
            existed,
            ..Default::default()
        }))
    }

    async fn list_state_keys(
        &self,
        request: Request<ListStateKeysRequest>,
    ) -> Result<Response<ListStateKeysResponse>, Status> {
        self.credentials("list_state_keys", &request);
        let request = request.into_inner();
        let mut keys: Vec<String> = self
            .script()
            .actor_state
            .keys()
            .filter(|(actor, key, state_key)| {
                *actor == request.actor_name
                    && *key == request.key
                    && state_key.starts_with(&request.prefix)
            })
            .map(|(_, _, state_key)| state_key.clone())
            .collect();
        keys.sort();
        Ok(Response::new(ListStateKeysResponse {
            keys,
            ..Default::default()
        }))
    }

    async fn invoke_operation(
        &self,
        _: Request<InvokeOperationRequest>,
    ) -> Result<Response<InvokeOperationResponse>, Status> {
        Err(Status::unimplemented("not a worker call"))
    }

    async fn register_handlers(
        &self,
        request: Request<RegisterHandlersRequest>,
    ) -> Result<Response<RegisterHandlersResponse>, Status> {
        self.credentials("register_handlers", &request);
        let mut script = self.script();
        script.handler_registrations.push(request.into_inner());
        let registration_id = format!("reg-{}", script.handler_registrations.len());
        Ok(Response::new(RegisterHandlersResponse {
            success: true,
            registration_id,
            ..Default::default()
        }))
    }

    async fn heartbeat(
        &self,
        request: Request<HeartbeatRequest>,
    ) -> Result<Response<HeartbeatResponse>, Status> {
        self.credentials("heartbeat", &request);
        let mut script = self.script();
        script
            .actor_heartbeats
            .push(request.into_inner().registration_id);
        let re_register = std::mem::take(&mut script.re_register);
        Ok(Response::new(HeartbeatResponse {
            success: true,
            re_register,
            ..Default::default()
        }))
    }
}
