//! A gRPC server standing in for the engine: it registers workers, hands out
//! scripted tasks, and records what the worker reports.

// Protocol messages are built to keep building when fields are added.
#![allow(clippy::needless_update, dead_code)]

use std::collections::VecDeque;
use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use orcher_sdk_core::proto::orcher::v1::execution_service_server::{
    ExecutionService, ExecutionServiceServer,
};
use orcher_sdk_core::proto::orcher::v1::worker_service_server::{
    WorkerService, WorkerServiceServer,
};
use orcher_sdk_core::proto::orcher::v1::*;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

#[derive(Default)]
pub struct Script {
    /// Tasks to hand out, one per poll.
    pub tasks: VecDeque<PollTaskExecutionResponse>,
    /// Workflow activations to hand out, one per poll.
    pub activations: VecDeque<PollWorkflowExecutionResponse>,
    pub workflow_completions: Vec<CompleteWorkflowExecutionRequest>,
    pub workflow_failures: Vec<FailWorkflowExecutionRequest>,
    pub releases: Vec<ReleaseWorkflowExecutionRequest>,
    /// Whether heartbeats are answered with a cancellation request.
    pub cancel_requested: bool,
    pub completed: Vec<CompleteTaskExecutionRequest>,
    pub failed: Vec<FailTaskExecutionRequest>,
    pub heartbeats: Vec<RecordTaskHeartbeatRequest>,
    pub registrations: Vec<RegisterWorkerRequest>,
    pub deregistrations: Vec<DeregisterWorkerRequest>,
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
        _: Request<WorkerHeartbeatRequest>,
    ) -> Result<Response<WorkerHeartbeatResponse>, Status> {
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
        _: Request<PollTaskExecutionRequest>,
    ) -> Result<Response<PollTaskExecutionResponse>, Status> {
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
