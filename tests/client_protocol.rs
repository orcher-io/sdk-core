//! The client protocol, end to end: encoded calls through a
//! `ProtocolClient` reach a fake engine as the calls they name, and come back
//! as encoded answers, a refusal as a typed `ClientError`.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use orcher_proto::orcher::v1::workflow_service_server::{WorkflowService, WorkflowServiceServer};
use orcher_proto::orcher::v1::*;
use orcher_sdk_core::proto::client::v1 as client;
use orcher_sdk_core::proto::client::v1::client_answer::Answer;
use orcher_sdk_core::proto::client::v1::client_call::Call;
use orcher_sdk_core::proto::prost::Message;
use orcher_sdk_core::{ConnectOptions, Connection, ProtocolClient};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

/// Records the starts it is sent. Starts `workflow_id` "taken" is refused as
/// in use by run "run-0"; a query answers with its own arguments; everything
/// else is unimplemented.
#[derive(Clone, Default)]
struct Engine {
    starts: Arc<Mutex<Vec<StartWorkflowRequest>>>,
}

#[tonic::async_trait]
impl WorkflowService for Engine {
    async fn start_workflow(
        &self,
        request: Request<StartWorkflowRequest>,
    ) -> Result<Response<StartWorkflowResponse>, Status> {
        let request = request.into_inner();
        if request.workflow_id == "taken" {
            let mut metadata = tonic::metadata::MetadataMap::new();
            metadata.insert("x-orcher-execution-id", "run-0".parse().unwrap());
            return Err(Status::with_metadata(
                tonic::Code::AlreadyExists,
                "in use",
                metadata,
            ));
        }
        self.starts.lock().unwrap().push(request.clone());
        Ok(Response::new(StartWorkflowResponse {
            workflow_id: request.workflow_id,
            execution_id: "run-1".into(),
            ..Default::default()
        }))
    }

    async fn query_workflow(
        &self,
        request: Request<QueryWorkflowRequest>,
    ) -> Result<Response<QueryWorkflowResponse>, Status> {
        Ok(Response::new(QueryWorkflowResponse {
            result: request.into_inner().query_args,
            ..Default::default()
        }))
    }

    async fn send_event(
        &self,
        _: Request<SendEventRequest>,
    ) -> Result<Response<SendEventResponse>, Status> {
        Err(Status::unimplemented("fake engine"))
    }

    async fn send_event_with_start_workflow(
        &self,
        _: Request<SendEventWithStartWorkflowRequest>,
    ) -> Result<Response<SendEventWithStartWorkflowResponse>, Status> {
        Err(Status::unimplemented("fake engine"))
    }

    async fn cancel_workflow(
        &self,
        _: Request<CancelWorkflowRequest>,
    ) -> Result<Response<CancelWorkflowResponse>, Status> {
        Err(Status::unimplemented("fake engine"))
    }

    async fn terminate_workflow(
        &self,
        _: Request<TerminateWorkflowRequest>,
    ) -> Result<Response<TerminateWorkflowResponse>, Status> {
        Err(Status::unimplemented("fake engine"))
    }

    async fn reset_workflow(
        &self,
        _: Request<ResetWorkflowRequest>,
    ) -> Result<Response<ResetWorkflowResponse>, Status> {
        Err(Status::unimplemented("fake engine"))
    }

    async fn get_workflow_status(
        &self,
        _: Request<GetWorkflowStatusRequest>,
    ) -> Result<Response<GetWorkflowStatusResponse>, Status> {
        Err(Status::unimplemented("fake engine"))
    }

    async fn get_workflow_result(
        &self,
        _: Request<GetWorkflowResultRequest>,
    ) -> Result<Response<GetWorkflowResultResponse>, Status> {
        Err(Status::unimplemented("fake engine"))
    }

    async fn update_workflow(
        &self,
        _: Request<UpdateWorkflowRequest>,
    ) -> Result<Response<UpdateWorkflowResponse>, Status> {
        Err(Status::unimplemented("fake engine"))
    }

    async fn batch_cancel_workflow(
        &self,
        _: Request<BatchCancelWorkflowRequest>,
    ) -> Result<Response<BatchCancelWorkflowResponse>, Status> {
        Err(Status::unimplemented("fake engine"))
    }

    async fn batch_terminate_workflow(
        &self,
        _: Request<BatchTerminateWorkflowRequest>,
    ) -> Result<Response<BatchTerminateWorkflowResponse>, Status> {
        Err(Status::unimplemented("fake engine"))
    }

    async fn batch_send_event(
        &self,
        _: Request<BatchSendEventRequest>,
    ) -> Result<Response<BatchSendEventResponse>, Status> {
        Err(Status::unimplemented("fake engine"))
    }

    async fn batch_start_workflow(
        &self,
        _: Request<BatchStartWorkflowRequest>,
    ) -> Result<Response<BatchStartWorkflowResponse>, Status> {
        Err(Status::unimplemented("fake engine"))
    }
}

async fn serve(engine: Engine) -> SocketAddr {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        Server::builder()
            .add_service(WorkflowServiceServer::new(engine))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    addr
}

async fn protocol_client(engine: Engine) -> ProtocolClient {
    let addr = serve(engine).await;
    Connection::lazy(ConnectOptions::new(format!("http://{addr}")).with_namespace("default"))
        .unwrap()
        .protocol_client()
}

async fn call(client: &ProtocolClient, call: client::ClientCall) -> Answer {
    let bytes = client.call(&call.encode_to_vec()).await;
    client::ClientAnswer::decode(bytes.as_slice())
        .expect("a ClientAnswer")
        .answer
        .expect("an answer")
}

fn on(call: Call) -> client::ClientCall {
    client::ClientCall {
        call: Some(call),
        ..Default::default()
    }
}

fn error(answer: Answer) -> client::ClientError {
    match answer {
        Answer::Error(error) => error,
        other => panic!("expected an error, got {other:?}"),
    }
}

/// A start reaches the engine with every option it was given, in the
/// namespace the call names, and answers with the run it began.
#[tokio::test]
async fn a_start_reaches_the_engine_with_its_options() {
    let engine = Engine::default();
    let client = protocol_client(engine.clone()).await;

    let answer = call(
        &client,
        client::ClientCall {
            call: Some(Call::StartWorkflow(client::StartWorkflow {
                workflow_id: "order-7".into(),
                workflow_type: "checkout".into(),
                task_queue: "orders".into(),
                input: br#"{"n":1}"#.to_vec(),
                cron_schedule: "0 * * * * *".into(),
                retry_policy: Some(client::RetryPolicy {
                    max_attempts: 4,
                    ..Default::default()
                }),
                id_reuse_policy: client::IdReusePolicy::RejectDuplicate as i32,
                ..Default::default()
            })),
            namespace: "payments".into(),
            ..Default::default()
        },
    )
    .await;

    let Answer::Started(started) = answer else {
        panic!("{answer:?}");
    };
    let workflow = started.workflow.unwrap();
    assert_eq!(
        (workflow.workflow_id.as_str(), workflow.run_id.as_str()),
        ("order-7", "run-1")
    );
    let sent = engine.starts.lock().unwrap()[0].clone();
    assert_eq!(sent.namespace, "payments");
    assert_eq!(sent.workflow_type, "checkout");
    assert_eq!(sent.task_queue, "orders");
    assert_eq!(sent.input, br#"{"n":1}"#);
    assert_eq!(sent.cron_schedule, "0 * * * * *");
    assert_eq!(sent.retry_policy.unwrap().maximum_attempts, 4);
    assert_eq!(
        sent.workflow_id_reuse_policy,
        WorkflowIdReusePolicy::RejectDuplicate as i32
    );
}

/// An id in use is answered with its own code, naming the run holding it,
/// and as not worth retrying.
#[tokio::test]
async fn an_id_in_use_is_a_typed_error_naming_the_run() {
    let client = protocol_client(Engine::default()).await;
    let refused = error(
        call(
            &client,
            on(Call::StartWorkflow(client::StartWorkflow {
                workflow_id: "taken".into(),
                workflow_type: "checkout".into(),
                ..Default::default()
            })),
        )
        .await,
    );
    assert_eq!(
        refused.code,
        client::ErrorCode::WorkflowAlreadyExists as i32
    );
    assert_eq!(refused.run_id, "run-0");
    assert!(!refused.retryable);
}

/// A query's arguments and answer cross as the bytes they are.
#[tokio::test]
async fn a_query_crosses_as_bytes() {
    let client = protocol_client(Engine::default()).await;
    let answer = call(
        &client,
        on(Call::Query(client::Query {
            workflow: Some(client::WorkflowRef {
                workflow_id: "order-7".into(),
                ..Default::default()
            }),
            query_name: "status".into(),
            args: b"[1,2]".to_vec(),
            ..Default::default()
        })),
    )
    .await;
    assert_eq!(answer, Answer::Value(b"[1,2]".to_vec()));
}

/// A refusal keeps the engine's status code; a call that cannot be decoded,
/// names no operation or no workflow is the caller's mistake.
#[tokio::test]
async fn refusals_and_malformed_calls_are_typed_errors() {
    let client = protocol_client(Engine::default()).await;

    let refused = error(
        call(
            &client,
            on(Call::Terminate(client::Terminate {
                workflow: Some(client::WorkflowRef {
                    workflow_id: "order-7".into(),
                    ..Default::default()
                }),
                reason: "done".into(),
                ..Default::default()
            })),
        )
        .await,
    );
    assert_eq!(refused.code, client::ErrorCode::Unimplemented as i32);

    let undecodable = client.call(&[0xff, 0xff, 0xff]).await;
    let undecodable = error(
        client::ClientAnswer::decode(undecodable.as_slice())
            .unwrap()
            .answer
            .unwrap(),
    );
    assert_eq!(undecodable.code, client::ErrorCode::InvalidArgument as i32);

    let empty = error(call(&client, client::ClientCall::default()).await);
    assert_eq!(empty.code, client::ErrorCode::InvalidArgument as i32);

    let nameless = error(call(&client, on(Call::GetStatus(client::GetStatus::default()))).await);
    assert_eq!(nameless.code, client::ErrorCode::InvalidArgument as i32);
}
