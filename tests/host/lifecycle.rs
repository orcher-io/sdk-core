//! The host's configuration and its registration lifecycle, against a real
//! gRPC server standing in for the engine.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use orcher_sdk_core::proto::orcher::v1::worker_service_server::{
    WorkerService, WorkerServiceServer,
};
use orcher_sdk_core::proto::orcher::v1::*;
use orcher_sdk_core::worker::{SESSION_COMPLETE_TASK, SESSION_CREATE_TASK};
use orcher_sdk_core::{Host, HostConfig, Registrations};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

#[derive(Default)]
struct Recorded {
    registrations: Vec<RegisterWorkerRequest>,
    deregistrations: Vec<DeregisterWorkerRequest>,
}

#[derive(Clone)]
struct FakeEngine {
    recorded: Arc<Mutex<Recorded>>,
}

#[tonic::async_trait]
impl WorkerService for FakeEngine {
    async fn register_worker(
        &self,
        request: Request<RegisterWorkerRequest>,
    ) -> Result<Response<RegisterWorkerResponse>, Status> {
        self.recorded
            .lock()
            .unwrap()
            .registrations
            .push(request.into_inner());
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
        self.recorded
            .lock()
            .unwrap()
            .deregistrations
            .push(request.into_inner());
        Ok(Response::new(DeregisterWorkerResponse {
            success: true,
            ..Default::default()
        }))
    }
}

async fn fake_engine() -> (SocketAddr, Arc<Mutex<Recorded>>) {
    let recorded = Arc::new(Mutex::new(Recorded::default()));
    let engine = FakeEngine {
        recorded: recorded.clone(),
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        Server::builder()
            .add_service(WorkerServiceServer::new(engine))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    (addr, recorded)
}

/// Every driver is built from the one config: the same identity, the same
/// connection settings, and the defaults that used to be retyped per SDK.

#[tokio::test]
async fn a_worker_that_registered_nothing_is_refused() {
    let error = Host::start(
        HostConfig::new("http://127.0.0.1:1", "q"),
        Registrations::default(),
    )
    .await
    .unwrap_err();
    assert!(
        error.to_string().contains("registered no workflow"),
        "{error}"
    );
}

/// The worker registers what it runs, with the built-in session tasks when it
/// runs tasks, and deregisters when it stops.
#[tokio::test]
async fn a_worker_registers_what_it_runs_and_deregisters_when_it_stops() {
    let (addr, recorded) = fake_engine().await;
    let config =
        HostConfig::new(format!("http://{addr}"), "orders").with_sdk("orcher-sdk-test", "1.2.3");
    let identity = config.identity.clone();
    let host = Host::start(
        config,
        Registrations::new(["checkout"], ["charge"], [] as [&str; 0]),
    )
    .await
    .unwrap();
    assert!(host.needed_drivers().workflows && host.needed_drivers().tasks);
    assert!(!host.needed_drivers().actors);

    {
        let recorded = recorded.lock().unwrap();
        assert_eq!(recorded.registrations.len(), 1);
        let registration = &recorded.registrations[0];
        assert_eq!(registration.identity, identity);
        let capabilities = registration.capabilities.as_ref().unwrap();
        assert_eq!(capabilities.workflow_types, ["checkout"]);
        assert_eq!(
            capabilities.task_types,
            ["charge", SESSION_CREATE_TASK, SESSION_COMPLETE_TASK]
        );
        assert_eq!(
            registration.metadata.get("sdk").map(String::as_str),
            Some("orcher-sdk-test")
        );
        assert_eq!(
            registration.metadata.get("sdk_version").map(String::as_str),
            Some("1.2.3")
        );
        assert!(registration.metadata.contains_key("host_version"));
    }

    let started = Instant::now();
    host.shutdown().await;
    assert!(
        started.elapsed() < Duration::from_secs(5),
        "shutdown took {:?}",
        started.elapsed()
    );
    assert_eq!(
        recorded.lock().unwrap().deregistrations.len(),
        1,
        "the worker did not deregister when it stopped"
    );
}

/// A worker that runs no tasks does not advertise the session tasks.
#[tokio::test]
async fn a_workflow_only_worker_advertises_no_tasks() {
    let (addr, recorded) = fake_engine().await;
    let host = Host::start(
        HostConfig::new(format!("http://{addr}"), "orders"),
        Registrations::new(["checkout"], [] as [&str; 0], [] as [&str; 0]),
    )
    .await
    .unwrap();
    assert!(!host.needed_drivers().tasks);
    let task_types = recorded.lock().unwrap().registrations[0]
        .capabilities
        .as_ref()
        .unwrap()
        .task_types
        .clone();
    assert!(task_types.is_empty(), "{task_types:?}");
    host.shutdown().await;
}
