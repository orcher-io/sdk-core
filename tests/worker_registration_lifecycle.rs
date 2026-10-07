//! Checks that a worker deregisters when its registration driver is stopped.
//!
//! `run` holds the driver mutably for as long as it runs, so a language SDK
//! that spawns it can only stop it from outside, through a shutdown handle.
//! Without one the SDK has to abort the task, the `DeregisterWorker` call is
//! never made, and the server keeps listing the worker until the registration
//! expires. These tests run the driver against a real gRPC server and read
//! what arrived.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use orcher_sdk_core::poller::worker_registration::DEREGISTER_TIMEOUT;
use orcher_sdk_core::proto::orcher::v1::worker_service_server::{
    WorkerService, WorkerServiceServer,
};
use orcher_sdk_core::proto::orcher::v1::*;
use orcher_sdk_core::{WorkerRegistrationConfig, WorkerRegistrationDriver};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

const REGISTRATION_ID: &str = "reg-under-test";

#[derive(Default)]
struct Recorded {
    registered: usize,
    deregistered: Vec<DeregisterWorkerRequest>,
}

/// Accepts registration and heartbeats, and records each deregistration.
/// With `hang_on_deregister`, it never answers `DeregisterWorker`.
#[derive(Clone)]
struct FakeWorkerService {
    recorded: Arc<Mutex<Recorded>>,
    hang_on_deregister: bool,
}

#[tonic::async_trait]
impl WorkerService for FakeWorkerService {
    async fn register_worker(
        &self,
        _: Request<RegisterWorkerRequest>,
    ) -> Result<Response<RegisterWorkerResponse>, Status> {
        self.recorded.lock().unwrap().registered += 1;
        Ok(Response::new(RegisterWorkerResponse {
            success: true,
            registration_id: REGISTRATION_ID.to_string(),
            error_message: String::new(),
        }))
    }

    async fn worker_heartbeat(
        &self,
        _: Request<WorkerHeartbeatRequest>,
    ) -> Result<Response<WorkerHeartbeatResponse>, Status> {
        Ok(Response::new(WorkerHeartbeatResponse {
            success: true,
            re_register: false,
            timestamp: 0,
        }))
    }

    async fn deregister_worker(
        &self,
        request: Request<DeregisterWorkerRequest>,
    ) -> Result<Response<DeregisterWorkerResponse>, Status> {
        self.recorded
            .lock()
            .unwrap()
            .deregistered
            .push(request.into_inner());
        if self.hang_on_deregister {
            std::future::pending::<()>().await;
        }
        Ok(Response::new(DeregisterWorkerResponse { success: true }))
    }
}

async fn fake_server(hang_on_deregister: bool) -> (SocketAddr, Arc<Mutex<Recorded>>) {
    let recorded = Arc::new(Mutex::new(Recorded::default()));
    let service = FakeWorkerService {
        recorded: recorded.clone(),
        hang_on_deregister,
    };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        Server::builder()
            .add_service(WorkerServiceServer::new(service))
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    (addr, recorded)
}

async fn registered_driver(addr: SocketAddr) -> WorkerRegistrationDriver {
    let mut config = WorkerRegistrationConfig::default();
    config.server_url = format!("http://{addr}");
    config.service_id = "service-under-test".to_string();
    // Long enough that no heartbeat is due while the test runs.
    config.heartbeat_interval = Duration::from_secs(3600);
    let driver = WorkerRegistrationDriver::new(config).await.unwrap();
    assert_eq!(
        driver.registration_id(),
        Some(REGISTRATION_ID),
        "the driver did not register with the fake server"
    );
    driver
}

#[tokio::test]
async fn stopping_through_the_handle_deregisters_the_worker() {
    let (addr, recorded) = fake_server(false).await;
    let mut driver = registered_driver(addr).await;
    let stop = driver.shutdown_handle();
    let running = tokio::spawn(async move { driver.run().await });

    stop.shutdown();
    tokio::time::timeout(Duration::from_secs(5), running)
        .await
        .expect("run did not return after shutdown")
        .unwrap()
        .unwrap();

    let recorded = recorded.lock().unwrap();
    assert_eq!(recorded.registered, 1);
    assert_eq!(
        recorded
            .deregistered
            .iter()
            .map(|r| (r.service_id.as_str(), r.registration_id.as_str()))
            .collect::<Vec<_>>(),
        [("service-under-test", REGISTRATION_ID)],
        "the worker was not deregistered exactly once with its registration"
    );
}

#[tokio::test]
async fn a_server_that_never_answers_deregistration_does_not_hold_up_shutdown() {
    let (addr, recorded) = fake_server(true).await;
    let mut driver = registered_driver(addr).await;
    let stop = driver.shutdown_handle();
    let running = tokio::spawn(async move { driver.run().await });

    let asked = Instant::now();
    stop.shutdown();
    tokio::time::timeout(DEREGISTER_TIMEOUT + Duration::from_secs(5), running)
        .await
        .expect("run waited on a deregistration that never answers")
        .unwrap()
        .unwrap();

    assert!(asked.elapsed() >= DEREGISTER_TIMEOUT - Duration::from_millis(100));
    assert_eq!(recorded.lock().unwrap().deregistered.len(), 1);
}
