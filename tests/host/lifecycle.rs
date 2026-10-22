//! The host's configuration and its registration lifecycle, against a real
//! gRPC server standing in for the engine.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use orcher_sdk_core::proto::orcher::v1::worker_service_server::{
    WorkerService, WorkerServiceServer,
};
use orcher_sdk_core::proto::orcher::v1::*;
use orcher_sdk_core::worker::host::config::{
    ACTOR_POLL_TIMEOUT, TASK_HEARTBEAT_INTERVAL, TASK_POLL_TIMEOUT, WORKFLOW_CACHE_CAPACITY,
    WORKFLOW_POLL_TIMEOUT,
};
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

/// The poll counts and the heartbeat interval a worker sets reach the
/// drivers that use them; zero polls counts as one.
#[test]
fn poll_counts_and_heartbeat_interval_reach_the_drivers() {
    let config = HostConfig::new("http://engine:50051", "orders")
        .with_pollers(2, 6, 0)
        .with_heartbeat_interval(std::time::Duration::from_secs(3));
    assert_eq!(config.workflow_driver().poller_count, 2);
    assert_eq!(config.task_driver().poller_count, 6);
    let actor = config.actor_driver(None);
    assert_eq!(actor.poller_count, 1);
    assert_eq!(actor.heartbeat_interval, std::time::Duration::from_secs(3));
    let registration = config.registration(&Registrations::new(
        ["checkout"],
        [] as [&str; 0],
        [] as [&str; 0],
    ));
    assert_eq!(
        registration.heartbeat_interval,
        std::time::Duration::from_secs(3)
    );
}

/// Every driver is built from the one config: the same identity, the same
/// connection settings, and the defaults that used to be retyped per SDK.

#[test]
fn every_driver_is_built_from_one_config() {
    let config = HostConfig::new("https://engine:443", "orders")
        .with_namespace("payments")
        .with_api_key("key")
        .with_organization_id("org-1")
        .with_version_id("release-7");

    let workflow = config.workflow_driver();
    let task = config.task_driver();
    let actor = config.actor_driver(Some("registration-1".into()));
    let registration = config.registration(&Registrations::new(
        ["checkout"],
        ["charge"],
        [] as [&str; 0],
    ));

    for identity in [
        &workflow.identity,
        &task.identity,
        &actor.identity,
        &registration.identity,
    ] {
        assert_eq!(identity, &config.identity);
    }
    assert_eq!(registration.service_id, config.identity);
    assert_eq!(actor.service_id, config.identity);
    for api_key in [
        &workflow.api_key,
        &task.api_key,
        &actor.api_key,
        &registration.api_key,
    ] {
        assert_eq!(api_key.as_deref(), Some("key"));
    }
    for org in [
        &workflow.organization_id,
        &task.organization_id,
        &actor.organization_id,
        &registration.organization_id,
    ] {
        assert_eq!(org.as_deref(), Some("org-1"));
    }
    assert_eq!(workflow.version_id.as_deref(), Some("release-7"));
    assert_eq!(task.version_id.as_deref(), Some("release-7"));
    assert_eq!(registration.version_id.as_deref(), Some("release-7"));

    assert_eq!(workflow.poll_timeout, WORKFLOW_POLL_TIMEOUT);
    assert_eq!(workflow.cache_capacity, WORKFLOW_CACHE_CAPACITY);
    assert!(!workflow.strict_determinism);
    assert_eq!(
        workflow.protocol_version,
        orcher_sdk_core::worker_protocol::CANCEL_REQUEST
    );
    assert_eq!(task.poll_timeout, TASK_POLL_TIMEOUT);
    assert!(task.auto_heartbeat);
    assert_eq!(task.heartbeat_interval, TASK_HEARTBEAT_INTERVAL);
    assert_eq!(actor.poll_timeout, ACTOR_POLL_TIMEOUT);
    assert_eq!(actor.registration_id.as_deref(), Some("registration-1"));
}

/// The worker protocol version is the language SDK's to raise, and every
/// report of it says the same: registration and the workflow driver alike.
#[test]
fn the_protocol_version_is_the_sdks_to_raise() {
    use orcher_sdk_core::worker_protocol::{CANCEL_REQUEST, WORKFLOW_FAILURE_RETRYABILITY};
    let registrations = Registrations::default();
    let config = HostConfig::new("http://engine", "q");
    assert_eq!(config.workflow_driver().protocol_version, CANCEL_REQUEST);
    assert_eq!(
        config.registration(&registrations).protocol_version,
        CANCEL_REQUEST
    );
    let config = config.with_protocol_version(WORKFLOW_FAILURE_RETRYABILITY);
    assert_eq!(
        config.workflow_driver().protocol_version,
        WORKFLOW_FAILURE_RETRYABILITY
    );
    assert_eq!(
        config.registration(&registrations).protocol_version,
        WORKFLOW_FAILURE_RETRYABILITY
    );
}

/// An identity is generated once; a blank one or a blank release is not a
/// value.
#[test]
fn identity_is_stable_and_blanks_are_unset() {
    let config = HostConfig::new("http://engine", "q");
    assert_eq!(
        config.workflow_driver().identity,
        config.task_driver().identity
    );
    let kept = config.identity.clone();
    let config = config.with_identity("  ").with_version_id(" ");
    assert_eq!(config.identity, kept);
    assert_eq!(config.version_id, None);
}

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
