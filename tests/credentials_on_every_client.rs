//! Checks that every client taken from a connection sends that connection's
//! credentials.
//!
//! A client that sent none would be refused as unauthenticated by a server
//! that requires an API key, while calls from the application's other clients
//! succeed. A unit test of the credentials cannot catch that: they can be
//! right and simply not attached. So these tests make real calls to a real
//! server and read the headers that arrived.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use orcher_sdk_core::proto::orcher::v1::actor_service_server::{ActorService, ActorServiceServer};
use orcher_sdk_core::proto::orcher::v1::namespace_service_server::{
    NamespaceService, NamespaceServiceServer,
};
use orcher_sdk_core::proto::orcher::v1::*;
use orcher_sdk_core::{ConnectOptions, Connection};
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::Server;
use tonic::{Request, Response, Status};

/// What each call carried: (authorization, x-organization-id).
type Seen = Arc<Mutex<Vec<(Option<String>, Option<String>)>>>;

// tonic fixes an interceptor's error type as `Status`, so its size is not ours to choose.
#[allow(clippy::result_large_err)]
fn recorder(seen: Seen) -> impl FnMut(Request<()>) -> Result<Request<()>, Status> + Clone {
    move |req: Request<()>| {
        let header = |name: &str| {
            req.metadata()
                .get(name)
                .and_then(|v| v.to_str().ok())
                .map(str::to_string)
        };
        seen.lock()
            .unwrap()
            .push((header("authorization"), header("x-organization-id")));
        Ok(req)
    }
}

/// Every method refuses. The server exists only to observe what arrives.
struct Refuse;

// The macro generates the whole attributed impl, not just the methods.
// `async_trait` rewrites the block it is attached to before any macro inside it
// expands, so methods produced by a nested macro would never be rewritten.
macro_rules! refuse {
    ($trait:ident { $($name:ident($req:ty) -> $res:ty;)* }) => {
        #[tonic::async_trait]
        impl $trait for Refuse {
            $(async fn $name(&self, _: Request<$req>) -> Result<Response<$res>, Status> {
                Err(Status::unimplemented("recording server"))
            })*
        }
    };
}

refuse! {
    ActorService {
        poll_actor_operation(PollActorOperationRequest) -> PollActorOperationResponse;
        complete_actor_operation(CompleteActorOperationRequest) -> CompleteActorOperationResponse;
        get_state(GetStateRequest) -> GetStateResponse;
        set_state(SetStateRequest) -> SetStateResponse;
        delete_state(DeleteStateRequest) -> DeleteStateResponse;
        list_state_keys(ListStateKeysRequest) -> ListStateKeysResponse;
        invoke_operation(InvokeOperationRequest) -> InvokeOperationResponse;
        register_handlers(RegisterHandlersRequest) -> RegisterHandlersResponse;
        heartbeat(HeartbeatRequest) -> HeartbeatResponse;
    }
}

refuse! {
    NamespaceService {
        create_namespace(CreateNamespaceRequest) -> CreateNamespaceResponse;
        get_namespace(GetNamespaceRequest) -> GetNamespaceResponse;
        list_namespaces(ListNamespacesRequest) -> ListNamespacesResponse;
        update_namespace(UpdateNamespaceRequest) -> UpdateNamespaceResponse;
        deprecate_namespace(DeprecateNamespaceRequest) -> DeprecateNamespaceResponse;
        delete_namespace(DeleteNamespaceRequest) -> DeleteNamespaceResponse;
    }
}

async fn recording_server() -> (SocketAddr, Seen) {
    let seen: Seen = Arc::default();
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();

    let actor = ActorServiceServer::with_interceptor(Refuse, recorder(seen.clone()));
    let namespace = NamespaceServiceServer::with_interceptor(Refuse, recorder(seen.clone()));
    tokio::spawn(async move {
        Server::builder()
            .add_service(actor)
            .add_service(namespace)
            .serve_with_incoming(TcpListenerStream::new(listener))
            .await
            .unwrap();
    });
    (addr, seen)
}

fn connection(addr: SocketAddr) -> Connection {
    Connection::lazy(
        ConnectOptions::new(format!("http://{addr}"))
            .with_api_key("orch_test_key")
            .with_organization_id("org-alpha"),
    )
    .unwrap()
}

#[tokio::test]
async fn actor_calls_carry_the_credentials() {
    let (addr, seen) = recording_server().await;
    let client = connection(addr).actor_client();

    let _ = client
        .invoke_operation("Cart".into(), "k".into(), "op".into(), vec![], None, None)
        .await;

    assert_eq!(
        seen.lock().unwrap().as_slice(),
        [(
            Some("Bearer orch_test_key".to_string()),
            Some("org-alpha".to_string())
        )],
        "the actor call did not carry the connection's credentials"
    );
}

#[tokio::test]
async fn namespace_calls_carry_the_credentials() {
    let (addr, seen) = recording_server().await;
    let client = connection(addr).namespace_client();

    let _ = client.list_namespaces(10, 0).await;

    assert_eq!(
        seen.lock().unwrap().as_slice(),
        [(
            Some("Bearer orch_test_key".to_string()),
            Some("org-alpha".to_string())
        )],
        "the namespace call did not carry the connection's credentials"
    );
}

/// A connection without credentials sends none, and its calls do not fail
/// for it.
#[tokio::test]
async fn a_connection_without_credentials_sends_none() {
    let (addr, seen) = recording_server().await;
    let connection = Connection::lazy(ConnectOptions::new(format!("http://{addr}"))).unwrap();

    let _ = connection
        .actor_client()
        .invoke_operation("Cart".into(), "k".into(), "op".into(), vec![], None, None)
        .await;
    let _ = connection.namespace_client().list_namespaces(10, 0).await;

    assert_eq!(
        seen.lock().unwrap().as_slice(),
        [(None, None), (None, None)]
    );
}
