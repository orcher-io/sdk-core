//! Checks that every client built over a connection sends that connection's
//! credentials.
//!
//! An actor or namespace client built from the bare channel sends no
//! credentials, so a server that requires an API key refuses its calls as
//! unauthenticated while workflow calls from the same application succeed. A
//! unit test of the interceptor cannot catch that: the interceptor can be
//! correct and simply not attached. So these tests make real calls to a real
//! server and read the headers that arrived.

use std::net::SocketAddr;
use std::sync::{Arc, Mutex};

use orcher_sdk_core::client::workflow::AuthInterceptor;
use orcher_sdk_core::client::{CoreActorClient, NamespaceClient};
use orcher_sdk_core::proto::orcher::v1::actor_service_server::{ActorService, ActorServiceServer};
use orcher_sdk_core::proto::orcher::v1::namespace_service_server::{
    NamespaceService, NamespaceServiceServer,
};
use orcher_sdk_core::proto::orcher::v1::*;
use tokio_stream::wrappers::TcpListenerStream;
use tonic::transport::{Channel, Server};
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

fn channel(addr: SocketAddr) -> Channel {
    Channel::from_shared(format!("http://{addr}"))
        .unwrap()
        .connect_lazy()
}

fn credentials() -> AuthInterceptor {
    AuthInterceptor {
        api_key: Some("orch_test_key".into()),
        organization_id: Some("org-alpha".into()),
    }
}

#[tokio::test]
async fn actor_calls_carry_the_credentials() {
    let (addr, seen) = recording_server().await;
    let client = CoreActorClient::with_auth(channel(addr), credentials());

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
    let client = NamespaceClient::with_auth(channel(addr), credentials());

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

/// The constructors that take no credentials send none, and do not fail.
#[tokio::test]
async fn the_credential_free_constructors_still_send_none() {
    let (addr, seen) = recording_server().await;

    let _ = CoreActorClient::new(channel(addr))
        .invoke_operation("Cart".into(), "k".into(), "op".into(), vec![], None, None)
        .await;
    let _ = NamespaceClient::from_channel(channel(addr))
        .list_namespaces(10, 0)
        .await;

    assert_eq!(
        seen.lock().unwrap().as_slice(),
        [(None, None), (None, None)]
    );
}
