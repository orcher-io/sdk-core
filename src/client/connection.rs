//! The connection to the engine every client shares: where it is, how to
//! reach it, and the credentials every call carries.
//!
//! A language SDK describes the connection once, in [`ConnectOptions`], and
//! gets its workflow, namespace and actor clients from one [`Connection`].
//! They share its channel, its credentials and its call measurements, so no
//! client can be built that forgets the TLS settings or the API key the
//! others use. A worker is described with the same options
//! ([`crate::HostConfig::connection`]).

use std::sync::Arc;
use std::time::Duration;

use crate::client::{ActorClient, NamespaceClient, WorkflowClient};
use crate::error::Result;
use crate::observability::{WireMetrics, WireMetricsSnapshot};
use crate::transport::channel::build_endpoint;
use crate::transport::instrument::{Instrumented, WireChannel};
use crate::transport::TlsConfig;
use crate::DEFAULT_NAMESPACE;

/// How long a connect may take, unless configured.
pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);

/// The longest any one client call may take on the wire. A client waiting
/// for longer (a workflow's result) does so in shorter calls.
const CLIENT_CALL_TIMEOUT: Duration = Duration::from_secs(180);

/// How to reach the engine, and what every call to it carries.
#[derive(Clone)]
#[non_exhaustive]
pub struct ConnectOptions {
    /// The engine's address, for example `http://localhost:50051`. An
    /// `https` address is reached over TLS, verified against the system
    /// trust store unless [`ConnectOptions::tls`] says otherwise.
    pub url: String,
    /// The namespace calls target unless a client is told another.
    pub namespace: String,
    /// Sent as `authorization: Bearer <key>` on every call.
    pub api_key: Option<String>,
    /// Sent as `x-organization-id` on every call.
    pub organization_id: Option<String>,
    /// TLS settings: a private CA, a client identity, a server name.
    pub tls: Option<TlsConfig>,
    /// How long a connect may take.
    pub connect_timeout: Duration,
    /// The largest message sent or received: a workflow's input, its
    /// result, an event's payload. A message over it on either side fails
    /// with an OUT_OF_RANGE status that says which limit to raise.
    /// `ORCHER_MAX_MESSAGE_BYTES`, or 32 MiB, the engine's default, unless
    /// set.
    pub max_message_bytes: usize,
}

impl ConnectOptions {
    /// The engine at `url`, in the `default` namespace, with no credentials
    /// and every other setting at its default.
    pub fn new(url: impl Into<String>) -> Self {
        Self {
            url: url.into(),
            namespace: DEFAULT_NAMESPACE.to_string(),
            api_key: None,
            organization_id: None,
            tls: None,
            connect_timeout: DEFAULT_CONNECT_TIMEOUT,
            max_message_bytes: crate::transport::limits::default_max_message_bytes(),
        }
    }

    /// Targets `namespace`.
    pub fn with_namespace(mut self, namespace: impl Into<String>) -> Self {
        self.namespace = namespace.into();
        self
    }

    /// Sends `api_key` on every call.
    pub fn with_api_key(mut self, api_key: impl Into<String>) -> Self {
        self.api_key = Some(api_key.into());
        self
    }

    /// Names `organization_id` on every call.
    pub fn with_organization_id(mut self, organization_id: impl Into<String>) -> Self {
        self.organization_id = Some(organization_id.into());
        self
    }

    /// Connects with `tls`.
    pub fn with_tls(mut self, tls: TlsConfig) -> Self {
        self.tls = Some(tls);
        self
    }

    /// Gives a connect `timeout` to finish.
    pub fn with_connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// Sends and receives messages of up to `max_message_bytes`.
    pub fn with_max_message_bytes(mut self, max_message_bytes: usize) -> Self {
        self.max_message_bytes = max_message_bytes;
        self
    }

    /// The credentials every call carries.
    pub(crate) fn auth(&self) -> AuthInterceptor {
        AuthInterceptor {
            api_key: self.api_key.clone(),
            organization_id: self.organization_id.clone(),
        }
    }
}

impl From<String> for ConnectOptions {
    fn from(url: String) -> Self {
        Self::new(url)
    }
}

impl From<&String> for ConnectOptions {
    fn from(url: &String) -> Self {
        Self::new(url.as_str())
    }
}

impl From<&str> for ConnectOptions {
    fn from(url: &str) -> Self {
        Self::new(url)
    }
}

// Written by hand so the API key is never printed; destructured so that a
// new field must be added here to compile.
impl std::fmt::Debug for ConnectOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            url,
            namespace,
            api_key,
            organization_id,
            tls,
            connect_timeout,
            max_message_bytes,
        } = self;
        f.debug_struct("ConnectOptions")
            .field("url", url)
            .field("namespace", namespace)
            .field("api_key", &crate::transport::credentials::redacted(api_key))
            .field("organization_id", organization_id)
            .field("tls", tls)
            .field("connect_timeout", connect_timeout)
            .field("max_message_bytes", max_message_bytes)
            .finish()
    }
}

/// A connection to the engine, and the clients that share it.
///
/// Cheap to clone: clones share the channel, which reconnects on its own if
/// the engine goes away and comes back.
#[derive(Clone)]
pub struct Connection {
    channel: WireChannel,
    options: Arc<ConnectOptions>,
    wire: Arc<WireMetrics>,
}

impl Connection {
    /// Connects to the engine now.
    ///
    /// # Errors
    ///
    /// [`Error::Configuration`](crate::Error::Configuration) for an address
    /// or TLS settings that cannot be used, and
    /// [`Error::Connection`](crate::Error::Connection), naming the address
    /// and why, if the engine cannot be reached.
    pub async fn connect(options: ConnectOptions) -> Result<Self> {
        let channel = endpoint(&options)?.connect().await.map_err(|e| {
            crate::Error::connection(format!(
                "Failed to connect to {}: {}",
                options.url,
                crate::error::with_causes(&e)
            ))
        })?;
        Ok(Self::over(channel, options))
    }

    /// A connection that is made on its first call, so an engine that is not
    /// up yet does not fail it.
    ///
    /// # Errors
    ///
    /// [`Error::Configuration`](crate::Error::Configuration) for an address
    /// or TLS settings that cannot be used.
    pub fn lazy(options: ConnectOptions) -> Result<Self> {
        let channel = endpoint(&options)?.connect_lazy();
        Ok(Self::over(channel, options))
    }

    fn over(channel: tonic::transport::Channel, options: ConnectOptions) -> Self {
        let wire = WireMetrics::new();
        Self {
            channel: Instrumented::new(channel, Arc::clone(&wire)),
            options: Arc::new(options),
            wire,
        }
    }

    /// A client for workflows, in the connection's namespace.
    pub fn workflow_client(&self) -> WorkflowClient {
        WorkflowClient::new(self)
    }

    /// A client for namespaces.
    pub fn namespace_client(&self) -> NamespaceClient {
        NamespaceClient::new(self)
    }

    /// A client for actors.
    pub fn actor_client(&self) -> ActorClient {
        ActorClient::new(self)
    }

    /// What the connection was made with.
    pub fn options(&self) -> &ConnectOptions {
        &self.options
    }

    /// The calls made on this connection so far, by method.
    pub fn wire_metrics(&self) -> WireMetricsSnapshot {
        self.wire.snapshot()
    }

    pub(crate) fn channel(&self) -> WireChannel {
        self.channel.clone()
    }
}

impl std::fmt::Debug for Connection {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Connection")
            .field("options", &self.options)
            .finish_non_exhaustive()
    }
}

fn endpoint(options: &ConnectOptions) -> Result<tonic::transport::Endpoint> {
    build_endpoint(
        &options.url,
        options.tls.as_ref(),
        options.connect_timeout,
        CLIENT_CALL_TIMEOUT,
    )
}

/// Attaches the credentials to every call a client makes.
///
/// It is bound when a client is built, so every call made through that
/// client carries the headers and no call site can forget them. The headers
/// match the ones the worker sends on its own calls.
#[derive(Clone, Default)]
pub(crate) struct AuthInterceptor {
    /// Sent as `authorization: Bearer <key>`.
    pub(crate) api_key: Option<String>,
    /// Sent as `x-organization-id`, matching the worker's poll requests.
    pub(crate) organization_id: Option<String>,
}

impl tonic::service::Interceptor for AuthInterceptor {
    fn call(
        &mut self,
        mut request: tonic::Request<()>,
    ) -> std::result::Result<tonic::Request<()>, tonic::Status> {
        if let Some(ref key) = self.api_key {
            match format!("Bearer {}", key).parse() {
                Ok(value) => {
                    request.metadata_mut().insert("authorization", value);
                }
                // A key that cannot be encoded as a header is a configuration
                // error; surface it instead of sending the request unauthenticated.
                Err(_) => return Err(tonic::Status::invalid_argument(
                    "api_key contains characters that cannot be sent in the authorization header",
                )),
            }
        }
        if let Some(ref org) = self.organization_id {
            if let Ok(value) = org.parse() {
                request.metadata_mut().insert("x-organization-id", value);
            }
        }
        Ok(request)
    }
}

/// The gRPC transport every client is built on.
pub(crate) type ClientChannel =
    tonic::service::interceptor::InterceptedService<WireChannel, AuthInterceptor>;

impl Connection {
    /// The channel with the connection's credentials on every call.
    pub(crate) fn authenticated(&self) -> ClientChannel {
        ClientChannel::new(self.channel(), self.options.auth())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    use tonic::service::Interceptor;

    #[test]
    fn auth_interceptor_sends_bearer_token() {
        let mut auth = AuthInterceptor {
            api_key: Some("secret-key".into()),
            organization_id: None,
        };
        let req = auth
            .call(tonic::Request::new(()))
            .expect("interceptor failed");
        assert_eq!(
            req.metadata().get("authorization").unwrap(),
            "Bearer secret-key"
        );
    }

    #[test]
    fn auth_interceptor_sends_organization_id() {
        let mut auth = AuthInterceptor {
            api_key: None,
            organization_id: Some("org-123".into()),
        };
        let req = auth
            .call(tonic::Request::new(()))
            .expect("interceptor failed");
        assert_eq!(req.metadata().get("x-organization-id").unwrap(), "org-123");
    }

    #[test]
    fn auth_interceptor_is_inert_without_credentials() {
        let mut auth = AuthInterceptor::default();
        let req = auth
            .call(tonic::Request::new(()))
            .expect("interceptor failed");
        assert!(req.metadata().get("authorization").is_none());
        assert!(req.metadata().get("x-organization-id").is_none());
    }

    #[test]
    fn auth_interceptor_rejects_unencodable_key() {
        // A newline cannot go in a header. Sending the request unauthenticated
        // instead would hide a configuration error, so the call must fail.
        let mut auth = AuthInterceptor {
            api_key: Some("bad\nkey".into()),
            organization_id: None,
        };
        assert!(auth.call(tonic::Request::new(())).is_err());
    }

    /// A connection made now reports an engine it cannot reach, naming the
    /// address; one made on first use is only refused an address it cannot
    /// use at all.
    #[tokio::test]
    async fn an_unreachable_or_unusable_engine_is_reported() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        drop(listener);

        let error = Connection::connect(ConnectOptions::new(&url))
            .await
            .unwrap_err();
        assert!(
            matches!(error, crate::Error::Connection(ref message) if message.contains(&url)),
            "{error}"
        );
        assert!(Connection::lazy(ConnectOptions::new(&url)).is_ok());
        assert!(matches!(
            Connection::lazy(ConnectOptions::new("not a url")),
            Err(crate::Error::Configuration(_))
        ));
    }

    /// Every client on a connection is measured in the connection's wire
    /// metrics, by method.
    #[tokio::test]
    async fn the_calls_of_every_client_are_measured_on_the_connection() {
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        drop(listener);
        let connection = Connection::lazy(ConnectOptions::new(url)).unwrap();

        let _ = connection.namespace_client().list_namespaces(1, 0).await;
        let _ = connection
            .workflow_client()
            .get_workflow_status("wf", None)
            .await;

        let methods = connection.wire_metrics().methods;
        for method in [
            "/orcher.v1.NamespaceService/ListNamespaces",
            "/orcher.v1.WorkflowService/GetWorkflowStatus",
        ] {
            assert_eq!(methods[method].calls, 1, "{method}: {methods:?}");
        }
    }
}
