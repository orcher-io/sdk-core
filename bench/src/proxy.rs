//! A counting gRPC proxy between the worker and the engine.
//!
//! Everything the worker says to the engine passes through it, so it counts,
//! per RPC method, the calls, their bytes each way and their gRPC status, and
//! from the poll responses, the work claimed (activations and tasks, eager
//! ones included) and the journal entries each activation carried. It also
//! counts the HTTP/2 connections the worker opens, which is what #38 needs to
//! know first.
//!
//! Upstream, it either opens one engine connection per worker connection
//! ([`Upstream::Mirror`], so the engine sees exactly the worker's
//! connections) or carries every worker connection over one shared engine
//! connection ([`Upstream::Shared`]), which is how #38's shared connection
//! can be tried without changing the worker: every long poll, completion and
//! heartbeat then shares one HTTP/2 connection's flow control into the
//! engine. The worker-to-proxy side stays one connection per driver either
//! way; see the README for what that does and does not show.
//!
//! Every RPC the worker makes is unary, so each request and response is
//! read whole, counted and passed on, trailers included.

use std::collections::BTreeMap;
use std::convert::Infallible;
use std::net::SocketAddr;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use bytes::{Buf, Bytes};
use http::{HeaderMap, HeaderValue, Request, Response};
use http_body::{Body, Frame, SizeHint};
use http_body_util::BodyExt;
use hyper::body::Incoming;
use hyper::client::conn::http2::SendRequest;
use hyper_util::rt::{TokioExecutor, TokioIo};
use orcher_proto::orcher::v1::{
    CompleteWorkflowExecutionResponse, PollTaskExecutionResponse, PollWorkflowExecutionResponse,
};
use prost::Message;
use serde::{Deserialize, Serialize};
use tokio::net::{TcpListener, TcpStream};

/// How the proxy connects to the engine.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Upstream {
    /// One engine connection per worker connection.
    Mirror,
    /// One engine connection for every worker connection.
    Shared,
}

/// What one RPC method cost.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct MethodStats {
    pub calls: u64,
    /// Calls that ended with a gRPC status other than OK.
    pub errors: u64,
    pub request_bytes: u64,
    pub response_bytes: u64,
    /// Polls that brought back work.
    pub with_work: u64,
    /// Journal entries the activations carried (workflow polls only).
    pub journal_entries: u64,
    /// Tasks handed back with a workflow completion.
    pub eager_tasks: u64,
    /// The errors, by status code and message (the first few kinds only).
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub error_kinds: BTreeMap<String, u64>,
}

/// How many kinds of error are kept per method.
const ERROR_KINDS: usize = 5;

/// Everything the proxy counted.
#[derive(Debug, Clone, Default, Serialize, Deserialize)]
pub struct ProxyStats {
    /// By method name, for example `PollWorkflowExecution`.
    pub methods: BTreeMap<String, MethodStats>,
    /// Connections the worker opened.
    pub connections_opened: u64,
    /// The most worker connections open at once.
    pub peak_open_connections: u64,
    /// The most RPCs started in the phase in flight at once over any one
    /// worker connection.
    pub peak_streams_per_connection: u64,
    /// The most RPCs in flight at once, over every worker connection.
    pub peak_in_flight_rpcs: u64,
    /// Engine connections the proxy opened.
    pub upstream_connections: u64,
}

impl ProxyStats {
    /// Workflow activations claimed: polled ones.
    pub fn activations_claimed(&self) -> u64 {
        self.method("PollWorkflowExecution").with_work
    }

    /// Tasks claimed: polled and eager ones.
    pub fn tasks_claimed(&self) -> u64 {
        self.method("PollTaskExecution").with_work
            + self.method("CompleteWorkflowExecution").eager_tasks
    }

    pub fn method(&self, name: &str) -> MethodStats {
        self.methods.get(name).cloned().unwrap_or_default()
    }

    pub fn total_calls(&self) -> u64 {
        self.methods.values().map(|m| m.calls).sum()
    }

    /// What was counted since `before`.
    pub fn since(&self, before: &ProxyStats) -> ProxyStats {
        let mut methods = BTreeMap::new();
        for (name, after) in &self.methods {
            let b = before.method(name);
            let delta = MethodStats {
                calls: after.calls - b.calls,
                errors: after.errors - b.errors,
                request_bytes: after.request_bytes - b.request_bytes,
                response_bytes: after.response_bytes - b.response_bytes,
                with_work: after.with_work - b.with_work,
                journal_entries: after.journal_entries - b.journal_entries,
                eager_tasks: after.eager_tasks - b.eager_tasks,
                error_kinds: after
                    .error_kinds
                    .iter()
                    .map(|(kind, n)| {
                        (
                            kind.clone(),
                            n - b.error_kinds.get(kind).copied().unwrap_or(0),
                        )
                    })
                    .filter(|(_, n)| *n > 0)
                    .collect(),
            };
            if delta.calls > 0 {
                methods.insert(name.clone(), delta);
            }
        }
        ProxyStats {
            methods,
            connections_opened: self.connections_opened - before.connections_opened,
            peak_open_connections: self.peak_open_connections,
            peak_streams_per_connection: self.peak_streams_per_connection,
            peak_in_flight_rpcs: self.peak_in_flight_rpcs,
            upstream_connections: self.upstream_connections - before.upstream_connections,
        }
    }
}

#[derive(Default)]
struct Counts {
    stats: Mutex<ProxyStats>,
    open: AtomicU64,
    in_flight: AtomicU64,
}

impl Counts {
    fn with<R>(&self, f: impl FnOnce(&mut ProxyStats) -> R) -> R {
        f(&mut self.stats.lock().unwrap_or_else(|p| p.into_inner()))
    }
}

/// A running proxy.
#[derive(Clone)]
pub struct Proxy {
    addr: SocketAddr,
    counts: Arc<Counts>,
}

impl Proxy {
    /// Listens on a free local port and forwards to `engine` (`host:port`).
    pub async fn start(engine: String, upstream: Upstream) -> std::io::Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let addr = listener.local_addr()?;
        let counts = Arc::new(Counts::default());
        let shared: Arc<tokio::sync::Mutex<Option<SendRequest<Replay>>>> = Arc::default();
        let accepting = counts.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let _ = stream.set_nodelay(true);
                let counts = accepting.clone();
                let engine = engine.clone();
                let shared = shared.clone();
                tokio::spawn(async move {
                    serve_connection(stream, engine, upstream, shared, counts).await;
                });
            }
        });
        Ok(Self { addr, counts })
    }

    /// The URL the worker connects to.
    pub fn url(&self) -> String {
        format!("http://{}", self.addr)
    }

    /// Everything counted so far.
    pub fn stats(&self) -> ProxyStats {
        self.counts.with(|stats| stats.clone())
    }

    /// Clears the peaks, so the next reading covers only what follows.
    pub fn reset_peaks(&self) {
        let open = self.counts.open.load(Ordering::Relaxed);
        let in_flight = self.counts.in_flight.load(Ordering::Relaxed);
        self.counts.with(|stats| {
            stats.peak_open_connections = open;
            stats.peak_streams_per_connection = 0;
            stats.peak_in_flight_rpcs = in_flight;
        });
    }
}

async fn connect_upstream(engine: &str, counts: &Counts) -> Option<SendRequest<Replay>> {
    let stream = TcpStream::connect(engine).await.ok()?;
    let _ = stream.set_nodelay(true);
    let (send, connection) =
        hyper::client::conn::http2::handshake(TokioExecutor::new(), TokioIo::new(stream))
            .await
            .ok()?;
    tokio::spawn(connection);
    counts.with(|stats| stats.upstream_connections += 1);
    Some(send)
}

async fn upstream_for(
    engine: &str,
    mode: Upstream,
    own: &tokio::sync::Mutex<Option<SendRequest<Replay>>>,
    shared: &tokio::sync::Mutex<Option<SendRequest<Replay>>>,
    counts: &Counts,
) -> Option<SendRequest<Replay>> {
    let slot = match mode {
        Upstream::Mirror => own,
        Upstream::Shared => shared,
    };
    let mut slot = slot.lock().await;
    if let Some(send) = slot.as_ref().filter(|send| !send.is_closed()) {
        return Some(send.clone());
    }
    *slot = connect_upstream(engine, counts).await;
    slot.clone()
}

async fn serve_connection(
    stream: TcpStream,
    engine: String,
    mode: Upstream,
    shared: Arc<tokio::sync::Mutex<Option<SendRequest<Replay>>>>,
    counts: Arc<Counts>,
) {
    let open = counts.open.fetch_add(1, Ordering::Relaxed) + 1;
    counts.with(|stats| {
        stats.connections_opened += 1;
        stats.peak_open_connections = stats.peak_open_connections.max(open);
    });
    let own: Arc<tokio::sync::Mutex<Option<SendRequest<Replay>>>> = Arc::default();
    let in_flight = Arc::new(AtomicU64::new(0));
    let service_counts = counts.clone();
    let service = hyper::service::service_fn(move |request: Request<Incoming>| {
        let engine = engine.clone();
        let own = own.clone();
        let shared = shared.clone();
        let counts = service_counts.clone();
        let in_flight = in_flight.clone();
        async move {
            let streams = in_flight.fetch_add(1, Ordering::Relaxed) + 1;
            let all = counts.in_flight.fetch_add(1, Ordering::Relaxed) + 1;
            counts.with(|stats| {
                stats.peak_streams_per_connection = stats.peak_streams_per_connection.max(streams);
                stats.peak_in_flight_rpcs = stats.peak_in_flight_rpcs.max(all);
            });
            let response = forward(request, &engine, mode, &own, &shared, &counts).await;
            in_flight.fetch_sub(1, Ordering::Relaxed);
            counts.in_flight.fetch_sub(1, Ordering::Relaxed);
            Ok::<_, Infallible>(response)
        }
    });
    let _ = hyper::server::conn::http2::Builder::new(TokioExecutor::new())
        .serve_connection(TokioIo::new(stream), service)
        .await;
    counts.open.fetch_sub(1, Ordering::Relaxed);
}

async fn forward(
    request: Request<Incoming>,
    engine: &str,
    mode: Upstream,
    own: &tokio::sync::Mutex<Option<SendRequest<Replay>>>,
    shared: &tokio::sync::Mutex<Option<SendRequest<Replay>>>,
    counts: &Counts,
) -> Response<Replay> {
    let (mut parts, body) = request.into_parts();
    let method = parts
        .uri
        .path()
        .rsplit('/')
        .next()
        .unwrap_or_default()
        .to_string();
    let Ok(body) = body.collect().await else {
        return unavailable("the worker's request could not be read");
    };
    let request_body = body.to_bytes();
    let request_bytes = request_body.len() as u64;
    let path = parts
        .uri
        .path_and_query()
        .map(|p| p.as_str().to_string())
        .unwrap_or_default();
    parts.uri = match format!("http://{engine}{path}").parse() {
        Ok(uri) => uri,
        Err(_) => return unavailable("bad request path"),
    };
    let Some(mut send) = upstream_for(engine, mode, own, shared, counts).await else {
        record(counts, &method, request_bytes, 0, Some("unreachable"), None);
        return unavailable("the engine could not be reached");
    };
    let upstream_request = Request::from_parts(parts, Replay::new(request_body, None));
    let response = match send.send_request(upstream_request).await {
        Ok(response) => response,
        Err(_) => {
            record(
                counts,
                &method,
                request_bytes,
                0,
                Some("connection failed"),
                None,
            );
            return unavailable("the engine connection failed");
        }
    };
    let (parts, body) = response.into_parts();
    let Ok(collected) = body.collect().await else {
        record(
            counts,
            &method,
            request_bytes,
            0,
            Some("unreadable response"),
            None,
        );
        return unavailable("the engine's response could not be read");
    };
    let trailers = collected.trailers().cloned();
    let data = collected.to_bytes();
    let error = grpc_error(&parts.headers, trailers.as_ref());
    let ok = error.is_none();
    record(
        counts,
        &method,
        request_bytes,
        data.len() as u64,
        error.as_deref(),
        ok.then_some(&data),
    );
    Response::from_parts(parts, Replay::new(data, trailers))
}

/// The call's error, `code: message`, or `None` when it succeeded.
fn grpc_error(headers: &HeaderMap, trailers: Option<&HeaderMap>) -> Option<String> {
    let get = |name: &str| {
        trailers
            .and_then(|t| t.get(name))
            .or_else(|| headers.get(name))
            .map(|v| String::from_utf8_lossy(v.as_bytes()).into_owned())
    };
    let status = get("grpc-status").unwrap_or_else(|| "no status".to_string());
    if status == "0" {
        return None;
    }
    let message = get("grpc-message").unwrap_or_default();
    // Messages carry ids; keep the start, which names the kind.
    let message: String = message.chars().take(100).collect();
    Some(format!("{status}: {message}"))
}

fn record(
    counts: &Counts,
    method: &str,
    request_bytes: u64,
    response_bytes: u64,
    error: Option<&str>,
    response: Option<&Bytes>,
) {
    let (with_work, journal_entries, eager_tasks) = match response.and_then(|r| message(r)) {
        Some(message) => decode(method, message),
        None => (false, 0, 0),
    };
    counts.with(|stats| {
        let m = stats.methods.entry(method.to_string()).or_default();
        m.calls += 1;
        if let Some(error) = error {
            m.errors += 1;
            if m.error_kinds.contains_key(error) || m.error_kinds.len() < ERROR_KINDS {
                *m.error_kinds.entry(error.to_string()).or_default() += 1;
            }
        }
        m.request_bytes += request_bytes;
        m.response_bytes += response_bytes;
        m.with_work += u64::from(with_work);
        m.journal_entries += journal_entries;
        m.eager_tasks += eager_tasks;
    });
}

/// The message in a unary gRPC body (one length-prefixed, uncompressed frame).
fn message(body: &Bytes) -> Option<&[u8]> {
    if body.len() < 5 || body[0] != 0 {
        return None;
    }
    let len = u32::from_be_bytes([body[1], body[2], body[3], body[4]]) as usize;
    body.get(5..5 + len)
}

fn decode(method: &str, message: &[u8]) -> (bool, u64, u64) {
    match method {
        "PollWorkflowExecution" => match PollWorkflowExecutionResponse::decode(message) {
            Ok(poll) if !poll.workflow_id.is_empty() => (true, poll.journal.len() as u64, 0),
            _ => (false, 0, 0),
        },
        "PollTaskExecution" => match PollTaskExecutionResponse::decode(message) {
            Ok(poll) if !poll.task_token.is_empty() => (true, 0, 0),
            _ => (false, 0, 0),
        },
        "CompleteWorkflowExecution" => match CompleteWorkflowExecutionResponse::decode(message) {
            Ok(done) => (false, 0, done.eager_tasks.len() as u64),
            _ => (false, 0, 0),
        },
        _ => (false, 0, 0),
    }
}

fn unavailable(message: &str) -> Response<Replay> {
    let mut response = Response::new(Replay::new(Bytes::new(), None));
    let headers = response.headers_mut();
    headers.insert("content-type", HeaderValue::from_static("application/grpc"));
    headers.insert("grpc-status", HeaderValue::from_static("14"));
    if let Ok(value) = HeaderValue::from_str(message) {
        headers.insert("grpc-message", value);
    }
    response
}

/// A body read whole, played back: its data, then its trailers.
pub struct Replay {
    data: Option<Bytes>,
    trailers: Option<HeaderMap>,
}

impl Replay {
    fn new(data: Bytes, trailers: Option<HeaderMap>) -> Self {
        Self {
            data: (!data.is_empty()).then_some(data),
            trailers,
        }
    }
}

impl Body for Replay {
    type Data = Bytes;
    type Error = Infallible;

    fn poll_frame(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Result<Frame<Bytes>, Infallible>>> {
        if let Some(data) = self.data.take() {
            return Poll::Ready(Some(Ok(Frame::data(data))));
        }
        Poll::Ready(self.trailers.take().map(|t| Ok(Frame::trailers(t))))
    }

    fn is_end_stream(&self) -> bool {
        self.data.is_none() && self.trailers.is_none()
    }

    fn size_hint(&self) -> SizeHint {
        SizeHint::with_exact(self.data.as_ref().map_or(0, |d| d.remaining() as u64))
    }
}
