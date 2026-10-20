//! A channel that measures every call it carries.

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use tonic::codegen::{http, Service};
use tonic::transport::Channel;

use crate::observability::{CallOutcome, WireMetrics};

/// The channel every client of a worker uses: a [`Channel`] whose calls are
/// recorded in the worker's [`WireMetrics`].
pub type WireChannel = Instrumented<Channel>;

/// Wraps a gRPC transport so each call's method, latency and outcome are
/// recorded.
#[derive(Debug, Clone)]
pub struct Instrumented<S> {
    inner: S,
    wire: Arc<WireMetrics>,
}

impl<S> Instrumented<S> {
    /// Record the calls `inner` carries in `wire`.
    pub fn new(inner: S, wire: Arc<WireMetrics>) -> Self {
        Self { inner, wire }
    }
}

/// How a call that got an answer ended: a gRPC status in the headers (a
/// trailers-only answer, as an error is sent) says it failed with that code;
/// otherwise it is OK. A failure reported later in trailers is not seen
/// here, which a unary call does not do.
fn outcome_of(headers: &http::HeaderMap) -> CallOutcome {
    match headers
        .get("grpc-status")
        .and_then(|value| value.to_str().ok())
        .and_then(|value| value.parse::<i32>().ok())
    {
        Some(0) | None => CallOutcome::Ok,
        Some(code) => CallOutcome::Status(code),
    }
}

impl<S, ReqBody, ResBody> Service<http::Request<ReqBody>> for Instrumented<S>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<ResBody>>,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = Pin<Box<dyn Future<Output = Result<Self::Response, Self::Error>> + Send>>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<ReqBody>) -> Self::Future {
        // A cheap clone: the URI shares its bytes, so nothing is copied per call.
        let uri = request.uri().clone();
        let wire = Arc::clone(&self.wire);
        let started = Instant::now();
        let call = self.inner.call(request);
        Box::pin(async move {
            let result = call.await;
            let outcome = match &result {
                Ok(response) => outcome_of(response.headers()),
                Err(_) => CallOutcome::Transport,
            };
            wire.record_call(uri.path(), started.elapsed(), outcome);
            result
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn the_outcome_is_read_from_the_grpc_status() {
        let mut headers = http::HeaderMap::new();
        assert_eq!(outcome_of(&headers), CallOutcome::Ok);
        headers.insert("grpc-status", "0".parse().unwrap());
        assert_eq!(outcome_of(&headers), CallOutcome::Ok);
        headers.insert("grpc-status", "14".parse().unwrap());
        assert_eq!(outcome_of(&headers), CallOutcome::Status(14));
    }
}
