//! An answer that did not come from the engine.
//!
//! Something in front of the engine (a load balancer, an ingress, a proxy)
//! can answer a call itself: with an HTML error page while the engine is
//! being replaced, or with a website when the URL points at the wrong port.
//! tonic reads that page as gRPC and reports the first byte, `<`, as a bad
//! compression flag: `Internal: protocol error: received message with invalid
//! compression flag: 60 ... while receiving response with status: 502 Bad
//! Gateway`. That sends an operator looking for a compression bug, and
//! `Internal` is not retried, so a caller waiting on a workflow during an
//! engine rollout gave up as if the workflow had failed.

use tonic::{Code, Status};

/// The suffix tonic gives a decode error, naming the response's HTTP status.
const HTTP_STATUS_MARKER: &str = "while receiving response with status: ";

/// Restates an answer that was not gRPC by what it was, with the code the
/// gRPC specification gives its HTTP status; anything else is returned as is.
///
/// A proxy's 429, 502, 503 or 504 becomes `Unavailable`, which is retried; a
/// page with any other status (a 200 from a website, a 404) says the address
/// is wrong, and is not.
pub fn clarify(status: Status) -> Status {
    if !matches!(status.code(), Code::Internal | Code::Unknown) {
        return status;
    }
    let Some(content_type) = status
        .metadata()
        .get("content-type")
        .and_then(|value| value.to_str().ok())
        .map(str::to_owned)
    else {
        return status;
    };
    if content_type.starts_with("application/grpc") {
        return status;
    }
    let http_status = status
        .message()
        .rsplit_once(HTTP_STATUS_MARKER)
        .map(|(_, http)| http.trim().to_owned());
    let http_code = http_status
        .as_deref()
        .and_then(|http| http.split_whitespace().next())
        .and_then(|code| code.parse::<u16>().ok());
    let answered = match &http_status {
        Some(http) => format!("HTTP {http} with a {content_type} page"),
        None => format!("a {content_type} page"),
    };

    let code = http_code.map_or(Code::Internal, code_for_http_status);
    let message = if code == Code::Unavailable {
        format!(
            "the engine is unavailable: something in front of it (a proxy or load \
             balancer) answered {answered} instead of gRPC. This is usually temporary, \
             for example while the engine is being redeployed."
        )
    } else {
        format!(
            "the server answered {answered} instead of gRPC. Check that the address is \
             the engine's gRPC endpoint."
        )
    };
    Status::with_metadata(code, message, status.metadata().clone())
}

/// The gRPC code for an HTTP status, as the gRPC specification maps them
/// ("HTTP to gRPC Status Code Mapping"), except that the statuses it maps to
/// `Unknown` stay `Internal`: this crate retries `Unknown`, since transport
/// errors surface as it mid-long-poll, and a page that says the address is
/// wrong must not be retried forever.
fn code_for_http_status(http: u16) -> Code {
    match http {
        401 => Code::Unauthenticated,
        403 => Code::PermissionDenied,
        404 => Code::Unimplemented,
        429 | 502 | 503 | 504 => Code::Unavailable,
        _ => Code::Internal,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use tonic::metadata::MetadataMap;

    /// tonic's error for a page answered with `http`, as it reports it.
    fn page(http: &str, content_type: &str) -> Status {
        let mut metadata = MetadataMap::new();
        metadata.insert("content-type", content_type.parse().unwrap());
        Status::with_metadata(
            Code::Internal,
            format!(
                "protocol error: received message with invalid compression flag: 60 \
                 (valid flags are 0 and 1) while receiving response with status: {http}"
            ),
            metadata,
        )
    }

    #[test]
    fn a_proxy_error_page_is_the_engine_being_unavailable() {
        for http in [
            "502 Bad Gateway",
            "503 Service Unavailable",
            "504 Gateway Timeout",
            "429 Too Many Requests",
        ] {
            let status = clarify(page(http, "text/html"));
            assert_eq!(status.code(), Code::Unavailable, "{http}");
            assert!(
                status.message().contains("unavailable"),
                "{}",
                status.message()
            );
            assert!(status.message().contains(http), "{}", status.message());
            assert!(
                !status.message().contains("compression"),
                "{}",
                status.message()
            );
        }
    }

    #[test]
    fn a_page_with_another_status_is_a_wrong_address_not_retried() {
        for (http, code) in [
            ("200 OK", Code::Internal),
            ("404 Not Found", Code::Unimplemented),
            ("401 Unauthorized", Code::Unauthenticated),
            ("403 Forbidden", Code::PermissionDenied),
        ] {
            let status = clarify(page(http, "text/html; charset=utf-8"));
            assert_eq!(status.code(), code, "{http}");
            assert!(
                status.message().contains("gRPC endpoint"),
                "{}",
                status.message()
            );
        }
    }

    #[test]
    fn a_grpc_answer_is_left_alone() {
        let real = page("200 OK", "application/grpc");
        let message = real.message().to_owned();
        let status = clarify(real);
        assert_eq!(status.code(), Code::Internal);
        assert_eq!(status.message(), message);

        let unavailable = Status::unavailable("engine restarting");
        assert_eq!(clarify(unavailable).message(), "engine restarting");
    }

    /// A bare HTTP/2 server answering every call with an HTML page and
    /// `http`, the way a load balancer does while the engine is away.
    async fn serve_page(http: u16) -> std::net::SocketAddr {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                tokio::spawn(async move {
                    let Ok(mut connection) = h2::server::handshake(socket).await else {
                        return;
                    };
                    while let Some(Ok((_request, mut respond))) = connection.accept().await {
                        let response = http::Response::builder()
                            .status(http)
                            .header("content-type", "text/html")
                            .body(())
                            .unwrap();
                        if let Ok(mut body) = respond.send_response(response, false) {
                            let page = bytes::Bytes::from_static(
                                b"<html><body><h1>Bad Gateway</h1></body></html>",
                            );
                            let _ = body.send_data(page, true);
                        }
                    }
                });
            }
        });
        addr
    }

    /// Through a real client call, so tonic's own wording is what is read.
    #[tokio::test]
    async fn a_real_proxy_page_reaches_the_caller_as_unavailable_and_retryable() {
        for (http, code, retryable) in [
            (502, Code::Unavailable, true),
            (503, Code::Unavailable, true),
            (200, Code::Internal, false),
        ] {
            let addr = serve_page(http).await;
            let client = crate::client::Connection::lazy(crate::client::ConnectOptions::new(
                format!("http://{addr}"),
            ))
            .unwrap()
            .workflow_client();
            let error = client.get_workflow_status("wf", None).await.unwrap_err();
            let crate::error::Error::Status(status) = &error else {
                panic!("{error:?}");
            };
            assert_eq!(
                status.code().as_i32(),
                code as i32,
                "HTTP {http}: {status:?}"
            );
            assert_eq!(error.is_retryable(), retryable, "HTTP {http}: {status:?}");
            assert!(!status.message().contains("compression"), "{status:?}");
        }
    }
}
