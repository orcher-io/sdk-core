//! Credential headers for worker-originated gRPC calls.
//!
//! A worker talks to the server on three paths: it polls for work, it reports
//! the outcome of that work, and it registers itself. All three have to carry
//! the same two headers, or a server with authentication enabled rejects them.
//!
//! Polling attaches the headers inline. The reporting and registration paths
//! share the single implementation in this module.

/// Wrap a request body with the credential headers the server expects.
///
/// `authorization: Bearer <key>` authenticates the worker and
/// `x-organization-id` scopes it to a tenant. Both are optional; without them
/// the request goes out with no credential headers, which is what a server
/// without authentication expects.
///
/// A value that cannot be encoded as a header is dropped rather than causing
/// a panic.
pub fn credentialed_request<T>(
    body: T,
    api_key: Option<&str>,
    organization_id: Option<&str>,
) -> tonic::Request<T> {
    let mut request = tonic::Request::new(body);
    if let Some(org_id) = organization_id {
        if let Ok(value) = org_id.parse() {
            request.metadata_mut().insert("x-organization-id", value);
        }
    }
    if let Some(key) = api_key {
        if let Ok(value) = format!("Bearer {}", key).parse() {
            request.metadata_mut().insert("authorization", value);
        }
    }
    request
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn attaches_both_headers_when_configured() {
        let request = credentialed_request((), Some("secret"), Some("org_abc"));
        let metadata = request.metadata();
        assert_eq!(
            metadata.get("authorization").unwrap().to_str().unwrap(),
            "Bearer secret"
        );
        assert_eq!(
            metadata.get("x-organization-id").unwrap().to_str().unwrap(),
            "org_abc"
        );
    }

    #[test]
    fn attaches_nothing_when_unconfigured() {
        let request = credentialed_request((), None, None);
        assert!(request.metadata().get("authorization").is_none());
        assert!(request.metadata().get("x-organization-id").is_none());
    }

    #[test]
    fn a_header_that_cannot_be_encoded_is_dropped_not_panicked_on() {
        let request = credentialed_request((), Some("key\nwith\nnewlines"), Some("org\nid"));
        assert!(request.metadata().get("authorization").is_none());
        assert!(request.metadata().get("x-organization-id").is_none());
    }
}
