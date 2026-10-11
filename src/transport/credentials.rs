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

/// Shows that a secret is set without showing it, for `Debug` output.
///
/// Configs carrying an API key or a private key implement `Debug` by hand
/// with this, because a derived `Debug` prints them in plain text into any
/// log line or panic message that formats the config.
pub(crate) fn redacted<T>(secret: &Option<T>) -> Option<Redacted> {
    secret.as_ref().map(|_| Redacted)
}

/// The `Debug` stand-in for a secret: prints `<redacted>`.
pub(crate) struct Redacted;

impl std::fmt::Debug for Redacted {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("<redacted>")
    }
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

    /// No config that carries a secret prints it when formatted with `{:?}`,
    /// as a log line or a panic message would; each still shows that one is
    /// set. The mTLS key travels inside every driver config's TLS settings.
    #[test]
    fn formatting_a_config_never_prints_its_secrets() {
        use crate::transport::TlsConfig;
        use crate::worker::drivers::{
            actor::ActorDriverConfig, registration::WorkerRegistrationConfig,
            task::TaskDriverConfig, workflow::WorkflowDriverConfig,
        };
        use crate::worker::poll::polling::{ActorPollerConfig, PollerConfig};

        const KEY: &str = "sk-live-do-not-print";
        const PRIVATE_KEY: &str = "client-key-material-do-not-print";
        let tls = || {
            Some(TlsConfig {
                client_key: Some(PRIVATE_KEY.as_bytes().to_vec()),
                ..TlsConfig::default()
            })
        };

        let formatted = [
            format!("{:?}", tls().unwrap()),
            format!(
                "{:?}",
                TaskDriverConfig {
                    api_key: Some(KEY.into()),
                    tls_config: tls(),
                    ..Default::default()
                }
            ),
            format!(
                "{:?}",
                WorkflowDriverConfig {
                    api_key: Some(KEY.into()),
                    tls_config: tls(),
                    ..Default::default()
                }
            ),
            format!(
                "{:?}",
                WorkerRegistrationConfig {
                    api_key: Some(KEY.into()),
                    tls_config: tls(),
                    ..Default::default()
                }
            ),
            format!(
                "{:?}",
                ActorDriverConfig {
                    api_key: Some(KEY.into()),
                    tls_config: tls(),
                    ..Default::default()
                }
            ),
            format!(
                "{:#?}",
                PollerConfig {
                    api_key: Some(KEY.into()),
                    ..Default::default()
                }
            ),
            format!(
                "{:?}",
                ActorPollerConfig {
                    api_key: Some(KEY.into()),
                    ..Default::default()
                }
            ),
        ];
        // Byte form too: a derived Debug printed the key as a byte array.
        let key_bytes = format!("{:?}", &PRIVATE_KEY.as_bytes()[..8]);
        let key_bytes = key_bytes.trim_end_matches(']');
        for output in &formatted {
            assert!(!output.contains(KEY), "API key printed: {output}");
            assert!(
                !output.contains(PRIVATE_KEY),
                "private key printed: {output}"
            );
            assert!(
                !output.contains(key_bytes),
                "private key bytes printed: {output}"
            );
            assert!(
                output.contains("<redacted>"),
                "no sign the secret is set: {output}"
            );
        }
    }
}
