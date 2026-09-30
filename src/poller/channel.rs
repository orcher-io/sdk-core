use std::time::{Duration, Instant};

use crate::error::{Error, Result};
use tonic::transport::{Channel, Endpoint};

/// TLS configuration for gRPC connections.
///
/// Used by `ChannelManager` and driver configs to establish TLS/mTLS connections.
#[derive(Clone, Debug, Default)]
#[non_exhaustive]
pub struct TlsConfig {
    /// CA certificate PEM bytes for verifying the server.
    ///
    /// `None` uses the system trust store, which is what you want when the
    /// server presents a publicly-trusted certificate — including for mTLS,
    /// where only the client identity below is custom. Supply a CA only for a
    /// private or self-signed authority.
    pub ca_cert: Option<Vec<u8>>,
    /// Client certificate PEM bytes, for mTLS.
    pub client_cert: Option<Vec<u8>>,
    /// Client private key PEM bytes, for mTLS.
    pub client_key: Option<Vec<u8>>,
    /// Domain name to use for SNI and certificate verification instead of the URL's host.
    pub domain_name: Option<String>,
}

impl TlsConfig {
    /// Creates a config that verifies the server against the system trust store.
    ///
    /// It carries no client identity. Add a CA or a client identity with the
    /// `with_*` methods.
    pub fn new() -> Self {
        Self::default()
    }

    /// Verify the server against this CA (PEM) instead of the system trust store.
    pub fn with_ca_cert(mut self, ca_cert: impl Into<Vec<u8>>) -> Self {
        self.ca_cert = Some(ca_cert.into());
        self
    }

    /// Present this client certificate and key (PEM) for mTLS.
    pub fn with_client_identity(
        mut self,
        client_cert: impl Into<Vec<u8>>,
        client_key: impl Into<Vec<u8>>,
    ) -> Self {
        self.client_cert = Some(client_cert.into());
        self.client_key = Some(client_key.into());
        self
    }

    /// Override the domain name used for SNI and certificate verification.
    pub fn with_domain_name(mut self, domain_name: impl Into<String>) -> Self {
        self.domain_name = Some(domain_name.into());
        self
    }
}

/// Manages a gRPC `Channel` with a circuit-breaker cool-off and exponential backoff.
///
/// The breaker keeps a down server from causing constant background activity
/// and aggressive reconnect loops.
///
/// Behavior:
/// - Keeps an optional `Channel` and recreates it on demand when allowed.
/// - Tracks consecutive failures. On each failure, computes an exponential backoff + jitter
///   and sets a `next_retry_at` instant.
/// - While in cool-off (before `next_retry_at`), `get()` returns a fast `Error::connection`
///   without attempting to reconnect, preventing busy loops.
/// - On successful connection, resets `fail_count` and clears cool-off.
///
/// Usage pattern:
/// - Call `get().await` to obtain a connected `Channel` when you need to issue an RPC.
/// - If an RPC fails due to transport/connectivity reasons, call `record_failure()`.
/// - If `get()` returns a cooling-off error, skip the operation until after `next_retry_at`.
///
/// The state sits behind its own lock, so the manager can also be shared
/// without an outer one; the workflow driver shares it this way between the
/// completions it sends concurrently. That lock is never held across a connect:
/// callers that find no channel wait for one connect in flight and share its
/// outcome, rather than queueing to connect one after another.
///
/// Notes:
/// - The manager deliberately sets no HTTP/2 or TCP keep-alive, to keep background CPU low.
/// - Timeouts are set on the `Endpoint`, so every connection attempt is bounded.
pub struct ChannelManager {
    server_url: String,
    state: parking_lot::Mutex<State>,
    // Held for the duration of a connect, so only one is in flight.
    connecting: tokio::sync::Mutex<()>,
    connect_timeout: Duration,
    default_rpc_timeout: Duration,
    // Backoff settings.
    initial_backoff: Duration,
    max_backoff: Duration,
    jitter_ms: u64,
    tls_config: Option<TlsConfig>,
}

struct State {
    channel: Option<Channel>,
    // Which connection `channel` is. Bumped on every successful connect, so a
    // failure reported against an older connection can be told apart from
    // one against the current one.
    generation: u64,
    // Connect attempts finished so far, successful or not. A caller that
    // waited for someone else's connect compares this to know one finished.
    connects: u64,
    // Consecutive failures since the last successful connect.
    fail_count: u32,
    // Earliest instant at which a reconnect is allowed.
    next_retry_at: Option<Instant>,
    // When a connect last finished or a connection last failed, whichever
    // was later: what a capped caller's probe interval is measured from.
    last_attempt_at: Option<Instant>,
}

/// How a caller of [`ChannelManager::connection`] treats an open breaker.
#[derive(Debug, Clone, Copy)]
pub(crate) enum Breaker {
    /// Wait it out: no connect while it is open.
    Respect,
    /// Wait at most this long after the last attempt, however long the
    /// breaker would hold. For callers with work that should land as soon as
    /// the server is back: the breaker's own cool-off doubles up to thirty
    /// seconds, so an outage a little longer than one step would leave them
    /// waiting out most of the next.
    CapAt(Duration),
    /// Connect regardless: a caller's last attempt before its deadline.
    Ignore,
}

/// Why no channel could be had.
#[derive(Debug)]
pub(crate) enum Unconnected {
    /// The breaker is open; nothing was tried. Ends after the duration.
    CoolingOff(Duration),
    /// A connect was tried and failed.
    Failed(Error),
}

impl ChannelManager {
    /// Creates a manager for the given server address, without TLS.
    pub fn new(server_url: impl Into<String>) -> Self {
        Self {
            server_url: server_url.into(),
            state: parking_lot::Mutex::new(State {
                channel: None,
                generation: 0,
                connects: 0,
                fail_count: 0,
                next_retry_at: None,
                last_attempt_at: None,
            }),
            connecting: tokio::sync::Mutex::new(()),
            connect_timeout: Duration::from_secs(5),
            default_rpc_timeout: Duration::from_secs(70),
            initial_backoff: Duration::from_secs(1),
            max_backoff: Duration::from_secs(30),
            jitter_ms: 250,
            tls_config: None,
        }
    }

    /// Creates a manager for the given server address that connects over TLS.
    pub fn with_tls(server_url: impl Into<String>, tls: TlsConfig) -> Self {
        Self {
            tls_config: Some(tls),
            ..Self::new(server_url)
        }
    }

    /// Returns whether the circuit is open, so no reconnect is attempted yet.
    pub fn is_cooling_off(&self) -> bool {
        !self.cooling_off_duration().is_zero()
    }

    /// Records a transport-level failure and opens the circuit.
    ///
    /// Call this when an RPC fails because of connectivity or a timeout, so
    /// the manager backs off before connecting again. The cached channel is
    /// dropped.
    pub fn record_failure(&mut self) {
        self.count_failure();
    }

    fn count_failure(&self) {
        let mut state = self.state.lock();
        self.open_breaker(&mut state);
    }

    fn open_breaker(&self, state: &mut State) {
        state.fail_count = state.fail_count.saturating_add(1);
        let backoff =
            Self::calculate_backoff(state.fail_count, self.initial_backoff, self.max_backoff);
        let jitter = Self::small_jitter(self.jitter_ms);
        state.next_retry_at = Some(Instant::now() + backoff + jitter);
        state.last_attempt_at = Some(Instant::now());
        // Drop the channel so its background tasks do not keep running.
        state.channel = None;
    }

    /// Report that a call on connection `generation` failed without an
    /// answer from the server.
    ///
    /// Counted once per connection: when a connection breaks, every call in
    /// flight on it fails at once, and counting each would open the breaker
    /// for as long as there were calls — thirty seconds with a handful —
    /// when the server is one reconnect away. `count` false only drops the
    /// connection, for a reset stream on a server that is still there.
    pub(crate) fn connection_failed(&self, generation: u64, count: bool) {
        let mut state = self.state.lock();
        if state.generation != generation || state.channel.is_none() {
            // Already reported, or already replaced by a newer connection.
            return;
        }
        if count {
            self.open_breaker(&mut state);
        } else {
            state.channel = None;
        }
    }

    /// Resets the failure count and cool-off, closing the circuit.
    ///
    /// Call this after a successful RPC so the next connect can happen at once.
    pub fn reset(&mut self) {
        let mut state = self.state.lock();
        state.fail_count = 0;
        state.next_retry_at = None;
    }

    /// Drops the cached channel without touching backoff state.
    ///
    /// Use this for transient stream-level errors (for example h2 `RST_STREAM`
    /// or gRPC `Cancelled`), where the server is still reachable but one HTTP/2
    /// stream was reset. Unlike `record_failure()`, this neither increments
    /// `fail_count` nor starts a cool-off, so the next `get()` reconnects
    /// immediately.
    pub fn reset_channel(&mut self) {
        self.state.lock().channel = None;
    }

    /// Whether a connection is held.
    #[cfg(test)]
    pub(crate) fn is_connected(&self) -> bool {
        self.state.lock().channel.is_some()
    }

    /// Returns how long until the current cool-off ends, or zero if there is none.
    pub fn cooling_off_duration(&self) -> std::time::Duration {
        match self.state.lock().next_retry_at {
            Some(t) => t.saturating_duration_since(Instant::now()),
            None => std::time::Duration::ZERO,
        }
    }

    /// Connect timeout used when nothing else is configured.
    pub const DEFAULT_CONNECT_TIMEOUT: Duration = Duration::from_secs(5);
    /// Per-RPC timeout used when nothing else is configured.
    pub const DEFAULT_RPC_TIMEOUT: Duration = Duration::from_secs(70);

    /// Returns a connected `Channel`, respecting the cool-off.
    ///
    /// - If a channel is cached, returns it.
    /// - If the circuit is open, fails fast without trying to connect.
    /// - Otherwise connects, and caches the channel on success.
    ///
    /// # Errors
    ///
    /// Returns `Error::connection` while cooling off (the message gives the
    /// remaining time), or when the connect attempt fails. Returns
    /// `Error::configuration` if the server address or TLS settings are invalid.
    pub async fn get(&mut self) -> Result<Channel> {
        match self.connection(Breaker::Respect).await {
            Ok((channel, _)) => Ok(channel),
            Err(Unconnected::CoolingOff(remaining)) => {
                let fail_count = self.state.lock().fail_count;
                Err(Error::connection(format!(
                    "ChannelManager: cooling off for {}ms (fail_count={})",
                    remaining.as_millis(),
                    fail_count
                )))
            }
            Err(Unconnected::Failed(e)) => Err(e),
        }
    }

    /// A connected channel and its generation, connecting if there is none.
    ///
    /// Callers that find no channel share one connect: whoever gets there
    /// first connects, and the rest wait for it and take its outcome.
    pub(crate) async fn connection(
        &self,
        breaker: Breaker,
    ) -> std::result::Result<(Channel, u64), Unconnected> {
        let connects_seen = {
            let state = self.state.lock();
            if let Some(channel) = &state.channel {
                return Ok((channel.clone(), state.generation));
            }
            if let Some(remaining) = Self::held_back(&state, breaker) {
                return Err(Unconnected::CoolingOff(remaining));
            }
            state.connects
        };

        let _connecting = self.connecting.lock().await;
        {
            let state = self.state.lock();
            if let Some(channel) = &state.channel {
                return Ok((channel.clone(), state.generation));
            }
            // A connect finished while this caller waited for it and left no
            // channel. If it failed — the breaker says so — share that
            // failure rather than trying again at once. If it succeeded and
            // its channel has since been dropped, there is nothing to share:
            // connect again, once, below.
            if state.connects != connects_seen && Self::cool_off_left(&state).is_some() {
                return Err(match Self::held_back(&state, breaker) {
                    Some(remaining) => Unconnected::CoolingOff(remaining),
                    None => Unconnected::Failed(Error::connection(format!(
                        "Failed to connect to {}",
                        self.server_url
                    ))),
                });
            }
        }

        let endpoint = build_endpoint(
            &self.server_url,
            self.tls_config.as_ref(),
            self.connect_timeout,
            self.default_rpc_timeout,
        )
        .map_err(Unconnected::Failed)?;
        let connected = endpoint.connect().await;

        let mut state = self.state.lock();
        state.connects += 1;
        state.last_attempt_at = Some(Instant::now());
        match connected {
            Ok(channel) => {
                // Cache the channel and close the circuit.
                state.generation += 1;
                state.channel = Some(channel.clone());
                state.fail_count = 0;
                state.next_retry_at = None;
                Ok((channel, state.generation))
            }
            Err(e) => {
                // Open the circuit with backoff.
                self.open_breaker(&mut state);
                Err(Unconnected::Failed(Error::connection(format!(
                    "Failed to connect to {}: {} (next retry after {:?})",
                    self.server_url,
                    e,
                    state
                        .next_retry_at
                        .map(|t| t.saturating_duration_since(Instant::now()))
                        .unwrap_or_else(|| Duration::from_secs(0))
                ))))
            }
        }
    }

    /// How much longer `breaker` holds this caller back, if at all.
    fn held_back(state: &State, breaker: Breaker) -> Option<Duration> {
        let cool_off = Self::cool_off_left(state)?;
        match breaker {
            Breaker::Respect => Some(cool_off),
            Breaker::Ignore => None,
            Breaker::CapAt(cap) => {
                let since = state
                    .last_attempt_at
                    .map(|at| at.elapsed())
                    .unwrap_or(Duration::MAX);
                let until_probe = cap.saturating_sub(since);
                Some(cool_off.min(until_probe)).filter(|d| !d.is_zero())
            }
        }
    }

    fn cool_off_left(state: &State) -> Option<Duration> {
        state
            .next_retry_at
            .map(|t| t.saturating_duration_since(Instant::now()))
            .filter(|d| !d.is_zero())
    }

    /// Exponential backoff, capped at `max_backoff`.
    fn calculate_backoff(attempt: u32, initial: Duration, max_backoff: Duration) -> Duration {
        if attempt == 0 {
            return initial;
        }
        // Exponential: initial * 2^(attempt-1)
        let multiplier = 1u64 << (attempt.saturating_sub(1)).min(31); // cap shift to avoid overflow
        let millis = initial.as_millis() as u64;
        let backoff = Duration::from_millis(millis.saturating_mul(multiplier));
        std::cmp::min(backoff, max_backoff)
    }

    /// A small jitter in `0..jitter_ms`, so many clients do not reconnect in lockstep.
    fn small_jitter(jitter_ms: u64) -> Duration {
        // Derived from the wall clock's nanoseconds, which avoids an RNG dependency.
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or(Duration::from_secs(0));
        let v = (now.as_nanos() % jitter_ms as u128) as u64;
        Duration::from_millis(v)
    }
}

/// Builds the endpoint that every connection to the server goes through.
///
/// The driver and the pollers all build their endpoints here so they agree
/// about TLS. A poller that dialed with a plain
/// `Channel::from_shared(url).connect()` would reach a TLS server in
/// plaintext and fail at the handshake with a bare "transport error", just
/// after the driver had logged that it was connected.
///
/// # Errors
///
/// Returns `Error::configuration` if `server_url` is not a valid address or
/// the TLS settings are rejected.
pub fn build_endpoint(
    server_url: &str,
    tls: Option<&TlsConfig>,
    connect_timeout: Duration,
    rpc_timeout: Duration,
) -> Result<Endpoint> {
    let mut endpoint = Endpoint::from_shared(server_url.to_string())
        .map_err(|e| {
            Error::configuration(format!("Invalid server address '{}': {}", server_url, e))
        })?
        .connect_timeout(connect_timeout)
        .timeout(rpc_timeout);

    if let Some(tls) = tls {
        // A custom CA pins verification to a private authority. Without one,
        // use the system trust store rather than failing: that is what mTLS
        // against a publicly-trusted server needs, where only the client
        // identity below is custom.
        let mut tls_cfg = match tls.ca_cert {
            Some(ref ca) => tonic::transport::ClientTlsConfig::new()
                .ca_certificate(tonic::transport::Certificate::from_pem(ca)),
            None => tonic::transport::ClientTlsConfig::new().with_native_roots(),
        };

        if let (Some(ref cert), Some(ref key)) = (&tls.client_cert, &tls.client_key) {
            let identity = tonic::transport::Identity::from_pem(cert, key);
            tls_cfg = tls_cfg.identity(identity);
        }

        if let Some(ref domain) = tls.domain_name {
            tls_cfg = tls_cfg.domain_name(domain.clone());
        }

        endpoint = endpoint
            .tls_config(tls_cfg)
            .map_err(|e| Error::configuration(format!("TLS configuration error: {}", e)))?;
    }

    Ok(endpoint)
}

/// Connects to the server with the given TLS settings and the default timeouts.
///
/// Pollers call this to open their own channel. It shares the manager's code
/// path, so the two cannot disagree about TLS.
///
/// # Errors
///
/// Returns `Error::configuration` for an invalid address or TLS settings, and
/// `Error::connection` (naming the address) if the connect fails.
pub async fn connect_channel(server_url: &str, tls: Option<&TlsConfig>) -> Result<Channel> {
    build_endpoint(
        server_url,
        tls,
        ChannelManager::DEFAULT_CONNECT_TIMEOUT,
        ChannelManager::DEFAULT_RPC_TIMEOUT,
    )?
    .connect()
    .await
    .map_err(|e| Error::connection(format!("Failed to connect to {}: {}", server_url, e)))
}

#[cfg(test)]
mod connect_tests {
    use super::*;

    #[test]
    fn an_https_endpoint_with_tls_builds() {
        let tls = TlsConfig {
            ca_cert: None,
            client_cert: None,
            client_key: None,
            domain_name: None,
        };
        assert!(build_endpoint(
            "https://orchestrator.example:443",
            Some(&tls),
            Duration::from_secs(1),
            Duration::from_secs(1)
        )
        .is_ok());
    }

    #[test]
    fn a_bad_address_is_a_configuration_error_not_a_panic() {
        let err = build_endpoint(
            "not a url",
            None,
            Duration::from_secs(1),
            Duration::from_secs(1),
        )
        .expect_err("must fail");
        assert!(err.to_string().contains("Invalid server address"), "{err}");
    }

    #[tokio::test]
    async fn a_closed_port_fails_to_connect_with_the_address_in_the_message() {
        // Nothing listens on a fresh ephemeral port once the listener is dropped.
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        drop(listener);
        let url = format!("http://127.0.0.1:{port}");
        let err = connect_channel(&url, None).await.expect_err("must fail");
        assert!(err.to_string().contains(&url), "{err}");
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn initial_state_not_cooling_off() {
        let mgr = ChannelManager::new("http://localhost:50051");
        assert!(!mgr.is_cooling_off());
    }

    #[test]
    fn backoff_grows_and_caps() {
        let initial = Duration::from_secs(1);
        let max = Duration::from_secs(30);

        assert_eq!(
            ChannelManager::calculate_backoff(0, initial, max),
            Duration::from_secs(1)
        );
        assert_eq!(
            ChannelManager::calculate_backoff(1, initial, max),
            Duration::from_secs(1)
        );
        assert_eq!(
            ChannelManager::calculate_backoff(2, initial, max),
            Duration::from_secs(2)
        );
        assert_eq!(
            ChannelManager::calculate_backoff(3, initial, max),
            Duration::from_secs(4)
        );
        assert_eq!(
            ChannelManager::calculate_backoff(6, initial, max),
            Duration::from_secs(30)
        );
    }

    #[test]
    fn jitter_is_bounded() {
        let j = ChannelManager::small_jitter(250);
        assert!(j <= Duration::from_millis(250));
    }

    #[tokio::test]
    async fn record_failure_sets_cool_off() {
        let mut mgr = ChannelManager::new("http://localhost:50051");
        assert!(!mgr.is_cooling_off());
        mgr.record_failure();
        assert!(mgr.is_cooling_off());
    }

    /// A caller that waited while someone else's connect succeeded, and
    /// found that connection already dropped again, connects rather than
    /// being told the connect failed.
    #[tokio::test]
    async fn a_connect_that_worked_is_not_reported_as_failed() {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            while let Ok((socket, _)) = listener.accept().await {
                tokio::spawn(async move {
                    if let Ok(mut connection) = h2::server::handshake(socket).await {
                        while connection.accept().await.is_some() {}
                    }
                });
            }
        });
        let manager = std::sync::Arc::new(ChannelManager::new(format!("http://{addr}")));

        let connecting = manager.connecting.lock().await;
        let waiting = {
            let manager = std::sync::Arc::clone(&manager);
            tokio::spawn(async move { manager.connection(Breaker::Respect).await.is_ok() })
        };
        tokio::time::sleep(Duration::from_millis(100)).await;
        // Someone else's connect succeeded, and its channel was dropped by a
        // reset stream before the waiter looked.
        manager.state.lock().connects += 1;
        drop(connecting);

        assert!(waiting.await.unwrap(), "told a working connect had failed");
    }

    #[tokio::test]
    async fn reset_closes_circuit() {
        let mut mgr = ChannelManager::new("http://localhost:50051");
        mgr.record_failure();
        assert!(mgr.is_cooling_off());
        mgr.reset();
        assert!(!mgr.is_cooling_off());
    }
}
