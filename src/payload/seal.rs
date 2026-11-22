//! Payloads as the engine stores them when a codec is configured.
//!
//! The engine's messages carry a payload as bytes holding JSON, and the
//! engine stores JSON. A payload that has been through a codec (compressed,
//! encrypted) is neither, so it travels inside a small JSON envelope:
//!
//! ```json
//! {"metadata": {"encoding": "binary/encrypted", "encryption-key-id": "k1"},
//!  "data": "<base64>"}
//! ```
//!
//! [`PayloadSeal::seal`] writes that envelope and [`PayloadSeal::open`] reads
//! it back. The engine never does either: it holds no key, stores the
//! envelope as it arrived and returns it unchanged.
//!
//! Sealing is applied where a message leaves for the engine and opening
//! where one arrives, so everything between (the kernel, the language SDKs,
//! the user's code) sees plain payloads and nothing else has to know.
//!
//! A value that is not an envelope is returned by `open` as it is. Workflows
//! started before a codec was configured therefore stay readable after it is.

use std::collections::BTreeMap;
use std::sync::Arc;

use base64::{engine::general_purpose::STANDARD as BASE64, Engine};
use serde::{Deserialize, Serialize};

use super::codec::{CodecChain, PayloadCodec};
use super::Payload;
use crate::error::{Error, Result};

/// The JSON a sealed payload is stored as.
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Envelope {
    /// What was done to the data, and with which key.
    metadata: BTreeMap<String, String>,
    /// The encoded bytes, as standard base64.
    data: String,
}

impl Envelope {
    /// The envelope in `bytes`, if they hold one whose data is not plain
    /// JSON. Anything else (a user's value, however shaped) is `None`.
    fn read(bytes: &[u8]) -> Option<Self> {
        // Most payloads are not envelopes: tell before parsing.
        if bytes.first() != Some(&b'{') || !contains(bytes, b"\"encoding\"") {
            return None;
        }
        let envelope: Self = serde_json::from_slice(bytes).ok()?;
        let encoding = envelope.metadata.get("encoding")?;
        (!encoding.contains("json")).then_some(envelope)
    }
}

fn contains(haystack: &[u8], needle: &[u8]) -> bool {
    haystack
        .windows(needle.len())
        .any(|window| window == needle)
}

/// A codec chain applied to every payload on its way to the engine and
/// removed from every payload on its way back.
#[derive(Clone)]
pub struct PayloadSeal {
    chain: Arc<CodecChain>,
}

impl std::fmt::Debug for PayloadSeal {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PayloadSeal")
            .field("codecs", &self.chain.len())
            .finish()
    }
}

impl PayloadSeal {
    /// Seals with `chain`. Clients and workers that exchange payloads must
    /// be given the same chain.
    pub fn new(chain: CodecChain) -> Self {
        Self {
            chain: Arc::new(chain),
        }
    }

    /// `plain` as the engine is to store it.
    ///
    /// An empty value stays empty: it says only that there is no value, which
    /// the engine is told in other ways too.
    ///
    /// # Errors
    ///
    /// Fails if a codec fails. The plain value is never returned in place of
    /// a sealed one.
    pub fn seal(&self, plain: &[u8]) -> Result<Vec<u8>> {
        if plain.is_empty() || self.chain.is_empty() {
            return Ok(plain.to_vec());
        }
        let encoded = self.chain.encode(&Payload::json(plain.to_vec()))?;
        let mut metadata = BTreeMap::new();
        for (key, value) in encoded.metadata {
            // The content type describes the plain value, which the reader
            // restores; it would only tell the engine what is inside.
            if key == "content-type" {
                continue;
            }
            let value = String::from_utf8(value)
                .map_err(|_| Error::codec(format!("payload metadata `{key}` is not UTF-8 text")))?;
            metadata.insert(key, value);
        }
        serde_json::to_vec(&Envelope {
            metadata,
            data: BASE64.encode(encoded.data),
        })
        .map_err(|e| Error::codec(format!("writing a sealed payload: {e}")))
    }

    /// The plain value in `stored`, or `None` if `stored` is not sealed and
    /// is to be used as it is.
    ///
    /// # Errors
    ///
    /// Fails if `stored` is sealed and cannot be decoded: the wrong key, a
    /// key this chain lacks, or data that was altered.
    pub fn open(&self, stored: &[u8]) -> Result<Option<Vec<u8>>> {
        let Some(envelope) = Envelope::read(stored) else {
            return Ok(None);
        };
        let data = BASE64
            .decode(&envelope.data)
            .map_err(|e| Error::codec(format!("a sealed payload's data is not base64: {e}")))?;
        let metadata = envelope
            .metadata
            .into_iter()
            .map(|(key, value)| (key, value.into_bytes()))
            .collect();
        Ok(Some(self.chain.decode(&Payload::new(data, metadata))?.data))
    }
}

/// Seals `bytes` in place when a seal is configured.
pub(crate) fn seal(seal: Option<&PayloadSeal>, bytes: &mut Vec<u8>) -> Result<()> {
    if let Some(seal) = seal {
        *bytes = seal.seal(bytes)?;
    }
    Ok(())
}

/// Opens `bytes` in place if they are sealed.
///
/// # Errors
///
/// Fails if they are sealed and cannot be opened, which includes there being
/// no seal configured: handing the envelope on as if it were the value would
/// surface far from here as a value of the wrong shape.
pub(crate) fn open(seal: Option<&PayloadSeal>, bytes: &mut Vec<u8>) -> Result<()> {
    match seal {
        Some(seal) => {
            if let Some(plain) = seal.open(bytes)? {
                *bytes = plain;
            }
            Ok(())
        }
        None => match Envelope::read(bytes) {
            None => Ok(()),
            Some(envelope) => Err(Error::codec(format!(
                "a payload arrived encoded as `{}` and no payload codec is configured to read it",
                envelope.metadata.get("encoding").map_or("", String::as_str)
            ))),
        },
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::payload::codec::{EncryptionPayloadCodec, GzipPayloadCodec};

    fn encrypting(key: u8) -> PayloadSeal {
        PayloadSeal::new(
            CodecChain::new().with_codec(EncryptionPayloadCodec::new(&[key; 32]).with_key_id("k1")),
        )
    }

    #[test]
    fn a_sealed_payload_is_json_that_does_not_hold_the_value() {
        let sealed = encrypting(1).seal(br#"{"card":"4242"}"#).unwrap();
        let json: serde_json::Value = serde_json::from_slice(&sealed).unwrap();
        assert_eq!(json["metadata"]["encoding"], "binary/encrypted");
        assert_eq!(json["metadata"]["encryption-key-id"], "k1");
        assert!(json["data"].is_string());
        assert!(!contains(&sealed, b"4242"));
        assert_eq!(json.as_object().unwrap().len(), 2);
        assert_eq!(json["metadata"].as_object().unwrap().len(), 2);
    }

    #[test]
    fn what_is_sealed_opens_to_what_it_was() {
        let seal = encrypting(1);
        for plain in [
            &br#"{"card":"4242"}"#[..],
            b"null",
            b"\"text\"",
            b"[1,2]",
            b"17",
        ] {
            let sealed = seal.seal(plain).unwrap();
            assert_eq!(seal.open(&sealed).unwrap().as_deref(), Some(plain));
        }
    }

    #[test]
    fn it_opens_after_the_engine_has_rewritten_the_json() {
        // A JSON column returns the same value with its keys reordered and
        // its spacing changed.
        let seal = encrypting(1);
        let sealed = seal.seal(b"{\"a\":1}").unwrap();
        let value: serde_json::Value = serde_json::from_slice(&sealed).unwrap();
        let rewritten = format!(
            "{{\"data\": {}, \"metadata\": {}}}",
            value["data"], value["metadata"]
        );
        assert_eq!(
            seal.open(rewritten.as_bytes()).unwrap().as_deref(),
            Some(&b"{\"a\":1}"[..])
        );
    }

    #[test]
    fn the_wrong_key_does_not_open_it() {
        let sealed = encrypting(1).seal(b"{\"a\":1}").unwrap();
        assert!(encrypting(2).open(&sealed).is_err());
    }

    #[test]
    fn a_plain_value_is_left_alone() {
        let seal = encrypting(1);
        for plain in [
            &b"{\"a\":1}"[..],
            b"",
            b"null",
            // Shaped like an envelope, and JSON: a user's own value.
            br#"{"metadata":{"encoding":"json"},"data":"e30="}"#,
            br#"{"metadata":{},"data":"e30="}"#,
            br#"{"metadata":{"encoding":"binary/encrypted"},"data":"e30=","more":1}"#,
        ] {
            assert_eq!(
                seal.open(plain).unwrap(),
                None,
                "{}",
                String::from_utf8_lossy(plain)
            );
            let mut bytes = plain.to_vec();
            open(None, &mut bytes).unwrap();
            assert_eq!(bytes, plain);
        }
    }

    #[test]
    fn an_empty_value_stays_empty() {
        assert!(encrypting(1).seal(b"").unwrap().is_empty());
    }

    #[test]
    fn a_sealed_payload_with_no_seal_configured_is_an_error_that_says_so() {
        let mut sealed = encrypting(1).seal(b"{\"a\":1}").unwrap();
        let error = open(None, &mut sealed).unwrap_err().to_string();
        assert!(error.contains("binary/encrypted"), "{error}");
        assert!(error.contains("no payload codec"), "{error}");
    }

    #[test]
    fn compression_and_encryption_stack() {
        let seal = PayloadSeal::new(
            CodecChain::new()
                .with_codec(GzipPayloadCodec::default())
                .with_codec(EncryptionPayloadCodec::new(&[3; 32])),
        );
        let plain = vec![b'a'; 4096];
        let sealed = seal.seal(&plain).unwrap();
        assert!(sealed.len() < plain.len());
        assert_eq!(seal.open(&sealed).unwrap().as_deref(), Some(&plain[..]));
    }

    #[test]
    fn the_helpers_do_nothing_without_a_seal() {
        let mut bytes = b"{\"a\":1}".to_vec();
        seal(None, &mut bytes).unwrap();
        assert_eq!(bytes, b"{\"a\":1}");
    }
}
