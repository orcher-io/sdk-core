//! Payload codec trait and implementations for compression and encryption.
//!
//! A [`PayloadCodec`] transforms payload bytes before they leave the process
//! and reverses the transformation when they come back:
//! - [`GzipPayloadCodec`] - gzip compression
//! - [`EncryptionPayloadCodec`] - AES-256-GCM encryption
//! - [`CodecChain`] - several codecs composed into one
//!
//! A chain encodes with its codecs in the order they were added and decodes in
//! reverse order.

use crate::error::Result;
use crate::payload::Payload;

pub mod encryption;
pub mod gzip;

pub use encryption::EncryptionPayloadCodec;
pub use gzip::GzipPayloadCodec;

/// A reversible transformation of payload bytes.
///
/// Typical codecs compress, encrypt, or re-encode payload data. Implement
/// this trait to add your own.
///
/// # Contract
///
/// - **Reversible**: `decode(encode(payload))` returns the original `data`.
/// - **Metadata passes through**: metadata other than `encoding` is carried
///   over unchanged.
/// - **Composable**: codecs can be stacked in a [`CodecChain`], so `decode`
///   must accept the output of its own `encode` even when other codecs have
///   been applied before it.
/// - **Descriptive errors**: failures say which codec failed and why.
/// - **Thread-safe**: codecs must be `Send + Sync`.
///
/// # Encoding Metadata
///
/// A codec records what it did in the `encoding` metadata field so that a
/// reader knows how to decode the bytes. The built-in codecs use
/// `binary/<name>` (`binary/gzip`, `binary/encrypted`) and, when stacked,
/// append `+<name>` in the order applied: `binary/encrypted+gzip` means
/// encrypted first, then compressed. Decoding the last codec strips its
/// suffix; decoding the only codec removes the field.
pub trait PayloadCodec: Send + Sync {
    /// Applies the transformation, returning a new payload.
    ///
    /// The result carries the transformed data and an updated `encoding`
    /// field; the input is left untouched.
    ///
    /// # Errors
    ///
    /// Returns an error if the transformation fails.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use orcher_sdk_core::codec::{PayloadCodec, GzipPayloadCodec};
    ///
    /// let codec = GzipPayloadCodec::default();
    /// let original = Payload::json(br#"{"key":"value"}"#.to_vec());
    ///
    /// let encoded = codec.encode(&original)?;
    /// assert!(encoded.data.len() < original.data.len());
    /// assert_eq!(encoded.encoding(), Some("binary/gzip".to_string()));
    /// # Ok::<(), orcher_sdk_core::error::Error>(())
    /// ```
    fn encode(&self, payload: &Payload) -> Result<Payload>;

    /// Reverses the transformation, returning a new payload.
    ///
    /// The result carries the original data and the `encoding` field as it
    /// was before this codec's `encode`.
    ///
    /// # Errors
    ///
    /// Returns an error if the payload was not encoded by this codec, or if
    /// the data is corrupted or has been tampered with.
    ///
    /// # Examples
    ///
    /// ```rust,ignore
    /// use orcher_sdk_core::codec::{PayloadCodec, GzipPayloadCodec};
    ///
    /// let codec = GzipPayloadCodec::default();
    /// let original = Payload::json(br#"{"key":"value"}"#.to_vec());
    ///
    /// let encoded = codec.encode(&original)?;
    /// let decoded = codec.decode(&encoded)?;
    ///
    /// assert_eq!(decoded.data, original.data);
    /// # Ok::<(), orcher_sdk_core::error::Error>(())
    /// ```
    fn decode(&self, payload: &Payload) -> Result<Payload>;
}

/// Several codecs composed into one.
///
/// Encoding runs the codecs in the order they were added; decoding runs them
/// in reverse, so each codec decodes exactly what it encoded. An empty chain
/// returns payloads unchanged.
///
/// # Examples
///
/// ```rust,ignore
/// use orcher_sdk_core::codec::{CodecChain, GzipPayloadCodec, EncryptionPayloadCodec};
///
/// let chain = CodecChain::new()
///     .with_codec(EncryptionPayloadCodec::new(b"32-byte-secret-key-for-aes-256!!"))
///     .with_codec(GzipPayloadCodec::default());
///
/// // Encode: original -> encrypted -> compressed
/// let encoded = chain.encode(&payload)?;
///
/// // Decode: compressed -> encrypted -> original
/// let decoded = chain.decode(&encoded)?;
/// # Ok::<(), orcher_sdk_core::error::Error>(())
/// ```
pub struct CodecChain {
    codecs: Vec<Box<dyn PayloadCodec>>,
}

impl std::fmt::Debug for CodecChain {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CodecChain")
            .field("codecs_count", &self.codecs.len())
            .finish()
    }
}

impl CodecChain {
    /// Creates an empty chain.
    pub fn new() -> Self {
        Self { codecs: Vec::new() }
    }

    /// Appends a codec to the chain.
    ///
    /// Codecs encode in the order they are added and decode in reverse order.
    pub fn with_codec<C: PayloadCodec + 'static>(mut self, codec: C) -> Self {
        self.codecs.push(Box::new(codec));
        self
    }

    /// Returns the number of codecs in the chain.
    pub fn len(&self) -> usize {
        self.codecs.len()
    }

    /// Returns `true` if the chain has no codecs.
    pub fn is_empty(&self) -> bool {
        self.codecs.is_empty()
    }
}

impl Default for CodecChain {
    fn default() -> Self {
        Self::new()
    }
}

impl PayloadCodec for CodecChain {
    fn encode(&self, payload: &Payload) -> Result<Payload> {
        let mut current = payload.clone();

        for codec in &self.codecs {
            current = codec.encode(&current)?;
        }

        Ok(current)
    }

    fn decode(&self, payload: &Payload) -> Result<Payload> {
        let mut current = payload.clone();

        // Reverse order: the last codec applied is the first undone.
        for codec in self.codecs.iter().rev() {
            current = codec.decode(&current)?;
        }

        Ok(current)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::payload::Payload;

    /// Reverses the bytes; its own inverse, which makes chain order easy to check.
    struct ReverseCodec;

    impl PayloadCodec for ReverseCodec {
        fn encode(&self, payload: &Payload) -> Result<Payload> {
            let mut reversed = payload.data.clone();
            reversed.reverse();

            let mut encoded = Payload::new(reversed, payload.metadata.clone());
            encoded.set_metadata_string("encoding", "binary/reversed");

            Ok(encoded)
        }

        fn decode(&self, payload: &Payload) -> Result<Payload> {
            let mut reversed = payload.data.clone();
            reversed.reverse();

            let mut decoded = Payload::new(reversed, payload.metadata.clone());
            decoded.remove_metadata("encoding");

            Ok(decoded)
        }
    }

    #[test]
    fn test_codec_encode_decode_roundtrip() {
        let codec = ReverseCodec;
        let original = Payload::json(b"hello world".to_vec());

        let encoded = codec.encode(&original).unwrap();
        assert_eq!(encoded.data, b"dlrow olleh".to_vec());
        assert_eq!(encoded.encoding(), Some("binary/reversed".to_string()));

        let decoded = codec.decode(&encoded).unwrap();
        assert_eq!(decoded.data, original.data);
    }

    #[test]
    fn test_codec_chain_empty() {
        let chain = CodecChain::new();
        assert_eq!(chain.len(), 0);
        assert!(chain.is_empty());

        let payload = Payload::json(b"test".to_vec());
        let encoded = chain.encode(&payload).unwrap();
        assert_eq!(encoded.data, payload.data); // An empty chain changes nothing.
    }

    #[test]
    fn test_codec_chain_single() {
        let chain = CodecChain::new().with_codec(ReverseCodec);
        assert_eq!(chain.len(), 1);

        let payload = Payload::json(b"hello".to_vec());
        let encoded = chain.encode(&payload).unwrap();
        assert_eq!(encoded.data, b"olleh".to_vec());

        let decoded = chain.decode(&encoded).unwrap();
        assert_eq!(decoded.data, payload.data);
    }

    #[test]
    fn test_codec_chain_multiple() {
        // Reversing twice gives back the original bytes.
        let chain = CodecChain::new()
            .with_codec(ReverseCodec)
            .with_codec(ReverseCodec);
        assert_eq!(chain.len(), 2);

        let payload = Payload::json(b"hello".to_vec());
        let encoded = chain.encode(&payload).unwrap();
        assert_eq!(encoded.data, b"hello".to_vec());

        let decoded = chain.decode(&encoded).unwrap();
        assert_eq!(decoded.data, payload.data);
    }
}
