//! # Gzip Payload Codec
//!
//! Gzip compression and decompression for payloads.
//!
//! [`GzipPayloadCodec`] compresses payload data, which cuts storage and
//! transfer size for large payloads. Text and JSON typically shrink by 60-90%.
//! It works on the bytes any data converter produces, and gzip is readable
//! by every language and platform.
//!
//! ## Usage
//!
//! ```rust,ignore
//! use orcher_sdk_core::codec::{PayloadCodec, GzipPayloadCodec};
//! use orcher_sdk_core::converter::{DataConverter, JsonPayloadConverter};
//!
//! let converter = JsonPayloadConverter::default();
//! let codec = GzipPayloadCodec::default();
//!
//! // A large JSON document.
//! let data = serde_json::json!({
//!     "users": vec![
//!         {"id": 1, "name": "Alice", "email": "alice@example.com"},
//!         {"id": 2, "name": "Bob", "email": "bob@example.com"},
//!         // ... many more users
//!     ]
//! });
//!
//! let payload = converter.to_payload(&data)?;
//! println!("Original size: {} bytes", payload.data.len());
//!
//! let compressed = codec.encode(&payload)?;
//! println!("Compressed size: {} bytes", compressed.data.len());
//! println!("Compression ratio: {:.1}%",
//!     100.0 * compressed.data.len() as f64 / payload.data.len() as f64);
//!
//! let decompressed = codec.decode(&compressed)?;
//! assert_eq!(decompressed.data, payload.data);
//! # Ok::<(), orcher_sdk_core::error::Error>(())
//! ```
//!
//! ## Compression Levels
//!
//! ```rust,ignore
//! use orcher_sdk_core::codec::GzipPayloadCodec;
//! use flate2::Compression;
//!
//! // Fastest compression (level 1).
//! let fast_codec = GzipPayloadCodec::new(Compression::fast());
//!
//! // Smallest output (level 9).
//! let best_codec = GzipPayloadCodec::new(Compression::best());
//!
//! // Balanced default (level 6).
//! let default_codec = GzipPayloadCodec::default();
//! ```

use crate::codec::PayloadCodec;
use crate::error::{Error, Result};
use crate::payload::Payload;
use flate2::read::{GzDecoder, GzEncoder};
use flate2::Compression;
use std::io::Read;

/// Gzip compression codec for payloads.
///
/// Compresses `data` in the gzip format (RFC 1952) and marks the payload's
/// `encoding` as gzip. The codec is `Send + Sync` and can be shared across
/// threads.
///
/// # Examples
///
/// ```rust,ignore
/// use orcher_sdk_core::codec::{PayloadCodec, GzipPayloadCodec};
/// use orcher_sdk_core::payload::Payload;
///
/// let codec = GzipPayloadCodec::default();
/// let payload = Payload::json(b"Hello, World!".to_vec());
///
/// let compressed = codec.encode(&payload)?;
/// let decompressed = codec.decode(&compressed)?;
///
/// assert_eq!(decompressed.data, payload.data);
/// # Ok::<(), orcher_sdk_core::error::Error>(())
/// ```
#[derive(Debug, Clone)]
pub struct GzipPayloadCodec {
    compression_level: Compression,
}

impl GzipPayloadCodec {
    /// Creates a codec with the given compression level.
    ///
    /// Levels run from 0 to 9: 0 stores data uncompressed, 1 is fastest with
    /// the largest output, 6 is the balanced default, and 9 gives the
    /// smallest output at the highest CPU cost.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use orcher_sdk_core::codec::GzipPayloadCodec;
    /// use flate2::Compression;
    ///
    /// let fast = GzipPayloadCodec::new(Compression::fast());
    /// let best = GzipPayloadCodec::new(Compression::best());
    /// let none = GzipPayloadCodec::new(Compression::none());
    /// ```
    pub fn new(compression_level: Compression) -> Self {
        Self { compression_level }
    }

    /// Creates a codec with the fastest compression (level 1).
    ///
    /// Suits latency-sensitive workflows, large payloads where speed matters
    /// more than ratio, and CPU-constrained workers.
    pub fn fast() -> Self {
        Self::new(Compression::fast())
    }

    /// Creates a codec with the smallest output (level 9).
    ///
    /// Suits payloads that are kept for a long time or rarely read, and
    /// bandwidth-constrained networks.
    pub fn best() -> Self {
        Self::new(Compression::best())
    }

    /// Returns the compression level (0-9).
    pub fn compression_level(&self) -> u32 {
        self.compression_level.level()
    }
}

impl Default for GzipPayloadCodec {
    /// Creates a codec with the default compression level (6), a balance
    /// between speed and output size.
    fn default() -> Self {
        Self::new(Compression::default())
    }
}

impl PayloadCodec for GzipPayloadCodec {
    fn encode(&self, payload: &Payload) -> Result<Payload> {
        let mut encoder = GzEncoder::new(&payload.data[..], self.compression_level);
        let mut compressed_data = Vec::new();

        encoder
            .read_to_end(&mut compressed_data)
            .map_err(|e| Error::codec(format!("Gzip compression failed: {}", e)))?;

        let mut encoded = Payload::new(compressed_data, payload.metadata.clone());

        // A payload already transformed by a codec ("binary/...") gets "+gzip"
        // appended so the chain can be unwound in reverse. A base encoding
        // such as "json" is replaced by "binary/gzip"; decoding then removes
        // the key instead of restoring the base encoding.
        let current_encoding = payload.encoding().unwrap_or_else(|| "binary".to_string());
        let new_encoding = if current_encoding.starts_with("binary/") {
            format!("{}+gzip", current_encoding)
        } else {
            "binary/gzip".to_string()
        };
        encoded.set_metadata_string("encoding", &new_encoding);

        Ok(encoded)
    }

    fn decode(&self, payload: &Payload) -> Result<Payload> {
        let encoding = payload.encoding().unwrap_or_default();
        if !encoding.contains("gzip") {
            return Err(Error::codec(format!(
                "Expected gzip encoding, got: {}. Cannot decode non-gzip payload.",
                encoding
            )));
        }

        let mut decoder = GzDecoder::new(&payload.data[..]);
        let mut decompressed_data = Vec::new();

        decoder.read_to_end(&mut decompressed_data).map_err(|e| {
            Error::codec(format!(
                "Gzip decompression failed: {}. Data may be corrupted.",
                e
            ))
        })?;

        let mut decoded = Payload::new(decompressed_data, payload.metadata.clone());

        // Undo the encoding change made by `encode`.
        let original_encoding = if encoding == "binary/gzip" {
            // Gzip was the only codec applied.
            None
        } else if let Some(stripped) = encoding.strip_suffix("+gzip") {
            // "binary/encrypted+gzip" -> "binary/encrypted"
            Some(stripped.to_string())
        } else {
            // Gzip was not the last codec applied; leave the encoding as-is.
            Some(encoding.clone())
        };

        if let Some(enc) = original_encoding {
            decoded.set_metadata_string("encoding", &enc);
        } else {
            decoded.remove_metadata("encoding");
        }

        Ok(decoded)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::payload::Payload;

    #[test]
    fn test_gzip_encode_decode_roundtrip() {
        let codec = GzipPayloadCodec::default();
        let original = Payload::json(b"Hello, World! This is a test payload.".to_vec());

        let encoded = codec.encode(&original).unwrap();
        assert_eq!(encoded.encoding(), Some("binary/gzip".to_string()));

        let decoded = codec.decode(&encoded).unwrap();
        assert_eq!(decoded.data, original.data);
        assert_eq!(decoded.encoding(), None); // Decoding removes the gzip encoding.
    }

    #[test]
    fn test_gzip_compression_reduces_size() {
        let codec = GzipPayloadCodec::default();

        // A repeated pattern compresses well.
        let repeated_data = b"Hello, World! ".repeat(100);
        let original = Payload::json(repeated_data.to_vec());

        let encoded = codec.encode(&original).unwrap();

        assert!(
            encoded.data.len() < original.data.len(),
            "Compressed size {} should be less than original size {}",
            encoded.data.len(),
            original.data.len()
        );

        let decoded = codec.decode(&encoded).unwrap();
        assert_eq!(decoded.data, original.data);
    }

    #[test]
    fn test_gzip_compression_levels() {
        let original = Payload::json(b"Test data for compression level comparison.".repeat(10));

        let fast_codec = GzipPayloadCodec::fast();
        let default_codec = GzipPayloadCodec::default();
        let best_codec = GzipPayloadCodec::best();

        let fast_encoded = fast_codec.encode(&original).unwrap();
        let default_encoded = default_codec.encode(&original).unwrap();
        let best_encoded = best_codec.encode(&original).unwrap();

        // Higher levels never produce larger output on this input.
        assert!(
            best_encoded.data.len() <= default_encoded.data.len(),
            "Best compression should be <= default"
        );
        assert!(
            default_encoded.data.len() <= fast_encoded.data.len(),
            "Default compression should be <= fast"
        );

        // Every level decompresses to the original.
        assert_eq!(
            fast_codec.decode(&fast_encoded).unwrap().data,
            original.data
        );
        assert_eq!(
            default_codec.decode(&default_encoded).unwrap().data,
            original.data
        );
        assert_eq!(
            best_codec.decode(&best_encoded).unwrap().data,
            original.data
        );
    }

    #[test]
    fn test_gzip_decode_requires_gzip_encoding() {
        let codec = GzipPayloadCodec::default();

        let uncompressed = Payload::json(b"Not compressed".to_vec());

        let result = codec.decode(&uncompressed);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Expected gzip encoding"));
    }

    #[test]
    fn test_gzip_corrupted_data() {
        let codec = GzipPayloadCodec::default();

        // Marked as gzip, but the bytes are not a gzip stream.
        let mut corrupted = Payload::new(vec![1, 2, 3, 4, 5], Default::default());
        corrupted.set_metadata_string("encoding", "binary/gzip");

        let result = codec.decode(&corrupted);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Gzip decompression failed"));
    }

    #[test]
    fn test_gzip_preserves_metadata() {
        let codec = GzipPayloadCodec::default();

        let mut original = Payload::json(b"Test data".to_vec());
        original.set_metadata_string("custom-key", "custom-value");
        original.set_metadata_string("version", "1.0");

        let encoded = codec.encode(&original).unwrap();

        // Metadata other than `encoding` passes through unchanged.
        assert_eq!(
            encoded.get_metadata_string("custom-key"),
            Some("custom-value".to_string())
        );
        assert_eq!(
            encoded.get_metadata_string("version"),
            Some("1.0".to_string())
        );

        let decoded = codec.decode(&encoded).unwrap();

        // ...and survives the roundtrip.
        assert_eq!(
            decoded.get_metadata_string("custom-key"),
            Some("custom-value".to_string())
        );
        assert_eq!(
            decoded.get_metadata_string("version"),
            Some("1.0".to_string())
        );
    }

    #[test]
    fn test_gzip_chained_encoding() {
        let codec = GzipPayloadCodec::default();

        // A payload that another codec (encryption) has already transformed.
        let mut encrypted_payload =
            Payload::new(b"fake encrypted data".to_vec(), Default::default());
        encrypted_payload.set_metadata_string("encoding", "binary/encrypted");

        let compressed = codec.encode(&encrypted_payload).unwrap();
        assert_eq!(
            compressed.encoding(),
            Some("binary/encrypted+gzip".to_string())
        );

        // Decompressing restores the encryption encoding.
        let decompressed = codec.decode(&compressed).unwrap();
        assert_eq!(
            decompressed.encoding(),
            Some("binary/encrypted".to_string())
        );
        assert_eq!(decompressed.data, encrypted_payload.data);
    }

    #[test]
    fn test_gzip_empty_payload() {
        let codec = GzipPayloadCodec::default();
        let empty = Payload::json(Vec::new());

        let encoded = codec.encode(&empty).unwrap();
        let decoded = codec.decode(&encoded).unwrap();

        assert_eq!(decoded.data, empty.data);
    }

    #[test]
    fn test_gzip_large_payload() {
        let codec = GzipPayloadCodec::default();

        // 1 MiB of a single repeated byte.
        let large_data = vec![b'A'; 1024 * 1024];
        let large_payload = Payload::json(large_data.clone());

        let encoded = codec.encode(&large_payload).unwrap();

        // Compresses to under 1% of its size.
        assert!(
            encoded.data.len() < large_payload.data.len() / 100,
            "1MB of repeated data should compress to < 10KB"
        );

        let decoded = codec.decode(&encoded).unwrap();
        assert_eq!(decoded.data, large_data);
    }
}
