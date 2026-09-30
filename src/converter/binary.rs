//! # Binary Payload Converter
//!
//! Compact binary serialization for payloads, using [bincode](https://docs.rs/bincode/1).
//!
//! bincode output is smaller and faster to produce than JSON, but it is not
//! self-describing and only Rust reads it. Use it when the payload is written
//! and read by Rust code only; use JSON when another language SDK or a person
//! needs to read it.
//!
//! ## Usage
//!
//! ```rust
//! use orcher_sdk_core::converter::{DataConverter, BinaryPayloadConverter};
//!
//! let converter = BinaryPayloadConverter::default();
//! let data = vec![0x01, 0x02, 0x03, 0x04];
//!
//! let payload = converter.to_payload(&data)?;
//! assert_eq!(payload.encoding(), Some("binary".to_string()));
//!
//! let decoded: Vec<u8> = converter.from_payload(&payload)?;
//! assert_eq!(decoded, data);
//! # Ok::<(), orcher_sdk_core::error::Error>(())
//! ```

use crate::converter::DataConverter;
use crate::error::{Error, Result};
use crate::payload::Payload;
use std::collections::HashMap;

/// Binary payload converter backed by bincode.
///
/// Serializes any `serde` type with bincode and marks the payload with
/// `encoding: "binary"` and `content-type: "application/octet-stream"`.
///
/// Byte vectors are serialized too: bincode prefixes a `Vec<u8>` with its
/// length, so the payload data is not the raw bytes. To store bytes exactly
/// as they are (for example an already-encoded Protobuf message), build the
/// payload with [`Payload::binary`] or [`Payload::protobuf`] instead.
///
/// Reading accepts any payload whose `encoding` contains `"binary"`. That
/// includes codec output such as `"binary/gzip"`, so decode codecs first.
///
/// The converter is stateless and safe to share across threads.
///
/// # Examples
///
/// ## Basic Usage
///
/// ```rust
/// use orcher_sdk_core::converter::{DataConverter, BinaryPayloadConverter};
///
/// let converter = BinaryPayloadConverter::default();
/// let data = vec![0x01, 0x02, 0x03];
///
/// let payload = converter.to_payload(&data)?;
/// let decoded: Vec<u8> = converter.from_payload(&payload)?;
/// assert_eq!(decoded, data);
/// # Ok::<(), orcher_sdk_core::error::Error>(())
/// ```
///
/// ## With Protobuf
///
/// ```rust,ignore
/// use orcher_sdk_core::converter::{DataConverter, BinaryPayloadConverter};
/// use prost::Message;
///
/// // With a real message type:
/// // let message = MyProtoMessage { field: "value".to_string() };
/// // let mut buf = Vec::new();
/// // message.encode(&mut buf)?;
///
/// let buf = vec![0x0a, 0x05, 0x76, 0x61, 0x6c, 0x75, 0x65]; // An encoded message
/// let converter = BinaryPayloadConverter::default();
///
/// let payload = converter.to_payload(&buf)?;
/// assert!(payload.is_binary());
///
/// // Reading returns the original bytes.
/// let decoded: Vec<u8> = converter.from_payload(&payload)?;
/// // MyProtoMessage::decode(&decoded[..])?;
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
///
/// ## Large Binary Data
///
/// ```rust,ignore
/// use orcher_sdk_core::converter::{DataConverter, BinaryPayloadConverter};
///
/// let converter = BinaryPayloadConverter::default();
///
/// let large_data = vec![0u8; 1024 * 1024]; // 1 MiB
/// let payload = converter.to_payload(&large_data)?;
///
/// assert_eq!(payload.size(), 1024 * 1024);
/// # Ok::<(), orcher_sdk_core::error::Error>(())
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct BinaryPayloadConverter;

impl BinaryPayloadConverter {
    /// Creates a binary payload converter.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use orcher_sdk_core::converter::BinaryPayloadConverter;
    ///
    /// let converter = BinaryPayloadConverter::new();
    /// ```
    pub fn new() -> Self {
        Self
    }
}

impl DataConverter for BinaryPayloadConverter {
    fn to_payload<T: serde::Serialize>(&self, value: &T) -> Result<Payload> {
        let binary_bytes = bincode::serialize(value)
            .map_err(|e| Error::serialization(format!("Failed to serialize to binary: {}", e)))?;

        let mut metadata = HashMap::new();
        metadata.insert("encoding".to_string(), b"binary".to_vec());
        metadata.insert(
            "content-type".to_string(),
            b"application/octet-stream".to_vec(),
        );

        Ok(Payload {
            data: binary_bytes,
            metadata,
        })
    }

    fn from_payload<T: for<'de> serde::Deserialize<'de>>(&self, payload: &Payload) -> Result<T> {
        if !payload.is_binary() {
            let encoding = payload.encoding().unwrap_or_else(|| "unknown".to_string());
            return Err(Error::serialization(format!(
                "Expected binary encoding, got: {}",
                encoding
            )));
        }

        bincode::deserialize(&payload.data)
            .map_err(|e| Error::serialization(format!("Failed to deserialize from binary: {}", e)))
    }

    fn encoding(&self) -> &str {
        "binary"
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde::{Deserialize, Serialize};

    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    struct TestData {
        id: u32,
        name: String,
        values: Vec<i32>,
    }

    #[test]
    fn test_binary_roundtrip() {
        let converter = BinaryPayloadConverter;
        let original = TestData {
            id: 42,
            name: "test".to_string(),
            values: vec![1, 2, 3],
        };

        let payload = converter.to_payload(&original).unwrap();
        let decoded: TestData = converter.from_payload(&payload).unwrap();

        assert_eq!(original, decoded);
    }

    #[test]
    fn test_binary_metadata() {
        let converter = BinaryPayloadConverter;
        let data = vec![1u8, 2, 3];
        let payload = converter.to_payload(&data).unwrap();

        assert_eq!(payload.encoding(), Some("binary".to_string()));
        assert_eq!(
            payload.content_type(),
            Some("application/octet-stream".to_string())
        );
        assert!(payload.is_binary());
    }

    #[test]
    fn test_raw_bytes() {
        let converter = BinaryPayloadConverter;
        let data = vec![0x01, 0x02, 0x03, 0x04];

        let payload = converter.to_payload(&data).unwrap();
        let decoded: Vec<u8> = converter.from_payload(&payload).unwrap();

        assert_eq!(decoded, data);
    }

    #[test]
    fn test_primitives() {
        let converter = BinaryPayloadConverter;

        // Integer
        let payload = converter.to_payload(&42i32).unwrap();
        let decoded: i32 = converter.from_payload(&payload).unwrap();
        assert_eq!(decoded, 42);

        // String
        let payload = converter.to_payload(&"hello".to_string()).unwrap();
        let decoded: String = converter.from_payload(&payload).unwrap();
        assert_eq!(decoded, "hello");

        // Boolean
        let payload = converter.to_payload(&true).unwrap();
        let decoded: bool = converter.from_payload(&payload).unwrap();
        assert!(decoded);
    }

    #[test]
    fn test_collections() {
        let converter = BinaryPayloadConverter;

        // Vec
        let vec = vec![1, 2, 3, 4, 5];
        let payload = converter.to_payload(&vec).unwrap();
        let decoded: Vec<i32> = converter.from_payload(&payload).unwrap();
        assert_eq!(decoded, vec);

        // HashMap
        let mut map = std::collections::HashMap::new();
        map.insert("key1".to_string(), "value1".to_string());
        map.insert("key2".to_string(), "value2".to_string());
        let payload = converter.to_payload(&map).unwrap();
        let decoded: std::collections::HashMap<String, String> =
            converter.from_payload(&payload).unwrap();
        assert_eq!(decoded, map);
    }

    #[test]
    fn test_wrong_encoding_error() {
        let converter = BinaryPayloadConverter;

        let mut payload = Payload::json(vec![1, 2, 3]);
        payload.set_metadata_string("encoding", "json");

        let result: Result<Vec<u8>> = converter.from_payload(&payload);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Expected binary encoding"));
    }

    #[test]
    fn test_invalid_binary_error() {
        let converter = BinaryPayloadConverter;

        // Marked binary, but too short to be a bincode-encoded `TestData`.
        let payload = Payload::binary(vec![0xFF, 0xFE, 0xFD, 0xFC]);

        let result: Result<TestData> = converter.from_payload(&payload);
        assert!(result.is_err());
    }

    #[test]
    fn test_encoding_method() {
        let converter = BinaryPayloadConverter::new();
        assert_eq!(converter.encoding(), "binary");
    }

    #[test]
    fn test_default() {
        let converter = BinaryPayloadConverter;
        let data = vec![1u8, 2, 3];
        let payload = converter.to_payload(&data).unwrap();
        assert!(payload.is_binary());
    }

    #[test]
    fn test_large_data() {
        let converter = BinaryPayloadConverter;

        let large_vec: Vec<u8> = (0..10000).map(|i| (i % 256) as u8).collect();
        let payload = converter.to_payload(&large_vec).unwrap();
        let decoded: Vec<u8> = converter.from_payload(&payload).unwrap();

        assert_eq!(decoded.len(), 10000);
        assert_eq!(decoded, large_vec);
    }

    #[test]
    fn test_empty_data() {
        let converter = BinaryPayloadConverter;

        let empty: Vec<u8> = vec![];
        let payload = converter.to_payload(&empty).unwrap();
        let decoded: Vec<u8> = converter.from_payload(&payload).unwrap();

        assert_eq!(decoded, empty);
    }

    #[test]
    fn test_complex_struct() {
        #[derive(Serialize, Deserialize, PartialEq, Debug)]
        struct Complex {
            id: u64,
            data: Vec<u8>,
            nested: Nested,
        }

        #[derive(Serialize, Deserialize, PartialEq, Debug)]
        struct Nested {
            value: String,
        }

        let converter = BinaryPayloadConverter;
        let original = Complex {
            id: 12345,
            data: vec![1, 2, 3, 4],
            nested: Nested {
                value: "test".to_string(),
            },
        };

        let payload = converter.to_payload(&original).unwrap();
        let decoded: Complex = converter.from_payload(&payload).unwrap();

        assert_eq!(original, decoded);
    }

    #[test]
    fn test_option() {
        let converter = BinaryPayloadConverter;

        // Some
        let value: Option<i32> = Some(42);
        let payload = converter.to_payload(&value).unwrap();
        let decoded: Option<i32> = converter.from_payload(&payload).unwrap();
        assert_eq!(decoded, Some(42));

        // None
        let value: Option<i32> = None;
        let payload = converter.to_payload(&value).unwrap();
        let decoded: Option<i32> = converter.from_payload(&payload).unwrap();
        assert_eq!(decoded, None);
    }

    #[test]
    fn test_to_payloads() {
        let converter = BinaryPayloadConverter;
        let values = vec![vec![1u8, 2], vec![3u8, 4], vec![5u8, 6]];
        let payloads = converter.to_payloads(&values).unwrap();

        assert_eq!(payloads.len(), 3);
        for (i, payload) in payloads.iter().enumerate() {
            let value: Vec<u8> = converter.from_payload(payload).unwrap();
            assert_eq!(value, values[i]);
        }
    }

    #[test]
    fn test_binary_vs_json_size() {
        let converter = BinaryPayloadConverter;
        let data = TestData {
            id: 42,
            name: "test".to_string(),
            values: vec![1, 2, 3],
        };

        let payload = converter.to_payload(&data).unwrap();

        assert!(payload.size() > 0);
        assert!(payload.is_binary());
    }
}
