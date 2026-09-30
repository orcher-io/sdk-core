//! # JSON Payload Converter
//!
//! JSON serialization for payloads, using `serde_json`.
//!
//! JSON is the default format because every language SDK reads it, people can
//! read it when debugging, and adding optional fields does not break existing
//! readers.
//!
//! ## Usage
//!
//! ```rust
//! use orcher_sdk_core::converter::{DataConverter, JsonPayloadConverter};
//! use serde::{Serialize, Deserialize};
//!
//! #[derive(Serialize, Deserialize, PartialEq, Debug)]
//! struct Person {
//!     name: String,
//!     age: u32,
//! }
//!
//! let converter = JsonPayloadConverter::default();
//! let person = Person { name: "Alice".to_string(), age: 30 };
//!
//! let payload = converter.to_payload(&person)?;
//! assert_eq!(payload.encoding(), Some("json".to_string()));
//!
//! let decoded: Person = converter.from_payload(&payload)?;
//! assert_eq!(decoded, person);
//! # Ok::<(), orcher_sdk_core::error::Error>(())
//! ```

use crate::converter::DataConverter;
use crate::error::{Error, Result};
use crate::payload::Payload;
use std::collections::HashMap;

/// JSON payload converter, the default [`DataConverter`].
///
/// Serializes any `serde` type to compact JSON and marks the payload with
/// `encoding: "json"` and `content-type: "application/json"`. Reading accepts
/// any payload whose `encoding` contains `"json"`; codec output
/// (`"binary/gzip"`, `"binary/encrypted"`) is rejected until it is decoded.
///
/// The converter is stateless and safe to share across threads.
///
/// # Examples
///
/// ## Basic Usage
///
/// ```rust
/// use orcher_sdk_core::converter::{DataConverter, JsonPayloadConverter};
///
/// let converter = JsonPayloadConverter::default();
/// let payload = converter.to_payload(&42)?;
///
/// let value: i32 = converter.from_payload(&payload)?;
/// assert_eq!(value, 42);
/// # Ok::<(), orcher_sdk_core::error::Error>(())
/// ```
///
/// ## Complex Types
///
/// ```rust
/// use orcher_sdk_core::converter::{DataConverter, JsonPayloadConverter};
/// use serde::{Serialize, Deserialize};
/// use std::collections::HashMap;
///
/// #[derive(Serialize, Deserialize, PartialEq, Debug)]
/// struct Order {
///     id: String,
///     items: Vec<String>,
///     metadata: HashMap<String, String>,
/// }
///
/// let converter = JsonPayloadConverter::default();
/// let order = Order {
///     id: "123".to_string(),
///     items: vec!["item1".to_string(), "item2".to_string()],
///     metadata: [("key".to_string(), "value".to_string())].iter().cloned().collect(),
/// };
///
/// let payload = converter.to_payload(&order)?;
/// let decoded: Order = converter.from_payload(&payload)?;
/// assert_eq!(decoded, order);
/// # Ok::<(), orcher_sdk_core::error::Error>(())
/// ```
///
/// ## Human-Readable Output
///
/// ```rust
/// use orcher_sdk_core::converter::{DataConverter, JsonPayloadConverter};
/// use serde::{Serialize, Deserialize};
///
/// #[derive(Serialize, Deserialize)]
/// struct Data {
///     message: String,
/// }
///
/// let converter = JsonPayloadConverter::default();
/// let data = Data { message: "hello".to_string() };
/// let payload = converter.to_payload(&data)?;
///
/// // The payload data is plain JSON text.
/// let json_string = String::from_utf8(payload.data.clone())?;
/// assert!(json_string.contains("message"));
/// assert!(json_string.contains("hello"));
/// # Ok::<(), Box<dyn std::error::Error>>(())
/// ```
#[derive(Debug, Clone, Copy, Default)]
pub struct JsonPayloadConverter;

impl JsonPayloadConverter {
    /// Creates a JSON payload converter.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use orcher_sdk_core::converter::JsonPayloadConverter;
    ///
    /// let converter = JsonPayloadConverter::new();
    /// ```
    pub fn new() -> Self {
        Self
    }
}

impl DataConverter for JsonPayloadConverter {
    fn to_payload<T: serde::Serialize>(&self, value: &T) -> Result<Payload> {
        let json_bytes = serde_json::to_vec(value)
            .map_err(|e| Error::serialization(format!("Failed to serialize to JSON: {}", e)))?;

        let mut metadata = HashMap::new();
        metadata.insert("encoding".to_string(), b"json".to_vec());
        metadata.insert("content-type".to_string(), b"application/json".to_vec());

        Ok(Payload {
            data: json_bytes,
            metadata,
        })
    }

    fn from_payload<T: for<'de> serde::Deserialize<'de>>(&self, payload: &Payload) -> Result<T> {
        if !payload.is_json() {
            let encoding = payload.encoding().unwrap_or_else(|| "unknown".to_string());
            return Err(Error::serialization(format!(
                "Expected JSON encoding, got: {}",
                encoding
            )));
        }

        serde_json::from_slice(&payload.data)
            .map_err(|e| Error::serialization(format!("Failed to deserialize from JSON: {}", e)))
    }

    fn encoding(&self) -> &str {
        "json"
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

    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    struct ComplexData {
        string: String,
        number: i64,
        float: f64,
        boolean: bool,
        array: Vec<String>,
        nested: NestedData,
    }

    #[derive(Serialize, Deserialize, PartialEq, Debug)]
    struct NestedData {
        inner: String,
    }

    #[test]
    fn test_json_roundtrip_simple() {
        let converter = JsonPayloadConverter;
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
    fn test_json_roundtrip_complex() {
        let converter = JsonPayloadConverter;
        let original = ComplexData {
            string: "hello".to_string(),
            number: -123456,
            float: 2.5,
            boolean: true,
            array: vec!["a".to_string(), "b".to_string()],
            nested: NestedData {
                inner: "nested value".to_string(),
            },
        };

        let payload = converter.to_payload(&original).unwrap();
        let decoded: ComplexData = converter.from_payload(&payload).unwrap();

        assert_eq!(original, decoded);
    }

    #[test]
    fn test_json_metadata() {
        let converter = JsonPayloadConverter;
        let payload = converter.to_payload(&42).unwrap();

        assert_eq!(payload.encoding(), Some("json".to_string()));
        assert_eq!(payload.content_type(), Some("application/json".to_string()));
        assert!(payload.is_json());
    }

    #[test]
    fn test_json_payload_readable() {
        let converter = JsonPayloadConverter;
        let original = TestData {
            id: 1,
            name: "readable".to_string(),
            values: vec![],
        };

        let payload = converter.to_payload(&original).unwrap();
        let json_string = String::from_utf8(payload.data).unwrap();

        assert!(json_string.contains("\"id\":1"));
        assert!(json_string.contains("\"name\":\"readable\""));
    }

    #[test]
    fn test_primitives() {
        let converter = JsonPayloadConverter;

        // Integer
        let payload = converter.to_payload(&42).unwrap();
        let decoded: i32 = converter.from_payload(&payload).unwrap();
        assert_eq!(decoded, 42);

        // String
        let payload = converter.to_payload(&"hello").unwrap();
        let decoded: String = converter.from_payload(&payload).unwrap();
        assert_eq!(decoded, "hello");

        // Boolean
        let payload = converter.to_payload(&true).unwrap();
        let decoded: bool = converter.from_payload(&payload).unwrap();
        assert!(decoded);

        // Float
        let payload = converter.to_payload(&1.25).unwrap();
        let decoded: f64 = converter.from_payload(&payload).unwrap();
        assert!((decoded - 1.25).abs() < 0.0001);
    }

    #[test]
    fn test_collections() {
        let converter = JsonPayloadConverter;

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
    fn test_option() {
        let converter = JsonPayloadConverter;

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
    fn test_result_type() {
        let converter = JsonPayloadConverter;

        // Ok
        let value: std::result::Result<i32, String> = Ok(42);
        let payload = converter.to_payload(&value).unwrap();
        let decoded: std::result::Result<i32, String> = converter.from_payload(&payload).unwrap();
        assert!(decoded.is_ok());
        assert_eq!(decoded.unwrap(), 42);

        // Err
        let value: std::result::Result<i32, String> = Err("error".to_string());
        let payload = converter.to_payload(&value).unwrap();
        let decoded: std::result::Result<i32, String> = converter.from_payload(&payload).unwrap();
        assert!(decoded.is_err());
        assert_eq!(decoded.unwrap_err(), "error".to_string());
    }

    #[test]
    fn test_wrong_encoding_error() {
        let converter = JsonPayloadConverter;

        let mut payload = Payload::binary(vec![1, 2, 3]);
        payload.set_metadata_string("encoding", "binary");

        let result: Result<TestData> = converter.from_payload(&payload);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Expected JSON encoding"));
    }

    #[test]
    fn test_invalid_json_error() {
        let converter = JsonPayloadConverter;

        // Marked JSON, but the bytes are not valid UTF-8.
        let payload = Payload::json(vec![0xFF, 0xFE, 0xFD]);

        let result: Result<TestData> = converter.from_payload(&payload);
        assert!(result.is_err());
    }

    #[test]
    fn test_type_mismatch_error() {
        let converter = JsonPayloadConverter;

        let payload = converter.to_payload(&"hello").unwrap();

        // A JSON string does not deserialize as an integer.
        let result: Result<i32> = converter.from_payload(&payload);
        assert!(result.is_err());
    }

    #[test]
    fn test_encoding_method() {
        let converter = JsonPayloadConverter::new();
        assert_eq!(converter.encoding(), "json");
    }

    #[test]
    fn test_default() {
        let converter = JsonPayloadConverter;
        let payload = converter.to_payload(&42).unwrap();
        assert!(payload.is_json());
    }

    #[test]
    fn test_empty_struct() {
        #[derive(Serialize, Deserialize, PartialEq, Debug)]
        struct Empty {}

        let converter = JsonPayloadConverter;
        let original = Empty {};
        let payload = converter.to_payload(&original).unwrap();
        let decoded: Empty = converter.from_payload(&payload).unwrap();
        assert_eq!(original, decoded);
    }

    #[test]
    fn test_large_data() {
        let converter = JsonPayloadConverter;

        let large_vec: Vec<i32> = (0..10000).collect();
        let payload = converter.to_payload(&large_vec).unwrap();
        let decoded: Vec<i32> = converter.from_payload(&payload).unwrap();

        assert_eq!(decoded.len(), 10000);
        assert_eq!(decoded, large_vec);
    }

    #[test]
    fn test_unicode() {
        let converter = JsonPayloadConverter;

        let text = "Hello 世界 🌍 café";
        let payload = converter.to_payload(&text).unwrap();
        let decoded: String = converter.from_payload(&payload).unwrap();

        assert_eq!(decoded, text);
    }

    #[test]
    fn test_to_payloads() {
        let converter = JsonPayloadConverter;
        let values = vec![1, 2, 3];
        let payloads = converter.to_payloads(&values).unwrap();

        assert_eq!(payloads.len(), 3);
        for (i, payload) in payloads.iter().enumerate() {
            let value: i32 = converter.from_payload(payload).unwrap();
            assert_eq!(value, (i + 1) as i32);
        }
    }
}
