//! # Payloads
//!
//! A [`Payload`] is the unit in which workflow and task inputs and results
//! travel: opaque bytes plus metadata saying how they are encoded. Because the
//! encoding travels with the data, every language SDK can read what another
//! wrote, serialization formats are pluggable (JSON, Protobuf, MessagePack),
//! and [codecs](crate::codec) can compress or encrypt data transparently.
//!
//! ## Usage
//!
//! ```rust
//! use orcher_sdk_core::payload::Payload;
//! use serde::{Serialize, Deserialize};
//!
//! #[derive(Serialize, Deserialize)]
//! struct Order {
//!     id: String,
//!     amount: f64,
//! }
//!
//! let order = Order { id: "123".to_string(), amount: 99.99 };
//! let json_bytes = serde_json::to_vec(&order)?;
//! let payload = Payload::json(json_bytes);
//!
//! assert_eq!(payload.encoding(), Some("json".to_string()));
//! assert_eq!(payload.content_type(), Some("application/json".to_string()));
//! assert!(payload.is_json());
//!
//! let decoded: Order = serde_json::from_slice(&payload.data)?;
//! # Ok::<(), Box<dyn std::error::Error>>(())
//! ```

use crate::error::Error;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

/// Serialized data together with metadata describing its encoding.
///
/// Mirrors the wire message `orcher_proto::orcher::v1::Payload` field for
/// field, with a Rust-native API on top.
///
/// # Metadata Keys
///
/// - `"encoding"`: how `data` is encoded, for example `"json"`, `"binary"`,
///   `"binary/protobuf"`, or codec output such as `"binary/gzip"`.
/// - `"content-type"`: the MIME type, for example `"application/json"`.
///
/// Any other key is carried through unchanged, including through codecs.
///
/// # Examples
///
/// ## Creating a JSON Payload
///
/// ```rust
/// use orcher_sdk_core::payload::Payload;
///
/// let data = br#"{"key":"value"}"#.to_vec();
/// let payload = Payload::json(data);
///
/// assert_eq!(payload.encoding(), Some("json".to_string()));
/// assert!(payload.is_json());
/// ```
///
/// ## Creating a Binary Payload
///
/// ```rust
/// use orcher_sdk_core::payload::Payload;
///
/// let data = vec![0x01, 0x02, 0x03, 0x04];
/// let payload = Payload::binary(data);
///
/// assert_eq!(payload.encoding(), Some("binary".to_string()));
/// ```
///
/// ## Custom Metadata
///
/// ```rust
/// use orcher_sdk_core::payload::Payload;
///
/// let mut payload = Payload::json(vec![]);
/// payload.set_metadata_string("custom-key", "custom-value");
/// payload.set_metadata_string("schema-version", "2.0");
///
/// assert_eq!(
///     payload.get_metadata_string("custom-key"),
///     Some("custom-value".to_string())
/// );
/// ```
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Payload {
    /// The serialized data; its format is given by the `encoding` metadata.
    pub data: Vec<u8>,

    /// Metadata about the data. Values are raw bytes, usually UTF-8 strings.
    pub metadata: HashMap<String, Vec<u8>>,
}

impl Payload {
    /// Creates a payload from data and metadata.
    pub fn new(data: Vec<u8>, metadata: HashMap<String, Vec<u8>>) -> Self {
        Self { data, metadata }
    }

    /// Creates a payload with no data and no metadata.
    pub fn empty() -> Self {
        Self {
            data: Vec::new(),
            metadata: HashMap::new(),
        }
    }

    /// Creates a payload for data that is already JSON.
    ///
    /// Sets `encoding` to `"json"` and `content-type` to `"application/json"`.
    pub fn json(data: Vec<u8>) -> Self {
        let mut metadata = HashMap::new();
        metadata.insert("encoding".to_string(), b"json".to_vec());
        metadata.insert("content-type".to_string(), b"application/json".to_vec());
        Self { data, metadata }
    }

    /// Creates a payload for raw bytes, stored as given.
    ///
    /// Sets `encoding` to `"binary"` and `content-type` to
    /// `"application/octet-stream"`.
    pub fn binary(data: Vec<u8>) -> Self {
        let mut metadata = HashMap::new();
        metadata.insert("encoding".to_string(), b"binary".to_vec());
        metadata.insert(
            "content-type".to_string(),
            b"application/octet-stream".to_vec(),
        );
        Self { data, metadata }
    }

    /// Creates a payload for an encoded Protobuf message.
    ///
    /// Sets `encoding` to `"binary/protobuf"` and `content-type` to
    /// `"application/x-protobuf"`.
    pub fn protobuf(data: Vec<u8>) -> Self {
        let mut metadata = HashMap::new();
        metadata.insert("encoding".to_string(), b"binary/protobuf".to_vec());
        metadata.insert(
            "content-type".to_string(),
            b"application/x-protobuf".to_vec(),
        );
        Self { data, metadata }
    }

    /// Returns a metadata value as a string.
    ///
    /// Returns `None` if the key is missing or its value is not valid UTF-8.
    pub fn get_metadata_string(&self, key: &str) -> Option<String> {
        self.metadata
            .get(key)
            .and_then(|bytes| String::from_utf8(bytes.clone()).ok())
    }

    /// Returns a metadata value as raw bytes, or `None` if the key is missing.
    pub fn get_metadata_bytes(&self, key: &str) -> Option<&[u8]> {
        self.metadata.get(key).map(|v| v.as_slice())
    }

    /// Sets a metadata value from a string, stored as UTF-8 bytes.
    ///
    /// Replaces any existing value for the key.
    pub fn set_metadata_string(&mut self, key: impl Into<String>, value: impl Into<String>) {
        self.metadata.insert(key.into(), value.into().into_bytes());
    }

    /// Sets a metadata value from raw bytes, replacing any existing value.
    pub fn set_metadata_bytes(&mut self, key: impl Into<String>, value: Vec<u8>) {
        self.metadata.insert(key.into(), value);
    }

    /// Removes a metadata entry and returns its value, if there was one.
    pub fn remove_metadata(&mut self, key: impl Into<String>) -> Option<Vec<u8>> {
        self.metadata.remove(&key.into())
    }

    /// Returns the `encoding` metadata value, if set.
    pub fn encoding(&self) -> Option<String> {
        self.get_metadata_string("encoding")
    }

    /// Returns the `content-type` metadata value, if set.
    pub fn content_type(&self) -> Option<String> {
        self.get_metadata_string("content-type")
    }

    /// Returns `true` if the `encoding` contains `"json"`.
    ///
    /// This is a substring match, so it is also `true` for layered encodings
    /// such as `"json/gzip"`.
    pub fn is_json(&self) -> bool {
        self.encoding()
            .map(|enc| enc.contains("json"))
            .unwrap_or(false)
    }

    /// Returns `true` if the `encoding` contains `"binary"`.
    ///
    /// This is a substring match, so it is also `true` for `"binary/protobuf"`
    /// and for codec output such as `"binary/gzip"`.
    pub fn is_binary(&self) -> bool {
        self.encoding()
            .map(|enc| enc.contains("binary"))
            .unwrap_or(false)
    }

    /// Returns `true` if the `encoding` contains `"gzip"` or `"zstd"`.
    pub fn is_compressed(&self) -> bool {
        self.encoding()
            .map(|enc| enc.contains("gzip") || enc.contains("zstd"))
            .unwrap_or(false)
    }

    /// Returns `true` if the `encoding` contains `"encrypted"`.
    pub fn is_encrypted(&self) -> bool {
        self.encoding()
            .map(|enc| enc.contains("encrypted"))
            .unwrap_or(false)
    }

    /// Returns the length of `data` in bytes; metadata is not counted.
    pub fn size(&self) -> usize {
        self.data.len()
    }

    /// Returns `true` if `data` is empty, regardless of metadata.
    pub fn is_empty(&self) -> bool {
        self.data.is_empty()
    }

    /// Creates a payload from bytes with no metadata, so no `encoding` either.
    pub fn new_data(data: Vec<u8>) -> Self {
        Self {
            data,
            metadata: HashMap::new(),
        }
    }

    /// Serializes a value to JSON and wraps it in a payload.
    ///
    /// Sets the same metadata as [`Payload::json`].
    ///
    /// # Errors
    ///
    /// Returns [`Error::Serialization`] if the value cannot be serialized.
    pub fn from_json<T: Serialize>(value: &T) -> Result<Self, Error> {
        let data = serde_json::to_vec(value).map_err(|e| Error::Serialization(e.to_string()))?;
        Ok(Self::json(data))
    }

    /// Deserializes `data` as JSON.
    ///
    /// The `encoding` metadata is not checked, so decode any codec first.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Deserialization`] if `data` is not valid JSON for `T`.
    pub fn to_json<T: for<'de> Deserialize<'de>>(&self) -> Result<T, Error> {
        serde_json::from_slice(&self.data).map_err(|e| Error::Deserialization(e.to_string()))
    }

    /// Creates a payload holding a plain string.
    ///
    /// Sets `encoding` to `"utf-8"` and `content-type` to `"text/plain"`.
    pub fn from_string(value: impl Into<String>) -> Self {
        let data = value.into().into_bytes();
        let mut metadata = HashMap::new();
        metadata.insert("encoding".to_string(), b"utf-8".to_vec());
        metadata.insert("content-type".to_string(), b"text/plain".to_vec());
        Self { data, metadata }
    }

    /// Returns `data` as a string.
    ///
    /// # Errors
    ///
    /// Returns [`Error::Deserialization`] if `data` is not valid UTF-8.
    pub fn as_string(&self) -> Result<String, Error> {
        String::from_utf8(self.data.clone())
            .map_err(|e| Error::Deserialization(format!("Invalid UTF-8: {}", e)))
    }

    /// Sets a metadata value and returns the payload, for builder-style construction.
    pub fn with_metadata(mut self, key: impl Into<String>, value: impl Into<Vec<u8>>) -> Self {
        self.metadata.insert(key.into(), value.into());
        self
    }

    /// Returns a metadata value as raw bytes.
    ///
    /// Same as [`get_metadata_bytes`](Self::get_metadata_bytes).
    pub fn get_metadata(&self, key: &str) -> Option<&[u8]> {
        self.get_metadata_bytes(key)
    }

    /// Returns the length of `data` in bytes.
    ///
    /// Same as [`size`](Self::size).
    pub fn len(&self) -> usize {
        self.data.len()
    }
}

impl Default for Payload {
    fn default() -> Self {
        Self::empty()
    }
}

/// An ordered list of payloads, typically the arguments of a workflow or task.
///
/// Each argument is serialized into its own [`Payload`], in parameter order.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Payloads {
    /// The payloads, in argument order.
    pub payloads: Vec<Payload>,
}

impl Payloads {
    /// Creates a list from a vector of payloads.
    pub fn new(payloads: Vec<Payload>) -> Self {
        Self { payloads }
    }

    /// Creates an empty list, meaning no arguments.
    pub fn empty() -> Self {
        Self {
            payloads: Vec::new(),
        }
    }

    /// Creates a list holding one payload.
    pub fn from_single(payload: Payload) -> Self {
        Self {
            payloads: vec![payload],
        }
    }

    /// Returns the number of payloads.
    pub fn len(&self) -> usize {
        self.payloads.len()
    }

    /// Returns `true` if the list has no payloads.
    pub fn is_empty(&self) -> bool {
        self.payloads.is_empty()
    }

    /// Returns the payload at `index`, or `None` if it is out of bounds.
    pub fn get(&self, index: usize) -> Option<&Payload> {
        self.payloads.get(index)
    }

    /// Returns the payload at `index` mutably, or `None` if it is out of bounds.
    pub fn get_mut(&mut self, index: usize) -> Option<&mut Payload> {
        self.payloads.get_mut(index)
    }

    /// Appends a payload to the end of the list.
    pub fn push(&mut self, payload: Payload) {
        self.payloads.push(payload);
    }

    /// Returns an iterator over the payloads, in order.
    pub fn iter(&self) -> impl Iterator<Item = &Payload> {
        self.payloads.iter()
    }

    /// Returns a mutable iterator over the payloads, in order.
    pub fn iter_mut(&mut self) -> impl Iterator<Item = &mut Payload> {
        self.payloads.iter_mut()
    }
}

impl Default for Payloads {
    fn default() -> Self {
        Self::empty()
    }
}

impl From<Vec<Payload>> for Payloads {
    fn from(payloads: Vec<Payload>) -> Self {
        Self::new(payloads)
    }
}

impl From<Payload> for Payloads {
    fn from(payload: Payload) -> Self {
        Self::from_single(payload)
    }
}

impl IntoIterator for Payloads {
    type Item = Payload;
    type IntoIter = std::vec::IntoIter<Payload>;

    fn into_iter(self) -> Self::IntoIter {
        self.payloads.into_iter()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_payload_json() {
        let data = br#"{"key":"value"}"#.to_vec();
        let payload = Payload::json(data.clone());

        assert_eq!(payload.data, data);
        assert_eq!(payload.encoding(), Some("json".to_string()));
        assert_eq!(payload.content_type(), Some("application/json".to_string()));
        assert!(payload.is_json());
        assert!(!payload.is_binary());
        assert!(!payload.is_compressed());
        assert!(!payload.is_encrypted());
    }

    #[test]
    fn test_payload_binary() {
        let data = vec![0x01, 0x02, 0x03];
        let payload = Payload::binary(data.clone());

        assert_eq!(payload.data, data);
        assert_eq!(payload.encoding(), Some("binary".to_string()));
        assert_eq!(
            payload.content_type(),
            Some("application/octet-stream".to_string())
        );
        assert!(!payload.is_json());
        assert!(payload.is_binary());
    }

    #[test]
    fn test_payload_protobuf() {
        let data = vec![0x08, 0x96, 0x01];
        let payload = Payload::protobuf(data.clone());

        assert_eq!(payload.data, data);
        assert_eq!(payload.encoding(), Some("binary/protobuf".to_string()));
        assert!(payload.is_binary());
    }

    #[test]
    fn test_payload_metadata() {
        let mut payload = Payload::json(vec![]);
        payload.set_metadata_string("custom-key", "custom-value");
        payload.set_metadata_bytes("binary-key", vec![0x01, 0x02]);

        assert_eq!(
            payload.get_metadata_string("custom-key"),
            Some("custom-value".to_string())
        );
        assert_eq!(
            payload.get_metadata_bytes("binary-key"),
            Some(&[0x01, 0x02][..])
        );
    }

    #[test]
    fn test_payload_compressed() {
        let mut payload = Payload::empty();
        payload.set_metadata_string("encoding", "json/gzip");

        assert!(payload.is_json());
        assert!(payload.is_compressed());
        assert!(!payload.is_encrypted());
    }

    #[test]
    fn test_payload_encrypted() {
        let mut payload = Payload::empty();
        payload.set_metadata_string("encoding", "json/gzip/encrypted");

        assert!(payload.is_json());
        assert!(payload.is_compressed());
        assert!(payload.is_encrypted());
    }

    #[test]
    fn test_payload_size() {
        let data = vec![1, 2, 3, 4, 5];
        let payload = Payload::json(data);

        assert_eq!(payload.size(), 5);
        assert!(!payload.is_empty());
    }

    #[test]
    fn test_payload_empty() {
        let payload = Payload::empty();

        assert!(payload.is_empty());
        assert_eq!(payload.size(), 0);
        assert!(payload.metadata.is_empty());
    }

    #[test]
    fn test_payloads_creation() {
        let payloads = Payloads::new(vec![
            Payload::json(br#"{"id":"123"}"#.to_vec()),
            Payload::json(br#"99.99"#.to_vec()),
        ]);

        assert_eq!(payloads.len(), 2);
        assert!(!payloads.is_empty());
    }

    #[test]
    fn test_payloads_empty() {
        let payloads = Payloads::empty();

        assert!(payloads.is_empty());
        assert_eq!(payloads.len(), 0);
    }

    #[test]
    fn test_payloads_from_single() {
        let payload = Payload::json(vec![]);
        let payloads = Payloads::from_single(payload);

        assert_eq!(payloads.len(), 1);
    }

    #[test]
    fn test_payloads_get() {
        let payloads = Payloads::new(vec![
            Payload::json(br#"first"#.to_vec()),
            Payload::json(br#"second"#.to_vec()),
        ]);

        assert!(payloads.get(0).is_some());
        assert!(payloads.get(1).is_some());
        assert!(payloads.get(2).is_none());
    }

    #[test]
    fn test_payloads_push() {
        let mut payloads = Payloads::empty();
        payloads.push(Payload::json(vec![]));
        payloads.push(Payload::binary(vec![]));

        assert_eq!(payloads.len(), 2);
    }

    #[test]
    fn test_payloads_iter() {
        let payloads = Payloads::new(vec![
            Payload::json(vec![]),
            Payload::binary(vec![]),
            Payload::json(vec![]),
        ]);

        let json_count = payloads.iter().filter(|p| p.is_json()).count();
        assert_eq!(json_count, 2);
    }

    #[test]
    fn test_payloads_from_vec() {
        let vec = vec![Payload::json(vec![]), Payload::binary(vec![])];
        let payloads: Payloads = vec.into();

        assert_eq!(payloads.len(), 2);
    }

    #[test]
    fn test_payloads_from_payload() {
        let payload = Payload::json(vec![]);
        let payloads: Payloads = payload.into();

        assert_eq!(payloads.len(), 1);
    }

    #[test]
    fn test_payloads_into_iter() {
        let payloads = Payloads::new(vec![Payload::json(vec![]), Payload::binary(vec![])]);

        let count = payloads.into_iter().count();
        assert_eq!(count, 2);
    }
}
