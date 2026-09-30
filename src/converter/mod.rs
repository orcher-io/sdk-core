//! Conversion between Rust values and [`Payload`]s.
//!
//! - [`JsonPayloadConverter`] - JSON, the default and the format every language SDK reads
//! - [`BinaryPayloadConverter`] - bincode, compact but readable only from Rust
//! - [`default_data_converter()`] - returns the JSON converter
//!
//! Implement [`DataConverter`] for other formats such as MessagePack or Protobuf.
//! A converter produces the payload; a [codec](crate::codec) can then compress or
//! encrypt it, and must be decoded before the converter reads it back.

use crate::error::Result;
use crate::payload::{Payload, Payloads};

pub mod binary;
pub mod json;

pub use binary::BinaryPayloadConverter;
pub use json::JsonPayloadConverter;

/// Converts between Rust values and [`Payload`]s.
///
/// Implementations must be `Send + Sync` because workflows and tasks run
/// concurrently and may share a converter.
pub trait DataConverter: Send + Sync {
    /// Serializes a value into a [`Payload`], setting its `encoding` metadata.
    ///
    /// # Errors
    ///
    /// Returns a serialization error if the value cannot be encoded.
    fn to_payload<T: serde::Serialize>(&self, value: &T) -> Result<Payload>;

    /// Deserializes a value from a [`Payload`].
    ///
    /// # Errors
    ///
    /// Returns a serialization error if the payload's `encoding` does not
    /// match this converter or its data does not decode as `T`.
    #[allow(clippy::wrong_self_convention)]
    fn from_payload<T: for<'de> serde::Deserialize<'de>>(&self, payload: &Payload) -> Result<T>;

    /// Returns the `encoding` value this converter writes, such as `"json"` or `"binary"`.
    fn encoding(&self) -> &str;

    /// Serializes each value into its own [`Payload`], preserving order.
    ///
    /// # Errors
    ///
    /// Returns the first serialization error encountered.
    fn to_payloads<T: serde::Serialize>(&self, values: &[T]) -> Result<Payloads> {
        let payloads = values
            .iter()
            .map(|v| self.to_payload(v))
            .collect::<Result<Vec<_>>>()?;
        Ok(Payloads::new(payloads))
    }

    /// Serializes one value into a one-element [`Payloads`].
    fn to_payloads_single<T: serde::Serialize>(&self, value: &T) -> Result<Payloads> {
        Ok(Payloads::from_single(self.to_payload(value)?))
    }

    /// Returns [`encoding`](Self::encoding) as UTF-8 bytes, the form metadata values take.
    fn encoding_bytes(&self) -> Vec<u8> {
        self.encoding().as_bytes().to_vec()
    }
}

/// Returns the default data converter ([`JsonPayloadConverter`]).
pub fn default_data_converter() -> impl DataConverter {
    JsonPayloadConverter
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_default_converter_is_json() {
        let converter = default_data_converter();
        assert_eq!(converter.encoding(), "json");
    }

    #[test]
    fn test_to_payloads_single() {
        let converter = JsonPayloadConverter;
        let payloads = converter.to_payloads_single(&42).unwrap();
        assert_eq!(payloads.len(), 1);
    }

    #[test]
    fn test_encoding_bytes() {
        let converter = JsonPayloadConverter;
        assert_eq!(converter.encoding_bytes(), b"json");
    }
}
