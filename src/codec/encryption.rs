//! # Encryption Payload Codec
//!
//! AES-256-GCM encryption and decryption for payloads.
//!
//! [`EncryptionPayloadCodec`] encrypts payload data with AES-256-GCM, an
//! authenticated cipher: besides keeping the data confidential, decryption
//! fails if the ciphertext was tampered with or corrupted.
//!
//! ## Security Properties
//!
//! - **Algorithm**: AES-256-GCM (AEAD, NIST approved)
//! - **Key size**: 256 bits (32 bytes)
//! - **Nonce**: 96 bits (12 bytes), randomly generated for every encryption
//! - **Authentication tag**: 128 bits (16 bytes), verified on every decryption
//! - **Wire format**: `[12-byte nonce][ciphertext][16-byte tag]`, so an encrypted
//!   payload is 28 bytes longer than the plaintext
//!
//! Payload metadata is not encrypted or authenticated; only `data` is.
//!
//! ## Usage
//!
//! ```rust,ignore
//! use orcher_sdk_core::codec::{PayloadCodec, EncryptionPayloadCodec};
//!
//! // AES-256 needs a 32-byte key.
//! let key = b"32-byte-secret-key-for-aes-256!!";
//! let codec = EncryptionPayloadCodec::new(key);
//!
//! let encrypted = codec.encode(&payload)?;
//! assert_eq!(encrypted.encoding(), Some("binary/encrypted".to_string()));
//!
//! let decrypted = codec.decode(&encrypted)?;
//! assert_eq!(decrypted.data, payload.data);
//! # Ok::<(), orcher_sdk_core::error::Error>(())
//! ```
//!
//! ## Key Management
//!
//! The application is responsible for generating, storing and rotating keys.
//! Anyone holding the key can read every payload encrypted with it, and losing
//! the key makes those payloads unreadable, including the history of running
//! workflows.
//!
//! ```rust,ignore
//! // Option 1: an environment variable.
//! let key = std::env::var("WORKFLOW_ENCRYPTION_KEY")
//!     .expect("WORKFLOW_ENCRYPTION_KEY not set");
//! let key_bytes = key.as_bytes();
//!
//! // Option 2: derive the key from a password.
//! use argon2::{Argon2, PasswordHasher};
//! let password = b"user-password";
//! let salt = b"application-salt";
//! // Derive a 32-byte key with Argon2.
//!
//! // Option 3: a key management service or hardware security module (HSM).
//! ```
//!
//! ## Chaining with Compression
//!
//! ```rust,ignore
//! use orcher_sdk_core::codec::{CodecChain, EncryptionPayloadCodec, GzipPayloadCodec};
//!
//! // Codecs run in the order added, so this chain encrypts, then compresses.
//! let chain = CodecChain::new()
//!     .with_codec(EncryptionPayloadCodec::new(key))
//!     .with_codec(GzipPayloadCodec::default());
//!
//! let encoded = chain.encode(&payload)?;
//! let decoded = chain.decode(&encoded)?;
//! ```
//!
//! **Choosing the order.** Ciphertext looks random and barely compresses, so the
//! chain above spends CPU on gzip for almost no size reduction. To save space,
//! add [`GzipPayloadCodec`](crate::codec::GzipPayloadCodec) first so data is
//! compressed before it is encrypted. Compressing before encrypting lets the
//! ciphertext length reveal something about the plaintext, which matters when
//! an attacker can control part of the input and observe the result.

use crate::codec::PayloadCodec;
use crate::error::{Error, Result};
use crate::payload::Payload;
use aes_gcm::{
    aead::{Aead, KeyInit},
    Aes256Gcm, Nonce,
};
use rand::Rng;

/// AES-256-GCM encryption codec for payloads.
///
/// Encrypts `data` and marks the payload's `encoding` as encrypted. Decoding
/// verifies the authentication tag, so tampered or corrupted data is rejected
/// rather than returned.
///
/// # Security Notes
///
/// - **Key storage**: keep keys in a secrets manager, HSM or protected
///   environment, never in source code.
/// - **Key rotation**: every encryption draws a random 96-bit nonce. GCM is
///   only safe while nonces never repeat under one key, so rotate keys well
///   before a single key has encrypted around 2^32 payloads.
/// - **Authentication**: GCM authenticates the ciphertext automatically.
///
/// The codec is `Send + Sync` and can be shared across threads.
///
/// # Examples
///
/// ```rust,ignore
/// use orcher_sdk_core::codec::{PayloadCodec, EncryptionPayloadCodec};
/// use orcher_sdk_core::payload::Payload;
///
/// let key = b"32-byte-secret-key-for-aes-256!!";
/// let codec = EncryptionPayloadCodec::new(key);
///
/// let payload = Payload::json(b"sensitive data".to_vec());
///
/// let encrypted = codec.encode(&payload)?;
/// assert!(encrypted.data != payload.data); // Data is encrypted
///
/// let decrypted = codec.decode(&encrypted)?;
/// assert_eq!(decrypted.data, payload.data);
/// # Ok::<(), orcher_sdk_core::error::Error>(())
/// ```
#[derive(Clone)]
pub struct EncryptionPayloadCodec {
    cipher: Aes256Gcm,
}

impl EncryptionPayloadCodec {
    /// Creates a codec from a 32-byte (256-bit) key.
    ///
    /// The key length is enforced by the type; use
    /// [`from_slice`](Self::from_slice) to validate a key of unknown length.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use orcher_sdk_core::codec::EncryptionPayloadCodec;
    ///
    /// let key = b"32-byte-secret-key-for-aes-256!!";
    /// let codec = EncryptionPayloadCodec::new(key);
    /// ```
    ///
    /// # Security
    ///
    /// The key should be randomly generated (see
    /// [`generate_key`](Self::generate_key)), stored securely rather than
    /// hardcoded, rotated periodically, and never logged or sent in the clear.
    pub fn new(key: &[u8; 32]) -> Self {
        let cipher = Aes256Gcm::new(key.into());
        Self { cipher }
    }

    /// Creates a codec from a key held in a byte slice.
    ///
    /// # Errors
    ///
    /// Returns a codec error if `key` is not exactly 32 bytes long.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use orcher_sdk_core::codec::EncryptionPayloadCodec;
    ///
    /// let key_vec = vec![0u8; 32];
    /// let codec = EncryptionPayloadCodec::from_slice(&key_vec).unwrap();
    /// ```
    pub fn from_slice(key: &[u8]) -> Result<Self> {
        if key.len() != 32 {
            return Err(Error::codec(format!(
                "Invalid key length: expected 32 bytes, got {}",
                key.len()
            )));
        }

        let mut key_array = [0u8; 32];
        key_array.copy_from_slice(key);
        Ok(Self::new(&key_array))
    }

    /// Generates a random 32-byte key.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use orcher_sdk_core::codec::EncryptionPayloadCodec;
    ///
    /// let key = EncryptionPayloadCodec::generate_key();
    /// let codec = EncryptionPayloadCodec::new(&key);
    /// ```
    ///
    /// # Security
    ///
    /// The key comes from `rand::thread_rng()`, a cryptographically secure
    /// generator. Store it securely: payloads encrypted with it cannot be
    /// decrypted without it.
    pub fn generate_key() -> [u8; 32] {
        let mut key = [0u8; 32];
        rand::thread_rng().fill(&mut key);
        key
    }
}

// Written by hand so that debug output can never print key material.
impl std::fmt::Debug for EncryptionPayloadCodec {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("EncryptionPayloadCodec")
            .field("cipher", &"<redacted>")
            .finish()
    }
}

impl PayloadCodec for EncryptionPayloadCodec {
    fn encode(&self, payload: &Payload) -> Result<Payload> {
        // A fresh random 96-bit nonce per encryption; GCM breaks if a nonce is
        // reused under the same key.
        let mut nonce_bytes = [0u8; 12];
        rand::thread_rng().fill(&mut nonce_bytes);
        let nonce = Nonce::from_slice(&nonce_bytes);

        let ciphertext = self
            .cipher
            .encrypt(nonce, payload.data.as_ref())
            .map_err(|e| Error::codec(format!("AES-GCM encryption failed: {}", e)))?;

        // The nonce is not secret but decryption needs it, so it travels in
        // front of the ciphertext: [12-byte nonce][ciphertext + 16-byte tag].
        let mut encrypted_data = Vec::with_capacity(12 + ciphertext.len());
        encrypted_data.extend_from_slice(&nonce_bytes);
        encrypted_data.extend_from_slice(&ciphertext);

        let mut encoded = Payload::new(encrypted_data, payload.metadata.clone());

        // A payload already transformed by a codec ("binary/...") gets
        // "+encrypted" appended so the chain can be unwound in reverse. A base
        // encoding such as "json" is replaced by "binary/encrypted"; decoding
        // then removes the key instead of restoring the base encoding.
        let current_encoding = payload.encoding().unwrap_or_else(|| "binary".to_string());
        let new_encoding = if current_encoding.starts_with("binary/") {
            format!("{}+encrypted", current_encoding)
        } else {
            "binary/encrypted".to_string()
        };
        encoded.set_metadata_string("encoding", &new_encoding);

        Ok(encoded)
    }

    fn decode(&self, payload: &Payload) -> Result<Payload> {
        let encoding = payload.encoding().unwrap_or_default();
        if !encoding.contains("encrypted") {
            return Err(Error::codec(format!(
                "Expected encrypted encoding, got: {}. Cannot decrypt non-encrypted payload.",
                encoding
            )));
        }

        // Layout: [12-byte nonce][ciphertext + 16-byte tag].
        if payload.data.len() < 12 {
            return Err(Error::codec(
                "Encrypted payload too short: missing nonce".to_string(),
            ));
        }

        let nonce = Nonce::from_slice(&payload.data[..12]);

        let ciphertext = &payload.data[12..];

        // Fails unless the authentication tag verifies, so tampered data is
        // never returned.
        let plaintext = self.cipher.decrypt(nonce, ciphertext).map_err(|e| {
            Error::codec(format!(
                "AES-GCM decryption failed: {}. Data may be corrupted or tampered with.",
                e
            ))
        })?;

        let mut decoded = Payload::new(plaintext, payload.metadata.clone());

        // Undo the encoding change made by `encode`.
        let original_encoding = if encoding == "binary/encrypted" {
            // Encryption was the only codec applied.
            None
        } else if let Some(stripped) = encoding.strip_suffix("+encrypted") {
            // "binary/gzip+encrypted" -> "binary/gzip"
            Some(stripped.to_string())
        } else {
            // Encryption was not the last codec applied; leave the encoding as-is.
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
    fn test_encryption_encode_decode_roundtrip() {
        let key = b"12345678901234567890123456789012"; // 32 bytes
        let codec = EncryptionPayloadCodec::new(key);
        let original = Payload::json(b"sensitive data".to_vec());

        let encrypted = codec.encode(&original).unwrap();
        assert_eq!(encrypted.encoding(), Some("binary/encrypted".to_string()));
        assert_ne!(encrypted.data, original.data); // Data is encrypted

        let decrypted = codec.decode(&encrypted).unwrap();
        assert_eq!(decrypted.data, original.data);
        assert_eq!(decrypted.encoding(), None); // Encoding removed
    }

    #[test]
    fn test_encryption_different_each_time() {
        let key = b"12345678901234567890123456789012"; // 32 bytes
        let codec = EncryptionPayloadCodec::new(key);
        let payload = Payload::json(b"test data".to_vec());

        let encrypted1 = codec.encode(&payload).unwrap();
        let encrypted2 = codec.encode(&payload).unwrap();

        // Random nonces make every ciphertext different.
        assert_ne!(encrypted1.data, encrypted2.data);

        // Both still decrypt to the same plaintext.
        let decrypted1 = codec.decode(&encrypted1).unwrap();
        let decrypted2 = codec.decode(&encrypted2).unwrap();
        assert_eq!(decrypted1.data, payload.data);
        assert_eq!(decrypted2.data, payload.data);
    }

    #[test]
    fn test_encryption_detects_tampering() {
        let key = b"12345678901234567890123456789012"; // 32 bytes
        let codec = EncryptionPayloadCodec::new(key);
        let original = Payload::json(b"important data".to_vec());

        let mut encrypted = codec.encode(&original).unwrap();

        // Flip bits in the ciphertext.
        encrypted.data[15] ^= 0xFF;

        // The authentication tag no longer verifies.
        let result = codec.decode(&encrypted);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("AES-GCM decryption failed"));
    }

    #[test]
    fn test_encryption_wrong_key() {
        let key1 = b"key1-32-bytes-for-aes-256!!!!!!!";
        let key2 = b"key2-32-bytes-for-aes-256!!!!!!!";

        let codec1 = EncryptionPayloadCodec::new(key1);
        let codec2 = EncryptionPayloadCodec::new(key2);

        let payload = Payload::json(b"secret".to_vec());
        let encrypted = codec1.encode(&payload).unwrap();

        // Decrypting with a different key must fail.
        let result = codec2.decode(&encrypted);
        assert!(result.is_err());
    }

    #[test]
    fn test_encryption_decode_requires_encrypted_encoding() {
        let key = b"12345678901234567890123456789012"; // 32 bytes
        let codec = EncryptionPayloadCodec::new(key);

        let unencrypted = Payload::json(b"not encrypted".to_vec());

        let result = codec.decode(&unencrypted);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Expected encrypted encoding"));
    }

    #[test]
    fn test_encryption_preserves_metadata() {
        let key = b"12345678901234567890123456789012"; // 32 bytes
        let codec = EncryptionPayloadCodec::new(key);

        let mut original = Payload::json(b"data".to_vec());
        original.set_metadata_string("custom-key", "custom-value");
        original.set_metadata_string("version", "1.0");

        let encrypted = codec.encode(&original).unwrap();

        // Metadata other than `encoding` passes through unchanged.
        assert_eq!(
            encrypted.get_metadata_string("custom-key"),
            Some("custom-value".to_string())
        );

        let decrypted = codec.decode(&encrypted).unwrap();
        assert_eq!(
            decrypted.get_metadata_string("custom-key"),
            Some("custom-value".to_string())
        );
    }

    #[test]
    fn test_encryption_chained_encoding() {
        let key = b"12345678901234567890123456789012"; // 32 bytes
        let codec = EncryptionPayloadCodec::new(key);

        // A payload that another codec (gzip) has already transformed.
        let mut gzipped = Payload::new(b"fake gzipped data".to_vec(), Default::default());
        gzipped.set_metadata_string("encoding", "binary/gzip");

        let encrypted = codec.encode(&gzipped).unwrap();
        assert_eq!(
            encrypted.encoding(),
            Some("binary/gzip+encrypted".to_string())
        );

        // Decrypting restores the gzip encoding.
        let decrypted = codec.decode(&encrypted).unwrap();
        assert_eq!(decrypted.encoding(), Some("binary/gzip".to_string()));
        assert_eq!(decrypted.data, gzipped.data);
    }

    #[test]
    fn test_encryption_empty_payload() {
        let key = b"12345678901234567890123456789012"; // 32 bytes
        let codec = EncryptionPayloadCodec::new(key);
        let empty = Payload::json(Vec::new());

        let encrypted = codec.encode(&empty).unwrap();
        let decrypted = codec.decode(&encrypted).unwrap();

        assert_eq!(decrypted.data, empty.data);
    }

    #[test]
    fn test_encryption_large_payload() {
        let key = b"12345678901234567890123456789012"; // 32 bytes
        let codec = EncryptionPayloadCodec::new(key);

        let large_data = vec![b'X'; 1024 * 1024]; // 1 MiB
        let large_payload = Payload::json(large_data.clone());

        let encrypted = codec.encode(&large_payload).unwrap();

        // Plaintext + 12-byte nonce + 16-byte tag.
        assert_eq!(encrypted.data.len(), large_data.len() + 28);

        let decrypted = codec.decode(&encrypted).unwrap();
        assert_eq!(decrypted.data, large_data);
    }

    #[test]
    fn test_encryption_from_slice_valid() {
        let key_vec = vec![0u8; 32];
        let codec = EncryptionPayloadCodec::from_slice(&key_vec).unwrap();

        let payload = Payload::json(b"test".to_vec());
        let encrypted = codec.encode(&payload).unwrap();
        let decrypted = codec.decode(&encrypted).unwrap();

        assert_eq!(decrypted.data, payload.data);
    }

    #[test]
    fn test_encryption_from_slice_invalid_length() {
        let short_key = vec![0u8; 16]; // Only 16 bytes
        let result = EncryptionPayloadCodec::from_slice(&short_key);
        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .to_string()
            .contains("Invalid key length"));

        let long_key = vec![0u8; 64]; // Too long
        let result = EncryptionPayloadCodec::from_slice(&long_key);
        assert!(result.is_err());
    }

    #[test]
    fn test_encryption_generate_key() {
        let key1 = EncryptionPayloadCodec::generate_key();
        let key2 = EncryptionPayloadCodec::generate_key();

        // Two generated keys differ.
        assert_ne!(key1, key2);

        // A generated key works for a roundtrip.
        let codec = EncryptionPayloadCodec::new(&key1);
        let payload = Payload::json(b"test".to_vec());
        let encrypted = codec.encode(&payload).unwrap();
        let decrypted = codec.decode(&encrypted).unwrap();
        assert_eq!(decrypted.data, payload.data);
    }

    #[test]
    fn test_encryption_corrupted_nonce() {
        let key = b"12345678901234567890123456789012"; // 32 bytes
        let codec = EncryptionPayloadCodec::new(key);
        let payload = Payload::json(b"data".to_vec());

        let mut encrypted = codec.encode(&payload).unwrap();

        // Corrupt the nonce (the first 12 bytes).
        encrypted.data[0] ^= 0xFF;

        // A different nonce means the tag no longer verifies.
        let result = codec.decode(&encrypted);
        assert!(result.is_err());
    }

    #[test]
    fn test_encryption_too_short_payload() {
        let key = b"12345678901234567890123456789012"; // 32 bytes
        let codec = EncryptionPayloadCodec::new(key);

        // Shorter than the 12-byte nonce.
        let mut invalid = Payload::new(vec![1, 2, 3], Default::default());
        invalid.set_metadata_string("encoding", "binary/encrypted");

        let result = codec.decode(&invalid);
        assert!(result.is_err());
        assert!(result.unwrap_err().to_string().contains("too short"));
    }
}
