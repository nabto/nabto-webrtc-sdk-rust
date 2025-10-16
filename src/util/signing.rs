//! Message signing layer implementation
//!
//! This module implements the signing layer of the WebRTC signaling protocol.
//! It supports two signing modes:
//! - JWT: Uses HS256 with a shared secret for message authentication
//! - None: No signing (for unauthorized access or service-based auth)

use crate::{Error, Result};
use base64::{engine::general_purpose::URL_SAFE_NO_PAD, Engine};
use jsonwebtoken::{decode, encode, Algorithm, DecodingKey, EncodingKey, Header, Validation};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;
use uuid::Uuid;

/// Signing message type
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SigningMessage {
    /// JWT signed message
    #[serde(rename = "JWT")]
    Jwt { jwt: String },
    /// Unsigned message
    #[serde(rename = "NONE")]
    None { message: JsonValue },
}

/// JWT Claims for signed messages
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JwtClaims {
    /// The actual message being signed
    pub message: JsonValue,
    /// Sequence number to prevent replay attacks
    pub message_seq: u64,
    /// Nonce from the signer
    pub signer_nonce: String,
    /// Nonce from the verifier (not present in first message)
    #[serde(skip_serializing_if = "Option::is_none")]
    pub verifier_nonce: Option<String>,
}

/// JWT Header with optional key ID
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct JwtHeader {
    pub alg: String,
    pub typ: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub kid: Option<String>,
}

/// Trait for message signing/verification
pub trait MessageSigner: Send + Sync {
    /// Sign a message
    fn sign_message(&mut self, message: JsonValue) -> Result<SigningMessage>;

    /// Verify a signed message
    fn verify_message(&mut self, message: JsonValue) -> Result<JsonValue>;
}

/// None message signer (no actual signing)
pub struct NoneMessageSigner;

impl NoneMessageSigner {
    pub fn new() -> Self {
        Self
    }
}

impl MessageSigner for NoneMessageSigner {
    fn sign_message(&mut self, message: JsonValue) -> Result<SigningMessage> {
        Ok(SigningMessage::None { message })
    }

    fn verify_message(&mut self, message: JsonValue) -> Result<JsonValue> {
        let signing_msg: SigningMessage = serde_json::from_value(message)
            .map_err(|e| Error::Signaling(format!("Failed to decode signing message: {}", e)))?;

        match signing_msg {
            SigningMessage::None { message } => Ok(message),
            SigningMessage::Jwt { .. } => Err(Error::Signaling(
                "Expected NONE signing message but got JWT".to_string(),
            )),
        }
    }
}

/// JWT message signer using HS256
pub struct JwtMessageSigner {
    shared_secret: String,
    key_id: Option<String>,
    next_sign_seq: u64,
    next_verify_seq: u64,
    my_nonce: Option<String>,
    remote_nonce: Option<String>,
}

impl JwtMessageSigner {
    /// Create a new JWT message signer
    pub fn new(shared_secret: String, key_id: Option<String>) -> Self {
        Self {
            shared_secret,
            key_id,
            next_sign_seq: 0,
            next_verify_seq: 0,
            my_nonce: None,
            remote_nonce: None,
        }
    }

    /// Extract key ID from a JWT signing message without verifying
    pub fn get_key_id(message: &JsonValue) -> Result<Option<String>> {
        let signing_msg: SigningMessage = serde_json::from_value(message.clone())
            .map_err(|e| Error::Signaling(format!("Failed to decode signing message: {}", e)))?;

        match signing_msg {
            SigningMessage::Jwt { jwt } => {
                let parts: Vec<&str> = jwt.split('.').collect();
                if parts.len() < 2 {
                    return Err(Error::Signaling("Invalid JWT format".to_string()));
                }

                // Decode header without verification
                let header_json = URL_SAFE_NO_PAD
                    .decode(parts[0])
                    .map_err(|e| Error::Signaling(format!("Failed to decode JWT header: {}", e)))?;

                let header: JwtHeader = serde_json::from_slice(&header_json)
                    .map_err(|e| Error::Signaling(format!("Failed to parse JWT header: {}", e)))?;

                Ok(header.kid)
            }
            SigningMessage::None { .. } => Err(Error::Signaling(
                "Expected JWT signing message but got NONE".to_string(),
            )),
        }
    }
}

impl MessageSigner for JwtMessageSigner {
    fn sign_message(&mut self, message: JsonValue) -> Result<SigningMessage> {
        if self.next_sign_seq > 0 && self.remote_nonce.is_none() {
            return Err(Error::Signaling(
                "Cannot sign message with seq > 0 without remote nonce".to_string(),
            ));
        }

        // Generate my nonce on first message
        if self.my_nonce.is_none() {
            self.my_nonce = Some(Uuid::new_v4().to_string());
        }

        let seq = self.next_sign_seq;
        self.next_sign_seq += 1;

        let claims = JwtClaims {
            message,
            message_seq: seq,
            signer_nonce: self.my_nonce.clone().unwrap(),
            verifier_nonce: self.remote_nonce.clone(),
        };

        // Create custom header with kid
        let mut header = Header::new(Algorithm::HS256);
        if let Some(kid) = &self.key_id {
            header.kid = Some(kid.clone());
        }

        let encoding_key = EncodingKey::from_secret(self.shared_secret.as_bytes());
        let jwt = encode(&header, &claims, &encoding_key)
            .map_err(|e| Error::Signaling(format!("Failed to sign JWT: {}", e)))?;

        Ok(SigningMessage::Jwt { jwt })
    }

    fn verify_message(&mut self, message: JsonValue) -> Result<JsonValue> {
        let signing_msg: SigningMessage = serde_json::from_value(message)
            .map_err(|e| Error::Signaling(format!("Failed to decode signing message: {}", e)))?;

        match signing_msg {
            SigningMessage::Jwt { jwt } => {
                let decoding_key = DecodingKey::from_secret(self.shared_secret.as_bytes());
                let mut validation = Validation::new(Algorithm::HS256);
                validation.validate_exp = false; // No expiration check
                validation.required_spec_claims.clear(); // No required claims

                let token_data = decode::<JwtClaims>(&jwt, &decoding_key, &validation)
                    .map_err(|e| Error::Signaling(format!("Failed to verify JWT: {}", e)))?;

                let claims = token_data.claims;

                // Verify sequence number
                if claims.message_seq != self.next_verify_seq {
                    return Err(Error::Signaling(format!(
                        "Expected seq {} but got {}",
                        self.next_verify_seq, claims.message_seq
                    )));
                }

                // Handle nonce verification
                if claims.message_seq == 0 {
                    // First message - store remote nonce
                    self.remote_nonce = Some(claims.signer_nonce.clone());
                } else {
                    // Verify nonces
                    if let Some(ref remote_nonce) = self.remote_nonce {
                        if *remote_nonce != claims.signer_nonce {
                            return Err(Error::Signaling("Signer nonce mismatch".to_string()));
                        }
                    }

                    if let Some(ref my_nonce) = self.my_nonce {
                        if claims.verifier_nonce.as_ref() != Some(my_nonce) {
                            return Err(Error::Signaling("Verifier nonce mismatch".to_string()));
                        }
                    }
                }

                self.next_verify_seq += 1;
                Ok(claims.message)
            }
            SigningMessage::None { .. } => Err(Error::Signaling(
                "Expected JWT signing message but got NONE".to_string(),
            )),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_none_signer() {
        let mut signer = NoneMessageSigner::new();
        let msg = serde_json::json!({"test": "message"});

        let signed = signer.sign_message(msg.clone()).unwrap();
        let signed_json = serde_json::to_value(&signed).unwrap();
        let verified = signer.verify_message(signed_json).unwrap();

        assert_eq!(verified, msg);
    }

    #[test]
    fn test_jwt_signer() {
        let mut alice = JwtMessageSigner::new("secret123".to_string(), Some("key1".to_string()));
        let mut bob = JwtMessageSigner::new("secret123".to_string(), None);

        // Alice sends first message
        let msg1 = serde_json::json!({"type": "SETUP_REQUEST"});
        let signed1 = alice.sign_message(msg1.clone()).unwrap();
        let signed1_json = serde_json::to_value(&signed1).unwrap();

        // Bob verifies
        let verified1 = bob.verify_message(signed1_json).unwrap();
        assert_eq!(verified1, msg1);

        // Bob sends response
        let msg2 = serde_json::json!({"type": "SETUP_RESPONSE"});
        let signed2 = bob.sign_message(msg2.clone()).unwrap();
        let signed2_json = serde_json::to_value(&signed2).unwrap();

        // Alice verifies
        let verified2 = alice.verify_message(signed2_json).unwrap();
        assert_eq!(verified2, msg2);
    }

    #[test]
    fn test_jwt_key_id_extraction() {
        let mut signer = JwtMessageSigner::new("secret".to_string(), Some("key123".to_string()));
        let msg = serde_json::json!({"test": "data"});
        let signed = signer.sign_message(msg).unwrap();
        let signed_json = serde_json::to_value(&signed).unwrap();

        let key_id = JwtMessageSigner::get_key_id(&signed_json).unwrap();
        assert_eq!(key_id, Some("key123".to_string()));
    }
}
