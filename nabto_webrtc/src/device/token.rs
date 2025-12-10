//! Device Token Generator
//!
//! Utility for generating JWT tokens for devices to connect to the Nabto WebRTC Signaling Service.

use crate::{Error, Result};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use p256::ecdsa::SigningKey;
use p256::pkcs8::{DecodePrivateKey, EncodePublicKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::time::{SystemTime, UNIX_EPOCH};

/// JWT claims for device authentication
#[derive(Debug, Serialize, Deserialize)]
struct Claims {
    /// Not Before time
    nbf: u64,
    /// Issued At time
    iat: u64,
    /// Expiration time
    exp: u64,
    /// Scopes for the token
    scope: String,
    /// Resource identifier
    resource: String,
}

/// Device Token Generator for creating JWT tokens
pub struct DeviceTokenGenerator {
    product_id: String,
    device_id: String,
    private_key: String,
}

impl DeviceTokenGenerator {
    /// Create a new DeviceTokenGenerator
    ///
    /// # Arguments
    ///
    /// * `product_id` - Product ID of the device
    /// * `device_id` - Device ID of the device
    /// * `private_key` - Private key in PEM format (PKCS#8)
    pub fn new(product_id: String, device_id: String, private_key: String) -> Self {
        Self {
            product_id,
            device_id,
            private_key,
        }
    }

    /// Get the key ID from the private key
    ///
    /// The key ID is computed as "device:" + hex(sha256(SubjectPublicKeyInfo))
    fn get_key_id(&self) -> Result<String> {
        // Parse the private key
        let signing_key = SigningKey::from_pkcs8_pem(&self.private_key)
            .map_err(|e| Error::Configuration(format!("Invalid private key: {}", e)))?;

        // Get the verifying key (public key)
        let verifying_key = signing_key.verifying_key();

        // Encode the public key as SubjectPublicKeyInfo (SPKI) DER format
        let spki_der = verifying_key
            .to_public_key_der()
            .map_err(|e| Error::Configuration(format!("Failed to encode public key: {}", e)))?;

        // Hash the SPKI DER
        let mut hasher = Sha256::new();
        hasher.update(spki_der.as_bytes());
        let digest = hasher.finalize();

        // Encode as hex
        let key_id_hash = hex::encode(digest);

        Ok(format!("device:{}", key_id_hash))
    }

    /// Generate a JWT token for the device
    ///
    /// The token will be valid for 24 hours and include the necessary scopes
    /// for device connection and TURN server access.
    ///
    /// # Returns
    ///
    /// A JWT token string that can be used to authenticate with the signaling service
    pub fn generate_token(&self) -> Result<String> {
        // Get the key ID
        let key_id = self.get_key_id()?;

        // Create the resource URN
        let resource = format!("urn:nabto:webrtc:{}:{}", self.product_id, self.device_id);

        // Get current timestamp
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| Error::Other(format!("System time error: {}", e)))?
            .as_secs();

        // Token valid for 1 day
        let expiration = now + (24 * 60 * 60);

        // Create claims
        let claims = Claims {
            nbf: now,
            iat: now,
            exp: expiration,
            scope: "device:connect turn".to_string(),
            resource,
        };

        // Create header with key ID
        let mut header = Header::new(Algorithm::ES256);
        header.kid = Some(key_id);
        header.typ = Some("JWT".to_string());

        // Create encoding key from PEM
        let encoding_key = EncodingKey::from_ec_pem(self.private_key.as_bytes()).map_err(|e| {
            Error::Configuration(format!("Failed to parse private key for signing: {}", e))
        })?;

        // Generate the JWT
        let token = encode(&header, &claims, &encoding_key)
            .map_err(|e| Error::Other(format!("Failed to generate JWT: {}", e)))?;

        Ok(token)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Valid PKCS#8 EC P-256 private key for testing
    const TEST_PRIVATE_KEY: &str = r#"-----BEGIN PRIVATE KEY-----
MIGHAgEAMBMGByqGSM49AgEGCCqGSM49AwEHBG0wawIBAQQg+oH56dcdavHeaJhO
kroaroSWQLA/A+6sCQRb8g+Ip4yhRANCAATc3dMAfNPk6dmWOLoYdOLwsuC6OQ4x
1vOzzk4iv+0GYsurToJkZ7FohDBPiup+FNLWyaiUnXgdWay/vLjH2P1k
-----END PRIVATE KEY-----"#;

    #[test]
    fn test_token_generator_creation() {
        let generator = DeviceTokenGenerator::new(
            "wp-test".to_string(),
            "wd-test".to_string(),
            TEST_PRIVATE_KEY.to_string(),
        );
        assert_eq!(generator.product_id, "wp-test");
        assert_eq!(generator.device_id, "wd-test");
    }

    #[test]
    fn test_generate_token() {
        let generator = DeviceTokenGenerator::new(
            "wp-test".to_string(),
            "wd-test".to_string(),
            TEST_PRIVATE_KEY.to_string(),
        );

        let result = generator.generate_token();
        assert!(
            result.is_ok(),
            "Token generation should succeed: {:?}",
            result.err()
        );

        let token = result.unwrap();
        assert!(!token.is_empty(), "Token should not be empty");

        // JWT tokens have 3 parts separated by dots
        let parts: Vec<&str> = token.split('.').collect();
        assert_eq!(parts.len(), 3, "JWT should have 3 parts");
    }

    #[test]
    fn test_get_key_id() {
        let generator = DeviceTokenGenerator::new(
            "wp-test".to_string(),
            "wd-test".to_string(),
            TEST_PRIVATE_KEY.to_string(),
        );

        let key_id = generator.get_key_id();
        assert!(
            key_id.is_ok(),
            "Key ID generation should succeed: {:?}",
            key_id.err()
        );

        let kid = key_id.unwrap();
        assert_eq!(
            kid,
            "device:d253a3df618f08d76696ddc66fdc35de5d75ed12e1908503b1575e005e79a516"
        );
    }
}
