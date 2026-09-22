//! Device Token Generator
//!
//! Utility for generating JWT tokens for devices to connect to the Nabto WebRTC Signaling Service.

use crate::{Error, Result};
use jsonwebtoken::{encode, Algorithm, EncodingKey, Header};
use p256::ecdsa::SigningKey;
use p256::pkcs8::{DecodePrivateKey, EncodePublicKey};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

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
///
/// The private key is parsed once, when the generator is constructed: deriving
/// the key id requires an elliptic curve point multiplication, which dominates
/// the cost of producing a token. Construct one generator and reuse it rather
/// than building one per token.
pub struct DeviceTokenGenerator {
    /// Pre-parsed signing key.
    encoding_key: EncodingKey,
    /// Pre-derived key id, see [`DeviceTokenGenerator::key_id`].
    key_id: String,
    /// Pre-rendered `resource` claim.
    resource: String,
    /// How long issued tokens remain valid.
    token_lifetime: Duration,
}

/// Default lifetime of a generated device token.
pub const DEFAULT_TOKEN_LIFETIME: Duration = Duration::from_secs(24 * 60 * 60);

impl DeviceTokenGenerator {
    /// Create a new DeviceTokenGenerator
    ///
    /// Parses `private_key`, so an invalid key is reported here rather than on
    /// the first connect attempt.
    ///
    /// # Arguments
    ///
    /// * `product_id` - Product ID of the device
    /// * `device_id` - Device ID of the device
    /// * `private_key` - Private key in PEM format (PKCS#8)
    pub fn new(product_id: String, device_id: String, private_key: String) -> Result<Self> {
        Self::with_token_lifetime(product_id, device_id, private_key, DEFAULT_TOKEN_LIFETIME)
    }

    /// Create a new DeviceTokenGenerator issuing tokens with a given lifetime.
    ///
    /// See [`DeviceTokenGenerator::new`]; the default lifetime is
    /// [`DEFAULT_TOKEN_LIFETIME`].
    pub fn with_token_lifetime(
        product_id: String,
        device_id: String,
        private_key: String,
        token_lifetime: Duration,
    ) -> Result<Self> {
        let key_id = Self::derive_key_id(&private_key)?;

        let encoding_key = EncodingKey::from_ec_pem(private_key.as_bytes()).map_err(|e| {
            Error::Configuration(format!("Failed to parse private key for signing: {}", e))
        })?;

        Ok(Self {
            encoding_key,
            key_id,
            resource: format!("urn:nabto:webrtc:{}:{}", product_id, device_id),
            token_lifetime,
        })
    }

    /// The key id this generator puts in the `kid` header of its tokens.
    ///
    /// Computed as `"device:" + hex(sha256(SubjectPublicKeyInfo))`. This is the
    /// value the signaling service looks up to find the device's public key, so
    /// a mismatch with what was registered shows up as an HTTP 401 on connect;
    /// log this to check it against the registered key.
    pub fn key_id(&self) -> &str {
        &self.key_id
    }

    /// Derive the key id from a PEM encoded private key.
    ///
    /// The key id is computed as "device:" + hex(sha256(SubjectPublicKeyInfo))
    fn derive_key_id(private_key: &str) -> Result<String> {
        // Parse the private key
        let signing_key = SigningKey::from_pkcs8_pem(private_key)
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
    /// The token is valid for the generator's token lifetime (24 hours by
    /// default) and includes the necessary scopes for device connection and
    /// TURN server access.
    ///
    /// # Returns
    ///
    /// A JWT token string that can be used to authenticate with the signaling service
    pub fn generate_token(&self) -> Result<String> {
        // Get current timestamp
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map_err(|e| Error::Other(format!("System time error: {}", e)))?
            .as_secs();

        let expiration = now + self.token_lifetime.as_secs();

        // Create claims
        let claims = Claims {
            nbf: now,
            iat: now,
            exp: expiration,
            scope: "device:connect turn".to_string(),
            resource: self.resource.clone(),
        };

        // Create header with key ID
        let mut header = Header::new(Algorithm::ES256);
        header.kid = Some(self.key_id.clone());
        header.typ = Some("JWT".to_string());

        // Generate the JWT
        let token = encode(&header, &claims, &self.encoding_key)
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

    fn test_generator() -> DeviceTokenGenerator {
        DeviceTokenGenerator::new(
            "wp-test".to_string(),
            "wd-test".to_string(),
            TEST_PRIVATE_KEY.to_string(),
        )
        .expect("test key should parse")
    }

    #[test]
    fn test_token_generator_creation() {
        let generator = test_generator();
        assert_eq!(generator.resource, "urn:nabto:webrtc:wp-test:wd-test");
        assert_eq!(generator.token_lifetime, DEFAULT_TOKEN_LIFETIME);
    }

    #[test]
    fn test_invalid_private_key_fails_at_construction() {
        let result = DeviceTokenGenerator::new(
            "wp-test".to_string(),
            "wd-test".to_string(),
            "-----BEGIN PRIVATE KEY-----\nbm90YWtleQ==\n-----END PRIVATE KEY-----".to_string(),
        );
        assert!(
            result.is_err(),
            "an invalid key should be rejected by new()"
        );
    }

    #[test]
    fn test_generate_token() {
        let generator = test_generator();

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
    fn test_key_id() {
        let generator = test_generator();

        assert_eq!(
            generator.key_id(),
            "device:d253a3df618f08d76696ddc66fdc35de5d75ed12e1908503b1575e005e79a516"
        );
    }

    #[test]
    fn test_key_id_is_in_token_header() {
        let generator = test_generator();
        let token = generator.generate_token().unwrap();

        let header = jsonwebtoken::decode_header(&token).unwrap();
        assert_eq!(header.kid.as_deref(), Some(generator.key_id()));
        assert_eq!(header.alg, Algorithm::ES256);
    }

    #[test]
    fn test_token_lifetime_is_honoured() {
        let generator = DeviceTokenGenerator::with_token_lifetime(
            "wp-test".to_string(),
            "wd-test".to_string(),
            TEST_PRIVATE_KEY.to_string(),
            Duration::from_secs(60),
        )
        .unwrap();

        let token = generator.generate_token().unwrap();
        // Decode without verifying: we only care about the claims we set.
        let payload = token.split('.').nth(1).unwrap();
        let decoded =
            base64::Engine::decode(&base64::engine::general_purpose::URL_SAFE_NO_PAD, payload)
                .unwrap();
        let claims: serde_json::Value = serde_json::from_slice(&decoded).unwrap();

        let exp = claims["exp"].as_u64().unwrap();
        let iat = claims["iat"].as_u64().unwrap();
        assert_eq!(exp - iat, 60);
        assert_eq!(claims["resource"], "urn:nabto:webrtc:wp-test:wd-test");
    }
}
