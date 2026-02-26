//! HTTP API implementation for the Nabto WebRTC Signaling Service
//!
//! Handles device connect and ICE servers requests.

#![allow(dead_code)]

use crate::{Error, Result};
use log::debug;
use serde::{Deserialize, Serialize};

/// HTTP client for the Nabto WebRTC Signaling Service
#[derive(Clone)]
pub struct HttpApi {
    endpoint_url: String,
    product_id: String,
    device_id: String,
    client: reqwest::Client,
}

#[derive(Debug, Serialize)]
struct ClientConnectRequest {
    #[serde(rename = "deviceId")]
    device_id: String,
    #[serde(rename = "productId")]
    product_id: String,
}

#[derive(Debug, Deserialize)]
pub struct ClientConnectResponse {
    #[serde(rename = "signalingUrl")]
    pub signaling_url: String,
    #[serde(rename = "deviceOnline")]
    pub device_online: Option<bool>,
    #[serde(rename = "channelId")]
    pub channel_id: Option<String>,
    #[serde(rename = "reconnectToken")]
    pub reconnect_token: Option<String>,
}

/// Request body for device connect
#[derive(Debug, Serialize)]
struct DeviceConnectRequest {
    #[serde(rename = "deviceId")]
    device_id: String,
    #[serde(rename = "productId")]
    product_id: String,
}

/// Response from device connect
#[derive(Debug, Deserialize)]
pub struct DeviceConnectResponse {
    #[serde(rename = "signalingUrl")]
    pub signaling_url: String,
}

/// Request body for ICE servers
#[derive(Debug, Serialize)]
struct IceServersRequest {
    #[serde(rename = "deviceId")]
    device_id: String,
    #[serde(rename = "productId")]
    product_id: String,
}

/// ICE server configuration from response
#[derive(Debug, Deserialize, Clone)]
pub struct IceServer {
    pub urls: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credential: Option<String>,
}

/// Response from ICE servers request
#[derive(Debug, Deserialize)]
struct IceServersResponse {
    #[serde(rename = "iceServers")]
    ice_servers: Vec<IceServer>,
}

/// HTTP error response body
#[derive(Debug, Deserialize)]
struct ErrorResponse {
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    code: Option<String>,
}

impl HttpApi {
    /// Create a new HTTP API client
    pub fn new(endpoint_url: String, product_id: String, device_id: String) -> Self {
        Self {
            endpoint_url,
            product_id,
            device_id,
            client: reqwest::Client::builder()
                .connect_timeout(std::time::Duration::from_secs(10))
                .timeout(std::time::Duration::from_secs(30))
                .pool_max_idle_per_host(0)
                .build()
                .expect("Failed to build HTTP client"),
        }
    }

    pub async fn client_connect(&self, auth_token: Option<&str>) -> Result<ClientConnectResponse> {
        let url = format!("{}/v1/client/connect", self.endpoint_url);

        let request_body = ClientConnectRequest {
            product_id: self.product_id.clone(),
            device_id: self.device_id.clone(),
        };

        let mut request = self
            .client
            .post(&url)
            .header("Content-Type", "application/json")
            .json(&request_body);

        if let Some(token) = auth_token {
            request = request.header("Authorization", format!("Bearer {}", token));
        }

        debug!("Sending POST {}", url);
        let response = request.send().await.map_err(|e| {
            Error::Connection(format!("Failed to send client connect request: {}", e))
        })?;
        debug!("Received {} from POST {}", response.status(), url);

        self.handle_response(response).await
    }

    /// Make a device connect request
    ///
    /// POST /v1/device/connect
    /// Returns the signaling URL for WebSocket connection
    pub async fn device_connect(&self, access_token: &str) -> Result<DeviceConnectResponse> {
        let url = format!("{}/v1/device/connect", self.endpoint_url);

        let request_body = DeviceConnectRequest {
            device_id: self.device_id.clone(),
            product_id: self.product_id.clone(),
        };

        debug!("Sending POST {}", url);
        let response = self
            .client
            .post(&url)
            .header("Content-Type", "application/json")
            .header("Authorization", format!("Bearer {}", access_token))
            .json(&request_body)
            .send()
            .await
            .map_err(|e| {
                Error::Connection(format!("Failed to send device connect request: {}", e))
            })?;
        debug!("Received {} from POST {}", response.status(), url);

        self.handle_response(response).await
    }

    /// Request ICE servers from the signaling service
    ///
    /// POST /v1/ice-servers
    /// Returns STUN and TURN server configurations
    pub async fn request_ice_servers(&self, access_token: &str) -> Result<Vec<IceServer>> {
        let url = format!("{}/v1/ice-servers", self.endpoint_url);

        let request_body = IceServersRequest {
            device_id: self.device_id.clone(),
            product_id: self.product_id.clone(),
        };

        debug!("Sending POST {}", url);
        let response = self
            .client
            .post(&url)
            .header("Content-Type", "application/json")
            .header("Authorization", format!("Bearer {}", access_token))
            .json(&request_body)
            .send()
            .await
            .map_err(|e| Error::Connection(format!("Failed to send ICE servers request: {}", e)))?;
        debug!("Received {} from POST {}", response.status(), url);

        let ice_response: IceServersResponse = self.handle_response(response).await?;
        Ok(ice_response.ice_servers)
    }

    /// Handle HTTP response with error checking
    async fn handle_response<T: for<'de> Deserialize<'de>>(
        &self,
        response: reqwest::Response,
    ) -> Result<T> {
        let status = response.status();

        if status.is_success() {
            response
                .json::<T>()
                .await
                .map_err(|e| Error::Other(format!("Failed to parse response: {}", e)))
        } else {
            // Try to parse error response
            let error_body = response.json::<ErrorResponse>().await.ok();

            let error_message = error_body
                .as_ref()
                .map(|e| e.message.clone())
                .unwrap_or_else(|| format!("HTTP error: {}", status));

            match status.as_u16() {
                400 => Err(Error::Configuration(format!(
                    "Bad request: {}",
                    error_message
                ))),
                401 => Err(Error::Configuration(format!(
                    "Unauthorized: {}",
                    error_message
                ))),
                403 => Err(Error::Configuration(format!(
                    "Forbidden: {}",
                    error_message
                ))),
                404 => {
                    if let Some(code) = error_body.and_then(|e| e.code) {
                        match code.as_str() {
                            "PRODUCT_ID_NOT_FOUND" => {
                                Err(Error::Configuration("Product ID not found".to_string()))
                            }
                            "DEVICE_ID_NOT_FOUND" => {
                                Err(Error::Configuration("Device ID not found".to_string()))
                            }
                            _ => Err(Error::Other(format!("Not found: {}", error_message))),
                        }
                    } else {
                        Err(Error::Other(format!("Not found: {}", error_message)))
                    }
                }
                429 => Err(Error::Other(format!(
                    "Too many requests: {}",
                    error_message
                ))),
                500..=599 => Err(Error::Other(format!("Server error: {}", error_message))),
                _ => Err(Error::Other(format!(
                    "HTTP error {}: {}",
                    status, error_message
                ))),
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_http_api_creation() {
        let api = HttpApi::new(
            "https://example.nabto.net".to_string(),
            "wp-test".to_string(),
            "wd-test".to_string(),
        );
        assert_eq!(api.endpoint_url, "https://example.nabto.net");
        assert_eq!(api.product_id, "wp-test");
        assert_eq!(api.device_id, "wd-test");
    }
}
