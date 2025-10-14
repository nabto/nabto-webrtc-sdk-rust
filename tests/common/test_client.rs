//! HTTP client for communicating with the integration test server

use serde::{Deserialize, Serialize};

const BASE_URL: &str = "http://localhost:13745";

/// Options for creating a device test instance
#[derive(Debug, Serialize, Default)]
#[serde(rename_all = "camelCase")]
pub struct DeviceTestOptions {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fail_http: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub fail_ws: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub extra_device_connect_response_data: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub product_id_not_found: Option<bool>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub device_id_not_found: Option<bool>,
}

/// Response from creating a test device instance
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TestDeviceResponse {
    pub product_id: String,
    pub device_id: String,
    pub endpoint_url: String,
    pub test_id: String,
    pub access_token: String,
}

/// Response from creating a test client
#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateClientResponse {
    pub client_id: String,
}

/// Request body for sending messages
#[derive(Debug, Serialize)]
struct SendMessagesRequest {
    messages: Vec<String>,
}

/// Request body for waiting for messages
#[derive(Debug, Serialize)]
struct WaitForMessagesRequest {
    messages: Vec<String>,
    timeout: u64,
}

/// Response from waiting for messages
#[derive(Debug, Deserialize)]
struct WaitForMessagesResponse {
    messages: Option<Vec<String>>,
}

/// HTTP client for the integration test server
pub struct TestClient {
    client: reqwest::Client,
    base_url: String,
}

impl TestClient {
    /// Create a new test client
    pub fn new() -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url: BASE_URL.to_string(),
        }
    }

    /// Create a new test client with custom base URL
    pub fn with_url(base_url: String) -> Self {
        Self {
            client: reqwest::Client::new(),
            base_url,
        }
    }

    /// Create a new device test instance
    pub async fn create_device_test(
        &self,
        options: DeviceTestOptions,
    ) -> Result<TestDeviceResponse, reqwest::Error> {
        self.client
            .post(&format!("{}/test/device", self.base_url))
            .json(&options)
            .send()
            .await?
            .json()
            .await
    }

    /// Delete a device test instance
    pub async fn delete_device_test(&self, test_id: &str) -> Result<(), reqwest::Error> {
        self.client
            .delete(&format!("{}/test/device/{}", self.base_url, test_id))
            .send()
            .await?;
        Ok(())
    }

    /// Create a client for the test device
    pub async fn create_client(
        &self,
        test_id: &str,
    ) -> Result<CreateClientResponse, reqwest::Error> {
        self.client
            .post(&format!(
                "{}/test/device/{}/clients",
                self.base_url, test_id
            ))
            .send()
            .await?
            .json()
            .await
    }

    /// Connect a client to the device
    pub async fn connect_client(
        &self,
        test_id: &str,
        client_id: &str,
    ) -> Result<(), reqwest::Error> {
        self.client
            .post(&format!(
                "{}/test/device/{}/clients/{}/connect",
                self.base_url, test_id, client_id
            ))
            .send()
            .await?;
        Ok(())
    }

    /// Disconnect a client from the device
    pub async fn disconnect_client(
        &self,
        test_id: &str,
        client_id: &str,
    ) -> Result<(), reqwest::Error> {
        self.client
            .post(&format!(
                "{}/test/device/{}/clients/{}/disconnect",
                self.base_url, test_id, client_id
            ))
            .send()
            .await?;
        Ok(())
    }

    /// Send messages from a client to the device
    pub async fn client_send_messages(
        &self,
        test_id: &str,
        client_id: &str,
        messages: Vec<String>,
    ) -> Result<(), reqwest::Error> {
        self.client
            .post(&format!(
                "{}/test/device/{}/clients/{}/send-messages",
                self.base_url, test_id, client_id
            ))
            .json(&SendMessagesRequest { messages })
            .send()
            .await?;
        Ok(())
    }

    /// Wait for messages to be received by the client
    pub async fn client_wait_for_messages(
        &self,
        test_id: &str,
        client_id: &str,
        messages: Vec<String>,
        timeout_ms: u64,
    ) -> Result<Vec<String>, reqwest::Error> {
        let response: WaitForMessagesResponse = self
            .client
            .post(&format!(
                "{}/test/device/{}/clients/{}/wait-for-messages",
                self.base_url, test_id, client_id
            ))
            .json(&WaitForMessagesRequest {
                messages,
                timeout: timeout_ms,
            })
            .send()
            .await?
            .json()
            .await?;

        Ok(response.messages.unwrap_or_default())
    }

    /// Disconnect the device from the WebSocket
    pub async fn disconnect_device(&self, test_id: &str) -> Result<(), reqwest::Error> {
        self.client
            .post(&format!(
                "{}/test/device/{}/disconnect-device",
                self.base_url, test_id
            ))
            .send()
            .await?;
        Ok(())
    }

    /// Drop device messages (for reliability testing)
    pub async fn drop_device_messages(&self, test_id: &str) -> Result<(), reqwest::Error> {
        self.client
            .post(&format!(
                "{}/test/device/{}/drop-device-messages",
                self.base_url, test_id
            ))
            .send()
            .await?;
        Ok(())
    }

    /// Drop client messages (for reliability testing)
    pub async fn drop_client_messages(
        &self,
        test_id: &str,
        client_id: &str,
    ) -> Result<(), reqwest::Error> {
        self.client
            .post(&format!(
                "{}/test/device/{}/clients/{}/drop-client-messages",
                self.base_url, test_id, client_id
            ))
            .send()
            .await?;
        Ok(())
    }

    /// Send a new unknown message type (for protocol extensibility testing)
    pub async fn send_new_message_type(&self, test_id: &str) -> Result<(), reqwest::Error> {
        self.client
            .post(&format!(
                "{}/test/device/{}/send-new-message-type",
                self.base_url, test_id
            ))
            .send()
            .await?;
        Ok(())
    }
}

impl Default for TestClient {
    fn default() -> Self {
        Self::new()
    }
}
