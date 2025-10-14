//! Test instance helper for integration tests

use super::test_client::{DeviceTestOptions, TestClient};
use nabto_webrtc_sdk::{SignalingDevice, SignalingDeviceOptions, ConnectionState};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

/// Helper struct for managing a device test instance
pub struct DeviceTestInstance {
    pub product_id: String,
    pub device_id: String,
    pub endpoint_url: String,
    pub test_id: String,
    pub access_token: String,
    pub observed_states: Arc<Mutex<Vec<ConnectionState>>>,
    test_client: TestClient,
}

impl DeviceTestInstance {
    /// Create a new device test instance with the given options
    pub async fn create(options: DeviceTestOptions) -> Result<Self, Box<dyn std::error::Error>> {
        let test_client = TestClient::new();
        let response = test_client.create_device_test(options).await?;

        Ok(Self {
            product_id: response.product_id,
            device_id: response.device_id,
            endpoint_url: response.endpoint_url,
            test_id: response.test_id,
            access_token: response.access_token,
            observed_states: Arc::new(Mutex::new(Vec::new())),
            test_client,
        })
    }

    /// Create a SignalingDevice configured for this test
    pub fn create_signaling_device(&self) -> SignalingDevice {
        let access_token = self.access_token.clone();
        let _observed_states = self.observed_states.clone();

        let token_generator = Box::new(move || {
            let token = access_token.clone();
            Box::pin(async move { Ok(token) })
                as std::pin::Pin<
                    Box<dyn std::future::Future<Output = Result<String, nabto_webrtc_sdk::Error>> + Send>,
                >
        });

        let device = SignalingDevice::new(SignalingDeviceOptions {
            endpoint_url: Some(self.endpoint_url.clone()),
            product_id: self.product_id.clone(),
            device_id: self.device_id.clone(),
            token_generator,
        });

        // TODO: Add event listener for connection state changes
        // This requires implementing an event emitter pattern in the SDK
        // For now, we'll need to manually track states in tests

        device
    }

    /// Record a connection state change (to be called manually from tests for now)
    pub fn record_state(&self, state: ConnectionState) {
        let mut states = self.observed_states.lock().unwrap();
        states.push(state);
    }

    /// Get the currently observed states
    pub fn get_observed_states(&self) -> Vec<ConnectionState> {
        self.observed_states.lock().unwrap().clone()
    }

    /// Wait for specific connection states to be observed
    /// This is a polling-based implementation until we add proper event listeners
    pub async fn wait_for_observed_states(
        &self,
        expected: Vec<ConnectionState>,
        timeout: Duration,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let start = Instant::now();
        loop {
            {
                let observed = self.observed_states.lock().unwrap();
                if *observed == expected {
                    return Ok(());
                }
            }

            if start.elapsed() > timeout {
                let observed = self.observed_states.lock().unwrap();
                return Err(format!(
                    "Timeout waiting for states.\nExpected: {:?}\nGot: {:?}",
                    expected, *observed
                )
                .into());
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Wait for the device to reach a specific state
    pub async fn wait_for_state(
        &self,
        device: &SignalingDevice,
        expected_state: ConnectionState,
        timeout: Duration,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let start = Instant::now();
        loop {
            if device.connection_state() == expected_state {
                return Ok(());
            }

            if start.elapsed() > timeout {
                return Err(format!(
                    "Timeout waiting for state {:?}. Current state: {:?}",
                    expected_state,
                    device.connection_state()
                )
                .into());
            }

            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    }

    /// Create a test client for this device
    pub async fn create_client(&self) -> Result<String, Box<dyn std::error::Error>> {
        let response = self.test_client.create_client(&self.test_id).await?;
        Ok(response.client_id)
    }

    /// Connect a client to the device
    pub async fn connect_client(&self, client_id: &str) -> Result<(), Box<dyn std::error::Error>> {
        self.test_client
            .connect_client(&self.test_id, client_id)
            .await?;
        Ok(())
    }

    /// Disconnect a client from the device
    pub async fn disconnect_client(
        &self,
        client_id: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.test_client
            .disconnect_client(&self.test_id, client_id)
            .await?;
        Ok(())
    }

    /// Send messages from a client to the device
    pub async fn client_send_messages(
        &self,
        client_id: &str,
        messages: Vec<String>,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.test_client
            .client_send_messages(&self.test_id, client_id, messages)
            .await?;
        Ok(())
    }

    /// Wait for client to receive messages
    pub async fn client_wait_for_messages(
        &self,
        client_id: &str,
        messages: Vec<String>,
        timeout_ms: u64,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let received = self
            .test_client
            .client_wait_for_messages(&self.test_id, client_id, messages, timeout_ms)
            .await?;
        Ok(received)
    }

    /// Disconnect the device (server-side)
    pub async fn disconnect_device(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.test_client.disconnect_device(&self.test_id).await?;
        Ok(())
    }

    /// Drop device messages (for reliability testing)
    pub async fn drop_device_messages(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.test_client
            .drop_device_messages(&self.test_id)
            .await?;
        Ok(())
    }

    /// Drop client messages (for reliability testing)
    pub async fn drop_client_messages(
        &self,
        client_id: &str,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.test_client
            .drop_client_messages(&self.test_id, client_id)
            .await?;
        Ok(())
    }

    /// Send a new unknown message type (for protocol extensibility testing)
    pub async fn send_new_message_type(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.test_client
            .send_new_message_type(&self.test_id)
            .await?;
        Ok(())
    }

    /// Destroy the test instance (cleanup)
    pub async fn destroy(self) -> Result<(), Box<dyn std::error::Error>> {
        self.test_client.delete_device_test(&self.test_id).await?;
        Ok(())
    }
}
