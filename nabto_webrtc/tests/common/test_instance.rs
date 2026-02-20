//! Test instance helper for integration tests

use super::test_client::{DeviceTestOptions, TestClient};
use nabto_webrtc::common::{SignalingConnectionState, WebSocketHandle};
use nabto_webrtc::device::{DeviceEvent, SignalingDevice, SignalingDeviceOptions};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::sync::{mpsc, Mutex as TokioMutex};

/// Helper struct for managing a device test instance
pub struct DeviceTestInstance {
    pub product_id: String,
    pub device_id: String,
    pub endpoint_url: String,
    pub test_id: String,
    pub access_token: String,
    pub observed_states: Arc<std::sync::Mutex<Vec<SignalingConnectionState>>>,
    /// Optional heartbeat interval override for the device WebSocket connection.
    /// When None, uses the library default (30s).
    pub heartbeat_interval: Option<Duration>,
    test_client: TestClient,
}

/// Handle to a running SignalingDevice
pub struct DeviceHandle {
    state_rx: Arc<TokioMutex<tokio::sync::watch::Receiver<SignalingConnectionState>>>,
    stop_tx: Arc<TokioMutex<Option<tokio::sync::oneshot::Sender<()>>>>,
    command_tx: mpsc::Sender<nabto_webrtc::device::DeviceCommand>,
    _task: tokio::task::JoinHandle<()>,
}

impl DeviceHandle {
    /// Get the current connection state
    pub async fn connection_state(&self) -> SignalingConnectionState {
        *self.state_rx.lock().await.borrow()
    }

    /// Stop the device
    pub async fn stop(&self) {
        if let Some(tx) = self.stop_tx.lock().await.take() {
            let _ = tx.send(());
        }
    }

    /// Trigger check_alive on the device to detect stale connections
    pub async fn check_alive(&self) -> Result<(), Box<dyn std::error::Error>> {
        self.command_tx
            .send(nabto_webrtc::device::DeviceCommand::CheckAlive)
            .await
            .map_err(|e| format!("Failed to send check_alive command: {}", e).into())
    }

    /// Wait for the device to reach a specific state
    pub async fn wait_for_state(
        &self,
        expected_state: SignalingConnectionState,
        timeout: Duration,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let start = Instant::now();
        let mut rx = self.state_rx.lock().await.clone();

        loop {
            let current_state = *rx.borrow_and_update();
            if current_state == expected_state {
                return Ok(());
            }

            if start.elapsed() > timeout {
                return Err(format!(
                    "Timeout waiting for state {:?}. Current state: {:?}",
                    expected_state, current_state
                )
                .into());
            }

            // Wait for state change or timeout
            tokio::select! {
                _ = rx.changed() => {
                    // State changed, loop to check if it matches
                }
                _ = tokio::time::sleep(Duration::from_millis(100)) => {
                    // Periodic check
                }
            }
        }
    }
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
            heartbeat_interval: None,
            test_client,
        })
    }

    /// Create a SignalingDevice configured for this test
    /// Returns the device, a receiver for device events, and a sender for device commands
    pub fn create_signaling_device(
        &self,
    ) -> (
        SignalingDevice,
        mpsc::Receiver<DeviceEvent>,
        mpsc::Sender<nabto_webrtc::device::DeviceCommand>,
    ) {
        let access_token = self.access_token.clone();

        let token_generator: nabto_webrtc::device::TokenGenerator = std::sync::Arc::new(move || {
            let token = access_token.clone();
            Box::pin(async move { Ok(token) })
                as std::pin::Pin<
                    Box<
                        dyn std::future::Future<Output = Result<String, nabto_webrtc::Error>>
                            + Send,
                    >,
                >
        });

        let mut builder = SignalingDeviceOptions::builder(
            self.product_id.clone(),
            self.device_id.clone(),
            token_generator,
        )
        .endpoint_url(self.endpoint_url.clone());

        if let Some(interval) = self.heartbeat_interval {
            builder = builder.heartbeat_interval(Some(interval));
        }

        SignalingDevice::new(builder.build())
    }

    /// Create and start a SignalingDevice, returning a handle to it
    /// The device will run in the background until stop() is called on the handle
    pub fn start_signaling_device(&self) -> (DeviceHandle, mpsc::Receiver<DeviceEvent>) {
        let (mut device, mut event_rx_from_device, command_tx) = self.create_signaling_device();

        // Create channels for state tracking and stop signal
        let (state_tx, state_rx) = tokio::sync::watch::channel(SignalingConnectionState::New);
        let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel();

        // Create a new event channel that we'll forward events to
        let (event_tx, event_rx) = mpsc::channel(32);

        // Spawn task that runs the device and monitors events
        let task = tokio::spawn(async move {
            // Spawn a task to forward events and update state
            let event_tx_clone = event_tx.clone();
            let state_tx_clone = state_tx.clone();
            tokio::spawn(async move {
                while let Some(event) = event_rx_from_device.recv().await {
                    // Update state if it's a StateChanged event
                    if let nabto_webrtc::device::DeviceEvent::StateChanged { new_state, .. } =
                        &event
                    {
                        let _ = state_tx_clone.send(*new_state);
                    }
                    // Forward the event
                    let _ = event_tx_clone.send(event).await;
                }
            });

            // Run the device with stop signal handling
            tokio::select! {
                result = device.run() => {
                    if let Err(e) = result {
                        eprintln!("Device run() error: {:?}", e);
                    }
                }
                _ = &mut stop_rx => {
                    device.stop().await;
                }
            }
        });

        let handle = DeviceHandle {
            state_rx: Arc::new(TokioMutex::new(state_rx)),
            stop_tx: Arc::new(TokioMutex::new(Some(stop_tx))),
            command_tx,
            _task: task,
        };

        (handle, event_rx)
    }

    /// Record a connection state change (to be called manually from tests for now)
    pub fn record_state(&self, state: SignalingConnectionState) {
        let mut states = self.observed_states.lock().unwrap();
        states.push(state);
    }

    /// Get the currently observed states
    pub fn get_observed_states(&self) -> Vec<SignalingConnectionState> {
        self.observed_states.lock().unwrap().clone()
    }

    /// Wait for specific connection states to be observed
    /// This is a polling-based implementation until we add proper event listeners
    pub async fn wait_for_observed_states(
        &self,
        expected: Vec<SignalingConnectionState>,
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
        expected_state: SignalingConnectionState,
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
        self.test_client.drop_device_messages(&self.test_id).await?;
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
