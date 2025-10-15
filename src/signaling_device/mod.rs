//! Signaling Device module
//!
//! This module contains the core implementation for device-side WebRTC signaling.

mod channel;
mod connection;
mod http;
mod reliability;
mod routing;
mod state;
mod token;

pub use channel::{SignalingChannel, SignalingChannelEventHandler, SignalingService};
pub use connection::{ConnectionEvent, WebSocketConfig, WebSocketConnection, WebSocketHandle};
pub use http::IceServer;
pub use state::{ChannelState, ConnectionState};
pub use token::DeviceTokenGenerator;

use http::HttpApi;
use routing::{ErrorInfo, RoutingMessage};

use crate::{Error, Result};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_tungstenite::connect_async;

/// Minimum time between a successful connection and reconnect counter reset (10 seconds)
const RECONNECT_COUNTER_RESET_TIMEOUT: Duration = Duration::from_secs(10);

/// Maximum reconnect wait time (60 seconds)
const MAX_RECONNECT_WAIT_SECONDS: u32 = 60;

/// Callback type for generating access tokens
pub type TokenGenerator =
    Box<dyn Fn() -> Pin<Box<dyn Future<Output = Result<String>> + Send>> + Send + Sync>;

/// Events emitted by the SignalingDevice
#[derive(Debug)]
pub enum DeviceEvent {
    /// A new signaling channel is ready
    NewChannel {
        channel: SignalingChannel,
        authorized: bool,
    },

    /// Connection state changed
    StateChanged {
        old_state: ConnectionState,
        new_state: ConnectionState,
    },
}

/// Options for creating a SignalingDevice
pub struct SignalingDeviceOptions {
    /// Optional URL for the signaling service
    pub endpoint_url: Option<String>,

    /// The product ID (e.g., "wp-abcdefghi")
    pub product_id: String,

    /// The device ID (e.g., "wd-jklmnopqr")
    pub device_id: String,

    /// Token generator called when a new access token is needed
    pub token_generator: TokenGenerator,
}

/// The main SignalingDevice interface
pub struct SignalingDevice {
    http_api: HttpApi,
    options: SignalingDeviceOptions,
    state: ConnectionState,

    // Event channel for emitting device events
    device_event_tx: mpsc::Sender<DeviceEvent>,

    // WebSocket connection
    ws_handle: Option<WebSocketHandle>,
    ws_event_rx: Option<mpsc::Receiver<ConnectionEvent>>,

    // Channel management
    channels: HashMap<String, SignalingChannel>,

    // Retry state
    reconnect_counter: u32,
    connected_at: Option<Instant>,
    should_stop: bool,
}

impl SignalingDevice {
    /// Create a new SignalingDevice
    ///
    /// Returns the device instance and a receiver for device events.
    /// The receiver should be polled to handle NewChannel events and other device events.
    pub fn new(options: SignalingDeviceOptions) -> (Self, mpsc::Receiver<DeviceEvent>) {
        let endpoint_url = options
            .endpoint_url
            .clone()
            .unwrap_or_else(|| format!("https://{}.webrtc.nabto.net", options.product_id));

        let http_api = HttpApi::new(
            endpoint_url,
            options.product_id.clone(),
            options.device_id.clone(),
        );

        let (device_event_tx, device_event_rx) = mpsc::channel(32);

        let device = Self {
            http_api,
            options,
            state: ConnectionState::New,
            device_event_tx,
            ws_handle: None,
            ws_event_rx: None,
            channels: HashMap::new(),
            reconnect_counter: 0,
            connected_at: None,
            should_stop: false,
        };

        (device, device_event_rx)
    }

    /// Run the signaling device event loop
    ///
    /// This method runs continuously, handling connection, reconnection, and message processing.
    /// It will only return when stop() is called or an unrecoverable error occurs.
    ///
    /// The user should spawn this on a tokio task:
    /// ```no_run
    /// # use nabto_webrtc_sdk::{SignalingDevice, SignalingDeviceOptions};
    /// # #[tokio::main]
    /// # async fn main() {
    /// # let token_generator = Box::new(|| {
    /// #     Box::pin(async { Ok("token".to_string()) })
    /// #         as std::pin::Pin<Box<dyn std::future::Future<Output = Result<String, nabto_webrtc_sdk::Error>> + Send>>
    /// # });
    /// # let options = SignalingDeviceOptions {
    /// #     endpoint_url: None,
    /// #     product_id: "wp-test".to_string(),
    /// #     device_id: "wd-test".to_string(),
    /// #     token_generator,
    /// # };
    /// let (mut device, event_rx) = SignalingDevice::new(options);
    ///
    /// // Spawn the device run loop
    /// tokio::spawn(async move {
    ///     if let Err(e) = device.run().await {
    ///         eprintln!("Device error: {:?}", e);
    ///     }
    /// });
    /// # }
    /// ```
    pub async fn run(&mut self) -> Result<()> {
        if self.state != ConnectionState::New {
            return Err(Error::Configuration(
                "Run can only be called once".to_string(),
            ));
        }

        self.should_stop = false;

        // Main event loop
        loop {
            if self.should_stop {
                break;
            }

            // Connection/reconnection logic
            match self.state {
                ConnectionState::New | ConnectionState::WaitRetry => {
                    // Calculate retry delay if we're in WaitRetry
                    if self.state == ConnectionState::WaitRetry {
                        let wait_seconds = self.calculate_reconnect_delay();
                        eprintln!("Waiting {} seconds before reconnecting...", wait_seconds);

                        tokio::select! {
                            _ = tokio::time::sleep(Duration::from_secs(wait_seconds as u64)) => {},
                            _ = async {
                                loop {
                                    if self.should_stop {
                                        break;
                                    }
                                    tokio::time::sleep(Duration::from_millis(100)).await;
                                }
                            } => {
                                if self.should_stop {
                                    break;
                                }
                            }
                        }
                    }

                    // Attempt to connect
                    self.set_state(ConnectionState::Connecting);

                    match self.try_connect().await {
                        Ok(()) => {
                            self.set_state(ConnectionState::Connected);
                            self.connected_at = Some(Instant::now());
                            self.reconnect_counter = 0;
                            eprintln!("Successfully connected to signaling service");
                        }
                        Err(e) => {
                            eprintln!("Connection failed: {:?}", e);
                            self.set_state(ConnectionState::WaitRetry);
                            self.reconnect_counter += 1;
                        }
                    }
                }
                ConnectionState::Connected => {
                    // Process WebSocket events
                    if let Some(rx) = &mut self.ws_event_rx {
                        tokio::select! {
                            event = rx.recv() => {
                                match event {
                                    Some(event) => {
                                        self.handle_connection_event(event).await;
                                    }
                                    None => {
                                        // WebSocket event channel closed
                                        eprintln!("WebSocket event channel closed");
                                        self.transition_to_reconnect();
                                    }
                                }
                            }
                            _ = async {
                                loop {
                                    if self.should_stop {
                                        break;
                                    }
                                    tokio::time::sleep(Duration::from_millis(100)).await;
                                }
                            } => {
                                if self.should_stop {
                                    break;
                                }
                            }
                        }
                    } else {
                        // No event receiver, shouldn't happen
                        eprintln!("No WebSocket event receiver in Connected state");
                        break;
                    }
                }
                ConnectionState::Connecting => {
                    // Shouldn't stay in Connecting state during the loop
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                ConnectionState::Closed => {
                    // Device is closed, exit loop
                    break;
                }
                ConnectionState::Failed => {
                    // Failed state, transition to retry
                    self.set_state(ConnectionState::WaitRetry);
                    self.reconnect_counter += 1;
                }
            }
        }

        Ok(())
    }

    /// Calculate the reconnect delay based on the reconnect counter
    fn calculate_reconnect_delay(&self) -> u32 {
        // Exponential backoff: 2^counter seconds, capped at MAX_RECONNECT_WAIT_SECONDS
        let delay = 2u32.pow(self.reconnect_counter);
        delay.min(MAX_RECONNECT_WAIT_SECONDS)
    }

    /// Set the connection state and emit a StateChanged event
    fn set_state(&mut self, new_state: ConnectionState) {
        if self.state != new_state {
            let old_state = self.state;
            self.state = new_state;

            // Emit StateChanged event
            let event = DeviceEvent::StateChanged {
                old_state,
                new_state,
            };
            let _ = self.device_event_tx.try_send(event);
        }
    }

    /// Transition to reconnect state (cleanup current connection)
    fn transition_to_reconnect(&mut self) {
        // Check if we should reset the reconnect counter
        if let Some(connected_at) = self.connected_at {
            if connected_at.elapsed() >= RECONNECT_COUNTER_RESET_TIMEOUT {
                self.reconnect_counter = 0;
            }
        }

        // Close current WebSocket
        self.ws_handle = None;
        self.ws_event_rx = None;

        // Transition to WaitRetry
        self.set_state(ConnectionState::WaitRetry);
    }

    /// Handle a single connection event
    async fn handle_connection_event(&mut self, event: ConnectionEvent) {
        match event {
            ConnectionEvent::Open => {
                eprintln!("WebSocket connection opened");
            }
            ConnectionEvent::Closed
            | ConnectionEvent::ConnectionError(_)
            | ConnectionEvent::PingTimeout => {
                eprintln!("WebSocket disconnected: {:?}", event);
                self.transition_to_reconnect();
            }
            ConnectionEvent::Message {
                channel_id,
                message,
                authorized,
            } => {
                eprintln!("Received MESSAGE event for channel {}", channel_id);
                self.handle_message(channel_id, message, authorized).await;
            }
            ConnectionEvent::Error {
                channel_id,
                code,
                message,
            } => {
                self.handle_channel_error(channel_id, code, message);
            }
            ConnectionEvent::PeerConnected { channel_id } => {
                self.handle_peer_connected(channel_id);
            }
            ConnectionEvent::PeerOffline { channel_id } => {
                self.handle_peer_offline(channel_id);
            }
        }
    }

    /// Try to establish connection (HTTP + WebSocket)
    async fn try_connect(&mut self) -> Result<()> {
        // Step 1: Perform device connect HTTP request to get signaling URL
        let signaling_url = self.device_connect().await?;

        // Step 2: Establish WebSocket connection to the signaling URL
        let (ws_stream, _response) = connect_async(&signaling_url)
            .await
            .map_err(|e| Error::WebSocket(format!("Failed to connect WebSocket: {}", e)))?;

        // Step 3: Create WebSocketConnection and spawn it as a task
        let config = WebSocketConfig::default();
        let (connection, handle, event_rx) = WebSocketConnection::new(ws_stream, config);

        self.ws_handle = Some(handle);
        self.ws_event_rx = Some(event_rx);

        tokio::spawn(async move {
            connection.run().await;
        });

        Ok(())
    }

    /// Stop the signaling device
    ///
    /// This signals the run() loop to stop. The caller should ensure the run() task
    /// completes before dropping the device.
    pub fn stop(&mut self) {
        self.should_stop = true;

        // Close WebSocket connection
        if let Some(handle) = &self.ws_handle {
            let handle_clone = handle.clone();
            tokio::spawn(async move {
                let _ = handle_clone.close().await;
            });
        }

        self.set_state(ConnectionState::Closed);
    }

    /// Request ICE servers from the signaling service
    pub async fn request_ice_servers(&self) -> Result<Vec<IceServer>> {
        let token = (self.options.token_generator)().await?;
        self.http_api.request_ice_servers(&token).await
    }

    /// The check alive function is used to send a PING on the websocket. This
    /// can be used if it has been detected that the WebRTC connection has
    /// disconnected, this could often mean that the WebSocket has a problem. If
    /// a PONG is not received timely after calling check_alive, then the
    /// websocket disconnects and a new signaling connection is made to the
    /// signaling service.
    pub async fn check_alive(&self) -> Result<()> {
        if let Some(handle) = &self.ws_handle {
            handle.send_ping().await.map_err(Error::WebSocket)?;
        }
        Ok(())
    }

    /// Get the current connection state
    ///
    /// Note: This is a snapshot of the state. In a multi-threaded environment,
    /// the state may change immediately after this call returns.
    pub fn connection_state(&self) -> ConnectionState {
        self.state
    }

    /// Handle incoming message on a channel
    async fn handle_message(&mut self, channel_id: String, message: JsonValue, authorized: bool) {
        eprintln!(
            "handle_message called for channel_id={}, authorized={}",
            channel_id, authorized
        );
        // Check if we have an existing channel
        if self.channels.contains_key(&channel_id) {
            eprintln!("Channel already exists");

            // Remove channel temporarily to avoid borrow issues
            let mut channel = self.channels.remove(&channel_id).unwrap();

            // Dispatch to existing channel
            if let Err(e) = channel.handle_routing_message(message, self) {
                eprintln!("Error handling message on channel {}: {:?}", channel_id, e);
            }

            // Put the channel back
            self.channels.insert(channel_id, channel);
        } else {
            eprintln!("No existing channel, checking if initial message...");
            // No existing channel - check if this is an initial message (seq 0)
            match SignalingChannel::is_initial_message(&message) {
                Ok(true) => {
                    eprintln!("Initial message detected, creating new channel");
                    // Create new channel
                    let mut channel = SignalingChannel::new(channel_id.clone());
                    channel.set_state(ChannelState::Connected);

                    // Handle the initial message
                    if let Err(e) = channel.handle_routing_message(message, self) {
                        eprintln!(
                            "Error handling initial message on channel {}: {:?}",
                            channel_id, e
                        );
                        return;
                    }

                    // Emit NewChannel event before adding to map
                    let event = DeviceEvent::NewChannel {
                        channel: channel.clone(), // TODO: Need to implement Clone or use Arc
                        authorized,
                    };

                    if let Err(e) = self.device_event_tx.try_send(event) {
                        eprintln!("Failed to emit NewChannel event: {:?}", e);
                    }

                    // Add to channels map
                    self.channels.insert(channel_id, channel);
                }
                Ok(false) => {
                    // Not an initial message and no channel exists - send error
                    eprintln!(
                        "Received non-initial message for unknown channel: {}",
                        channel_id
                    );
                    let error = ErrorInfo {
                        code: routing::error_codes::CHANNEL_NOT_FOUND.to_string(),
                        message: Some(format!("Channel {} not found", channel_id)),
                    };
                    self.send_error(&channel_id, error);
                }
                Err(e) => {
                    eprintln!(
                        "Failed to parse message for channel {}: {:?}",
                        channel_id, e
                    );
                }
            }
        }
    }

    /// Handle error on a channel
    fn handle_channel_error(&mut self, channel_id: String, code: String, message: Option<String>) {
        if let Some(channel) = self.channels.get_mut(&channel_id) {
            let error = Error::Signaling(format!(
                "Channel error: {} - {}",
                code,
                message.unwrap_or_default()
            ));
            channel.handle_error(error);
        }
    }

    /// Handle peer connected notification
    fn handle_peer_connected(&mut self, channel_id: String) {
        if self.channels.contains_key(&channel_id) {
            // Remove channel temporarily to avoid borrow issues
            let mut channel = self.channels.remove(&channel_id).unwrap();
            channel.handle_peer_connected(self);
            // Put the channel back
            self.channels.insert(channel_id, channel);
        }
    }

    /// Handle peer offline notification
    fn handle_peer_offline(&mut self, channel_id: String) {
        if let Some(channel) = self.channels.get_mut(&channel_id) {
            channel.handle_peer_offline();
        }
    }

    /// Internal method to perform device connect HTTP request
    pub(crate) async fn device_connect(&self) -> Result<String> {
        let token = (self.options.token_generator)().await?;
        let response = self.http_api.device_connect(&token).await?;
        Ok(response.signaling_url)
    }
}

/// Implement SignalingService trait so channels can send messages through the device
impl SignalingService for SignalingDevice {
    fn send_routing_message(&self, channel_id: &str, message: JsonValue) {
        if self.state != ConnectionState::Connected {
            return; // Can't send if not connected
        }

        if let Some(handle) = &self.ws_handle {
            // Wrap message in routing layer
            let routing_msg = RoutingMessage::Message {
                channel_id: channel_id.to_string(),
                message,
                authorized: None,
            };

            // Serialize and send
            if let Ok(_json) = serde_json::to_value(&routing_msg) {
                // Send through WebSocket (fire and forget)
                let handle_clone = handle.clone();
                tokio::spawn(async move {
                    if let Err(e) = handle_clone.send_message(routing_msg).await {
                        eprintln!("Failed to send routing message: {}", e);
                    }
                });
            }
        }
    }

    fn send_error(&self, channel_id: &str, error: ErrorInfo) {
        if self.state != ConnectionState::Connected {
            return;
        }

        if let Some(handle) = &self.ws_handle {
            let routing_msg = RoutingMessage::Error {
                channel_id: channel_id.to_string(),
                error,
            };

            let handle_clone = handle.clone();
            tokio::spawn(async move {
                if let Err(e) = handle_clone.send_message(routing_msg).await {
                    eprintln!("Failed to send error message: {}", e);
                }
            });
        }
    }

    fn close_channel(&mut self, channel_id: &str) {
        self.channels.remove(channel_id);
    }
}
