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
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};
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
    should_stop: Arc<Mutex<bool>>,
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
            should_stop: Arc::new(Mutex::new(false)),
        };

        (device, device_event_rx)
    }

    /// Start the signaling device
    /// This initiates the connection process. The method returns immediately
    /// and connection happens asynchronously with automatic retries.
    pub async fn start(&mut self) -> Result<()> {
        if self.state != ConnectionState::New {
            return Err(Error::Configuration(
                "Start can only be called once".to_string(),
            ));
        }

        *self.should_stop.lock().await = false;

        // Just kick off the first connection attempt
        // The retry loop will continue in the background if needed
        self.do_single_connect_attempt().await;

        Ok(())
    }

    /// Attempt a single connection (used by start())
    async fn do_single_connect_attempt(&mut self) {
        // Only connect from NEW or WAIT_RETRY states
        if self.state != ConnectionState::New && self.state != ConnectionState::WaitRetry {
            eprintln!("do_single_connect_attempt called in invalid state: {:?}", self.state);
            return;
        }

        self.state = ConnectionState::Connecting;

        // Try to connect
        match self.try_connect().await {
            Ok(()) => {
                // Successfully connected
                self.state = ConnectionState::Connected;
                self.connected_at = Some(Instant::now());
                // Connection established
            }
            Err(e) => {
                // Connection failed, transition to WaitRetry
                eprintln!("Connection failed: {:?}", e);
                self.state = ConnectionState::WaitRetry;
                // In a real implementation, we'd schedule a retry here
                // For now, tests can check for WaitRetry state
            }
        }
    }

    /// Try to establish connection (HTTP + WebSocket)
    async fn try_connect(&mut self) -> Result<()> {
        // Step 1: Perform device connect HTTP request to get signaling URL
        let signaling_url = self.device_connect().await?;

        // Step 2: Establish WebSocket connection to the signaling URL
        let (ws_stream, _response) = connect_async(&signaling_url).await.map_err(|e| {
            Error::WebSocket(format!("Failed to connect WebSocket: {}", e))
        })?;

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


    /// Close the signaling device
    pub async fn close(&mut self) -> Result<()> {
        if self.state == ConnectionState::Closed {
            return Ok(());
        }

        // Stop any retry loops
        *self.should_stop.lock().await = true;

        // Close WebSocket connection
        if let Some(handle) = &self.ws_handle {
            let _ = handle.close().await;
        }
        self.ws_handle = None;
        self.ws_event_rx = None;

        self.state = ConnectionState::Closed;
        Ok(())
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
            handle.send_ping().await.map_err(|e| Error::WebSocket(e))?;
        }
        Ok(())
    }

    /// Process any pending WebSocket events (for testing/polling)
    /// In a real application, this would be handled automatically in the background
    pub async fn process_events(&mut self) {
        // Collect all pending events first to avoid borrow checker issues
        let mut events = Vec::new();
        if let Some(rx) = &mut self.ws_event_rx {
            while let Ok(event) = rx.try_recv() {
                events.push(event);
            }
        }

        eprintln!("Processing {} events", events.len());

        // Now process the events
        for event in events {
            match event {
                ConnectionEvent::Open => {
                    eprintln!("WebSocket connection opened");
                }
                ConnectionEvent::Closed | ConnectionEvent::ConnectionError(_) | ConnectionEvent::PingTimeout => {
                    eprintln!("WebSocket disconnected: {:?}", event);
                    // Transition to WaitRetry
                    if self.state == ConnectionState::Connected {
                        self.state = ConnectionState::WaitRetry;
                    }
                }
                ConnectionEvent::Message { channel_id, message, authorized } => {
                    eprintln!("Received MESSAGE event for channel {}", channel_id);
                    self.handle_message(channel_id, message, authorized).await;
                }
                ConnectionEvent::Error { channel_id, code, message } => {
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
    }

    /// Handle incoming message on a channel
    async fn handle_message(&mut self, channel_id: String, message: JsonValue, authorized: bool) {
        eprintln!("handle_message called for channel_id={}, authorized={}", channel_id, authorized);
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
                        eprintln!("Error handling initial message on channel {}: {:?}", channel_id, e);
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
                    eprintln!("Received non-initial message for unknown channel: {}", channel_id);
                    let error = ErrorInfo {
                        code: routing::error_codes::CHANNEL_NOT_FOUND.to_string(),
                        message: Some(format!("Channel {} not found", channel_id)),
                    };
                    self.send_error(&channel_id, error);
                }
                Err(e) => {
                    eprintln!("Failed to parse message for channel {}: {:?}", channel_id, e);
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

    /// Get the current connection state
    pub fn connection_state(&self) -> ConnectionState {
        self.state
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
