//! Device module
//!
//! This module contains the core device-side WebRTC signaling implementation,
//! including connection management, channel handling, and protocol layers.

mod token;

pub use token::DeviceTokenGenerator;

use crate::common::channel::{ChannelHandle, ChannelRequest, SignalingChannel, SignalingService};
use crate::common::routing::error_codes;
use crate::common::routing::ErrorInfo;
use crate::common::routing::RoutingMessage;
use crate::common::{ConnectionEvent, WebSocketConfig, WebSocketConnection, WebSocketHandle};
use crate::common::{HttpApi, IceServer};
use crate::common::{SignalingChannelState, SignalingConnectionState};
use crate::{Error, Result};
use log::{debug, error, info, trace, warn};
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

/// Commands that can be sent to the SignalingDevice
pub enum DeviceCommand {
    /// Send a ping to check if the connection is alive
    CheckAlive,
}

/// Events emitted by the SignalingDevice
pub enum DeviceEvent {
    /// A new signaling channel is ready
    /// The handle can be used to send messages on the channel
    /// The message_rx can be used to receive messages from the channel
    NewChannel {
        handle: ChannelHandle,
        message_rx: mpsc::Receiver<JsonValue>,
        authorized: bool,
    },

    /// Connection state changed
    StateChanged {
        old_state: SignalingConnectionState,
        new_state: SignalingConnectionState,
    },
}

// Manual Debug implementation since mpsc::Receiver doesn't implement Debug
impl std::fmt::Debug for DeviceEvent {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            DeviceEvent::NewChannel {
                handle, authorized, ..
            } => f
                .debug_struct("NewChannel")
                .field("channel_id", &handle.channel_id())
                .field("authorized", authorized)
                .field("message_rx", &"<mpsc::Receiver>")
                .finish(),
            DeviceEvent::StateChanged {
                old_state,
                new_state,
            } => f
                .debug_struct("StateChanged")
                .field("old_state", old_state)
                .field("new_state", new_state)
                .finish(),
        }
    }
}

/// Options for creating a SignalingDevice
pub struct SignalingDeviceOptions {
    /// Optional URL for the signaling service
    pub(crate) endpoint_url: Option<String>,

    /// The product ID (e.g., "wp-abcdefghi")
    pub(crate) product_id: String,

    /// The device ID (e.g., "wd-jklmnopqr")
    pub(crate) device_id: String,

    /// Token generator called when a new access token is needed
    pub(crate) token_generator: TokenGenerator,
}

/// Builder for [`SignalingDeviceOptions`].
///
/// # Example
///
/// ```no_run
/// use nabto_webrtc::device::SignalingDeviceOptions;
///
/// # let token_generator: nabto_webrtc::device::TokenGenerator = Box::new(|| {
/// #     Box::pin(async { Ok("token".to_string()) })
/// #         as std::pin::Pin<Box<dyn std::future::Future<Output = Result<String, nabto_webrtc::Error>> + Send>>
/// # });
/// let options = SignalingDeviceOptions::builder("wp-test".to_string(), "wd-test".to_string(), token_generator)
///     .endpoint_url("https://custom.endpoint.net".to_string())
///     .build();
/// ```
pub struct SignalingDeviceOptionsBuilder {
    product_id: String,
    device_id: String,
    token_generator: TokenGenerator,
    endpoint_url: Option<String>,
}

impl SignalingDeviceOptions {
    /// Create a new builder for `SignalingDeviceOptions`.
    pub fn builder(
        product_id: String,
        device_id: String,
        token_generator: TokenGenerator,
    ) -> SignalingDeviceOptionsBuilder {
        SignalingDeviceOptionsBuilder {
            product_id,
            device_id,
            token_generator,
            endpoint_url: None,
        }
    }
}

impl SignalingDeviceOptionsBuilder {
    /// Set a custom endpoint URL for the signaling service.
    ///
    /// If not set, defaults to `https://<product_id>.webrtc.nabto.net`.
    pub fn endpoint_url(mut self, url: String) -> Self {
        self.endpoint_url = Some(url);
        self
    }

    /// Build the `SignalingDeviceOptions`.
    pub fn build(self) -> SignalingDeviceOptions {
        SignalingDeviceOptions {
            endpoint_url: self.endpoint_url,
            product_id: self.product_id,
            device_id: self.device_id,
            token_generator: self.token_generator,
        }
    }
}

/// The main SignalingDevice interface
pub struct SignalingDevice {
    /// Name used for logging purposes
    name: &'static str,

    http_api: HttpApi,
    options: SignalingDeviceOptions,
    state: SignalingConnectionState,

    // Event channel for emitting device events
    device_event_tx: mpsc::Sender<DeviceEvent>,

    // Command channel for receiving commands
    command_rx: Option<mpsc::Receiver<DeviceCommand>>,

    // WebSocket connection
    ws_handle: Option<WebSocketHandle>,
    ws_event_rx: Option<mpsc::Receiver<ConnectionEvent>>,

    // Channel management
    channels: HashMap<String, SignalingChannel>,
    channel_request_tx: mpsc::Sender<ChannelRequest>,
    channel_request_rx: Option<mpsc::Receiver<ChannelRequest>>,

    // Retry state
    reconnect_counter: u32,
    connected_at: Option<Instant>,
    should_stop: bool,
}

impl SignalingDevice {
    /// Create a new SignalingDevice
    ///
    /// Returns the device instance, a receiver for device events, and a sender for device commands.
    /// The event receiver should be polled to handle NewChannel events and other device events.
    /// The command sender can be used to send commands to the device (e.g., CheckAlive).
    pub fn new(
        options: SignalingDeviceOptions,
    ) -> (
        Self,
        mpsc::Receiver<DeviceEvent>,
        mpsc::Sender<DeviceCommand>,
    ) {
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
        let (channel_request_tx, channel_request_rx) = mpsc::channel(32);
        let (command_tx, command_rx) = mpsc::channel(32);

        let device = Self {
            name: "device",
            http_api,
            options,
            state: SignalingConnectionState::New,
            device_event_tx,
            command_rx: Some(command_rx),
            ws_handle: None,
            ws_event_rx: None,
            channels: HashMap::new(),
            channel_request_tx,
            channel_request_rx: Some(channel_request_rx),
            reconnect_counter: 0,
            connected_at: None,
            should_stop: false,
        };

        (device, device_event_rx, command_tx)
    }

    /// Run the signaling device event loop
    ///
    /// This method runs continuously, handling connection, reconnection, and message processing.
    /// It will only return when stop() is called or an unrecoverable error occurs.
    ///
    /// The user should spawn this on a tokio task:
    /// ```no_run
    /// # use nabto_webrtc::device::{SignalingDevice, SignalingDeviceOptions};
    /// # #[tokio::main]
    /// # async fn main() {
    /// # let token_generator = Box::new(|| {
    /// #     Box::pin(async { Ok("token".to_string()) })
    /// #         as std::pin::Pin<Box<dyn std::future::Future<Output = Result<String, nabto_webrtc::Error>> + Send>>
    /// # });
    /// # let options = SignalingDeviceOptions::builder(
    /// #     "wp-test".to_string(),
    /// #     "wd-test".to_string(),
    /// #     token_generator,
    /// # ).build();
    /// let (mut device, event_rx, command_tx) = SignalingDevice::new(options);
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
        if self.state != SignalingConnectionState::New {
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
                SignalingConnectionState::New | SignalingConnectionState::WaitRetry => {
                    // Calculate retry delay if we're in WaitRetry
                    if self.state == SignalingConnectionState::WaitRetry {
                        let wait_seconds = self.calculate_reconnect_delay();
                        info!(
                            "[{}] Waiting {} seconds before reconnecting...",
                            self.name, wait_seconds
                        );

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
                    self.set_state(SignalingConnectionState::Connecting);

                    match self.try_connect().await {
                        Ok(()) => {
                            self.set_state(SignalingConnectionState::Connected);
                            self.connected_at = Some(Instant::now());
                            self.reconnect_counter = 0;
                            info!(
                                "[{}] Successfully connected to signaling service",
                                self.name
                            );

                            // Retransmit unacked messages for all existing channels after reconnection
                            self.retransmit_unacked_messages().await;
                        }
                        Err(e) => {
                            warn!("[{}] Connection failed: {:?}", self.name, e);
                            self.set_state(SignalingConnectionState::WaitRetry);
                            self.reconnect_counter += 1;
                        }
                    }
                }
                SignalingConnectionState::Connected => {
                    // Process WebSocket events, channel requests, and commands
                    if let Some(ws_rx) = &mut self.ws_event_rx {
                        if let Some(ch_rx) = &mut self.channel_request_rx {
                            if let Some(cmd_rx) = &mut self.command_rx {
                                tokio::select! {
                                    event = ws_rx.recv() => {
                                        match event {
                                            Some(event) => {
                                                self.handle_connection_event(event).await;
                                            }
                                            None => {
                                                // WebSocket event channel closed
                                                warn!("[{}] WebSocket event channel closed", self.name);
                                                self.transition_to_reconnect();
                                            }
                                        }
                                    }
                                    request = ch_rx.recv() => {
                                        if let Some(request) = request {
                                            self.handle_channel_request(request).await;
                                        }
                                    }
                                    command = cmd_rx.recv() => {
                                        if let Some(command) = command {
                                            self.handle_command(command).await;
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
                            }
                        }
                    } else {
                        // No event receiver, shouldn't happen
                        error!(
                            "[{}] No WebSocket event receiver in Connected state",
                            self.name
                        );
                        break;
                    }
                }
                SignalingConnectionState::Connecting => {
                    // Shouldn't stay in Connecting state during the loop
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }
                SignalingConnectionState::Closed => {
                    // Device is closed, exit loop
                    break;
                }
                SignalingConnectionState::Failed => {
                    // Failed state, transition to retry
                    self.set_state(SignalingConnectionState::WaitRetry);
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
    fn set_state(&mut self, new_state: SignalingConnectionState) {
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
        self.set_state(SignalingConnectionState::WaitRetry);
    }

    /// Retransmit unacked messages for all channels after reconnection
    async fn retransmit_unacked_messages(&mut self) {
        let channel_ids: Vec<String> = self.channels.keys().cloned().collect();

        debug!(
            "[{}] Retransmitting unacked messages for {} channels",
            self.name,
            channel_ids.len()
        );

        for channel_id in channel_ids {
            debug!("[{}] Retransmitting for channel: {}", self.name, channel_id);
            if let Some(mut channel) = self.channels.remove(&channel_id) {
                channel.handle_websocket_reconnect(self).await;
                self.channels.insert(channel_id, channel);
            }
        }

        debug!("[{}] Retransmission complete", self.name);
    }

    /// Handle a single connection event
    async fn handle_connection_event(&mut self, event: ConnectionEvent) {
        match event {
            ConnectionEvent::Open => {
                debug!("[{}] WebSocket connection opened", self.name);
            }
            ConnectionEvent::Closed
            | ConnectionEvent::ConnectionError(_)
            | ConnectionEvent::PingTimeout => {
                warn!("[{}] WebSocket disconnected: {:?}", self.name, event);
                self.transition_to_reconnect();
            }
            ConnectionEvent::Message {
                channel_id,
                message,
                authorized,
            } => {
                trace!(
                    "[{}] Received MESSAGE event for channel {}",
                    self.name,
                    channel_id
                );
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
                self.handle_peer_connected(channel_id).await;
            }
            ConnectionEvent::PeerOffline { channel_id } => {
                self.handle_peer_offline(channel_id);
            }
        }
    }

    /// Handle a channel request (from a ChannelHandle)
    async fn handle_channel_request(&mut self, request: ChannelRequest) {
        match request {
            ChannelRequest::SendMessage {
                channel_id,
                message,
            } => {
                trace!(
                    "[{}] Handling SendMessage request for channel {}",
                    self.name,
                    channel_id
                );
                trace!(
                    "[{}] Message preview: {:?}",
                    self.name,
                    serde_json::to_string(&message)
                        .unwrap_or_else(|_| "failed to serialize".to_string())
                        .chars()
                        .take(200)
                        .collect::<String>()
                );

                // Send through the channel's reliability layer
                // We need to remove the channel temporarily to avoid borrowing issues
                if let Some(mut channel) = self.channels.remove(&channel_id) {
                    if let Err(e) = channel.send_message_async(message, self).await {
                        error!(
                            "[{}] Failed to send message on channel {}: {:?}",
                            self.name, channel_id, e
                        );
                    }
                    // Put the channel back
                    self.channels.insert(channel_id, channel);
                } else {
                    warn!(
                        "[{}] Channel {} not found for sending message",
                        self.name, channel_id
                    );
                }
            }
            ChannelRequest::SendError { channel_id, error } => {
                self.send_error(&channel_id, error).await;
            }
            ChannelRequest::Close { channel_id } => {
                self.channels.remove(&channel_id);
                debug!("[{}] Closed channel {}", self.name, channel_id);
            }
        }
    }

    /// Handle a command sent to the device
    async fn handle_command(&self, command: DeviceCommand) {
        match command {
            DeviceCommand::CheckAlive => {
                if let Err(e) = self.check_alive().await {
                    warn!("[{}] check_alive failed: {:?}", self.name, e);
                }
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
        let (connection, handle, event_rx) = WebSocketConnection::new(self.name, ws_stream, config);

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
    pub async fn stop(&mut self) {
        self.should_stop = true;

        // Close WebSocket connection
        if let Some(handle) = &self.ws_handle {
            let _ = handle.close().await;
        }

        self.set_state(SignalingConnectionState::Closed);
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
    pub fn connection_state(&self) -> SignalingConnectionState {
        self.state
    }

    /// Get a clone of the WebSocket handle if connected
    ///
    /// Returns None if not currently connected
    pub fn get_websocket_handle(&self) -> Option<WebSocketHandle> {
        self.ws_handle.clone()
    }

    /// Handle incoming message on a channel
    async fn handle_message(&mut self, channel_id: String, message: JsonValue, authorized: bool) {
        trace!(
            "[{}] handle_message called for channel_id={}, authorized={}",
            self.name,
            channel_id,
            authorized
        );
        // Check if we have an existing channel
        if self.channels.contains_key(&channel_id) {
            trace!("[{}] Channel already exists", self.name);

            // Remove channel temporarily to avoid borrow issues
            let mut channel = self.channels.remove(&channel_id).unwrap();

            // Dispatch to existing channel
            if let Err(e) = channel.handle_routing_message(message, self).await {
                error!(
                    "[{}] Error handling message on channel {}: {:?}",
                    self.name, channel_id, e
                );
            }

            // Put the channel back
            self.channels.insert(channel_id, channel);
        } else {
            trace!(
                "[{}] No existing channel, checking if initial message...",
                self.name
            );
            // No existing channel - check if this is an initial message (seq 0)
            match SignalingChannel::is_initial_message(&message) {
                Ok(true) => {
                    debug!(
                        "[{}] Initial message detected, creating new channel",
                        self.name
                    );
                    // Create new channel with message channel set up
                    let channel_for_map = SignalingChannel::new(self.name, channel_id.clone());
                    let (mut channel_with_rx, message_rx) = channel_for_map.with_message_channel();
                    channel_with_rx.set_state(SignalingChannelState::Connected);

                    // Set the device sender so the channel can send messages back
                    channel_with_rx.set_device_sender(self.channel_request_tx.clone());

                    // Handle the initial message
                    if let Err(e) = channel_with_rx.handle_routing_message(message, self).await {
                        error!(
                            "[{}] Error handling initial message on channel {}: {:?}",
                            self.name, channel_id, e
                        );
                        return;
                    }

                    // Add to channels map
                    self.channels.insert(channel_id.clone(), channel_with_rx);

                    // Create a handle for the channel (lightweight, can be cloned)
                    let handle =
                        ChannelHandle::new(channel_id.clone(), self.channel_request_tx.clone());

                    // Emit NewChannel event after adding to map
                    // The message_rx is sent along with the handle so tests can receive messages
                    let event = DeviceEvent::NewChannel {
                        handle,
                        message_rx,
                        authorized,
                    };

                    if let Err(e) = self.device_event_tx.try_send(event) {
                        error!("[{}] Failed to emit NewChannel event: {:?}", self.name, e);
                    }
                }
                Ok(false) => {
                    // Not an initial message and no channel exists - send error
                    warn!(
                        "[{}] Received non-initial message for unknown channel: {}",
                        self.name, channel_id
                    );
                    let error = ErrorInfo {
                        code: error_codes::CHANNEL_NOT_FOUND.to_string(),
                        message: Some(format!("Channel {} not found", channel_id)),
                    };
                    self.send_error(&channel_id, error).await;
                }
                Err(e) => {
                    error!(
                        "[{}] Failed to parse message for channel {}: {:?}",
                        self.name, channel_id, e
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
    async fn handle_peer_connected(&mut self, channel_id: String) {
        if self.channels.contains_key(&channel_id) {
            // Remove channel temporarily to avoid borrow issues
            let mut channel = self.channels.remove(&channel_id).unwrap();
            channel.handle_peer_connected(self).await;
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
    async fn send_routing_message(&self, channel_id: &str, message: JsonValue) {
        if self.state != SignalingConnectionState::Connected {
            return; // Can't send if not connected
        }

        if let Some(handle) = &self.ws_handle {
            // Wrap message in routing layer
            let routing_msg = RoutingMessage::Message {
                channel_id: channel_id.to_string(),
                message,
                authorized: None,
            };

            // Await the send to ensure messages are sent in order
            // This prevents the race condition where tokio::spawn would allow
            // messages to be reordered
            if let Err(e) = handle.send_message(routing_msg).await {
                error!("[{}] Failed to send routing message: {}", self.name, e);
            }
        }
    }

    async fn send_error(&self, channel_id: &str, error: ErrorInfo) {
        if self.state != SignalingConnectionState::Connected {
            return;
        }

        if let Some(handle) = &self.ws_handle {
            let routing_msg = RoutingMessage::Error {
                channel_id: channel_id.to_string(),
                error,
            };

            if let Err(e) = handle.send_message(routing_msg).await {
                error!("[{}] Failed to send error message: {}", self.name, e);
            }
        }
    }

    fn close_channel(&mut self, channel_id: &str) {
        self.channels.remove(channel_id);
    }
}
