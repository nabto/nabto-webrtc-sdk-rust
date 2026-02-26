//! Device module
//!
//! This module contains the core device-side WebRTC signaling implementation,
//! including connection management, channel handling, and protocol layers.

mod channel_actor;
mod token;

pub use token::DeviceTokenGenerator;

use crate::common::channel::{ChannelHandle, ChannelRequest, SignalingChannel};
use crate::common::routing::error_codes;
use crate::common::routing::ErrorInfo;
use crate::common::routing::RoutingMessage;
use crate::common::SignalingConnectionState;
use crate::common::{ConnectionEvent, WebSocketConnection, WebSocketHandle};
use crate::common::{HttpApi, IceServer};
use crate::util::IceServer as SignalingIceServer;
use crate::{Error, Result};
use channel_actor::{run_channel_actor, ChannelActorMessage, ChannelActorParams};
use log::{debug, error, info, trace, warn};
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
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
    Arc<dyn Fn() -> Pin<Box<dyn Future<Output = Result<String>> + Send>> + Send + Sync>;

/// A Clone-able handle for requesting ICE servers from the signaling service.
///
/// Obtain one by calling [`SignalingDevice::ice_server_requester`] before
/// starting the device run loop. It can then be passed into
/// [`DeviceMessageTransportOptions`](crate::util::DeviceMessageTransportOptions).
#[derive(Clone)]
pub struct IceServerRequester {
    http_api: HttpApi,
    token_generator: TokenGenerator,
}

impl IceServerRequester {
    /// Request ICE servers from the signaling service.
    pub async fn request_ice_servers(&self) -> Result<Vec<SignalingIceServer>> {
        let token = (self.token_generator)().await?;
        let servers = self.http_api.request_ice_servers(&token).await?;
        Ok(servers.into_iter().map(Into::into).collect())
    }
}

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

    /// Interval between heartbeat PINGs, or None to disable heartbeat.
    /// Defaults to 30 seconds.
    pub(crate) heartbeat_interval: Option<Duration>,
}

/// Builder for [`SignalingDeviceOptions`].
///
/// # Example
///
/// ```no_run
/// use nabto_webrtc::device::SignalingDeviceOptions;
///
/// # let token_generator: nabto_webrtc::device::TokenGenerator = std::sync::Arc::new(|| {
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
    heartbeat_interval: Option<Duration>,
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
            heartbeat_interval: Some(Duration::from_secs(30)),
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

    /// Set the heartbeat interval for WebSocket keepalive PINGs.
    ///
    /// Defaults to 30 seconds. Pass `None` to disable the heartbeat.
    pub fn heartbeat_interval(mut self, interval: Option<Duration>) -> Self {
        self.heartbeat_interval = interval;
        self
    }

    /// Build the `SignalingDeviceOptions`.
    pub fn build(self) -> SignalingDeviceOptions {
        SignalingDeviceOptions {
            endpoint_url: self.endpoint_url,
            product_id: self.product_id,
            device_id: self.device_id,
            token_generator: self.token_generator,
            heartbeat_interval: self.heartbeat_interval,
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

    // Channel management — per-channel actor tasks
    channel_actors: HashMap<String, mpsc::Sender<ChannelActorMessage>>,
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
            channel_actors: HashMap::new(),
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
    /// # let token_generator: nabto_webrtc::device::TokenGenerator = std::sync::Arc::new(|| {
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

        // Notify all channel actors that the connection is lost
        for tx in self.channel_actors.values() {
            let _ = tx.try_send(ChannelActorMessage::ConnectionLost);
        }

        // Transition to WaitRetry
        self.set_state(SignalingConnectionState::WaitRetry);
    }

    /// Send new WebSocket handle to all channel actors for retransmission
    async fn retransmit_unacked_messages(&mut self) {
        let ws_handle = match &self.ws_handle {
            Some(handle) => handle.clone(),
            None => return,
        };

        debug!(
            "[{}] Sending WebSocketReconnected to {} channel actors",
            self.name,
            self.channel_actors.len()
        );

        let mut dead_actors = Vec::new();
        for (channel_id, tx) in &self.channel_actors {
            if tx
                .send(ChannelActorMessage::WebSocketReconnected(ws_handle.clone()))
                .await
                .is_err()
            {
                debug!(
                    "[{}] Channel actor {} is no longer running",
                    self.name, channel_id
                );
                dead_actors.push(channel_id.clone());
            }
        }

        for channel_id in dead_actors {
            self.channel_actors.remove(&channel_id);
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
                self.handle_channel_error(channel_id, code, message).await;
            }
            ConnectionEvent::PeerConnected { channel_id } => {
                self.handle_peer_connected(channel_id).await;
            }
            ConnectionEvent::PeerOffline { channel_id } => {
                self.handle_peer_offline(channel_id).await;
            }
        }
    }

    /// Handle a channel request (from a ChannelHandle) by forwarding to the channel actor
    async fn handle_channel_request(&mut self, request: ChannelRequest) {
        match request {
            ChannelRequest::SendMessage {
                channel_id,
                message,
            } => {
                if let Some(tx) = self.channel_actors.get(&channel_id) {
                    if tx
                        .send(ChannelActorMessage::SendMessage(message))
                        .await
                        .is_err()
                    {
                        debug!(
                            "[{}] Channel actor {} is no longer running",
                            self.name, channel_id
                        );
                        self.channel_actors.remove(&channel_id);
                    }
                } else {
                    warn!(
                        "[{}] Channel {} not found for sending message",
                        self.name, channel_id
                    );
                }
            }
            ChannelRequest::SendError { channel_id, error } => {
                if let Some(tx) = self.channel_actors.get(&channel_id) {
                    if tx
                        .send(ChannelActorMessage::SendError(error))
                        .await
                        .is_err()
                    {
                        self.channel_actors.remove(&channel_id);
                    }
                }
            }
            ChannelRequest::Close { channel_id } => {
                if let Some(tx) = self.channel_actors.remove(&channel_id) {
                    let _ = tx.send(ChannelActorMessage::Close).await;
                }
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
        let (connection, handle, event_rx) =
            WebSocketConnection::new(self.name, ws_stream, self.options.heartbeat_interval);

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

    /// Create an [`IceServerRequester`] that can independently request ICE servers.
    ///
    /// Call this before wrapping the device in `Arc<Mutex<>>` and starting
    /// the run loop, then pass the requester into
    /// [`DeviceMessageTransportOptions`](crate::util::DeviceMessageTransportOptions).
    pub fn ice_server_requester(&self) -> IceServerRequester {
        IceServerRequester {
            http_api: self.http_api.clone(),
            token_generator: self.options.token_generator.clone(),
        }
    }

    /// The check alive function is used to send a PING on the websocket. This
    /// can be used if it has been detected that the WebRTC connection has
    /// disconnected, this could often mean that the WebSocket has a problem. If
    /// a PONG is not received timely after calling check_alive, then the
    /// websocket disconnects and a new signaling connection is made to the
    /// signaling service.
    pub async fn check_alive(&self) -> Result<()> {
        const CHECK_ALIVE_TIMEOUT_MS: u64 = 2_000;
        if let Some(handle) = &self.ws_handle {
            handle
                .check_alive(CHECK_ALIVE_TIMEOUT_MS)
                .await
                .map_err(Error::WebSocket)?;
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

    /// Handle incoming message on a channel by routing to the appropriate channel actor
    async fn handle_message(&mut self, channel_id: String, message: JsonValue, authorized: bool) {
        trace!(
            "[{}] handle_message called for channel_id={}, authorized={}",
            self.name,
            channel_id,
            authorized
        );

        if let Some(tx) = self.channel_actors.get(&channel_id) {
            // Forward to existing channel actor
            if tx
                .send(ChannelActorMessage::RoutingMessage(message))
                .await
                .is_err()
            {
                debug!(
                    "[{}] Channel actor {} is no longer running",
                    self.name, channel_id
                );
                self.channel_actors.remove(&channel_id);
            }
        } else {
            // No existing channel actor - check if this is an initial message (seq 0)
            match SignalingChannel::is_initial_message(&message) {
                Ok(true) => {
                    debug!(
                        "[{}] Initial message detected, spawning channel actor",
                        self.name
                    );

                    let ws_handle = match &self.ws_handle {
                        Some(handle) => handle.clone(),
                        None => {
                            error!("[{}] No WebSocket handle in Connected state", self.name);
                            return;
                        }
                    };

                    let (tx, rx) = mpsc::channel(32);
                    tokio::spawn(run_channel_actor(ChannelActorParams {
                        name: self.name,
                        channel_id: channel_id.clone(),
                        initial_message: message,
                        authorized,
                        ws_handle,
                        channel_request_tx: self.channel_request_tx.clone(),
                        device_event_tx: self.device_event_tx.clone(),
                        rx,
                    }));
                    self.channel_actors.insert(channel_id, tx);
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
                    self.send_error_direct(&channel_id, error).await;
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

    /// Handle error on a channel by forwarding to its actor
    async fn handle_channel_error(
        &mut self,
        channel_id: String,
        code: String,
        message: Option<String>,
    ) {
        if let Some(tx) = self.channel_actors.get(&channel_id) {
            if tx
                .send(ChannelActorMessage::Error { code, message })
                .await
                .is_err()
            {
                self.channel_actors.remove(&channel_id);
            }
        }
    }

    /// Handle peer connected notification by forwarding to the channel actor
    async fn handle_peer_connected(&mut self, channel_id: String) {
        if let Some(tx) = self.channel_actors.get(&channel_id) {
            if tx.send(ChannelActorMessage::PeerConnected).await.is_err() {
                self.channel_actors.remove(&channel_id);
            }
        }
    }

    /// Handle peer offline notification by forwarding to the channel actor
    async fn handle_peer_offline(&mut self, channel_id: String) {
        if let Some(tx) = self.channel_actors.get(&channel_id) {
            if tx.send(ChannelActorMessage::PeerOffline).await.is_err() {
                self.channel_actors.remove(&channel_id);
            }
        }
    }

    /// Internal method to perform device connect HTTP request
    pub(crate) async fn device_connect(&self) -> Result<String> {
        let token = (self.options.token_generator)().await?;
        let response = self.http_api.device_connect(&token).await?;
        Ok(response.signaling_url)
    }
}

impl SignalingDevice {
    /// Send an error directly via the WebSocket for channels without an actor
    async fn send_error_direct(&self, channel_id: &str, error: ErrorInfo) {
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
}
