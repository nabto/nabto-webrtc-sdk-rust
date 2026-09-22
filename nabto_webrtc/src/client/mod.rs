use crate::common::channel::ChannelHandle;
use crate::common::channel::ChannelRequest;
use crate::common::channel::SignalingChannel;
use crate::common::channel::SignalingService;
use crate::common::http::HttpApi;
use crate::common::routing::RoutingMessage;
use crate::common::websocket::ConnectionEvent;
use crate::common::websocket::WebSocketConnection;
use crate::common::websocket::WebSocketHandle;
use crate::common::SignalingChannelState;
use crate::common::SignalingConnectionState;
use crate::Error;
use log::{debug, error, info, warn};
use serde_json::Value as JsonValue;
use std::time::Duration;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_tungstenite::connect_async;

/// Events emitted by a [`SignalingClient`] on the receiver returned from
/// [`SignalingClient::new`].
pub enum SignalingClientEvent {
    /// A signaling message was received from the device.
    ///
    /// Not emitted if something has taken over message delivery by calling
    /// [`SignalingChannel::with_msg_channel`](crate::common::channel::SignalingChannel::with_msg_channel)
    /// on [`SignalingClient::channel`] — which is what
    /// [`ClientMessageTransport`](crate::util::ClientMessageTransport) does.
    Message(JsonValue),
    /// The connection to the signaling service was lost and will be retried.
    ConnectionReconnect,
    /// The connection to the signaling service changed state.
    ConnectionStateChange(SignalingConnectionState),
    /// The signaling channel to the device changed state.
    ChannelStateChange(SignalingChannelState),
    /// The signaling service reported an error on the channel.
    Error(Error),
}

pub struct SignalingClientOptions {
    pub(crate) product_id: String,
    pub(crate) device_id: String,
    pub(crate) require_online: Option<bool>,
    pub(crate) endpoint_url: Option<String>,
    pub(crate) access_token: Option<String>,
}

/// Builder for [`SignalingClientOptions`].
///
/// # Example
///
/// ```no_run
/// use nabto_webrtc::client::SignalingClientOptions;
///
/// let options = SignalingClientOptions::builder("wp-test".to_string(), "wd-test".to_string())
///     .endpoint_url("https://custom.endpoint.net".to_string())
///     .require_online(true)
///     .access_token("my-token".to_string())
///     .build();
/// ```
pub struct SignalingClientOptionsBuilder {
    product_id: String,
    device_id: String,
    endpoint_url: Option<String>,
    require_online: Option<bool>,
    access_token: Option<String>,
}

impl SignalingClientOptions {
    /// Create a new builder for `SignalingClientOptions`.
    pub fn builder(product_id: String, device_id: String) -> SignalingClientOptionsBuilder {
        SignalingClientOptionsBuilder {
            product_id,
            device_id,
            endpoint_url: None,
            require_online: None,
            access_token: None,
        }
    }
}

impl SignalingClientOptionsBuilder {
    /// Set a custom endpoint URL for the signaling service.
    ///
    /// If not set, defaults to `https://<product_id>.webrtc.nabto.net`.
    pub fn endpoint_url(mut self, url: String) -> Self {
        self.endpoint_url = Some(url);
        self
    }

    /// Require the device to be online when the client connects.
    ///
    /// When set, [`SignalingClient::new`] fails with [`Error::DeviceOffline`]
    /// instead of returning a client that cannot reach the device.
    pub fn require_online(mut self, val: bool) -> Self {
        self.require_online = Some(val);
        self
    }

    /// Set an access token for authentication.
    pub fn access_token(mut self, token: String) -> Self {
        self.access_token = Some(token);
        self
    }

    /// Build the `SignalingClientOptions`.
    pub fn build(self) -> SignalingClientOptions {
        SignalingClientOptions {
            product_id: self.product_id,
            device_id: self.device_id,
            endpoint_url: self.endpoint_url,
            require_online: self.require_online,
            access_token: self.access_token,
        }
    }
}

pub struct SignalingClientService {
    pub ws_handle: Option<WebSocketHandle>,
    pub ws_event_rx: Option<mpsc::Receiver<ConnectionEvent>>,
    pub connection_state: SignalingConnectionState,
}

pub struct SignalingClient {
    /// Name used for logging purposes
    name: &'static str,

    signaling_url: String,
    event_tx: mpsc::Sender<SignalingClientEvent>,

    // Channel
    pub channel: SignalingChannel,
    pub channel_handle: ChannelHandle,
    channel_request_rx: mpsc::Receiver<ChannelRequest>,

    // Websocket
    service: SignalingClientService,

    // Retry state
    reconnect_counter: u32,
    connected_at: Option<Instant>,
    should_stop: bool,
}

/// Minimum time between a successful connection and reconnect counter reset (10 seconds)
const RECONNECT_COUNTER_RESET_TIMEOUT: Duration = Duration::from_secs(10);

/// Maximum reconnect wait time (60 seconds)
const MAX_RECONNECT_WAIT_SECONDS: u32 = 60;

impl SignalingClient {
    pub async fn new(
        options: SignalingClientOptions,
    ) -> Result<(Self, mpsc::Receiver<SignalingClientEvent>), Error> {
        let (event_tx, event_rx) = mpsc::channel(32);

        let endpoint_url = options
            .endpoint_url
            .clone()
            .unwrap_or_else(|| format!("https://{}.webrtc.nabto.net", options.product_id));

        let http_api = HttpApi::new(
            endpoint_url,
            options.product_id.clone(),
            options.device_id.clone(),
        );

        let (channel_request_tx, channel_request_rx) = mpsc::channel(32);

        let response = http_api
            .client_connect(options.access_token.as_deref())
            .await?;
        let signaling_url = response.signaling_url;
        let device_online = response.device_online.unwrap_or(false);
        let require_online = options.require_online.unwrap_or(false);

        if require_online && !device_online {
            return Err(Error::DeviceOffline);
        }

        if let Some(cid) = response.channel_id {
            let mut client = Self {
                name: "client",
                signaling_url,

                channel: SignalingChannel::new("client", cid.clone()),
                channel_handle: ChannelHandle::new(cid.clone(), channel_request_tx.clone()),
                channel_request_rx,

                event_tx,

                service: SignalingClientService {
                    connection_state: SignalingConnectionState::New,
                    ws_handle: None,
                    ws_event_rx: None,
                },

                reconnect_counter: 0,
                connected_at: None,
                should_stop: false,
            };

            // Forward the channel's messages and state changes onto the event
            // channel, so the receiver returned here is a complete view of what
            // the client is doing.
            client.spawn_message_forwarder();
            client.spawn_channel_state_forwarder();

            if device_online {
                client
                    .channel
                    .set_state(crate::SignalingChannelState::Connected);
            }

            Ok((client, event_rx))
        } else {
            Err(Error::Configuration(
                "Failed to create SignalingClient".to_string(),
            ))
        }
    }

    pub async fn run(&mut self) -> Result<(), crate::error::Error> {
        if self.service.connection_state != SignalingConnectionState::New {
            // @TODO: Return error
            return Err(Error::Configuration(
                "Run can only be called once".to_string(),
            ));
        }

        loop {
            match self.service.connection_state {
                SignalingConnectionState::New | SignalingConnectionState::WaitRetry => {
                    if self.service.connection_state == SignalingConnectionState::WaitRetry {
                        // @TODO: Handle WaitRetry case
                        let wait_seconds = self.calculate_reconnect_delay();
                        info!(
                            "[{}] Waiting {} seconds before reconnecting.",
                            self.name, wait_seconds
                        );

                        tokio::select! {
                            _ = tokio::time::sleep(Duration::from_secs(wait_seconds as u64)) => {}

                            _ = async {
                                loop {
                                    if self.should_stop {
                                        break;
                                    }
                                    tokio::time::sleep(Duration::from_millis(200)).await;
                                }
                            } => {
                                if self.should_stop {
                                    break;
                                }
                            }
                        }
                    }

                    self.set_connection_state(SignalingConnectionState::Connecting);

                    match self.try_connect().await {
                        Ok(()) => {
                            self.set_connection_state(SignalingConnectionState::Connected);
                            self.connected_at = Some(Instant::now());
                            self.reconnect_counter = 0;
                            info!(
                                "[{}] Successfully connected to signaling service",
                                self.name
                            );
                        }

                        Err(e) => {
                            warn!("[{}] Connection failed: {:?}", self.name, e);
                            self.set_connection_state(SignalingConnectionState::WaitRetry);
                            self.reconnect_counter += 1;
                        }
                    }
                }

                SignalingConnectionState::Connected => {
                    if let Some(ws_rx) = &mut self.service.ws_event_rx {
                        tokio::select! {
                            event = ws_rx.recv() => {
                                match event {
                                    Some(event) => {
                                        self.handle_websocket_event(event).await;
                                    }

                                    None => {
                                        warn!("[{}] Websocket event channel was closed", self.name);
                                        self.transition_to_reconnect();
                                    }
                                }
                            }

                            ch_req = self.channel_request_rx.recv() => {
                                if let Some(request) = ch_req {
                                    self.handle_channel_request(request).await;
                                }
                            }
                        }
                    } else {
                        error!("[{}] SignalingClient is in CONNECTED state but there is no websocket handle", self.name);
                        break;
                    }
                }

                SignalingConnectionState::Connecting => {
                    // Sleep for a bit
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }

                SignalingConnectionState::Closed => {
                    break;
                }

                SignalingConnectionState::Failed => {
                    // @TODO: Go to retry state
                }
            }
        }

        Ok(())
    }

    async fn handle_channel_request(&mut self, request: ChannelRequest) {
        match request {
            ChannelRequest::SendMessage {
                channel_id,
                message,
            } => {
                if let Err(e) = self
                    .channel
                    .send_message_async(message, &self.service)
                    .await
                {
                    error!(
                        "[{}] Failed to send message on channel {}: {:?}",
                        self.name, channel_id, e
                    );
                }
            }

            ChannelRequest::SendError { channel_id, error } => {
                self.send_error(&channel_id, error).await;
            }

            ChannelRequest::Close { channel_id: _ } => {
                // @TODO
            }
        }
    }

    async fn try_connect(&mut self) -> Result<(), Error> {
        let (ws_stream, _response) = connect_async(&self.signaling_url)
            .await
            .map_err(|e| Error::WebSocket(format!("Failed to connect websocket: {}", e)))?;

        let (connection, handle, event_rx) =
            WebSocketConnection::new(self.name, ws_stream, Some(Duration::from_secs(30)));

        self.service.ws_handle = Some(handle);
        self.service.ws_event_rx = Some(event_rx);

        tokio::spawn(async move {
            connection.run().await;
        });

        Ok(())
    }

    async fn handle_websocket_event(&mut self, event: ConnectionEvent) {
        match event {
            ConnectionEvent::Open => {
                debug!("[{}] Websocket connection opened", self.name);
            }

            ConnectionEvent::Closed
            | ConnectionEvent::ConnectionError(_)
            | ConnectionEvent::PingTimeout => {
                warn!("[{}] Websocket disconnected: {:?}", self.name, event);
                self.transition_to_reconnect();
            }

            ConnectionEvent::Message {
                channel_id,
                message,
                authorized,
            } => {
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
                self.handle_peer_offline(channel_id).await;
            }
        }
    }

    async fn handle_message(&mut self, _channel_id: String, message: JsonValue, _authorized: bool) {
        if let Err(e) = self
            .channel
            .handle_routing_message(message, &self.service)
            .await
        {
            error!("[{}] handle_message error: {}", self.name, e);
        }
    }

    async fn handle_peer_connected(&mut self, _channel_id: String) {
        self.channel.handle_peer_connected(&self.service).await;
    }

    async fn handle_peer_offline(&mut self, _channel_id: String) {
        self.channel.handle_peer_offline();
    }

    fn handle_channel_error(&mut self, _channel_id: String, code: String, message: Option<String>) {
        // Build the description once. Wrapping an Error in another Error gave
        // the event a doubled "Signaling error: Signaling error: ..." message,
        // and unwrap_or_default left a dangling " - " when the peer sent no
        // message.
        let description = match &message {
            Some(message) => format!("Channel error: {} - {}", code, message),
            None => format!("Channel error: {}", code),
        };

        self.emit(SignalingClientEvent::Error(Error::Signaling(
            description.clone(),
        )));
        self.channel.handle_error(Error::Signaling(description));
    }

    fn calculate_reconnect_delay(&self) -> u32 {
        let delay = 2u32.pow(self.reconnect_counter);
        delay.min(MAX_RECONNECT_WAIT_SECONDS)
    }

    fn transition_to_reconnect(&mut self) {
        if let Some(connected_at) = self.connected_at {
            if connected_at.elapsed() >= RECONNECT_COUNTER_RESET_TIMEOUT {
                self.reconnect_counter = 0;
            }
        }

        self.service.ws_handle = None;
        self.service.ws_event_rx = None;

        self.emit(SignalingClientEvent::ConnectionReconnect);
        self.set_connection_state(SignalingConnectionState::WaitRetry);
    }

    fn set_connection_state(&mut self, new_state: SignalingConnectionState) {
        if self.service.connection_state != new_state {
            self.service.connection_state = new_state;
            let event = SignalingClientEvent::ConnectionStateChange(self.service.connection_state);
            self.emit(event);
        }
    }

    /// Emit an event, logging rather than silently dropping when the
    /// application is not draining the event channel.
    fn emit(&self, event: SignalingClientEvent) {
        if self.event_tx.try_send(event).is_err() {
            warn!(
                "[{}] Dropped event: the event receiver is full or has been dropped",
                self.name
            );
        }
    }

    /// Deliver inbound signaling messages as [`SignalingClientEvent::Message`].
    ///
    /// Installs the channel's message sender. Anything that later calls
    /// [`SignalingChannel::with_msg_channel`] replaces it and takes over
    /// delivery, which ends this task when the sender is dropped.
    fn spawn_message_forwarder(&mut self) {
        let mut message_rx = self.channel.with_msg_channel();
        let event_tx = self.event_tx.clone();
        let name = self.name;

        tokio::spawn(async move {
            while let Some(message) = message_rx.recv().await {
                if event_tx
                    .send(SignalingClientEvent::Message(message))
                    .await
                    .is_err()
                {
                    debug!(
                        "[{}] Event receiver dropped, stopping message forwarder",
                        name
                    );
                    break;
                }
            }
        });
    }

    /// Deliver channel state changes as
    /// [`SignalingClientEvent::ChannelStateChange`].
    fn spawn_channel_state_forwarder(&mut self) {
        let (state_tx, mut state_rx) = mpsc::channel(32);
        self.channel.set_state_sender(state_tx);
        let event_tx = self.event_tx.clone();
        let name = self.name;

        tokio::spawn(async move {
            while let Some(state) = state_rx.recv().await {
                if event_tx
                    .send(SignalingClientEvent::ChannelStateChange(state))
                    .await
                    .is_err()
                {
                    debug!(
                        "[{}] Event receiver dropped, stopping channel state forwarder",
                        name
                    );
                    break;
                }
            }
        });
    }
}

impl SignalingService for SignalingClientService {
    async fn send_routing_message(&self, channel_id: &str, message: JsonValue) {
        if self.connection_state != SignalingConnectionState::Connected {
            return;
        }

        if let Some(handle) = &self.ws_handle {
            let routing_msg = RoutingMessage::Message {
                channel_id: channel_id.to_string(),
                message,
                authorized: None,
            };

            if let Err(e) = handle.send_message(routing_msg).await {
                error!("Failed to send routing message: {}", e);
            }
        }
    }

    async fn send_error(&self, channel_id: &str, error: crate::common::ErrorInfo) {
        if self.connection_state != SignalingConnectionState::Connected {
            return;
        }

        if let Some(handle) = &self.ws_handle {
            let routing_msg = RoutingMessage::Error {
                channel_id: channel_id.to_string(),
                error,
            };

            if let Err(e) = handle.send_message(routing_msg).await {
                error!("Failed to send error message: {}", e);
            }
        }
    }

    fn close_channel(&mut self, _channel_id: &str) {
        // @TODO
    }
}

impl SignalingService for SignalingClient {
    async fn send_routing_message(&self, channel_id: &str, message: JsonValue) {
        self.service.send_routing_message(channel_id, message).await;
    }

    async fn send_error(&self, channel_id: &str, error: crate::common::ErrorInfo) {
        self.service.send_error(channel_id, error).await;
    }

    fn close_channel(&mut self, channel_id: &str) {
        self.service.close_channel(channel_id);
    }
}
