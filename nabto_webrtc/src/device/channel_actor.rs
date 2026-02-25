//! Per-channel actor for handling signaling messages without cross-channel blocking.
//!
//! Each channel gets its own tokio task that processes reliability, ACK sending,
//! and message delivery independently, eliminating head-of-line blocking between channels.

use crate::common::channel::{ChannelHandle, ChannelRequest, SignalingChannel, SignalingService};
use crate::common::routing::{ErrorInfo, RoutingMessage};
use crate::common::{SignalingChannelState, WebSocketHandle};
use crate::Error;
use log::{debug, error};
use serde_json::Value as JsonValue;
use tokio::sync::mpsc;

use super::DeviceEvent;

/// Messages that can be sent to a channel actor.
pub(crate) enum ChannelActorMessage {
    /// Inbound routing message from WebSocket
    RoutingMessage(JsonValue),
    /// Outbound message from application (via ChannelRequest)
    SendMessage(JsonValue),
    /// Outbound error from application (via ChannelRequest)
    SendError(ErrorInfo),
    /// Peer connected notification
    PeerConnected,
    /// Peer offline notification
    PeerOffline,
    /// Channel error from signaling server
    Error {
        code: String,
        message: Option<String>,
    },
    /// WebSocket reconnected with new handle
    WebSocketReconnected(WebSocketHandle),
    /// WebSocket connection lost
    ConnectionLost,
    /// Shut down actor
    Close,
}

/// SignalingService implementation for channel actors.
/// Sends messages directly via the WebSocket handle.
struct ChannelActorService {
    ws_handle: Option<WebSocketHandle>,
    should_close: bool,
}

impl ChannelActorService {
    fn new(ws_handle: WebSocketHandle) -> Self {
        Self {
            ws_handle: Some(ws_handle),
            should_close: false,
        }
    }
}

impl SignalingService for ChannelActorService {
    async fn send_routing_message(&self, channel_id: &str, message: JsonValue) {
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

    async fn send_error(&self, channel_id: &str, error: ErrorInfo) {
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
        self.should_close = true;
    }
}

/// Parameters for spawning a channel actor.
pub(crate) struct ChannelActorParams {
    pub name: &'static str,
    pub channel_id: String,
    pub initial_message: JsonValue,
    pub authorized: bool,
    pub ws_handle: WebSocketHandle,
    pub channel_request_tx: mpsc::Sender<ChannelRequest>,
    pub device_event_tx: mpsc::Sender<DeviceEvent>,
    pub rx: mpsc::Receiver<ChannelActorMessage>,
}

/// Run a channel actor task.
///
/// Processes the initial message, emits a `DeviceEvent::NewChannel`, then loops
/// handling `ChannelActorMessage`s until `Close` is received or the sender is dropped.
pub(crate) async fn run_channel_actor(params: ChannelActorParams) {
    let ChannelActorParams {
        name,
        channel_id,
        initial_message,
        authorized,
        ws_handle,
        channel_request_tx,
        device_event_tx,
        mut rx,
    } = params;

    let mut service = ChannelActorService::new(ws_handle);

    // Create the channel with a message delivery channel
    let channel_base = SignalingChannel::new(name, channel_id.clone());
    let (mut channel, message_rx) = channel_base.with_message_channel();
    channel.set_state(SignalingChannelState::Connected);
    channel.set_device_sender(channel_request_tx.clone());

    // Process the initial message
    if let Err(e) = channel
        .handle_routing_message(initial_message, &service)
        .await
    {
        error!(
            "[{}] Error handling initial message on channel {}: {:?}",
            name, channel_id, e
        );
        return;
    }

    // Emit NewChannel event to the application
    let handle = ChannelHandle::new(channel_id.clone(), channel_request_tx);
    let event = DeviceEvent::NewChannel {
        handle,
        message_rx,
        authorized,
    };
    if let Err(e) = device_event_tx.try_send(event) {
        error!("[{}] Failed to emit NewChannel event: {:?}", name, e);
        return;
    }

    // Main actor loop
    while let Some(msg) = rx.recv().await {
        match msg {
            ChannelActorMessage::RoutingMessage(message) => {
                if let Err(e) = channel.handle_routing_message(message, &service).await {
                    error!(
                        "[{}] Error handling message on channel {}: {:?}",
                        name, channel_id, e
                    );
                }
            }
            ChannelActorMessage::SendMessage(message) => {
                if let Err(e) = channel.send_message_async(message, &service).await {
                    error!(
                        "[{}] Failed to send message on channel {}: {:?}",
                        name, channel_id, e
                    );
                }
            }
            ChannelActorMessage::SendError(error_info) => {
                service.send_error(&channel_id, error_info).await;
            }
            ChannelActorMessage::PeerConnected => {
                channel.handle_peer_connected(&service).await;
            }
            ChannelActorMessage::PeerOffline => {
                channel.handle_peer_offline();
            }
            ChannelActorMessage::Error { code, message } => {
                let error = Error::Signaling(format!(
                    "Channel error: {} - {}",
                    code,
                    message.unwrap_or_default()
                ));
                channel.handle_error(error);
            }
            ChannelActorMessage::WebSocketReconnected(new_handle) => {
                service.ws_handle = Some(new_handle);
                channel.handle_websocket_reconnect(&service).await;
            }
            ChannelActorMessage::ConnectionLost => {
                service.ws_handle = None;
            }
            ChannelActorMessage::Close => {
                break;
            }
        }

        if service.should_close {
            break;
        }
    }

    debug!("[{}] Channel actor {} shutting down", name, channel_id);
}
