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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::common::reliability::ReliabilityMessage;
    use futures::StreamExt;
    use std::collections::HashMap;
    use std::time::Duration;
    use tokio::net::TcpListener;

    /// Create a reliability DATA message as JSON.
    fn make_data_message(seq: u32, data: JsonValue) -> JsonValue {
        serde_json::to_value(ReliabilityMessage::Data { seq, data }).unwrap()
    }

    /// Create a WebSocketHandle backed by a local WebSocket server that drains
    /// all incoming messages (no backpressure).
    async fn create_draining_ws_handle() -> WebSocketHandle {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let mut ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            while let Some(Ok(_)) = ws.next().await {}
        });

        let url = format!("ws://127.0.0.1:{}", addr.port());
        let (ws_stream, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
        let (connection, handle, _event_rx) =
            crate::common::WebSocketConnection::new("test", ws_stream, None);
        tokio::spawn(connection.run());
        handle
    }

    /// Create a WebSocketHandle where the server does NOT read messages.
    /// This eventually causes backpressure on sends once TCP buffers fill.
    async fn create_non_reading_ws_handle() -> WebSocketHandle {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let _ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            // Hold the connection open but never read — causes backpressure
            std::future::pending::<()>().await;
        });

        let url = format!("ws://127.0.0.1:{}", addr.port());
        let (ws_stream, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
        let (connection, handle, _event_rx) =
            crate::common::WebSocketConnection::new("test", ws_stream, None);
        tokio::spawn(connection.run());
        handle
    }

    /// Spawn a channel actor and return its sender.
    async fn spawn_actor(
        channel_id: &str,
        ws_handle: WebSocketHandle,
        device_event_tx: mpsc::Sender<DeviceEvent>,
        channel_request_tx: mpsc::Sender<ChannelRequest>,
    ) -> mpsc::Sender<ChannelActorMessage> {
        let (tx, rx) = mpsc::channel(32);
        let init_msg = make_data_message(0, serde_json::json!(format!("init_{}", channel_id)));
        tokio::spawn(run_channel_actor(ChannelActorParams {
            name: "test",
            channel_id: channel_id.to_string(),
            initial_message: init_msg,
            authorized: false,
            ws_handle,
            channel_request_tx,
            device_event_tx,
            rx,
        }));
        tx
    }

    /// Collect NewChannel events and return message receivers keyed by channel ID.
    async fn collect_channels(
        event_rx: &mut mpsc::Receiver<DeviceEvent>,
        count: usize,
    ) -> HashMap<String, mpsc::Receiver<JsonValue>> {
        let mut result = HashMap::new();
        for _ in 0..count {
            match tokio::time::timeout(Duration::from_secs(2), event_rx.recv())
                .await
                .expect("Timeout waiting for NewChannel")
                .expect("Event channel closed")
            {
                DeviceEvent::NewChannel {
                    handle, message_rx, ..
                } => {
                    result.insert(handle.channel_id().to_string(), message_rx);
                }
                other => panic!("Expected NewChannel, got {:?}", other),
            }
        }
        result
    }

    /// Both channel actors receive and deliver messages independently.
    #[tokio::test]
    async fn test_channel_actors_deliver_messages_independently() {
        let ws_handle = create_draining_ws_handle().await;
        let (device_event_tx, mut device_event_rx) = mpsc::channel(32);
        let (channel_request_tx, _channel_request_rx) = mpsc::channel(32);

        let actor_a = spawn_actor(
            "ch_a",
            ws_handle.clone(),
            device_event_tx.clone(),
            channel_request_tx.clone(),
        )
        .await;
        let actor_b = spawn_actor(
            "ch_b",
            ws_handle.clone(),
            device_event_tx.clone(),
            channel_request_tx.clone(),
        )
        .await;

        let mut rxs = collect_channels(&mut device_event_rx, 2).await;
        let mut msg_rx_a = rxs.remove("ch_a").unwrap();
        let mut msg_rx_b = rxs.remove("ch_b").unwrap();

        // Consume initial messages
        assert_eq!(
            msg_rx_a.recv().await.unwrap(),
            serde_json::json!("init_ch_a")
        );
        assert_eq!(
            msg_rx_b.recv().await.unwrap(),
            serde_json::json!("init_ch_b")
        );

        // Send 5 messages to each actor
        for i in 1..=5u32 {
            actor_a
                .send(ChannelActorMessage::RoutingMessage(make_data_message(
                    i,
                    serde_json::json!(format!("a_{}", i)),
                )))
                .await
                .unwrap();
            actor_b
                .send(ChannelActorMessage::RoutingMessage(make_data_message(
                    i,
                    serde_json::json!(format!("b_{}", i)),
                )))
                .await
                .unwrap();
        }

        // Verify both channels receive all messages in order
        for i in 1..=5u32 {
            let msg = tokio::time::timeout(Duration::from_secs(1), msg_rx_a.recv())
                .await
                .expect("Timeout receiving from channel A")
                .unwrap();
            assert_eq!(msg, serde_json::json!(format!("a_{}", i)));
        }
        for i in 1..=5u32 {
            let msg = tokio::time::timeout(Duration::from_secs(1), msg_rx_b.recv())
                .await
                .expect("Timeout receiving from channel B")
                .unwrap();
            assert_eq!(msg, serde_json::json!(format!("b_{}", i)));
        }
    }

    /// Channel B delivers messages even when Channel A's actor is blocked on
    /// WebSocket ACK sends. This is the core head-of-line blocking test.
    ///
    /// Because `handle_routing_message` delivers to `message_rx` (via `try_send`)
    /// *before* sending the ACK, message delivery succeeds even when the ACK
    /// send would block. With per-channel actors, each channel processes in its
    /// own task, so one channel's blocked ACK send cannot delay another channel's
    /// message delivery.
    #[tokio::test]
    async fn test_no_hol_blocking_under_ws_backpressure() {
        let ws_handle = create_non_reading_ws_handle().await;
        let (device_event_tx, mut device_event_rx) = mpsc::channel(32);
        let (channel_request_tx, _channel_request_rx) = mpsc::channel(32);

        let actor_a = spawn_actor(
            "ch_a",
            ws_handle.clone(),
            device_event_tx.clone(),
            channel_request_tx.clone(),
        )
        .await;
        let actor_b = spawn_actor(
            "ch_b",
            ws_handle.clone(),
            device_event_tx.clone(),
            channel_request_tx.clone(),
        )
        .await;

        let mut rxs = collect_channels(&mut device_event_rx, 2).await;
        let mut _msg_rx_a = rxs.remove("ch_a").unwrap();
        let mut msg_rx_b = rxs.remove("ch_b").unwrap();

        // Consume initial messages (these succeed before any backpressure)
        _msg_rx_a.recv().await.unwrap();
        msg_rx_b.recv().await.unwrap();

        // Flood actor A with many messages. Each triggers an ACK send through
        // ws_handle. Once the WebSocket write buffer fills, actor A's task blocks
        // in the ACK send. We use try_send to avoid blocking the test itself.
        for i in 1..=200u32 {
            let _ = actor_a.try_send(ChannelActorMessage::RoutingMessage(make_data_message(
                i,
                serde_json::json!("flood"),
            )));
        }

        // Give actor A time to start processing and potentially block on ACK sends
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Send a message to actor B
        actor_b
            .send(ChannelActorMessage::RoutingMessage(make_data_message(
                1,
                serde_json::json!("important"),
            )))
            .await
            .unwrap();

        // Channel B should receive its message quickly. In the old sequential
        // model, this would be blocked by channel A's processing. With per-channel
        // actors, channel B processes independently.
        let result = tokio::time::timeout(Duration::from_millis(500), msg_rx_b.recv()).await;
        assert!(
            result.is_ok(),
            "Channel B should receive message without HOL blocking from channel A"
        );
        assert_eq!(result.unwrap().unwrap(), serde_json::json!("important"));
    }

    /// Closing one channel actor does not affect other channels.
    #[tokio::test]
    async fn test_channel_close_does_not_affect_other_channels() {
        let ws_handle = create_draining_ws_handle().await;
        let (device_event_tx, mut device_event_rx) = mpsc::channel(32);
        let (channel_request_tx, _channel_request_rx) = mpsc::channel(32);

        let actor_a = spawn_actor(
            "ch_a",
            ws_handle.clone(),
            device_event_tx.clone(),
            channel_request_tx.clone(),
        )
        .await;
        let actor_b = spawn_actor(
            "ch_b",
            ws_handle.clone(),
            device_event_tx.clone(),
            channel_request_tx.clone(),
        )
        .await;

        let mut rxs = collect_channels(&mut device_event_rx, 2).await;
        let mut _msg_rx_a = rxs.remove("ch_a").unwrap();
        let mut msg_rx_b = rxs.remove("ch_b").unwrap();

        _msg_rx_a.recv().await.unwrap();
        msg_rx_b.recv().await.unwrap();

        // Close channel A
        actor_a.send(ChannelActorMessage::Close).await.unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Channel B should still work
        actor_b
            .send(ChannelActorMessage::RoutingMessage(make_data_message(
                1,
                serde_json::json!("still_works"),
            )))
            .await
            .unwrap();

        let msg = tokio::time::timeout(Duration::from_secs(1), msg_rx_b.recv())
            .await
            .expect("Channel B should still work after channel A is closed")
            .unwrap();
        assert_eq!(msg, serde_json::json!("still_works"));
    }

    /// Channel error on one channel does not affect other channels.
    #[tokio::test]
    async fn test_channel_error_does_not_affect_other_channels() {
        let ws_handle = create_draining_ws_handle().await;
        let (device_event_tx, mut device_event_rx) = mpsc::channel(32);
        let (channel_request_tx, _channel_request_rx) = mpsc::channel(32);

        let actor_a = spawn_actor(
            "ch_a",
            ws_handle.clone(),
            device_event_tx.clone(),
            channel_request_tx.clone(),
        )
        .await;
        let actor_b = spawn_actor(
            "ch_b",
            ws_handle.clone(),
            device_event_tx.clone(),
            channel_request_tx.clone(),
        )
        .await;

        let mut rxs = collect_channels(&mut device_event_rx, 2).await;
        let mut _msg_rx_a = rxs.remove("ch_a").unwrap();
        let mut msg_rx_b = rxs.remove("ch_b").unwrap();

        _msg_rx_a.recv().await.unwrap();
        msg_rx_b.recv().await.unwrap();

        // Trigger an error on channel A (sets it to Failed state)
        actor_a
            .send(ChannelActorMessage::Error {
                code: "TEST_ERROR".to_string(),
                message: Some("test error".to_string()),
            })
            .await
            .unwrap();
        tokio::time::sleep(Duration::from_millis(50)).await;

        // Channel B should still receive messages
        actor_b
            .send(ChannelActorMessage::RoutingMessage(make_data_message(
                1,
                serde_json::json!("unaffected"),
            )))
            .await
            .unwrap();

        let msg = tokio::time::timeout(Duration::from_secs(1), msg_rx_b.recv())
            .await
            .expect("Channel B should still work after channel A errors")
            .unwrap();
        assert_eq!(msg, serde_json::json!("unaffected"));
    }

    /// WebSocket reconnection is forwarded to actors and they resume sending.
    #[tokio::test]
    async fn test_websocket_reconnect_updates_actor() {
        let ws_handle_1 = create_draining_ws_handle().await;
        let (device_event_tx, mut device_event_rx) = mpsc::channel(32);
        let (channel_request_tx, _channel_request_rx) = mpsc::channel(32);

        let actor = spawn_actor(
            "ch_a",
            ws_handle_1,
            device_event_tx.clone(),
            channel_request_tx.clone(),
        )
        .await;

        let mut rxs = collect_channels(&mut device_event_rx, 1).await;
        let mut msg_rx = rxs.remove("ch_a").unwrap();
        msg_rx.recv().await.unwrap(); // consume initial

        // Simulate connection lost
        actor
            .send(ChannelActorMessage::ConnectionLost)
            .await
            .unwrap();

        // Create a new WebSocket handle (simulating reconnection)
        let ws_handle_2 = create_draining_ws_handle().await;
        actor
            .send(ChannelActorMessage::WebSocketReconnected(ws_handle_2))
            .await
            .unwrap();

        // Actor should still deliver messages after reconnection
        actor
            .send(ChannelActorMessage::RoutingMessage(make_data_message(
                1,
                serde_json::json!("after_reconnect"),
            )))
            .await
            .unwrap();

        let msg = tokio::time::timeout(Duration::from_secs(1), msg_rx.recv())
            .await
            .expect("Actor should deliver messages after reconnection")
            .unwrap();
        assert_eq!(msg, serde_json::json!("after_reconnect"));
    }
}
