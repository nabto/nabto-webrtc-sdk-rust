//! Signaling channel implementation

use super::reliability::{Reliability, ReliabilityMessage};
use super::routing::{error_codes, ErrorInfo};
use super::state::ChannelState;
use crate::{Error, Result};
use serde_json::Value as JsonValue;
use std::cell::RefCell;
use std::collections::VecDeque;
use tokio::sync::mpsc;

/// Request from a SignalingChannel to the SignalingDevice
#[derive(Debug, Clone)]
pub enum ChannelRequest {
    /// Send a routing message on this channel
    SendMessage {
        channel_id: String,
        message: JsonValue,
    },
    /// Send an error on this channel
    SendError {
        channel_id: String,
        error: ErrorInfo,
    },
    /// Close this channel
    Close { channel_id: String },
}

/// Lightweight handle to a signaling channel for sending messages
/// This handle can be cloned and shared, while the actual channel state
/// remains owned by the SignalingDevice
#[derive(Clone)]
pub struct ChannelHandle {
    channel_id: String,
    device_tx: mpsc::Sender<ChannelRequest>,
}

impl ChannelHandle {
    /// Create a new channel handle
    pub fn new(channel_id: String, device_tx: mpsc::Sender<ChannelRequest>) -> Self {
        Self {
            channel_id,
            device_tx,
        }
    }

    /// Get the channel ID
    pub fn channel_id(&self) -> &str {
        &self.channel_id
    }

    /// Send a message through this channel
    pub async fn send_message(&self, message: JsonValue) -> crate::Result<()> {
        self.device_tx
            .send(ChannelRequest::SendMessage {
                channel_id: self.channel_id.clone(),
                message,
            })
            .await
            .map_err(|e| crate::Error::Signaling(format!("Failed to send message: {}", e)))
    }

    /// Send an error through this channel
    pub async fn send_error(&self, error: ErrorInfo) -> crate::Result<()> {
        self.device_tx
            .send(ChannelRequest::SendError {
                channel_id: self.channel_id.clone(),
                error,
            })
            .await
            .map_err(|e| crate::Error::Signaling(format!("Failed to send error: {}", e)))
    }

    /// Close this channel
    pub async fn close(&self) -> crate::Result<()> {
        self.device_tx
            .send(ChannelRequest::Close {
                channel_id: self.channel_id.clone(),
            })
            .await
            .map_err(|e| crate::Error::Signaling(format!("Failed to close channel: {}", e)))
    }
}

/// Trait for SignalingChannel to communicate with SignalingDevice
pub trait SignalingService {
    /// Send a routing message on this channel
    fn send_routing_message(&self, channel_id: &str, message: JsonValue) -> impl std::future::Future<Output = ()> + Send;

    /// Send an error message for this channel
    fn send_error(&self, channel_id: &str, error: ErrorInfo) -> impl std::future::Future<Output = ()> + Send;

    /// Notify device that this channel is being closed
    fn close_channel(&mut self, channel_id: &str);
}

/// Event handler trait for signaling channel events
pub trait SignalingChannelEventHandler: Send + Sync {
    /// Called when a message is received
    fn on_message(&self, message: JsonValue);

    /// Called when the channel state changes
    fn on_channel_state_change(&self, state: ChannelState);

    /// Called when an error occurs
    fn on_error(&self, error: crate::Error);
}

/// Operations to be processed in order
#[derive(Debug, Clone)]
#[allow(dead_code)] // NewChannel variant planned for future use
enum Operation {
    /// Initial channel setup operation
    NewChannel,
    /// Message to be delivered to application
    Message(JsonValue),
}

/// Represents a logical channel between two peers through the WebSocket relay
#[derive(Debug, Clone)]
pub struct SignalingChannel {
    channel_id: String,
    state: ChannelState,
    reliability: Reliability,
    operations: VecDeque<Operation>,
    handling_operations: bool,
    message_tx: Option<mpsc::Sender<JsonValue>>,
    /// Channel for sending requests back to the device
    device_tx: Option<mpsc::Sender<ChannelRequest>>,
}

impl SignalingChannel {
    /// Create a new signaling channel for an incoming connection
    pub fn new(channel_id: String) -> Self {
        Self {
            channel_id,
            state: ChannelState::New,
            reliability: Reliability::new(),
            operations: VecDeque::new(),
            handling_operations: false,
            message_tx: None,
            device_tx: None,
        }
    }

    /// Set the device sender for this channel
    /// This allows the channel to send requests back to the device
    pub fn set_device_sender(&mut self, tx: mpsc::Sender<ChannelRequest>) {
        self.device_tx = Some(tx);
    }

    /// Set the message event sender for this channel
    /// This allows the channel to emit message events to the application
    pub fn set_message_sender(&mut self, tx: mpsc::Sender<JsonValue>) {
        self.message_tx = Some(tx);
    }

    /// Get a receiver for messages on this channel
    /// Returns (channel, message_rx) where message_rx receives incoming messages
    pub fn with_message_channel(mut self) -> (Self, mpsc::Receiver<JsonValue>) {
        let (tx, rx) = mpsc::channel(32);
        self.message_tx = Some(tx);
        (self, rx)
    }

    // @TODO: stop-in gap as the above with_message_channel for some reason tries to take ownership
    pub fn with_msg_channel(&mut self) -> mpsc::Receiver<JsonValue> {
        let (tx, rx) = mpsc::channel(32);
        self.message_tx = Some(tx);
        rx
    }

    /// Get the channel ID
    pub fn channel_id(&self) -> &str {
        &self.channel_id
    }

    /// Get the channel state
    pub fn state(&self) -> ChannelState {
        self.state
    }

    /// Set the channel state
    pub fn set_state(&mut self, state: ChannelState) {
        if self.state == state {
            return; // Skip duplicate state changes
        }
        self.state = state;
        // TODO: Emit channelstatechange event
    }

    /// Check if a reliability message is an initial message (seq 0)
    pub fn is_initial_message(message: &JsonValue) -> Result<bool> {
        // Parse as reliability message
        let rel_msg: ReliabilityMessage = serde_json::from_value(message.clone())
            .map_err(|e| Error::Signaling(format!("Failed to parse reliability message: {}", e)))?;

        Ok(Reliability::is_initial_message(&rel_msg))
    }

    /// Handle an incoming routing message
    pub async fn handle_routing_message<S: SignalingService>(
        &mut self,
        message: JsonValue,
        service: &S,
    ) -> Result<()> {
        if self.state == ChannelState::Closed || self.state == ChannelState::Failed {
            return Ok(()); // Ignore messages for closed/failed channels
        }

        // Parse as reliability message
        let rel_msg: ReliabilityMessage = serde_json::from_value(message)
            .map_err(|e| Error::Signaling(format!("Failed to parse reliability message: {}", e)))?;

        // Process through reliability layer
        if let Some(data) = self.reliability.handle_routing_message(rel_msg.clone()) {
            // Queue the message for ordered delivery
            self.operations.push_back(Operation::Message(data));
            self.handle_operations();
        }

        // Send ACK if needed
        if let ReliabilityMessage::Data { seq, .. } = rel_msg {
            if seq <= self.reliability.recv_seq() {
                let ack = self.reliability.create_ack(seq);
                let ack_json = serde_json::to_value(&ack)
                    .map_err(|e| Error::Signaling(format!("Failed to serialize ACK: {}", e)))?;
                service.send_routing_message(&self.channel_id, ack_json).await;
            }
        }

        Ok(())
    }

    /// Handle WebSocket reconnection - retransmit unacked messages
    pub async fn handle_websocket_reconnect<S: SignalingService>(&mut self, service: &S) {
        if self.state == ChannelState::Closed || self.state == ChannelState::Failed {
            eprintln!(
                "[CHANNEL {}] Skipping retransmit - channel state: {:?}",
                self.channel_id, self.state
            );
            return;
        }

        let messages = self.reliability.handle_connect();
        eprintln!(
            "[CHANNEL {}] Retransmitting {} unacked messages",
            self.channel_id,
            messages.len()
        );

        for msg in messages {
            eprintln!(
                "[CHANNEL {}] Retransmitting message: {:?}",
                self.channel_id, msg
            );
            if let Ok(json) = serde_json::to_value(&msg) {
                service.send_routing_message(&self.channel_id, json).await;
            }
        }
    }

    /// Handle peer connected notification
    pub async fn handle_peer_connected<S: SignalingService>(&mut self, service: &S) {
        if self.state == ChannelState::Closed || self.state == ChannelState::Failed {
            return;
        }

        self.set_state(ChannelState::Connected);

        // Retransmit unacked messages
        for msg in self.reliability.handle_peer_connected() {
            if let Ok(json) = serde_json::to_value(&msg) {
                service.send_routing_message(&self.channel_id, json).await;
            }
        }
    }

    /// Handle peer offline notification
    pub fn handle_peer_offline(&mut self) {
        if self.state == ChannelState::Closed || self.state == ChannelState::Failed {
            return;
        }
        self.set_state(ChannelState::Disconnected);
    }

    /// Handle error for this channel
    pub fn handle_error(&mut self, error: Error) {
        if self.state == ChannelState::Closed || self.state == ChannelState::Failed {
            return;
        }
        // TODO: Emit error event
        eprintln!("Channel {} error: {:?}", self.channel_id, error);
        self.set_state(ChannelState::Failed);
    }

    /// Send a message to the other peer
    pub async fn send_message<S: SignalingService>(
        &mut self,
        message: JsonValue,
        service: &S,
    ) -> Result<()> {
        if self.state == ChannelState::Closed || self.state == ChannelState::Failed {
            return Err(Error::Signaling(
                "Cannot send message on closed or failed channel".to_string(),
            ));
        }

        let rel_msg = self.reliability.send_reliable_message(message);
        let json = serde_json::to_value(&rel_msg)
            .map_err(|e| Error::Signaling(format!("Failed to serialize message: {}", e)))?;

        service.send_routing_message(&self.channel_id, json).await;
        Ok(())
    }

    /// Send a message to the other peer (async version)
    /// This version sends directly through the WebSocket without going through SignalingService
    pub async fn send_message_async<S: SignalingService>(&mut self, message: JsonValue, service: &S) -> Result<()> {
        if self.state == ChannelState::Closed || self.state == ChannelState::Failed {
            return Err(Error::Signaling(
                "Cannot send message on closed or failed channel".to_string(),
            ));
        }

        let rel_msg = self.reliability.send_reliable_message(message);
        let json = serde_json::to_value(&rel_msg)
            .map_err(|e| Error::Signaling(format!("Failed to serialize message: {}", e)))?;

        service.send_routing_message(&self.channel_id, json).await;
        Ok(())
    }

    /// Send an error to the other peer
    pub async fn send_error<S: SignalingService>(
        &mut self,
        error_code: &str,
        error_message: Option<&str>,
        service: &S,
    ) {
        if self.state == ChannelState::Closed || self.state == ChannelState::Failed {
            return;
        }

        let error = ErrorInfo {
            code: error_code.to_string(),
            message: error_message.map(|s| s.to_string()),
        };

        service.send_error(&self.channel_id, error).await;
        self.set_state(ChannelState::Failed);
    }

    /// Close the signaling channel
    pub async fn close<S: SignalingService>(&mut self, service: &mut S) {
        if self.state == ChannelState::Closed {
            return;
        }

        if self.state != ChannelState::Failed {
            let error = ErrorInfo {
                code: error_codes::CHANNEL_CLOSED.to_string(),
                message: Some("The channel has been closed".to_string()),
            };
            service.send_error(&self.channel_id, error).await;
        }

        self.operations.clear();
        self.set_state(ChannelState::Closed);
        service.close_channel(&self.channel_id);
    }

    /// Process queued operations
    fn handle_operations(&mut self) {
        if self.state == ChannelState::Closed || self.state == ChannelState::Failed {
            return;
        }

        if self.handling_operations {
            return; // Already processing
        }

        self.handling_operations = true;

        while let Some(op) = self.operations.pop_front() {
            match op {
                Operation::NewChannel => {
                    // Initial channel setup already done
                }
                Operation::Message(msg) => {
                    eprintln!("Channel {} received message: {:?}", self.channel_id, msg);
                    // Send message through channel if available
                    if let Some(tx) = &self.message_tx {
                        // Try to send, ignore if receiver is dropped
                        let _ = tx.try_send(msg);
                    }
                }
            }
        }

        self.handling_operations = false;
    }
}
