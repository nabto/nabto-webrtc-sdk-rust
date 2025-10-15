//! Signaling channel implementation

use super::state::ChannelState;
use crate::Result;
use serde_json::Value as JsonValue;

/// Event handler trait for signaling channel events
pub trait SignalingChannelEventHandler: Send + Sync {
    /// Called when a message is received
    fn on_message(&self, message: JsonValue);

    /// Called when the channel state changes
    fn on_channel_state_change(&self, state: ChannelState);

    /// Called when an error occurs
    fn on_error(&self, error: crate::Error);
}

/// Represents a logical channel between two peers through the WebSocket relay
#[derive(Debug)]
pub struct SignalingChannel {
    channel_id: Option<String>,
    state: ChannelState,
}

impl SignalingChannel {
    /// Create a new signaling channel
    pub fn new(channel_id: Option<String>) -> Self {
        Self {
            channel_id,
            state: ChannelState::New,
        }
    }

    /// Send a message to the other peer
    pub async fn send_message(&self, _message: JsonValue) -> Result<()> {
        // Implementation will be added later
        Ok(())
    }

    /// Send an error to the other peer
    pub async fn send_error(&self, _error_code: &str, _message: Option<&str>) -> Result<()> {
        // Implementation will be added later
        Ok(())
    }

    /// Close the signaling channel
    pub fn close(&mut self) {
        self.state = ChannelState::Closed;
    }

    /// Get the channel state
    pub fn state(&self) -> ChannelState {
        self.state
    }

    /// Get the channel ID
    pub fn channel_id(&self) -> Option<&str> {
        self.channel_id.as_deref()
    }
}
