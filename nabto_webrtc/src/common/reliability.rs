//! Reliability layer implementation
//!
//! Handles message retransmission and acknowledgments using sequence numbers.

#![allow(dead_code)]

use log::trace;
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

/// Reliability message types
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum ReliabilityMessage {
    /// Data message with sequence number
    #[serde(rename = "DATA")]
    Data { seq: u32, data: JsonValue },

    /// Acknowledgment for a sequence number
    #[serde(rename = "ACK")]
    Ack { seq: u32 },
}

/// Reliability layer for ensuring ordered, reliable message delivery
#[derive(Debug, Clone)]
pub struct Reliability {
    /// Name used for logging purposes (e.g., "client" or "device")
    name: &'static str,

    /// Channel ID used for logging purposes
    channel_id: String,

    /// Next sequence number to send
    send_seq: u32,

    /// Next sequence number expected to receive
    recv_seq: u32,

    /// Unacknowledged messages waiting for ACK
    unacked_messages: Vec<ReliabilityMessage>,
}

impl Reliability {
    /// Create a new reliability layer
    pub fn new(name: &'static str, channel_id: String) -> Self {
        Self {
            name,
            channel_id,
            send_seq: 0,
            recv_seq: 0,
            unacked_messages: Vec::new(),
        }
    }

    /// Send a reliable message
    pub fn send_reliable_message(&mut self, data: JsonValue) -> ReliabilityMessage {
        let assigned_seq = self.send_seq;
        trace!(
            "[{}] [{}] Assigning sequence number {} to message",
            self.name,
            self.channel_id,
            assigned_seq
        );
        trace!(
            "[{}] [{}] Message preview: {:?}",
            self.name,
            self.channel_id,
            serde_json::to_string(&data)
                .unwrap_or_else(|_| "failed to serialize".to_string())
                .chars()
                .take(150)
                .collect::<String>()
        );

        let msg = ReliabilityMessage::Data {
            seq: assigned_seq,
            data,
        };
        self.send_seq += 1;
        trace!(
            "[{}] [{}] Next sequence number will be {}",
            self.name,
            self.channel_id,
            self.send_seq
        );

        self.unacked_messages.push(msg.clone());
        msg
    }

    /// Handle an incoming routing message
    pub fn handle_routing_message(&mut self, message: ReliabilityMessage) -> Option<JsonValue> {
        match message {
            ReliabilityMessage::Ack { seq } => {
                self.handle_ack(seq);
                None
            }
            ReliabilityMessage::Data { seq, data } => self.handle_data(seq, data),
        }
    }

    /// Handle ACK message
    fn handle_ack(&mut self, seq: u32) {
        trace!(
            "[{}] [{}] Received ACK for seq {}",
            self.name,
            self.channel_id,
            seq
        );
        trace!(
            "[{}] [{}] Unacked queue has {} messages",
            self.name,
            self.channel_id,
            self.unacked_messages.len()
        );
        if let Some(ReliabilityMessage::Data { seq: first_seq, .. }) = self.unacked_messages.first()
        {
            trace!(
                "[{}] [{}] First unacked seq: {}",
                self.name,
                self.channel_id,
                first_seq
            );
            if *first_seq == seq {
                self.unacked_messages.remove(0);
                trace!(
                    "[{}] [{}] Removed message with seq {} from unacked queue. {} remaining",
                    self.name,
                    self.channel_id,
                    seq,
                    self.unacked_messages.len()
                );
            } else {
                trace!(
                    "[{}] [{}] ACK seq {} doesn't match first unacked seq {}",
                    self.name,
                    self.channel_id,
                    seq,
                    first_seq
                );
            }
        } else {
            trace!(
                "[{}] [{}] No unacked messages to ACK",
                self.name,
                self.channel_id
            );
        }
    }

    /// Handle DATA message
    fn handle_data(&mut self, seq: u32, data: JsonValue) -> Option<JsonValue> {
        if seq < self.recv_seq {
            // Duplicate - send ACK but discard data
            None
        } else if seq == self.recv_seq {
            // Expected message
            self.recv_seq += 1;
            Some(data)
        } else {
            // Future message - discard
            None
        }
    }

    /// Get ACK message for a sequence number
    pub fn create_ack(&self, seq: u32) -> ReliabilityMessage {
        ReliabilityMessage::Ack { seq }
    }

    /// Called when peer connects/reconnects - retransmit unacked messages
    pub fn handle_peer_connected(&self) -> Vec<ReliabilityMessage> {
        self.unacked_messages.clone()
    }

    /// Called when websocket connects/reconnects - retransmit unacked messages
    pub fn handle_connect(&self) -> Vec<ReliabilityMessage> {
        trace!(
            "[{}] [{}] handle_connect called, {} unacked messages",
            self.name,
            self.channel_id,
            self.unacked_messages.len()
        );
        for (i, msg) in self.unacked_messages.iter().enumerate() {
            trace!(
                "[{}] [{}] Unacked message {}: {:?}",
                self.name,
                self.channel_id,
                i,
                msg
            );
        }
        self.unacked_messages.clone()
    }

    /// Check if a message is an initial message (seq 0)
    pub fn is_initial_message(message: &ReliabilityMessage) -> bool {
        matches!(message, ReliabilityMessage::Data { seq: 0, .. })
    }

    /// Get the current receive sequence number
    pub fn recv_seq(&self) -> u32 {
        self.recv_seq
    }
}

impl Default for Reliability {
    fn default() -> Self {
        Self::new("unknown", String::new())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn init_logger() {
        let _ = env_logger::builder().is_test(true).try_init();
    }

    #[test]
    fn test_reliability_send_and_ack() {
        init_logger();

        let mut reliability = Reliability::new("test", "test-channel".to_string());

        // Send a message - should log at trace level
        let data = serde_json::json!({"test": "message"});
        let msg = reliability.send_reliable_message(data);

        // Verify it's a DATA message with seq 0
        match &msg {
            ReliabilityMessage::Data { seq, .. } => assert_eq!(*seq, 0),
            _ => panic!("Expected DATA message"),
        }

        // Handle an ACK - should log at trace level
        let ack = ReliabilityMessage::Ack { seq: 0 };
        let result = reliability.handle_routing_message(ack);
        assert!(result.is_none()); // ACK returns None
    }
}
