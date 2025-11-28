//! Routing layer message types
//!
//! Defines the WebSocket routing protocol messages.

#![allow(dead_code)]

use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

/// Routing layer message types
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum RoutingMessage {
    /// Message containing data to relay
    #[serde(rename = "MESSAGE")]
    Message {
        #[serde(rename = "channelId")]
        channel_id: String,
        message: JsonValue,
        #[serde(skip_serializing_if = "Option::is_none")]
        authorized: Option<bool>,
    },

    /// Error message
    #[serde(rename = "ERROR")]
    Error {
        #[serde(rename = "channelId")]
        channel_id: String,
        error: ErrorInfo,
    },

    /// Peer connected notification
    #[serde(rename = "PEER_CONNECTED")]
    PeerConnected {
        #[serde(rename = "channelId")]
        channel_id: String,
    },

    /// Peer offline notification
    #[serde(rename = "PEER_OFFLINE")]
    PeerOffline {
        #[serde(rename = "channelId")]
        channel_id: String,
    },

    /// Ping message
    #[serde(rename = "PING")]
    Ping,

    /// Pong response
    #[serde(rename = "PONG")]
    Pong,
}

/// Error information in routing messages
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct ErrorInfo {
    pub code: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
}

/// Routing error codes
pub mod error_codes {
    pub const DECODE_ERROR: &str = "DECODE_ERROR";
    pub const VERIFICATION_ERROR: &str = "VERIFICATION_ERROR";
    pub const CHANNEL_CLOSED: &str = "CHANNEL_CLOSED";
    pub const CHANNEL_NOT_FOUND: &str = "CHANNEL_NOT_FOUND";
    pub const NO_MORE_CHANNELS: &str = "NO_MORE_CHANNELS";
    pub const ACCESS_DENIED: &str = "ACCESS_DENIED";
    pub const INTERNAL_ERROR: &str = "INTERNAL_ERROR";
}
