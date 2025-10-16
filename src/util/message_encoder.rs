//! Message encoder/decoder for WebRTC signaling messages
//!
//! This module implements the WebRTC Signaling layer, which defines the message
//! formats for:
//! - SETUP_REQUEST/SETUP_RESPONSE: Initial channel setup
//! - DESCRIPTION: WebRTC SDP descriptions (offer/answer)
//! - CANDIDATE: WebRTC ICE candidates

use crate::{Error, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value as JsonValue;

/// WebRTC signaling message types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[allow(dead_code)] // Used for documentation and future extensions
pub enum WebrtcSignalingMessageType {
    #[serde(rename = "DESCRIPTION")]
    Description,
    #[serde(rename = "CANDIDATE")]
    Candidate,
}

/// Setup message types
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
#[allow(dead_code)] // Used for documentation and future extensions
pub enum SetupMessageType {
    #[serde(rename = "SETUP_REQUEST")]
    SetupRequest,
    #[serde(rename = "SETUP_RESPONSE")]
    SetupResponse,
}

/// RTCIceServer configuration
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IceServer {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credential: Option<String>,
    pub urls: Vec<String>,
}

/// WebRTC session description
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionDescription {
    #[serde(rename = "type")]
    pub desc_type: String, // "offer", "answer", "pranswer", "rollback"
    pub sdp: String,
}

/// WebRTC ICE candidate
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IceCandidate {
    pub candidate: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sdp_mid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub sdp_m_line_index: Option<u32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username_fragment: Option<String>,
}

/// All possible signaling messages
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum SignalingMessage {
    /// Setup request from client
    #[serde(rename = "SETUP_REQUEST")]
    SetupRequest,

    /// Setup response from device with optional ICE servers
    #[serde(rename = "SETUP_RESPONSE")]
    SetupResponse {
        #[serde(rename = "iceServers", skip_serializing_if = "Option::is_none")]
        ice_servers: Option<Vec<IceServer>>,
    },

    /// WebRTC description (offer/answer)
    #[serde(rename = "DESCRIPTION")]
    Description { description: SessionDescription },

    /// WebRTC ICE candidate
    #[serde(rename = "CANDIDATE")]
    Candidate { candidate: IceCandidate },
}

/// WebRTC signaling messages only (no setup messages)
#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(tag = "type")]
pub enum WebrtcSignalingMessage {
    #[serde(rename = "DESCRIPTION")]
    Description { description: SessionDescription },
    #[serde(rename = "CANDIDATE")]
    Candidate { candidate: IceCandidate },
}

impl SignalingMessage {
    /// Check if this is a setup request
    pub fn is_setup_request(&self) -> bool {
        matches!(self, SignalingMessage::SetupRequest)
    }

    /// Check if this is a setup response
    pub fn is_setup_response(&self) -> bool {
        matches!(self, SignalingMessage::SetupResponse { .. })
    }

    /// Check if this is a WebRTC signaling message
    pub fn is_webrtc_signaling(&self) -> bool {
        matches!(
            self,
            SignalingMessage::Description { .. } | SignalingMessage::Candidate { .. }
        )
    }

    /// Convert to WebrtcSignalingMessage if applicable
    pub fn as_webrtc_signaling(&self) -> Option<WebrtcSignalingMessage> {
        match self {
            SignalingMessage::Description { description } => {
                Some(WebrtcSignalingMessage::Description {
                    description: description.clone(),
                })
            }
            SignalingMessage::Candidate { candidate } => Some(WebrtcSignalingMessage::Candidate {
                candidate: candidate.clone(),
            }),
            _ => None,
        }
    }

    /// Get ICE servers from setup response
    pub fn ice_servers(&self) -> Option<Vec<IceServer>> {
        match self {
            SignalingMessage::SetupResponse { ice_servers } => ice_servers.clone(),
            _ => None,
        }
    }
}

/// Message encoder/decoder for signaling messages
pub struct MessageEncoder;

impl MessageEncoder {
    pub fn new() -> Self {
        Self
    }

    /// Encode a signaling message to JSON
    pub fn encode(&self, message: &SignalingMessage) -> Result<JsonValue> {
        serde_json::to_value(message)
            .map_err(|e| Error::Signaling(format!("Failed to encode message: {}", e)))
    }

    /// Decode a JSON value to a signaling message
    pub fn decode(&self, value: JsonValue) -> Result<SignalingMessage> {
        serde_json::from_value(value)
            .map_err(|e| Error::Signaling(format!("Failed to decode message: {}", e)))
    }
}

impl Default for MessageEncoder {
    fn default() -> Self {
        Self::new()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_decode_setup_request() {
        let encoder = MessageEncoder::new();
        let msg = SignalingMessage::SetupRequest;

        let encoded = encoder.encode(&msg).unwrap();
        let decoded = encoder.decode(encoded).unwrap();

        assert!(decoded.is_setup_request());
    }

    #[test]
    fn test_encode_decode_setup_response() {
        let encoder = MessageEncoder::new();
        let msg = SignalingMessage::SetupResponse {
            ice_servers: Some(vec![IceServer {
                username: Some("user".to_string()),
                credential: Some("pass".to_string()),
                urls: vec!["stun:stun.example.com".to_string()],
            }]),
        };

        let encoded = encoder.encode(&msg).unwrap();
        let decoded = encoder.decode(encoded).unwrap();

        assert!(decoded.is_setup_response());
        assert!(decoded.ice_servers().is_some());
    }

    #[test]
    fn test_encode_decode_description() {
        let encoder = MessageEncoder::new();
        let msg = SignalingMessage::Description {
            description: SessionDescription {
                desc_type: "offer".to_string(),
                sdp: "v=0...".to_string(),
            },
        };

        let encoded = encoder.encode(&msg).unwrap();
        let decoded = encoder.decode(encoded).unwrap();

        assert!(decoded.is_webrtc_signaling());
        assert!(decoded.as_webrtc_signaling().is_some());
    }

    #[test]
    fn test_encode_decode_candidate() {
        let encoder = MessageEncoder::new();
        let msg = SignalingMessage::Candidate {
            candidate: IceCandidate {
                candidate: "candidate:...".to_string(),
                sdp_mid: Some("0".to_string()),
                sdp_m_line_index: Some(0),
                username_fragment: None,
            },
        };

        let encoded = encoder.encode(&msg).unwrap();
        let decoded = encoder.decode(encoded).unwrap();

        assert!(decoded.is_webrtc_signaling());
    }
}
