//! Util module
//!
//! This module contains utility components that build on top of the device module,
//! including message transport, signing, and encoding for WebRTC signaling.

mod client_message_transport;
mod device_message_transport;
mod message_encoder;
mod message_transport;
mod signing;

// Re-export public types
pub use client_message_transport::{ClientMessageTransport, ClientSecurityMode};
pub use device_message_transport::{
    DeviceMessageTransport, DeviceMessageTransportOptions, IceServerProvider, SecurityMode,
};
pub use message_encoder::{
    IceCandidate, IceServer, MessageEncoder, SessionDescription, SignalingMessage,
    WebrtcSignalingMessage,
};
pub use message_transport::{MessageTransportEvent, MessageTransportMode};

pub use signing::{JwtMessageSigner, MessageSigner, NoneMessageSigner, SigningMessage};
