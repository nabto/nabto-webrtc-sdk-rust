//! Util module
//!
//! This module contains utility components that build on top of the device module,
//! including message transport, signing, and encoding for WebRTC signaling.

mod message_encoder;
mod message_transport;
mod signing;

// Re-export public types
pub use message_encoder::{
    IceCandidate, IceServer, MessageEncoder, SessionDescription, SignalingMessage,
    WebrtcSignalingMessage,
};
pub use message_transport::{
    DeviceMessageTransport, DeviceMessageTransportOptions, DeviceTransportEvent,
    IceServerProvider, MessageTransportMode, SecurityMode,
};
pub use signing::{JwtMessageSigner, MessageSigner, NoneMessageSigner, SigningMessage};
