use super::message_encoder::{IceServer, WebrtcSignalingMessage};

/// Events emitted by the Message Transport
#[derive(Debug)]
pub enum MessageTransportEvent {
    /// WebRTC signaling message received
    WebrtcSignalingMessage(WebrtcSignalingMessage),

    /// Setup completed with optional ICE servers
    SetupDone(Option<Vec<IceServer>>),

    /// Error occurred
    Error(String),
}

/// State of the DeviceMessageTransport
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
 pub(super) enum State {
    /// Waiting for the first message from the client
    WaitFirstMessage,

    /// In setup phase (exchanging SETUP_REQUEST/RESPONSE)
    Setup,

    /// In signaling phase (exchanging WebRTC messages)
    Signaling,
}

/// Transport mode for perfect negotiation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageTransportMode {
    /// Client mode (impolite peer in perfect negotiation)
    Client,

    /// Device mode (polite peer in perfect negotiation)
    Device,
}
