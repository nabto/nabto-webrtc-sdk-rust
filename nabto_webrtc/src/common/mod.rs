pub mod http;
pub mod websocket;
pub mod routing;
pub mod reliability;
pub mod channel;
pub mod connection;

pub use connection::{ConnectionEvent, WebSocketConfig, WebSocketConnection, WebSocketHandle};
pub use routing::{RoutingMessage, ErrorInfo, error_codes};
pub use http::{HttpApi, IceServer};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignalingConnectionState {
    /// Initial state
    New,
    /// Attempting to connect
    Connecting,
    /// Successfully connected
    Connected,
    /// Waiting before retry
    WaitRetry,
    /// Connection failed
    Failed,
    /// Connection closed
    Closed,
}

/// Channel state between device and client peers
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SignalingChannelState {
    /// Initial state
    New,
    /// Peer is connected
    Connected,
    /// Peer is disconnected
    Disconnected,
    /// Channel failed
    Failed,
    /// Channel closed
    Closed,
}
