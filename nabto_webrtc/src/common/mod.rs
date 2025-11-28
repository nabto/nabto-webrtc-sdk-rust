pub mod channel;
pub mod connection;
pub mod http;
pub mod reliability;
pub mod routing;
pub mod websocket;

pub use connection::{ConnectionEvent, WebSocketConfig, WebSocketConnection, WebSocketHandle};
pub use http::{HttpApi, IceServer};
pub use routing::{error_codes, ErrorInfo, RoutingMessage};

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
