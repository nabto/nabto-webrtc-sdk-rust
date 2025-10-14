//! Error types for the Nabto WebRTC SDK

use std::fmt;

/// Result type for SDK operations
pub type Result<T> = std::result::Result<T, Error>;

/// Main error type for the SDK
#[derive(Debug)]
pub enum Error {
    /// Connection error
    Connection(String),

    /// Signaling error
    Signaling(String),

    /// Invalid configuration
    Configuration(String),

    /// WebRTC error
    WebRTC(String),

    /// WebSocket error
    WebSocket(String),

    /// Generic error
    Other(String),
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Connection(msg) => write!(f, "Connection error: {}", msg),
            Error::Signaling(msg) => write!(f, "Signaling error: {}", msg),
            Error::Configuration(msg) => write!(f, "Configuration error: {}", msg),
            Error::WebRTC(msg) => write!(f, "WebRTC error: {}", msg),
            Error::WebSocket(msg) => write!(f, "WebSocket error: {}", msg),
            Error::Other(msg) => write!(f, "Error: {}", msg),
        }
    }
}

impl std::error::Error for Error {}
