//! Error types for the Nabto WebRTC SDK

use std::fmt;
use std::time::Duration;

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

    /// An HTTP request to the signaling service returned a non-2xx status.
    ///
    /// `retry_after` is populated from the response's `Retry-After` header for
    /// the statuses where the service uses it (429 and 503). Callers that
    /// implement their own backoff should honour it; see [`Error::retry_after`].
    Http {
        /// HTTP status code of the response.
        status: u16,
        /// How long the service asked the caller to wait before retrying.
        retry_after: Option<Duration>,
        /// Human readable message, from the response body when available.
        message: String,
    },

    /// The device was not online, and
    /// [`require_online`](crate::client::SignalingClientOptionsBuilder::require_online)
    /// was set.
    DeviceOffline,

    /// Generic error
    Other(String),
}

impl Error {
    /// The HTTP status code, if this error came from an HTTP response.
    pub fn status(&self) -> Option<u16> {
        match self {
            Error::Http { status, .. } => Some(*status),
            _ => None,
        }
    }

    /// How long the signaling service asked us to wait before retrying, if it
    /// said so.
    ///
    /// Only populated for the statuses that carry a `Retry-After` header (429
    /// and 503). Callers doing their own retries should prefer this over a
    /// locally invented backoff.
    pub fn retry_after(&self) -> Option<Duration> {
        match self {
            Error::Http { retry_after, .. } => *retry_after,
            _ => None,
        }
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Error::Connection(msg) => write!(f, "Connection error: {}", msg),
            Error::Signaling(msg) => write!(f, "Signaling error: {}", msg),
            Error::Configuration(msg) => write!(f, "Configuration error: {}", msg),
            Error::WebRTC(msg) => write!(f, "WebRTC error: {}", msg),
            Error::WebSocket(msg) => write!(f, "WebSocket error: {}", msg),
            Error::Http {
                status,
                retry_after,
                message,
            } => {
                write!(f, "HTTP error {}: {}", status, message)?;
                if let Some(retry_after) = retry_after {
                    write!(f, " (retry after {}s)", retry_after.as_secs())?;
                }
                Ok(())
            }
            Error::DeviceOffline => write!(f, "The device is not online"),
            Error::Other(msg) => write!(f, "Error: {}", msg),
        }
    }
}

impl std::error::Error for Error {}
