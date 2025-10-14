//! Signaling Device module
//!
//! This module contains the core implementation for device-side WebRTC signaling.

mod channel;
mod connection;
mod reliability;
mod routing;
mod state;

pub use channel::{SignalingChannel, SignalingChannelEventHandler};
pub use state::{ChannelState, ConnectionState};

use crate::Result;
use std::future::Future;
use std::pin::Pin;

/// Callback type for generating access tokens
pub type TokenGenerator =
    Box<dyn Fn() -> Pin<Box<dyn Future<Output = Result<String>> + Send>> + Send + Sync>;

/// Callback type for handling new signaling channels
pub type NewChannelHandler =
    Box<dyn Fn(SignalingChannel, bool) -> Pin<Box<dyn Future<Output = ()> + Send>> + Send + Sync>;

/// Options for creating a SignalingDevice
pub struct SignalingDeviceOptions {
    /// Optional URL for the signaling service
    pub endpoint_url: Option<String>,

    /// The product ID (e.g., "wp-abcdefghi")
    pub product_id: String,

    /// The device ID (e.g., "wd-jklmnopqr")
    pub device_id: String,

    /// Token generator called when a new access token is needed
    pub token_generator: TokenGenerator,
}

/// The main SignalingDevice interface
pub struct SignalingDevice {
    // Internal state will be added during implementation
}

impl SignalingDevice {
    /// Create a new SignalingDevice
    pub fn new(_options: SignalingDeviceOptions) -> Self {
        Self {}
    }

    /// Start the signaling device
    pub async fn start(&mut self) -> Result<()> {
        Ok(())
    }

    /// Close the signaling device
    pub async fn close(&mut self) -> Result<()> {
        Ok(())
    }

    /// Request ICE servers from the signaling service
    pub async fn request_ice_servers(&self) -> Result<Vec<IceServer>> {
        Ok(vec![])
    }

    /// Check if the connection is still alive
    pub fn check_alive(&self) {}

    /// Get the current connection state
    pub fn connection_state(&self) -> ConnectionState {
        ConnectionState::New
    }

    /// Set the handler for new signaling channels
    pub fn set_new_channel_handler(&mut self, _handler: NewChannelHandler) {}
}

/// ICE server configuration
#[derive(Debug, Clone)]
pub struct IceServer {
    pub urls: Vec<String>,
    pub username: Option<String>,
    pub credential: Option<String>,
}
