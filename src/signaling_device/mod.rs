//! Signaling Device module
//!
//! This module contains the core implementation for device-side WebRTC signaling.

mod channel;
mod connection;
mod http;
mod reliability;
mod routing;
mod state;
mod token;

pub use channel::{SignalingChannel, SignalingChannelEventHandler};
pub use http::IceServer;
pub use state::{ChannelState, ConnectionState};
pub use token::DeviceTokenGenerator;

use http::HttpApi;

use crate::{Error, Result};
use std::future::Future;
use std::pin::Pin;
use tokio_tungstenite::{connect_async, WebSocketStream, MaybeTlsStream};
use tokio::net::TcpStream;

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
    http_api: HttpApi,
    options: SignalingDeviceOptions,
    state: ConnectionState,
    new_channel_handler: Option<NewChannelHandler>,
    ws_connection: Option<WebSocketStream<MaybeTlsStream<TcpStream>>>,
}

impl SignalingDevice {
    /// Create a new SignalingDevice
    pub fn new(options: SignalingDeviceOptions) -> Self {
        let endpoint_url = options
            .endpoint_url
            .clone()
            .unwrap_or_else(|| format!("https://{}.webrtc.nabto.net", options.product_id));

        let http_api = HttpApi::new(
            endpoint_url,
            options.product_id.clone(),
            options.device_id.clone(),
        );

        Self {
            http_api,
            options,
            state: ConnectionState::New,
            new_channel_handler: None,
            ws_connection: None,
        }
    }

    /// Start the signaling device
    pub async fn start(&mut self) -> Result<()> {
        // Update state to Connecting
        self.state = ConnectionState::Connecting;

        // Step 1: Perform device connect HTTP request to get signaling URL
        let signaling_url = self.device_connect().await.map_err(|e| {
            self.state = ConnectionState::Failed;
            e
        })?;

        // Step 2: Establish WebSocket connection to the signaling URL
        let (ws_stream, _response) = connect_async(&signaling_url).await.map_err(|e| {
            self.state = ConnectionState::Failed;
            Error::WebSocket(format!("Failed to connect WebSocket: {}", e))
        })?;

        // Store the WebSocket connection
        self.ws_connection = Some(ws_stream);

        // Step 3: Update state to Connected
        self.state = ConnectionState::Connected;

        Ok(())
    }

    /// Close the signaling device
    pub async fn close(&mut self) -> Result<()> {
        // Implementation will be added later
        self.state = ConnectionState::Closed;
        Ok(())
    }

    /// Request ICE servers from the signaling service
    pub async fn request_ice_servers(&self) -> Result<Vec<IceServer>> {
        let token = (self.options.token_generator)().await?;
        self.http_api.request_ice_servers(&token).await
    }

    /// Check if the connection is still alive
    pub fn check_alive(&self) {
        // Implementation will be added later
    }

    /// Get the current connection state
    pub fn connection_state(&self) -> ConnectionState {
        self.state
    }

    /// Set the handler for new signaling channels
    pub fn set_new_channel_handler(&mut self, handler: NewChannelHandler) {
        self.new_channel_handler = Some(handler);
    }

    /// Internal method to perform device connect HTTP request
    pub(crate) async fn device_connect(&self) -> Result<String> {
        let token = (self.options.token_generator)().await?;
        let response = self.http_api.device_connect(&token).await?;
        Ok(response.signaling_url)
    }
}
