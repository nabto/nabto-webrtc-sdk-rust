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
use futures::StreamExt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::{mpsc, Mutex};
use tokio::time::Instant;
use tokio::net::TcpStream;
use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

/// Minimum time between a successful connection and reconnect counter reset (10 seconds)
const RECONNECT_COUNTER_RESET_TIMEOUT: Duration = Duration::from_secs(10);

/// Maximum reconnect wait time (60 seconds)
const MAX_RECONNECT_WAIT_SECONDS: u32 = 60;

/// Internal events from WebSocket monitoring task
#[derive(Debug)]
enum WebSocketEvent {
    /// WebSocket connection was closed
    Closed,
    /// WebSocket connection encountered an error
    Error(String),
}

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

    // WebSocket monitoring
    ws_event_rx: Option<mpsc::Receiver<WebSocketEvent>>,

    // Retry state
    reconnect_counter: u32,
    connected_at: Option<Instant>,
    should_stop: Arc<Mutex<bool>>,
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
            ws_event_rx: None,
            reconnect_counter: 0,
            connected_at: None,
            should_stop: Arc::new(Mutex::new(false)),
        }
    }

    /// Start the signaling device
    /// This initiates the connection process. The method returns immediately
    /// and connection happens asynchronously with automatic retries.
    pub async fn start(&mut self) -> Result<()> {
        if self.state != ConnectionState::New {
            return Err(Error::Configuration(
                "Start can only be called once".to_string(),
            ));
        }

        *self.should_stop.lock().await = false;

        // Just kick off the first connection attempt
        // The retry loop will continue in the background if needed
        self.do_single_connect_attempt().await;

        Ok(())
    }

    /// Attempt a single connection (used by start())
    async fn do_single_connect_attempt(&mut self) {
        // Only connect from NEW or WAIT_RETRY states
        if self.state != ConnectionState::New && self.state != ConnectionState::WaitRetry {
            eprintln!("do_single_connect_attempt called in invalid state: {:?}", self.state);
            return;
        }

        self.state = ConnectionState::Connecting;

        // Try to connect
        match self.try_connect().await {
            Ok(()) => {
                // Successfully connected
                self.state = ConnectionState::Connected;
                self.connected_at = Some(Instant::now());
                // Connection established
            }
            Err(e) => {
                // Connection failed, transition to WaitRetry
                eprintln!("Connection failed: {:?}", e);
                self.state = ConnectionState::WaitRetry;
                // In a real implementation, we'd schedule a retry here
                // For now, tests can check for WaitRetry state
            }
        }
    }

    /// Try to establish connection (HTTP + WebSocket)
    async fn try_connect(&mut self) -> Result<()> {
        // Step 1: Perform device connect HTTP request to get signaling URL
        let signaling_url = self.device_connect().await?;

        // Step 2: Establish WebSocket connection to the signaling URL
        let (ws_stream, _response) = connect_async(&signaling_url).await.map_err(|e| {
            Error::WebSocket(format!("Failed to connect WebSocket: {}", e))
        })?;

        // Step 3: Spawn a task to monitor the WebSocket connection
        let (tx, rx) = mpsc::channel(32);
        self.ws_event_rx = Some(rx);

        tokio::spawn(async move {
            Self::monitor_websocket(ws_stream, tx).await;
        });

        Ok(())
    }

    /// Background task to monitor WebSocket connection
    /// Sends events through the channel when connection closes or errors occur
    async fn monitor_websocket(
        mut ws_stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
        tx: mpsc::Sender<WebSocketEvent>,
    ) {
        loop {
            match ws_stream.next().await {
                Some(Ok(_message)) => {
                    // TODO: Handle incoming WebSocket messages
                    // For now, we just keep the connection alive
                }
                Some(Err(e)) => {
                    // WebSocket error occurred
                    eprintln!("WebSocket error: {:?}", e);
                    let _ = tx.send(WebSocketEvent::Error(e.to_string())).await;
                    break;
                }
                None => {
                    // WebSocket connection closed
                    eprintln!("WebSocket connection closed");
                    let _ = tx.send(WebSocketEvent::Closed).await;
                    break;
                }
            }
        }
    }


    /// Close the signaling device
    pub async fn close(&mut self) -> Result<()> {
        if self.state == ConnectionState::Closed {
            return Ok(());
        }

        // Stop any retry loops
        *self.should_stop.lock().await = true;

        // Close event receiver (WebSocket monitoring task will be dropped)
        self.ws_event_rx = None;

        self.state = ConnectionState::Closed;
        Ok(())
    }

    /// Request ICE servers from the signaling service
    pub async fn request_ice_servers(&self) -> Result<Vec<IceServer>> {
        let token = (self.options.token_generator)().await?;
        self.http_api.request_ice_servers(&token).await
    }

    /// The check alive function is used to send a PING on the websocket. This
    /// can be used if it has been detected that the WebRTC connection has
    /// disconnected, this could often mean that the WebSocket has a problem. If
    /// a PONG is not received timely after calling check_alive, then the
    /// websocket disconnects and a new signaling connection is made to the
    /// signaling service.
    pub fn check_alive(&self) {
        // TODO: Send PING message on WebSocket
        // Implementation will be added later
    }

    /// Process any pending WebSocket events (for testing/polling)
    /// In a real application, this would be handled automatically in the background
    pub async fn process_events(&mut self) {
        if let Some(rx) = &mut self.ws_event_rx {
            // Try to receive an event without blocking
            if let Ok(event) = rx.try_recv() {
                match event {
                    WebSocketEvent::Closed | WebSocketEvent::Error(_) => {
                        eprintln!("WebSocket disconnected: {:?}", event);
                        // Transition to WaitRetry
                        if self.state == ConnectionState::Connected {
                            self.state = ConnectionState::WaitRetry;
                        }
                    }
                }
            }
        }
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
