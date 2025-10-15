//! WebSocket connection management
//!
//! This module implements the WebSocket layer for the Nabto WebRTC Signaling protocol.
//! It handles:
//! - WebSocket stream management
//! - Routing layer message encoding/decoding
//! - PING/PONG keepalive handling
//! - Event emission for connection lifecycle

use super::routing::RoutingMessage;
use futures::{SinkExt, StreamExt};
use serde_json::Value as JsonValue;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::{interval, Instant};
use tokio_tungstenite::{
    tungstenite::{protocol::CloseFrame, Message as WsMessage},
    MaybeTlsStream, WebSocketStream,
};

/// Events emitted by the WebSocket connection
#[derive(Debug, Clone)]
pub enum ConnectionEvent {
    /// Connection successfully opened
    Open,

    /// Received a MESSAGE routing message
    Message {
        channel_id: String,
        message: JsonValue,
        authorized: bool,
    },

    /// Received an ERROR routing message
    Error {
        channel_id: String,
        code: String,
        message: Option<String>,
    },

    /// Received PEER_CONNECTED notification
    PeerConnected { channel_id: String },

    /// Received PEER_OFFLINE notification
    PeerOffline { channel_id: String },

    /// Connection closed
    Closed,

    /// Connection error occurred
    ConnectionError(String),

    /// PING timeout - no PONG received
    PingTimeout,
}

/// Configuration for WebSocket connection
#[derive(Debug, Clone)]
pub struct WebSocketConfig {
    /// How often to send PING messages (milliseconds)
    pub ping_interval_ms: u64,

    /// Maximum time to wait for PONG response (milliseconds)
    pub pong_timeout_ms: u64,
}

impl Default for WebSocketConfig {
    fn default() -> Self {
        Self {
            ping_interval_ms: 30_000, // 30 seconds
            pong_timeout_ms: 5_000,   // 5 seconds
        }
    }
}

/// WebSocket connection manager
pub struct WebSocketConnection {
    /// Channel to send events to the application
    event_tx: mpsc::Sender<ConnectionEvent>,

    /// Channel to receive commands from the application
    command_rx: mpsc::Receiver<ConnectionCommand>,

    /// WebSocket stream
    ws_stream: WebSocketStream<MaybeTlsStream<TcpStream>>,

    /// Configuration
    config: WebSocketConfig,

    /// Last time a PONG was received
    last_pong: Instant,
}

/// Commands that can be sent to the WebSocket connection
#[derive(Debug)]
pub enum ConnectionCommand {
    /// Send a routing message
    SendMessage(RoutingMessage),

    /// Send a PING
    SendPing,

    /// Close the connection
    Close,
}

/// Handle for controlling the WebSocket connection
#[derive(Clone)]
pub struct WebSocketHandle {
    command_tx: mpsc::Sender<ConnectionCommand>,
}

impl WebSocketHandle {
    /// Send a routing message through the WebSocket
    pub async fn send_message(&self, message: RoutingMessage) -> Result<(), String> {
        self.command_tx
            .send(ConnectionCommand::SendMessage(message))
            .await
            .map_err(|e| format!("Failed to send message: {}", e))
    }

    /// Send a PING message
    pub async fn send_ping(&self) -> Result<(), String> {
        self.command_tx
            .send(ConnectionCommand::SendPing)
            .await
            .map_err(|e| format!("Failed to send ping: {}", e))
    }

    /// Close the connection
    pub async fn close(&self) -> Result<(), String> {
        self.command_tx
            .send(ConnectionCommand::Close)
            .await
            .map_err(|e| format!("Failed to close connection: {}", e))
    }
}

impl WebSocketConnection {
    /// Create a new WebSocket connection
    ///
    /// Returns the connection object, a handle for sending commands, and a receiver for events
    pub fn new(
        ws_stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
        config: WebSocketConfig,
    ) -> (Self, WebSocketHandle, mpsc::Receiver<ConnectionEvent>) {
        let (event_tx, event_rx) = mpsc::channel(128);
        let (command_tx, command_rx) = mpsc::channel(32);

        let connection = Self {
            event_tx,
            command_rx,
            ws_stream,
            config,
            last_pong: Instant::now(),
        };

        let handle = WebSocketHandle { command_tx };

        (connection, handle, event_rx)
    }

    /// Run the WebSocket connection
    ///
    /// This should be spawned as a task: `tokio::spawn(connection.run())`
    pub async fn run(mut self) {
        // Notify that connection is open
        let _ = self.event_tx.send(ConnectionEvent::Open).await;

        // Set up PING timer
        let ping_duration = Duration::from_millis(self.config.ping_interval_ms);
        let mut ping_interval = interval(ping_duration);
        ping_interval.tick().await; // First tick completes immediately

        loop {
            tokio::select! {
                // Check for PING timeout and send PING
                _ = ping_interval.tick() => {
                    if self.check_ping_timeout() {
                        eprintln!("PING timeout - closing connection");
                        let _ = self.event_tx.send(ConnectionEvent::PingTimeout).await;
                        break;
                    }

                    // Send PING
                    if let Err(e) = self.send_routing_message(&RoutingMessage::Ping).await {
                        eprintln!("Failed to send PING: {}", e);
                        let _ = self.event_tx.send(ConnectionEvent::ConnectionError(e)).await;
                        break;
                    }
                }

                // Receive WebSocket messages
                result = self.ws_stream.next() => {
                    match result {
                        Some(Ok(msg)) => {
                            if let Err(e) = self.handle_ws_message(msg).await {
                                eprintln!("Error handling WebSocket message: {}", e);
                            }
                        }
                        Some(Err(e)) => {
                            eprintln!("WebSocket error: {}", e);
                            let _ = self.event_tx.send(ConnectionEvent::ConnectionError(e.to_string())).await;
                            break;
                        }
                        None => {
                            // WebSocket closed
                            let _ = self.event_tx.send(ConnectionEvent::Closed).await;
                            break;
                        }
                    }
                }

                // Receive commands from application
                cmd = self.command_rx.recv() => {
                    match cmd {
                        Some(ConnectionCommand::SendMessage(msg)) => {
                            if let Err(e) = self.send_routing_message(&msg).await {
                                eprintln!("Failed to send message: {}", e);
                                let _ = self.event_tx.send(ConnectionEvent::ConnectionError(e)).await;
                            }
                        }
                        Some(ConnectionCommand::SendPing) => {
                            if let Err(e) = self.send_routing_message(&RoutingMessage::Ping).await {
                                eprintln!("Failed to send PING: {}", e);
                            }
                        }
                        Some(ConnectionCommand::Close) | None => {
                            // Close requested or command channel closed
                            let _ = self.ws_stream.close(None).await;
                            let _ = self.event_tx.send(ConnectionEvent::Closed).await;
                            break;
                        }
                    }
                }
            }
        }
    }

    /// Check if PING timeout has occurred
    fn check_ping_timeout(&self) -> bool {
        let timeout_duration = Duration::from_millis(self.config.pong_timeout_ms);
        self.last_pong.elapsed() > timeout_duration
    }

    /// Handle incoming WebSocket message
    async fn handle_ws_message(&mut self, msg: WsMessage) -> Result<(), String> {
        match msg {
            WsMessage::Text(text) => {
                self.handle_routing_message(&text).await?;
            }
            WsMessage::Binary(_) => {
                // Binary messages not expected in this protocol
                return Err("Received unexpected binary message".to_string());
            }
            WsMessage::Ping(_) => {
                // WebSocket library handles PING/PONG at WebSocket protocol level
                // This is different from our application-level PING/PONG
            }
            WsMessage::Pong(_) => {
                // WebSocket protocol level PONG
            }
            WsMessage::Close(frame) => {
                if let Some(CloseFrame { code, reason }) = frame {
                    eprintln!("WebSocket closed with code {} reason: {}", code, reason);
                }
            }
            WsMessage::Frame(_) => {
                // Raw frames not expected
            }
        }
        Ok(())
    }

    /// Handle incoming routing message
    async fn handle_routing_message(&mut self, text: &str) -> Result<(), String> {
        let routing_msg: RoutingMessage = serde_json::from_str(text)
            .map_err(|e| format!("Failed to parse routing message: {}", e))?;

        match routing_msg {
            RoutingMessage::Message {
                channel_id,
                message,
                authorized,
            } => {
                let _ = self
                    .event_tx
                    .send(ConnectionEvent::Message {
                        channel_id,
                        message,
                        authorized: authorized.unwrap_or(false),
                    })
                    .await;
            }
            RoutingMessage::Error {
                channel_id,
                error,
            } => {
                let _ = self
                    .event_tx
                    .send(ConnectionEvent::Error {
                        channel_id,
                        code: error.code,
                        message: error.message,
                    })
                    .await;
            }
            RoutingMessage::PeerConnected { channel_id } => {
                let _ = self
                    .event_tx
                    .send(ConnectionEvent::PeerConnected { channel_id })
                    .await;
            }
            RoutingMessage::PeerOffline { channel_id } => {
                let _ = self
                    .event_tx
                    .send(ConnectionEvent::PeerOffline { channel_id })
                    .await;
            }
            RoutingMessage::Pong => {
                // Update last PONG time
                self.last_pong = Instant::now();
            }
            RoutingMessage::Ping => {
                // Respond with PONG
                self.send_routing_message(&RoutingMessage::Pong).await?;
            }
        }

        Ok(())
    }

    /// Send a routing message through the WebSocket
    async fn send_routing_message(&mut self, msg: &RoutingMessage) -> Result<(), String> {
        let json = serde_json::to_string(msg)
            .map_err(|e| format!("Failed to serialize message: {}", e))?;

        self.ws_stream
            .send(WsMessage::Text(json))
            .await
            .map_err(|e| format!("Failed to send WebSocket message: {}", e))?;

        Ok(())
    }
}
