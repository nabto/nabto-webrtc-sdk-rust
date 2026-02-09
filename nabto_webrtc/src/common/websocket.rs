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
use log::{debug, error, trace, warn};
use serde_json::Value as JsonValue;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio::sync::mpsc;
use tokio::time::Instant;
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

/// WebSocket connection manager
pub struct WebSocketConnection {
    /// Name used for logging purposes (e.g., "client" or "device")
    name: &'static str,

    /// Channel to send events to the application
    event_tx: mpsc::Sender<ConnectionEvent>,

    /// Channel to receive commands from the application
    command_rx: mpsc::Receiver<ConnectionCommand>,

    /// WebSocket stream
    ws_stream: WebSocketStream<MaybeTlsStream<TcpStream>>,

    /// Counter incremented for each PONG received
    pong_counter: u64,

    /// State for check_alive: (pong_counter at check time, deadline instant)
    check_alive_state: Option<(u64, Instant)>,
}

/// Commands that can be sent to the WebSocket connection
#[derive(Debug)]
pub enum ConnectionCommand {
    /// Send a routing message
    SendMessage(RoutingMessage),

    /// Send a PING and expect a PONG within the given timeout (milliseconds)
    CheckAlive { timeout_ms: u64 },

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

    /// Send a PING and expect a PONG within the given timeout (milliseconds)
    pub async fn check_alive(&self, timeout_ms: u64) -> Result<(), String> {
        self.command_tx
            .send(ConnectionCommand::CheckAlive { timeout_ms })
            .await
            .map_err(|e| format!("Failed to send check_alive: {}", e))
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
        name: &'static str,
        ws_stream: WebSocketStream<MaybeTlsStream<TcpStream>>,
    ) -> (Self, WebSocketHandle, mpsc::Receiver<ConnectionEvent>) {
        let (event_tx, event_rx) = mpsc::channel(128);
        let (command_tx, command_rx) = mpsc::channel(32);

        let connection = Self {
            name,
            event_tx,
            command_rx,
            ws_stream,
            pong_counter: 0,
            check_alive_state: None,
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

        loop {
            // Calculate the timeout future - only active when check_alive is pending
            let timeout_future = async {
                match self.check_alive_state {
                    Some((_pong_counter_at_check, deadline)) => {
                        tokio::time::sleep_until(deadline).await;
                    }
                    None => {
                        // No check_alive pending, wait forever (other branches will wake us)
                        std::future::pending::<()>().await;
                    }
                }
            };

            tokio::select! {
                // Check for PONG timeout if check_alive is pending
                _ = timeout_future => {
                    if let Some((pong_counter_at_check, _deadline)) = self.check_alive_state.take() {
                        if self.pong_counter == pong_counter_at_check {
                            warn!("[{}] PING timeout - no PONG received since check_alive", self.name);
                            let _ = self.event_tx.send(ConnectionEvent::PingTimeout).await;
                            break;
                        }
                        debug!("[{}] check_alive succeeded (pong_counter incremented)", self.name);
                    }
                }

                // Receive WebSocket messages
                result = self.ws_stream.next() => {
                    match result {
                        Some(Ok(msg)) => {
                            if let Err(e) = self.handle_ws_message(msg).await {
                                error!("[{}] Error handling WebSocket message: {}", self.name, e);
                            }
                        }
                        Some(Err(e)) => {
                            error!("[{}] WebSocket error: {}", self.name, e);
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
                                error!("[{}] Failed to send message: {}", self.name, e);
                                let _ = self.event_tx.send(ConnectionEvent::ConnectionError(e)).await;
                            }
                        }
                        Some(ConnectionCommand::CheckAlive { timeout_ms }) => {
                            // Store current pong counter and set deadline
                            let deadline = Instant::now() + Duration::from_millis(timeout_ms);
                            self.check_alive_state = Some((self.pong_counter, deadline));

                            if let Err(e) = self.send_routing_message(&RoutingMessage::Ping).await {
                                error!("[{}] Failed to send PING: {}", self.name, e);
                                let _ = self.event_tx.send(ConnectionEvent::ConnectionError(e)).await;
                                break;
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
                    debug!(
                        "[{}] WebSocket closed with code {} reason: {}",
                        self.name, code, reason
                    );
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
        // Log the WebSocket message being received
        trace!("[{}] Received message: {}", self.name, text);

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
            RoutingMessage::Error { channel_id, error } => {
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
                // Increment pong counter (check_alive will compare against this)
                self.pong_counter += 1;
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

        // Log the WebSocket message being sent
        trace!("[{}] Sending message: {}", self.name, json);

        self.ws_stream
            .send(WsMessage::Text(json))
            .await
            .map_err(|e| format!("Failed to send WebSocket message: {}", e))?;

        Ok(())
    }
}
