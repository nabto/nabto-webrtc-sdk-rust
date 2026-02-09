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

    /// Pong counter stored at the last heartbeat ping, used to verify liveness
    heartbeat_pong_counter: Option<u64>,

    /// Interval between heartbeat PINGs, or None to disable heartbeat
    heartbeat_interval: Option<Duration>,
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
        heartbeat_interval: Option<Duration>,
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
            heartbeat_pong_counter: None,
            heartbeat_interval,
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

        let mut heartbeat_interval = self.heartbeat_interval.map(|interval| {
            tokio::time::interval_at(Instant::now() + interval, interval)
        });

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

                // Heartbeat: periodically send PING and check for PONG
                _ = async {
                    match heartbeat_interval.as_mut() {
                        Some(interval) => interval.tick().await,
                        None => { std::future::pending::<tokio::time::Instant>().await }
                    }
                } => {
                    if let Some(last_pong_counter) = self.heartbeat_pong_counter {
                        if self.pong_counter <= last_pong_counter {
                            warn!("[{}] Heartbeat timeout - no PONG received since last heartbeat", self.name);
                            let _ = self.event_tx.send(ConnectionEvent::PingTimeout).await;
                            break;
                        }
                    }
                    self.heartbeat_pong_counter = Some(self.pong_counter);
                    if let Err(e) = self.send_routing_message(&RoutingMessage::Ping).await {
                        error!("[{}] Failed to send heartbeat PING: {}", self.name, e);
                        let _ = self.event_tx.send(ConnectionEvent::ConnectionError(e)).await;
                        break;
                    }
                    debug!("[{}] Heartbeat PING sent", self.name);
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

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpListener;

    /// Helper: start a local WebSocket server and return the address.
    /// `handler` is called with the accepted server-side WebSocket stream.
    async fn start_ws_server<F, Fut>(handler: F) -> std::net::SocketAddr
    where
        F: FnOnce(WebSocketStream<TcpStream>) -> Fut + Send + 'static,
        Fut: std::future::Future<Output = ()> + Send,
    {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            let ws = tokio_tungstenite::accept_async(stream).await.unwrap();
            handler(ws).await;
        });
        addr
    }

    /// Connect to a local WebSocket server and create a WebSocketConnection.
    async fn connect(
        addr: std::net::SocketAddr,
        heartbeat_interval: Option<Duration>,
    ) -> (WebSocketHandle, mpsc::Receiver<ConnectionEvent>) {
        let url = format!("ws://127.0.0.1:{}", addr.port());
        let (ws_stream, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
        let (connection, handle, event_rx) =
            WebSocketConnection::new("test", ws_stream, heartbeat_interval);
        tokio::spawn(connection.run());
        (handle, event_rx)
    }

    #[tokio::test]
    async fn test_heartbeat_succeeds_when_pongs_received() {
        let ping_count = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let ping_count_clone = ping_count.clone();

        let addr = start_ws_server(move |mut ws| async move {
            // Respond to every PING with a PONG
            while let Some(Ok(msg)) = ws.next().await {
                if let WsMessage::Text(text) = msg {
                    if let Ok(RoutingMessage::Ping) = serde_json::from_str(&text) {
                        ping_count_clone
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                        let pong = serde_json::to_string(&RoutingMessage::Pong).unwrap();
                        if ws.send(WsMessage::Text(pong)).await.is_err() {
                            break;
                        }
                    }
                }
            }
        })
        .await;

        let (handle, mut event_rx) = connect(addr, Some(Duration::from_millis(100))).await;

        // Consume the Open event
        let event = event_rx.recv().await.unwrap();
        assert!(matches!(event, ConnectionEvent::Open));

        // Wait long enough for several heartbeat cycles
        tokio::time::sleep(Duration::from_millis(350)).await;

        // Close cleanly
        handle.close().await.unwrap();

        // Drain remaining events — should get Closed, never PingTimeout
        while let Some(event) = event_rx.recv().await {
            assert!(
                !matches!(event, ConnectionEvent::PingTimeout),
                "Should not get PingTimeout when server responds to PINGs"
            );
        }

        // Verify the server received multiple heartbeat PINGs
        let count = ping_count.load(std::sync::atomic::Ordering::Relaxed);
        assert!(count >= 3, "Expected at least 3 heartbeat PINGs, got {count}");
    }

    #[tokio::test]
    async fn test_heartbeat_timeout_when_no_pong() {
        let addr = start_ws_server(|mut ws| async move {
            // Read messages but never respond — let the heartbeat time out
            while let Some(Ok(_)) = ws.next().await {}
        })
        .await;

        let (_handle, mut event_rx) = connect(addr, Some(Duration::from_millis(100))).await;

        // Consume the Open event
        let event = event_rx.recv().await.unwrap();
        assert!(matches!(event, ConnectionEvent::Open));

        // The first heartbeat tick sends a PING (at +100ms).
        // The second tick (at +200ms) detects no PONG and emits PingTimeout.
        let event = tokio::time::timeout(Duration::from_secs(1), event_rx.recv())
            .await
            .expect("Should receive PingTimeout within 1s")
            .expect("Channel should not be closed");
        assert!(matches!(event, ConnectionEvent::PingTimeout));
    }

    #[tokio::test]
    async fn test_no_heartbeat_when_disabled() {
        let ping_count = std::sync::Arc::new(std::sync::atomic::AtomicU32::new(0));
        let ping_count_clone = ping_count.clone();

        let addr = start_ws_server(move |mut ws| async move {
            while let Some(Ok(msg)) = ws.next().await {
                if let WsMessage::Text(text) = msg {
                    if let Ok(RoutingMessage::Ping) = serde_json::from_str(&text) {
                        ping_count_clone
                            .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                    }
                }
            }
        })
        .await;

        let (handle, mut event_rx) = connect(addr, None).await;

        let event = event_rx.recv().await.unwrap();
        assert!(matches!(event, ConnectionEvent::Open));

        // Wait — no heartbeat PINGs should be sent
        tokio::time::sleep(Duration::from_millis(300)).await;

        handle.close().await.unwrap();

        let count = ping_count.load(std::sync::atomic::Ordering::Relaxed);
        assert_eq!(count, 0, "No heartbeat PINGs should be sent when disabled");
    }
}
