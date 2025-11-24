use tokio::sync::mpsc;
use serde_json::Value as JsonValue;
use tokio::time::Instant;
use tokio_tungstenite::connect_async;
use std::time::Duration;
use crate::Error;
use crate::common::http::HttpApi;
use crate::common::SignalingConnectionState;
use crate::common::SignalingChannelState;
use crate::common::websocket::ConnectionEvent;
use crate::common::websocket::WebSocketConfig;
use crate::common::websocket::WebSocketConnection;
use crate::common::websocket::WebSocketHandle;

pub enum SignalingClientEvent {
    Message(JsonValue),
    ConnectionReconnect,
    ConnectionStateChange(SignalingConnectionState),
    ChannelStateChange(SignalingChannelState),
    Error
}

pub struct SignalingClientOptions {
    pub product_id: String,
    pub device_id: String,
    pub require_online: Option<bool>,
    pub endpoint_url: Option<String>,
    pub access_token: Option<String>    
}

pub struct SignalingClient {
    http_api: HttpApi,

    event_tx: mpsc::Sender<SignalingClientEvent>,
    event_rx: mpsc::Receiver<SignalingClientEvent>,

    // Websocket
    ws_handle: Option<WebSocketHandle>,
    ws_event_rx: Option<mpsc::Receiver<ConnectionEvent>>,

    connection_state: SignalingConnectionState
}

impl SignalingClient {
    pub fn new(options: SignalingClientOptions) -> Self {
        let (event_tx, event_rx) = mpsc::channel(32);

        let endpoint_url = options
            .endpoint_url
            .clone()
            .unwrap_or_else(|| format!("https://{}.webrtc.nabto.net", options.product_id));

        let http_api = HttpApi::new(
            endpoint_url,
            options.product_id.clone(),
            options.device_id.clone()
        );

        let client = Self {
            http_api,
            event_tx,
            event_rx,
            connection_state: SignalingConnectionState::New,
            ws_handle: None,
            ws_event_rx: None
        };

        client
    }

    pub async fn run(&mut self) -> Result<(), crate::error::Error> {
        if self.connection_state != SignalingConnectionState::New {
            // @TODO: Return error
            return Ok(());
        }

        println!("Looping!");

        loop {
            match self.connection_state {
                SignalingConnectionState::New | SignalingConnectionState::WaitRetry => {
                    if self.connection_state == SignalingConnectionState::WaitRetry {
                        // @TODO: Handle WaitRetry case
                    }

                    self.set_connection_state(SignalingConnectionState::Connecting);

                    match self.try_connect().await {
                        Ok(()) => {
                            self.set_connection_state(SignalingConnectionState::Connected);
                            //self.connected_at = Some(Instant::now());
                            //self.reconnect_counter = 0;
                            self.try_connect().await?;
                            println!("Successfully connected to signaling service");
                        }

                        Err(e) => {
                            eprintln!("Connection failed: {:?}", e);
                            self.set_connection_state(SignalingConnectionState::WaitRetry);
                            // @TODO: reconnect counter
                        }
                    }
                }

                SignalingConnectionState::Connected => {
                    if let Some(ws_rx) = &mut self.ws_event_rx {
                        match ws_rx.recv().await {
                            Some(event) => {
                                self.handle_websocket_event(event).await;
                            }

                            None => {
                                eprintln!("Websocket event channel was closed");
                                // @TODO: Transition to reconnect?
                            }
                        }
                    } else {
                        eprintln!("SignalingClient is in CONNECTED state but there is no websocket handle");
                        break;
                    }
                }

                SignalingConnectionState::Connecting => {
                    // Sleep for a bit
                    tokio::time::sleep(Duration::from_millis(100)).await;
                }

                SignalingConnectionState::Closed => {
                    break;
                }

                SignalingConnectionState::Failed => {
                    // @TODO: Go to retry state
                }
            }
        }

        Ok(())
    }

    async fn try_connect(&mut self) -> Result<(), Error> {
        let response = self.http_api.client_connect(None).await?;
        let signaling_url = response.signaling_url;

        let (ws_stream, _response) = connect_async(&signaling_url)
            .await
            .map_err(|e| Error::WebSocket(format!("Failed to connect websocket: {}", e)))?;

        let config = WebSocketConfig::default();
        let (connection, handle, event_rx) = WebSocketConnection::new(ws_stream, config);

        self.ws_handle = Some(handle);
        self.ws_event_rx = Some(event_rx);

        tokio::spawn(async move {
            connection.run().await;
        });

        Ok(())
    }

    async fn handle_websocket_event(&mut self, event: ConnectionEvent) {
        match event {
            ConnectionEvent::Open => {
                println!("Websocket connection opened");
            }

            ConnectionEvent::Closed |
            ConnectionEvent::ConnectionError(_) |
            ConnectionEvent::PingTimeout => {
                eprintln!("Websocket disconnected: {:?}", event);
                // @TODO: Transition to reconnect
            }

            ConnectionEvent::Message { channel_id, message, authorized } => {
                self.handle_message(channel_id, message, authorized).await;
            }

            ConnectionEvent::Error { channel_id, code, message } => {
                eprintln!("Websocket error: {}, {:?}", code, message);
            }

            ConnectionEvent::PeerConnected { channel_id } => {
                self.handle_peer_connected(channel_id).await;
            }

            ConnectionEvent::PeerOffline { channel_id } => {
                self.handle_peer_offline(channel_id).await;
            }
        }
    }

    async fn handle_message(&mut self, channel_id: String, message: JsonValue, authorized: bool) {

    }

    async fn handle_peer_connected(&mut self, channel_id: String) {

    }

    async fn handle_peer_offline(&mut self, channel_id: String) {
        
    }

    fn set_connection_state(&mut self, new_state: SignalingConnectionState) {
        if self.connection_state != new_state {
            self.connection_state = new_state;
            let event = SignalingClientEvent::ConnectionStateChange(self.connection_state);
            let _ = self.event_tx.try_send(event);
        }
    }
}