use tokio::signal;
use tokio::sync::mpsc;
use serde_json::Value as JsonValue;
use tokio::time::Instant;
use tokio_tungstenite::connect_async;
use std::cell::RefCell;
use std::sync::{Arc, Mutex, RwLock};
use std::time::Duration;
use crate::Error;
use crate::common::http::HttpApi;
use crate::common::SignalingConnectionState;
use crate::common::SignalingChannelState;
use crate::common::websocket::ConnectionEvent;
use crate::common::websocket::WebSocketConfig;
use crate::common::websocket::WebSocketConnection;
use crate::common::websocket::WebSocketHandle;
use crate::device::ChannelHandle;
use crate::device::ChannelRequest;
use crate::device::ConnectionState;
use crate::device::SignalingChannel;
use crate::device::SignalingService;
use crate::device::routing::RoutingMessage;

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

pub struct SignalingClientService {
    pub ws_handle: Option<WebSocketHandle>,
    pub ws_event_rx: Option<mpsc::Receiver<ConnectionEvent>>,
    pub connection_state: SignalingConnectionState
}

pub struct SignalingClient {
    signaling_url: String,
    
    event_tx: mpsc::Sender<SignalingClientEvent>,
    
    // Channel
    channel_id: String,
    pub channel: SignalingChannel,
    pub channel_handle: ChannelHandle,
    channel_request_tx: mpsc::Sender<ChannelRequest>,
    channel_request_rx: mpsc::Receiver<ChannelRequest>,

    // Websocket
    service: SignalingClientService
}

impl SignalingClient {
    pub async fn new(options: SignalingClientOptions) -> Result<(
        Self,
        mpsc::Receiver<SignalingClientEvent>
    ), Error> {
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

        let (channel_request_tx, channel_request_rx) = mpsc::channel(32);

        let response = http_api.client_connect(None).await?;
        let signaling_url = response.signaling_url;
        let device_online = response.device_online.unwrap_or(false);

        if let Some(cid) = response.channel_id {
            let mut client = Self {
                signaling_url: signaling_url,

                channel: SignalingChannel::new(cid.clone()),
                channel_id: cid.clone(),
                channel_handle: ChannelHandle::new(cid.clone(), channel_request_tx.clone()),
                channel_request_tx,
                channel_request_rx,

                event_tx,

                service: SignalingClientService {
                    connection_state: SignalingConnectionState::New,
                    ws_handle: None,
                    ws_event_rx: None
                }
            };

            if device_online {
                client.channel.set_state(crate::device::ChannelState::Connected);
            }

            Ok((client, event_rx))
        } else {
            Err(Error::Configuration("Failed to create SignalingClient".to_string()))
        }
    }

    pub async fn run(&mut self) -> Result<(), crate::error::Error> {
        if self.service.connection_state != SignalingConnectionState::New {
            // @TODO: Return error
            return Ok(());
        }


        loop {
            match self.service.connection_state {
                SignalingConnectionState::New | SignalingConnectionState::WaitRetry => {
                    if self.service.connection_state == SignalingConnectionState::WaitRetry {
                        // @TODO: Handle WaitRetry case
                    }

                    self.set_connection_state(SignalingConnectionState::Connecting);

                    match self.try_connect().await {
                        Ok(()) => {
                            //self.connected_at = Some(Instant::now());
                            //self.reconnect_counter = 0;
                            self.set_connection_state(SignalingConnectionState::Connected);
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
                    if let Some(ws_rx) = &mut self.service.ws_event_rx {
                        tokio::select! {
                            event = ws_rx.recv() => {
                                match event {
                                    Some(event) => {
                                        self.handle_websocket_event(event).await;
                                    }

                                    None => {
                                        eprintln!("Websocket event channel was closed");
                                        // @TODO: Transition to reconnect?
                                    }
                                }
                            }

                            ch_req = self.channel_request_rx.recv() => {
                                if let Some(request) = ch_req {
                                    self.handle_channel_request(request).await;
                                }
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

    async fn handle_channel_request(&mut self, request: ChannelRequest) {
        match request {
            ChannelRequest::SendMessage { channel_id, message } => {
                self.channel.send_message_async(message, &self.service).await;
            }

            ChannelRequest::SendError { channel_id, error } => {
            }

            ChannelRequest::Close { channel_id } => {
            }
        }
    }

    async fn try_connect(&mut self) -> Result<(), Error> {
        let (ws_stream, _response) = connect_async(&self.signaling_url)
            .await
            .map_err(|e| Error::WebSocket(format!("Failed to connect websocket: {}", e)))?;

        let config = WebSocketConfig::default();
        let (connection, handle, event_rx) = WebSocketConnection::new(ws_stream, config);

        self.service.ws_handle = Some(handle);
        self.service.ws_event_rx = Some(event_rx);

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

    async fn handle_message(&mut self, _channel_id: String, message: JsonValue, _authorized: bool) {
        if let Err(e) = self.channel.handle_routing_message(message, &self.service).await {
            eprintln!("SignalingClient::handle_message error: {}", e);
        }
    }

    async fn handle_peer_connected(&mut self, _channel_id: String) {
        self.channel.handle_peer_connected(&self.service).await;
    }

    async fn handle_peer_offline(&mut self, _channel_id: String) {
        self.channel.handle_peer_offline();
    }

    fn set_connection_state(&mut self, new_state: SignalingConnectionState) {
        if self.service.connection_state != new_state {
            self.service.connection_state = new_state;
            let event = SignalingClientEvent::ConnectionStateChange(self.service.connection_state);
            let _ = self.event_tx.try_send(event);
        }
    }
}

impl SignalingService for SignalingClientService {
    async fn send_routing_message(&self, channel_id: &str, message: JsonValue) {
        if self.connection_state != SignalingConnectionState::Connected {
            return;
        }

        if let Some(handle) = &self.ws_handle {
            let routing_msg = RoutingMessage::Message {
                channel_id: channel_id.to_string(),
                message,
                authorized: None
            };

            if let Err(e) = handle.send_message(routing_msg).await {
                eprintln!("Failed to send routing message: {}", e);
            }
        }

    }

    async fn send_error(&self, channel_id: &str, error: crate::device::ErrorInfo) {
        todo!()
    }

    fn close_channel(&mut self, channel_id: &str) {
        todo!()
    }
}

impl SignalingService for SignalingClient {
    async fn send_routing_message(&self, channel_id: &str, message: JsonValue) {
        self.service.send_routing_message(channel_id, message).await;
    }

    async fn send_error(&self, channel_id: &str, error: crate::device::ErrorInfo) {
        self.service.send_error(channel_id, error).await;
    }

    fn close_channel(&mut self, channel_id: &str) {
        self.service.close_channel(channel_id);
    }
}
