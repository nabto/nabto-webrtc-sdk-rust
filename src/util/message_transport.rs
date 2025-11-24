//! Device Message Transport implementation
//!
//! This module provides the DeviceMessageTransport, which sits on top of a
//! ChannelHandle and handles:
//! - Message encoding/decoding for WebRTC signaling
//! - Message signing/verification (JWT or None)
//! - Channel setup (SETUP_REQUEST/RESPONSE exchange)
//! - Event emission for WebRTC messages, errors, and setup completion

use super::message_encoder::{IceServer, MessageEncoder, SignalingMessage, WebrtcSignalingMessage};
use super::signing::{JwtMessageSigner, MessageSigner, NoneMessageSigner};
use crate::client::SignalingClient;
use crate::device::routing::ErrorInfo;
use crate::device::ChannelHandle;
use crate::{Error, Result};
use serde_json::Value as JsonValue;
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

/// Security mode for the device transport
#[derive(Clone)]
pub enum SecurityMode {
    /// No signing (for unauthorized access or service-based auth)
    None,
    /// JWT signing with shared secret
    SharedSecret {
        /// Callback to get shared secret for a given key ID
        callback: Arc<dyn Fn(Option<String>) -> Result<String> + Send + Sync>,
    },
}

pub enum ClientSecurityMode {
    None,
    SharedSecret { shared_secret: String, key_id: Option<String> }
}

/// Callback type for requesting ICE servers
pub type IceServerProvider = Arc<dyn Fn() -> Pin<Box<dyn Future<Output = Result<Vec<IceServer>>> + Send>> + Send + Sync>;

/// Device message transport options
#[derive(Clone)]
pub struct DeviceMessageTransportOptions {
    pub security_mode: SecurityMode,
    /// Optional callback to request ICE servers from the signaling service
    pub ice_server_provider: Option<IceServerProvider>,
}

/// Events emitted by the DeviceMessageTransport
#[derive(Debug)]
pub enum DeviceTransportEvent {
    /// WebRTC signaling message received
    WebrtcSignalingMessage(WebrtcSignalingMessage),
    /// Setup completed with optional ICE servers
    SetupDone(Option<Vec<IceServer>>),
    /// Error occurred
    Error(String),
}

/// State of the DeviceMessageTransport
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    /// Waiting for the first message from the client
    WaitFirstMessage,
    /// In setup phase (exchanging SETUP_REQUEST/RESPONSE)
    Setup,
    /// In signaling phase (exchanging WebRTC messages)
    Signaling,
}

/// Transport mode for perfect negotiation
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum MessageTransportMode {
    /// Client mode (impolite peer in perfect negotiation)
    Client,
    /// Device mode (polite peer in perfect negotiation)
    Device,
}

/// Device message transport implementation
pub struct DeviceMessageTransport {
    handle: ChannelHandle,
    encoder: MessageEncoder,
    signer: Arc<Mutex<Option<Box<dyn MessageSigner>>>>,
    state: Arc<Mutex<State>>,
    options: DeviceMessageTransportOptions,
    event_tx: mpsc::UnboundedSender<DeviceTransportEvent>,
    event_rx: Arc<Mutex<Option<mpsc::UnboundedReceiver<DeviceTransportEvent>>>>,
}

pub struct ClientMessageTransport {
    handle: ChannelHandle,
    encoder: MessageEncoder,
    signer: Mutex<Option<Box<dyn MessageSigner>>>,

    event_tx: mpsc::UnboundedSender<DeviceTransportEvent>,
    event_rx: Mutex<Option<mpsc::UnboundedReceiver<DeviceTransportEvent>>>,
    state: Mutex<State>
}

impl DeviceMessageTransport {
    /// Create a new DeviceMessageTransport
    ///
    /// Takes a ChannelHandle for sending messages and a message receiver for receiving messages.
    /// The transport will spawn a background task to process incoming messages.
    pub fn new(
        handle: ChannelHandle,
        mut message_rx: mpsc::Receiver<JsonValue>,
        options: DeviceMessageTransportOptions,
    ) -> Self {
        let (event_tx, event_rx) = mpsc::unbounded_channel();

        let transport = Self {
            handle,
            encoder: MessageEncoder::new(),
            signer: Arc::new(Mutex::new(None)),
            state: Arc::new(Mutex::new(State::WaitFirstMessage)),
            options,
            event_tx,
            event_rx: Arc::new(Mutex::new(Some(event_rx))),
        };

        // Spawn a background task to handle incoming messages
        let transport_clone = transport.clone_for_task();
        tokio::spawn(async move {
            while let Some(message) = message_rx.recv().await {
                if let Err(e) = transport_clone
                    .handle_channel_message_internal(message)
                    .await
                {
                    eprintln!("Error handling channel message: {:?}", e);
                    let _ = transport_clone
                        .event_tx
                        .send(DeviceTransportEvent::Error(e.to_string()));
                }
            }
        });

        transport
    }

    /// Clone the parts needed for the background task
    fn clone_for_task(&self) -> Self {
        Self {
            handle: self.handle.clone(),
            encoder: MessageEncoder::new(), // Create new encoder
            signer: Arc::clone(&self.signer),
            state: Arc::clone(&self.state),
            options: self.options.clone(),
            event_tx: self.event_tx.clone(),
            event_rx: Arc::new(Mutex::new(None)), // Task doesn't need the receiver
        }
    }

    /// Get the event receiver (can only be called once)
    pub fn take_event_receiver(&self) -> Option<mpsc::UnboundedReceiver<DeviceTransportEvent>> {
        self.event_rx.lock().unwrap().take()
    }

    /// Get the transport mode (always Device for DeviceMessageTransport)
    pub fn mode(&self) -> MessageTransportMode {
        MessageTransportMode::Device
    }

    /// Setup message signer based on the first message received
    async fn setup_message_signer(&self, message: &JsonValue) -> Result<()> {
        let mut signer_lock = self.signer.lock().unwrap();

        match &self.options.security_mode {
            SecurityMode::None => {
                *signer_lock = Some(Box::new(NoneMessageSigner::new()));
            }
            SecurityMode::SharedSecret { callback } => {
                // Extract key ID from the JWT
                let key_id = JwtMessageSigner::get_key_id(message)?;

                // Get shared secret from callback
                let shared_secret = callback(key_id.clone())?;

                *signer_lock = Some(Box::new(JwtMessageSigner::new(shared_secret, key_id)));
            }
        }

        Ok(())
    }

    /// Handle device setup request
    async fn handle_device_setup_request(&self) -> Result<()> {
        // Request ICE servers from the signaling service if provider is available
        let ice_servers = if let Some(ref provider) = self.options.ice_server_provider {
            match provider().await {
                Ok(servers) => Some(servers),
                Err(e) => {
                    eprintln!("Failed to request ICE servers: {:?}", e);
                    None
                }
            }
        } else {
            None
        };

        // Send SETUP_RESPONSE
        let response = SignalingMessage::SetupResponse {
            ice_servers: ice_servers.clone(),
        };
        self.send_signaling_message(&response).await?;

        // Emit setup done event
        self.emit_setup_done(ice_servers).await;

        Ok(())
    }

    /// Handle a signaling message
    async fn handle_signaling_message(&self, message: SignalingMessage) -> Result<()> {
        let state = *self.state.lock().unwrap();

        match state {
            State::Setup => {
                if message.is_setup_request() {
                    // Handle setup request
                    self.handle_device_setup_request().await?;
                } else {
                    return Err(Error::Signaling(format!(
                        "Expected SETUP_REQUEST but got {:?}",
                        message
                    )));
                }
            }
            State::Signaling => {
                if message.is_webrtc_signaling() {
                    if let Some(webrtc_msg) = message.as_webrtc_signaling() {
                        self.emit_webrtc_signaling_message(webrtc_msg).await;
                    }
                }
            }
            State::WaitFirstMessage => {
                return Err(Error::Signaling(
                    "Should not receive messages in WaitFirstMessage state".to_string(),
                ));
            }
        }

        Ok(())
    }

    /// Handle a message from the signaling channel (internal version for background task)
    async fn handle_channel_message_internal(&self, message: JsonValue) -> Result<()> {
        let state = *self.state.lock().unwrap();

        // Setup message signer on first message
        if state == State::WaitFirstMessage {
            self.setup_message_signer(&message).await?;
            *self.state.lock().unwrap() = State::Setup;
        }

        // Verify message
        let verified = {
            let mut signer = self.signer.lock().unwrap();
            if let Some(ref mut signer) = *signer {
                signer.verify_message(message)?
            } else {
                return Err(Error::Signaling(
                    "Message signer not initialized".to_string(),
                ));
            }
        };

        // Decode message
        let decoded = self.encoder.decode(verified)?;

        // Handle the message
        self.handle_signaling_message(decoded).await
    }

    /// Send a WebRTC signaling message
    pub async fn send_webrtc_signaling_message(
        &self,
        message: &WebrtcSignalingMessage,
    ) -> Result<()> {
        let state = *self.state.lock().unwrap();

        if state != State::Signaling {
            return Err(Error::Signaling(
                "Cannot send WebRTC message before setup is complete".to_string(),
            ));
        }

        // Convert to SignalingMessage
        let signaling_msg = match message {
            WebrtcSignalingMessage::Description { description } => SignalingMessage::Description {
                description: description.clone(),
            },
            WebrtcSignalingMessage::Candidate { candidate } => SignalingMessage::Candidate {
                candidate: candidate.clone(),
            },
        };

        self.send_signaling_message(&signaling_msg).await
    }

    /// Send a signaling message (internal)
    async fn send_signaling_message(&self, message: &SignalingMessage) -> Result<()> {
        // Encode message
        let encoded = self.encoder.encode(message)?;

        // Sign message
        let signed = {
            let mut signer = self.signer.lock().unwrap();
            if let Some(ref mut signer) = *signer {
                signer.sign_message(encoded)?
            } else {
                return Err(Error::Signaling(
                    "Message signer not initialized".to_string(),
                ));
            }
        };

        // Convert to JSON
        let signed_json = serde_json::to_value(&signed)
            .map_err(|e| Error::Signaling(format!("Failed to serialize signed message: {}", e)))?;

        // Send through channel handle
        self.handle.send_message(signed_json).await?;

        Ok(())
    }

    /// Emit a WebRTC signaling message event
    async fn emit_webrtc_signaling_message(&self, message: WebrtcSignalingMessage) {
        let _ = self
            .event_tx
            .send(DeviceTransportEvent::WebrtcSignalingMessage(message));
    }

    /// Emit setup done event
    async fn emit_setup_done(&self, ice_servers: Option<Vec<IceServer>>) {
        *self.state.lock().unwrap() = State::Signaling;
        let _ = self
            .event_tx
            .send(DeviceTransportEvent::SetupDone(ice_servers));
    }

    /// Emit error event
    pub async fn emit_error(&self, error: Error) {
        // Send error to client via handle
        let error_info = ErrorInfo {
            code: "INTERNAL_ERROR".to_string(),
            message: Some(error.to_string()),
        };
        let _ = self.handle.send_error(error_info).await;

        // Emit error event
        let _ = self
            .event_tx
            .send(DeviceTransportEvent::Error(error.to_string()));
    }

    /// Get the channel ID
    pub fn channel_id(&self) -> &str {
        self.handle.channel_id()
    }
}

// Manual Clone implementation for DeviceMessageTransport
impl Clone for DeviceMessageTransport {
    fn clone(&self) -> Self {
        self.clone_for_task()
    }
}

impl ClientMessageTransport {
    pub fn new(
        client: &mut SignalingClient,
        security_mode: ClientSecurityMode
    ) -> Arc<Self> {
        let (event_tx, event_rx) = mpsc::unbounded_channel();

        let transport = Arc::new(Self {
            handle: client.channel_handle.clone(),
            encoder: MessageEncoder::new(),
            signer: match security_mode {
                ClientSecurityMode::None => {
                    Mutex::new(
                        Some(Box::new(NoneMessageSigner::new()))
                    )
                },

                ClientSecurityMode::SharedSecret { shared_secret, key_id  } => { 
                    Mutex::new(Some(Box::new(
                        JwtMessageSigner::new(shared_secret, key_id)
                    )))
                }
            },
            state: Mutex::new(State::Setup),
            event_tx,
            event_rx: Mutex::new(Some(event_rx))
        });

        
        let this = transport.clone();
        let mut message_rx = client.channel.with_msg_channel();
        tokio::spawn(async move {
            while let Some(message) = message_rx.recv().await {
                if let Err(e) = this.handle_channel_message_internal(message).await {
                    eprintln!("Error handling channel message: {:?}", e);
                    let _ = this.event_tx.send(DeviceTransportEvent::Error(e.to_string()));
                }
            }
        });

        transport
    }

    pub async fn start(&self) -> Result<()> {
        self.send_signaling_message(&SignalingMessage::SetupRequest).await
    }

    pub fn mode(&self) -> MessageTransportMode { MessageTransportMode::Client }

    pub async fn send_webrtc_signaling_message(&self, msg: &WebrtcSignalingMessage) -> Result<()> {
        let state = *self.state.lock().unwrap();
        if state != State::Signaling {
            return Self::make_err("Cannot send signaling message before setup is complete");
        }

        let signaling_msg = match msg {
            WebrtcSignalingMessage::Description { description } => SignalingMessage::Description {
                description: description.clone()
            },
            WebrtcSignalingMessage::Candidate { candidate } => SignalingMessage::Candidate {
                candidate: candidate.clone()
            }
        };

        self.send_signaling_message(&signaling_msg).await
    }

    async fn handle_channel_message_internal(&self, message: JsonValue) -> Result<()> {
        //let state = *self.state.lock().unwrap();

        let verified = {
            let mut signer = self.signer.lock().unwrap();
            if let Some(ref mut signer) = *signer {
                signer.verify_message(message)?
            } else {
                return Self::make_err("Message signer not initialized")
            }
        };

        let decoded = self.encoder.decode(verified)?;

        self.handle_signaling_message(decoded).await
    }

    async fn handle_signaling_message(&self, msg: SignalingMessage) -> Result<()> {
        let state = *self.state.lock().unwrap();

        match state {
            State::Setup => {
                if msg.is_setup_response() {
                    let ice_servers = msg.ice_servers();
                    self.emit_setup_done(ice_servers).await;
                    Ok(())
                } else {
                    Err(Error::Signaling(
                        format!("Expected SETUP_RESPONSE but got {:?}", msg)
                    ))
                }
            }

            State::Signaling => {
                if msg.is_webrtc_signaling() {
                    if let Some(msg) = msg.as_webrtc_signaling() {
                        self.emit_webrtc_signaling_message(msg).await;
                    }
                } else {
                    // @TODO
                }
                Ok(())
            }

            State::WaitFirstMessage => {
                // We should never be in this state
                Self::make_err("ClientMessageTransport was in WaitFirstMessage state, this is a bug in the code.")
            }
        }
    }

    async fn send_signaling_message(&self, message: &SignalingMessage) -> Result<()> {
        let encoded = self.encoder.encode(message)?;

        let signed = {
            let mut signer = self.signer.lock().unwrap();
            if let Some(ref mut signer) = *signer {
                signer.sign_message(encoded)?
            } else {
                return Self::make_err("Message signer is not initialized");
            }
        };

        let signed_json = serde_json::to_value(&signed)
            .map_err(|e| Error::Signaling(
                format!("Failed to serializie signed message: {}", e)
            ))?;

        self.handle.send_message(signed_json).await?;

        Ok(())
    }

    async fn emit_webrtc_signaling_message(&self, msg: WebrtcSignalingMessage) {
        println!("webrtc msg received");
    }

    async fn emit_setup_done(&self, ice_servers: Option<Vec<IceServer>>) {
        println!("SETUP done: {:?}", ice_servers);
    }

    fn make_err(str: &str) -> Result<()> {
        return Err(Error::Signaling(str.to_string()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::ChannelRequest;

    #[tokio::test]
    async fn test_transport_mode() {
        let (tx, _rx) = mpsc::channel::<ChannelRequest>(32);
        let handle = ChannelHandle::new("test-channel".to_string(), tx);
        let (_msg_tx, msg_rx) = mpsc::channel::<JsonValue>(32);

        let options = DeviceMessageTransportOptions {
            security_mode: SecurityMode::None,
            ice_server_provider: None,
        };
        let transport = DeviceMessageTransport::new(handle, msg_rx, options);

        assert_eq!(transport.mode(), MessageTransportMode::Device);
    }
}
