//! Device Message Transport implementation
//!
//! This module provides the DeviceMessageTransport, which sits on top of a
//! SignalingChannel and handles:
//! - Message encoding/decoding for WebRTC signaling
//! - Message signing/verification (JWT or None)
//! - Channel setup (SETUP_REQUEST/RESPONSE exchange)
//! - Event emission for WebRTC messages, errors, and setup completion

use super::message_encoder::{IceServer, MessageEncoder, SignalingMessage, WebrtcSignalingMessage};
use super::signing::{JwtMessageSigner, MessageSigner, NoneMessageSigner};
use crate::device::routing::ErrorInfo;
use crate::device::{SignalingChannel, SignalingService};
use crate::{Error, Result};
use serde_json::Value as JsonValue;
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

/// Device message transport options
#[derive(Clone)]
pub struct DeviceMessageTransportOptions {
    pub security_mode: SecurityMode,
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
    channel: Arc<Mutex<SignalingChannel>>,
    encoder: MessageEncoder,
    signer: Arc<Mutex<Option<Box<dyn MessageSigner>>>>,
    state: Arc<Mutex<State>>,
    options: DeviceMessageTransportOptions,
    event_tx: mpsc::UnboundedSender<DeviceTransportEvent>,
    event_rx: Arc<Mutex<Option<mpsc::UnboundedReceiver<DeviceTransportEvent>>>>,
}

impl DeviceMessageTransport {
    /// Create a new DeviceMessageTransport
    pub fn new(channel: SignalingChannel, options: DeviceMessageTransportOptions) -> Self {
        let (event_tx, event_rx) = mpsc::unbounded_channel();

        Self {
            channel: Arc::new(Mutex::new(channel)),
            encoder: MessageEncoder::new(),
            signer: Arc::new(Mutex::new(None)),
            state: Arc::new(Mutex::new(State::WaitFirstMessage)),
            options,
            event_tx,
            event_rx: Arc::new(Mutex::new(Some(event_rx))),
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
    async fn handle_device_setup_request<S: SignalingService>(&self, service: &S) -> Result<()> {
        // Request ICE servers from the device
        // For now, we'll return None - the device implementation should provide this
        let ice_servers: Option<Vec<IceServer>> = None;

        // Send SETUP_RESPONSE
        let response = SignalingMessage::SetupResponse {
            ice_servers: ice_servers.clone(),
        };
        self.send_signaling_message(&response, service).await?;

        // Emit setup done event
        self.emit_setup_done(ice_servers).await;

        Ok(())
    }

    /// Handle a signaling message
    async fn handle_signaling_message<S: SignalingService>(
        &self,
        message: SignalingMessage,
        service: &S,
    ) -> Result<()> {
        let state = *self.state.lock().unwrap();

        match state {
            State::Setup => {
                if message.is_setup_request() {
                    // Handle setup request
                    self.handle_device_setup_request(service).await?;
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

    /// Handle a message from the signaling channel
    pub async fn handle_channel_message<S: SignalingService>(
        &self,
        message: JsonValue,
        service: &S,
    ) -> Result<()> {
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
        self.handle_signaling_message(decoded, service).await
    }

    /// Send a WebRTC signaling message
    pub async fn send_webrtc_signaling_message<S: SignalingService>(
        &self,
        message: &WebrtcSignalingMessage,
        service: &S,
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

        self.send_signaling_message(&signaling_msg, service).await
    }

    /// Send a signaling message (internal)
    async fn send_signaling_message<S: SignalingService>(
        &self,
        message: &SignalingMessage,
        service: &S,
    ) -> Result<()> {
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

        // Send through channel
        let mut channel = self.channel.lock().unwrap();
        channel.send_message(signed_json, service)?;

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
    pub async fn emit_error<S: SignalingService>(&self, error: Error, service: &S) {
        let channel = self.channel.lock().unwrap();

        // Send error to client
        let error_info = ErrorInfo {
            code: "INTERNAL_ERROR".to_string(),
            message: Some(error.to_string()),
        };
        service.send_error(channel.channel_id(), error_info);

        // Emit error event
        let _ = self
            .event_tx
            .send(DeviceTransportEvent::Error(error.to_string()));
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::device::SignalingChannel;

    #[allow(dead_code)] // Used in tests
    struct MockService;

    impl SignalingService for MockService {
        fn send_routing_message(&self, _channel_id: &str, _message: JsonValue) {}
        fn send_error(&self, _channel_id: &str, _error: ErrorInfo) {}
        fn close_channel(&mut self, _channel_id: &str) {}
    }

    #[tokio::test]
    async fn test_transport_mode() {
        let channel = SignalingChannel::new("test-channel".to_string());
        let options = DeviceMessageTransportOptions {
            security_mode: SecurityMode::None,
        };
        let transport = DeviceMessageTransport::new(channel, options);

        assert_eq!(transport.mode(), MessageTransportMode::Device);
    }
}
