use super::message_encoder::{IceServer, MessageEncoder, SignalingMessage, WebrtcSignalingMessage};
use super::message_transport::{MessageTransportEvent, MessageTransportMode, State};
use super::signing::{JwtMessageSigner, MessageSigner, NoneMessageSigner};
use crate::client::SignalingClient;
use crate::common::channel::ChannelHandle;
use crate::{Error, Result};
use serde_json::Value as JsonValue;
use std::sync::{Arc, Mutex};
use tokio::sync::mpsc;

pub enum ClientSecurityMode {
    None,
    SharedSecret {
        shared_secret: String,
        key_id: Option<String>,
    },
}

pub struct ClientMessageTransport {
    handle: ChannelHandle,
    encoder: MessageEncoder,
    signer: Mutex<Option<Box<dyn MessageSigner>>>,

    event_tx: mpsc::UnboundedSender<MessageTransportEvent>,
    state: Mutex<State>,
}

impl ClientMessageTransport {
    pub fn new(
        client: &mut SignalingClient,
        security_mode: ClientSecurityMode,
    ) -> (Arc<Self>, mpsc::UnboundedReceiver<MessageTransportEvent>) {
        let (event_tx, event_rx) = mpsc::unbounded_channel();

        let transport = Arc::new(Self {
            handle: client.channel_handle.clone(),
            encoder: MessageEncoder::new(),
            signer: match security_mode {
                ClientSecurityMode::None => Mutex::new(Some(Box::new(NoneMessageSigner::new()))),

                ClientSecurityMode::SharedSecret {
                    shared_secret,
                    key_id,
                } => Mutex::new(Some(Box::new(JwtMessageSigner::new(shared_secret, key_id)))),
            },
            state: Mutex::new(State::Setup),
            event_tx,
        });

        let this = transport.clone();
        let mut message_rx = client.channel.with_msg_channel();
        tokio::spawn(async move {
            while let Some(message) = message_rx.recv().await {
                if let Err(e) = this.handle_channel_message_internal(message).await {
                    eprintln!("Error handling channel message: {:?}", e);
                    let _ = this
                        .event_tx
                        .send(MessageTransportEvent::Error(e.to_string()));
                }
            }
        });

        (transport, event_rx)
    }

    pub async fn start(&self) -> Result<()> {
        self.send_signaling_message(&SignalingMessage::SetupRequest)
            .await
    }

    pub fn mode(&self) -> MessageTransportMode {
        MessageTransportMode::Client
    }

    pub async fn send_webrtc_signaling_message(&self, msg: &WebrtcSignalingMessage) -> Result<()> {
        let state = *self.state.lock().unwrap();
        if state != State::Signaling {
            return Self::make_err("Cannot send signaling message before setup is complete");
        }

        let signaling_msg = match msg {
            WebrtcSignalingMessage::Description { description } => SignalingMessage::Description {
                description: description.clone(),
            },
            WebrtcSignalingMessage::Candidate { candidate } => SignalingMessage::Candidate {
                candidate: candidate.clone(),
            },
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
                return Self::make_err("Message signer not initialized");
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
                    Err(Error::Signaling(format!(
                        "Expected SETUP_RESPONSE but got {:?}",
                        msg
                    )))
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
            .map_err(|e| Error::Signaling(format!("Failed to serializie signed message: {}", e)))?;

        self.handle.send_message(signed_json).await?;

        Ok(())
    }

    async fn emit_webrtc_signaling_message(&self, msg: WebrtcSignalingMessage) {
        let _ = self
            .event_tx
            .send(MessageTransportEvent::WebrtcSignalingMessage(msg));
    }

    async fn emit_setup_done(&self, ice_servers: Option<Vec<IceServer>>) {
        *self.state.lock().unwrap() = State::Signaling;
        let _ = self
            .event_tx
            .send(MessageTransportEvent::SetupDone(ice_servers));
    }

    fn make_err(str: &str) -> Result<()> {
        Err(Error::Signaling(str.to_string()))
    }
}
