use nabto_webrtc_sdk::client::{SignalingClient, SignalingClientOptions, SignalingClientEvent};
use nabto_webrtc_sdk::common::SignalingConnectionState;
use nabto_webrtc_sdk::util::{ClientMessageTransport, ClientSecurityMode, MessageTransportEvent};
use nabto_webrtc_sdk::util::{IceCandidate, SessionDescription, WebrtcSignalingMessage};
use nabto_webrtc_sdk::util::{MessageTransportMode};
use webrtc::api::APIBuilder;
use webrtc::api::interceptor_registry::{configure_rtcp_reports, configure_twcc_receiver_only};
use webrtc::api::media_engine::MediaEngine;
use webrtc::ice_transport::ice_server::RTCIceServer;
use webrtc::interceptor::registry::Registry;
use webrtc::peer_connection::configuration::RTCConfiguration;
use std::sync::Arc;
use tokio::sync::Mutex;
use webrtc::ice_transport::ice_candidate::{RTCIceCandidate, RTCIceCandidateInit};
use webrtc::ice_transport::ice_connection_state::RTCIceConnectionState;
use webrtc::peer_connection::sdp::sdp_type::RTCSdpType;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;
use webrtc::peer_connection::signaling_state::RTCSignalingState;
use webrtc::peer_connection::RTCPeerConnection;
use anyhow::Result;


pub struct PerfectNegotiation {
    making_offer: Arc<Mutex<bool>>,
    ignore_offer: Arc<Mutex<bool>>,
    is_setting_remote_answer_pending: Arc<Mutex<bool>>,
    polite: bool,
    pc: Arc<RTCPeerConnection>,
    message_transport: Arc<ClientMessageTransport>,
}

#[allow(dead_code)]
impl PerfectNegotiation {
    pub fn new(
        pc: Arc<RTCPeerConnection>,
        message_transport: Arc<ClientMessageTransport>,
    ) -> Arc<Self> {
        let polite = message_transport.mode() == MessageTransportMode::Device;

        let negotiation = Arc::new(Self {
            making_offer: Arc::new(Mutex::new(false)),
            ignore_offer: Arc::new(Mutex::new(false)),
            is_setting_remote_answer_pending: Arc::new(Mutex::new(false)),
            polite,
            pc,
            message_transport,
        });

        negotiation.setup_handlers();
        negotiation
    }

    fn setup_handlers(self: &Arc<Self>) {
        // Handle negotiationneeded
        let negotiation_clone = Arc::clone(self);
        let pc_clone = Arc::clone(&self.pc);
        pc_clone.on_negotiation_needed(Box::new(move || {
            let negotiation = Arc::clone(&negotiation_clone);
            Box::pin(async move {
                negotiation.on_negotiation_needed().await;
            })
        }));

        // Handle icecandidate
        let negotiation_clone = Arc::clone(self);
        let pc_clone = Arc::clone(&self.pc);
        pc_clone.on_ice_candidate(Box::new(move |candidate| {
            let negotiation = Arc::clone(&negotiation_clone);
            Box::pin(async move {
                negotiation.on_ice_candidate(candidate).await;
            })
        }));

        // Handle iceconnectionstatechange
        let negotiation_clone = Arc::clone(self);
        let pc_clone = Arc::clone(&self.pc);
        pc_clone.on_ice_connection_state_change(Box::new(move |state| {
            let negotiation = Arc::clone(&negotiation_clone);
            Box::pin(async move {
                negotiation.on_ice_connection_state_change(state).await;
            })
        }));
    }

    pub async fn handle_signaling_message(&self, msg: WebrtcSignalingMessage) {
        match msg {
            WebrtcSignalingMessage::Candidate { candidate } => {
                self.handle_candidate(candidate).await;
            }
            WebrtcSignalingMessage::Description { description } => {
                self.handle_description(description).await;
            }
        }
    }

    async fn on_negotiation_needed(&self) {
        let result = async {
            *self.making_offer.lock().await = true;

            let offer = self.pc.create_offer(None).await?;
            self.pc.set_local_description(offer).await?;

            if let Some(description) = self.pc.local_description().await {
                self.send_description(description).await?;
            }

            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        }
        .await;

        if let Err(err) = result {
            eprintln!("Error in on_negotiation_needed: {:?}", err);
        }

        *self.making_offer.lock().await = false;
    }

    async fn on_ice_candidate(&self, candidate: Option<RTCIceCandidate>) {
        if let Some(candidate) = candidate {
            let _ = self.send_candidate(candidate).await;
        }
    }

    async fn on_ice_connection_state_change(&self, state: RTCIceConnectionState) {
        if state == RTCIceConnectionState::Failed {
            let _ = self.pc.restart_ice().await;
        }
    }

    async fn send_description(
        &self,
        description: RTCSessionDescription,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let session_desc = SessionDescription {
            desc_type: format!("{:?}", description.sdp_type).to_lowercase(),
            sdp: description.sdp,
        };

        let message = WebrtcSignalingMessage::Description {
            description: session_desc,
        };

        self.message_transport.send_webrtc_signaling_message(&message).await?;

        Ok(())
    }

    async fn send_candidate(
        &self,
        candidate: RTCIceCandidate,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let candidate_init = candidate.to_json()?;
        let ice_candidate = IceCandidate {
            candidate: candidate_init.candidate,
            sdp_mid: candidate_init.sdp_mid,
            sdp_m_line_index: candidate_init.sdp_mline_index.map(|v| v as u32),
            username_fragment: candidate_init.username_fragment,
        };

        let message = WebrtcSignalingMessage::Candidate {
            candidate: ice_candidate,
        };

        self.message_transport
            .send_webrtc_signaling_message(&message)
            .await?;
        Ok(())
    }

    async fn handle_description(&self, description: SessionDescription) {
        let result = async {
            let making_offer = *self.making_offer.lock().await;
            let is_setting_remote_answer_pending = *self.is_setting_remote_answer_pending.lock().await;
            let signaling_state = self.pc.signaling_state();

            let sdp_type = match description.desc_type.as_str() {
                "offer" => RTCSdpType::Offer,
                "answer" => RTCSdpType::Answer,
                "pranswer" => RTCSdpType::Pranswer,
                "rollback" => RTCSdpType::Rollback,
                _ => {
                    return Err(format!("Unknown SDP type: {}", description.desc_type).into());
                }
            };

            let ready_for_offer = 
                !making_offer &&
                (signaling_state == RTCSignalingState::Stable || is_setting_remote_answer_pending);
            let offer_collision =
                sdp_type == RTCSdpType::Offer && !ready_for_offer;

            let ignore_offer = !self.polite && offer_collision;
            *self.ignore_offer.lock().await = ignore_offer;


            if ignore_offer {
                return Ok(());
            }

            *self.is_setting_remote_answer_pending.lock().await = sdp_type == RTCSdpType::Answer;

            let rtc_description = match sdp_type {
                RTCSdpType::Offer => RTCSessionDescription::offer(description.sdp)?,
                RTCSdpType::Answer => RTCSessionDescription::answer(description.sdp)?,
                RTCSdpType::Pranswer => RTCSessionDescription::pranswer(description.sdp)?,
                RTCSdpType::Rollback => {
                    return Err("Rollback not supported".into());
                }
                RTCSdpType::Unspecified => {
                    return Err("Unspecified SDP type".into());
                }
            };

            self.pc.set_remote_description(rtc_description).await?;

            *self.is_setting_remote_answer_pending.lock().await = false;

            if sdp_type == RTCSdpType::Offer {
                let answer = self.pc.create_answer(None).await?;
                self.pc.set_local_description(answer).await?;

                if let Some(local_description) = self.pc.local_description().await {
                    self.send_description(local_description).await?;
                }
            }

            Ok::<(), Box<dyn std::error::Error + Send + Sync>>(())
        }
        .await;

        if let Err(err) = result {
            eprintln!("Error in handle_description: {:?}", err);
        }
    }

    async fn handle_candidate(&self, candidate: IceCandidate) {
        let ignore_offer = *self.ignore_offer.lock().await;

        let rtc_candidate = RTCIceCandidateInit {
            candidate: candidate.candidate,
            sdp_mid: candidate.sdp_mid,
            sdp_mline_index: candidate.sdp_m_line_index.map(|v| v as u16),
            username_fragment: candidate.username_fragment,
        };

        let result = self.pc.add_ice_candidate(rtc_candidate).await;

        if let Err(err) = result {
            if !ignore_offer {
                eprintln!("Error adding ICE candidate: {:?}", err);
            }
        }
    }
}


#[tokio::main]
async fn main() -> Result<()> {

    // WebRTC setup
    let mut pc: Option<Arc<RTCPeerConnection>> = None;
    let mut perfect_negotiation: Option<Arc<PerfectNegotiation>> = None;

    let mut media_engine = MediaEngine::default();
    media_engine.register_default_codecs()?;

    let mut registry = Registry::new();
    registry = configure_rtcp_reports(registry);
    registry = configure_twcc_receiver_only(registry, &mut media_engine)?;

    let api = Arc::new(
        APIBuilder::new()
            .with_media_engine(media_engine)
            .with_interceptor_registry(registry)
            .build(),
    );

    //

    let options = SignalingClientOptions {
        product_id: "wp-ooraxfzr".to_string(),
        device_id: "wd-qpjx37pf9utuzwbq".to_string(),
        access_token: None,
        endpoint_url: None,
        require_online: None
    };

    let (mut client, mut event_rx) = SignalingClient::new(options).await.unwrap();

    let security_mode = ClientSecurityMode::SharedSecret {
        shared_secret: "bar".to_string(),
        key_id: None
    };

    let (transport, mut transport_rx) = ClientMessageTransport::new(&mut client, security_mode);

    let client_task = tokio::spawn(async move {
        if let Err(e) = client.run().await {
            eprintln!("Error: {:?}", e);
        }
    });

    let transport_clone = Arc::clone(&transport);
    tokio::spawn(async move {
        let pc = &mut pc;
        let perfect_negotiation = &mut perfect_negotiation;
        loop {
            let event = transport_rx.recv().await.unwrap();
            match event {
                MessageTransportEvent::SetupDone(ice_servers) => {
                    println!("Ice servers: {:?}", ice_servers);
                    let mut rtc_ice_servers = vec![];
                    if let Some(servers) = ice_servers {
                        for server in servers {
                            let rtc_server = RTCIceServer {
                                urls: server.urls.to_owned(),
                                username: server.username.clone().unwrap_or("".to_owned()),
                                credential: server.credential.clone().unwrap_or("".to_owned())
                            };
                            rtc_ice_servers.push(rtc_server);
                        }
                    }
                    
                    let config = RTCConfiguration {
                        ice_servers: rtc_ice_servers.to_owned(),
                        ..Default::default()
                    };

                    *pc = match api.new_peer_connection(config).await {
                        Ok(peer_connection) => {
                            let arc_pc = Arc::new(peer_connection);
                            *perfect_negotiation = Some(PerfectNegotiation::new(
                                Arc::clone(&arc_pc),
                                Arc::clone(&transport_clone)
                            ));

                            arc_pc.on_track(Box::new(move |track, rtp_receiver, rtp_transceiver| {
                                Box::pin(async move {
                                    println!("*** RECEIVED NEW TRACK: {} {}", track.kind(), track.id())
                                })
                            }));

                            Some(arc_pc)
                        }

                        Err(e) => {
                            None
                        }
                    }
                }

                MessageTransportEvent::WebrtcSignalingMessage(msg) => {
                    if let Some(perfect_negotiation) = perfect_negotiation {
                        perfect_negotiation.handle_signaling_message(msg).await;
                    }
                }

                MessageTransportEvent::Error(err) => {

                }
            }
        }
    });

    tokio::spawn(async move {
        loop {
            let event = event_rx.recv().await.unwrap();
            match event {
                SignalingClientEvent::Message(value) => {
                    println!("SignalingClientEvent::Message");
                },

                SignalingClientEvent::ConnectionReconnect => {
                    println!("SignalingClientEvent::ConnectionReconnect");
                },

                SignalingClientEvent::ConnectionStateChange(signaling_connection_state) => {
                    println!("SignalingClientEvent::ConnectionStateChange: {:?}", signaling_connection_state);
                    if signaling_connection_state == SignalingConnectionState::Connected {
                        // @TODO: should transport.start() be in here or should we try a different pattern?
                        // @TODO: Check this error
                        let err = transport.start().await;
                    }
                },

                SignalingClientEvent::ChannelStateChange(signaling_channel_state) => {
                    println!("SignalingClientEvent::ChannelStateChange {:?}", signaling_channel_state);
                },

                SignalingClientEvent::Error => {
                    println!("SignalingClientEvent::Error");
                },
            }
        }
    });

    client_task.await?;
    Ok(())
}