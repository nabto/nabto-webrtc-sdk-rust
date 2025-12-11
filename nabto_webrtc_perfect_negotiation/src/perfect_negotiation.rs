use log::{error, trace};
use nabto_webrtc::Result as NabtoResult;
use nabto_webrtc::util::ClientMessageTransport;
use nabto_webrtc::util::DeviceMessageTransport;
use nabto_webrtc::util::{IceCandidate, SessionDescription, WebrtcSignalingMessage};
use std::sync::Arc;
use tokio::sync::Mutex;
use webrtc::ice_transport::ice_candidate::{RTCIceCandidate, RTCIceCandidateInit};
use webrtc::ice_transport::ice_connection_state::RTCIceConnectionState;
use webrtc::peer_connection::RTCPeerConnection;
use webrtc::peer_connection::sdp::sdp_type::RTCSdpType;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;
use webrtc::peer_connection::signaling_state::RTCSignalingState;

pub enum PerfectNegotiationTransport {
    Client {
        transport: Arc<ClientMessageTransport>,
    },
    Device {
        transport: DeviceMessageTransport,
    },
}

pub struct PerfectNegotiation {
    /// Name used for logging purposes (e.g., "client" or "device")
    name: &'static str,
    making_offer: Arc<Mutex<bool>>,
    ignore_offer: Arc<Mutex<bool>>,
    is_setting_remote_answer_pending: Arc<Mutex<bool>>,
    polite: bool,
    pc: Arc<RTCPeerConnection>,
    message_transport: PerfectNegotiationTransport,
}

#[allow(dead_code)]
impl PerfectNegotiation {
    pub fn new(
        pc: Arc<RTCPeerConnection>,
        message_transport: PerfectNegotiationTransport,
    ) -> Arc<Self> {
        let (name, polite) = match message_transport {
            PerfectNegotiationTransport::Client { transport: _ } => ("client", false),
            PerfectNegotiationTransport::Device { transport: _ } => ("device", true),
        };

        let negotiation = Arc::new(Self {
            name,
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
            error!("Error in on_negotiation_needed: {:?}", err);
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

        trace!(
            "[{}] Sending description: type={}, sdp={}",
            self.name,
            session_desc.desc_type,
            session_desc.sdp
        );

        let message = WebrtcSignalingMessage::Description {
            description: session_desc,
        };

        self.send_webrtc_signaling_message(&message).await?;
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

        self.send_webrtc_signaling_message(&message).await?;
        Ok(())
    }

    async fn handle_description(&self, description: SessionDescription) {
        trace!(
            "[{}] Received description: type={}, sdp={}",
            self.name,
            description.desc_type,
            description.sdp
        );

        let result = async {
            let making_offer = *self.making_offer.lock().await;
            let is_setting_remote_answer_pending =
                *self.is_setting_remote_answer_pending.lock().await;
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

            let ready_for_offer = !making_offer
                && (signaling_state == RTCSignalingState::Stable
                    || is_setting_remote_answer_pending);
            let offer_collision = sdp_type == RTCSdpType::Offer && !ready_for_offer;

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
            error!("Error in handle_description: {:?}", err);
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
                error!("Error adding ICE candidate: {:?}", err);
            }
        }
    }

    async fn send_webrtc_signaling_message(&self, msg: &WebrtcSignalingMessage) -> NabtoResult<()> {
        match &self.message_transport {
            PerfectNegotiationTransport::Client { transport } => {
                transport.send_webrtc_signaling_message(msg).await
            }
            PerfectNegotiationTransport::Device { transport } => {
                transport.send_webrtc_signaling_message(msg).await
            }
        }
    }
}
