use anyhow::Result;
use nabto_webrtc::client::{SignalingClient, SignalingClientEvent, SignalingClientOptions};
use nabto_webrtc::common::SignalingConnectionState;
use nabto_webrtc::util::{ClientMessageTransport, ClientSecurityMode, MessageTransportEvent};
use nabto_webrtc_perfect_negotiation::PerfectNegotiation;
use std::sync::Arc;
use webrtc::api::APIBuilder;
use webrtc::api::interceptor_registry::{configure_rtcp_reports, configure_twcc_receiver_only};
use webrtc::api::media_engine::MediaEngine;
use webrtc::ice_transport::ice_server::RTCIceServer;
use webrtc::interceptor::registry::Registry;
use webrtc::peer_connection::RTCPeerConnection;
use webrtc::peer_connection::configuration::RTCConfiguration;

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
        require_online: None,
    };

    let (mut client, mut event_rx) = SignalingClient::new(options).await.unwrap();

    let security_mode = ClientSecurityMode::SharedSecret {
        shared_secret: "bar".to_string(),
        key_id: None,
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
                                credential: server.credential.clone().unwrap_or("".to_owned()),
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
                                Arc::clone(&transport_clone),
                            ));

                            arc_pc.on_track(Box::new(
                                move |track, rtp_receiver, rtp_transceiver| {
                                    Box::pin(async move {
                                        println!(
                                            "*** RECEIVED NEW TRACK: {} {}",
                                            track.kind(),
                                            track.id()
                                        )
                                    })
                                },
                            ));

                            Some(arc_pc)
                        }

                        Err(e) => None,
                    }
                }

                MessageTransportEvent::WebrtcSignalingMessage(msg) => {
                    if let Some(perfect_negotiation) = perfect_negotiation {
                        perfect_negotiation.handle_signaling_message(msg).await;
                    }
                }

                MessageTransportEvent::Error(err) => {}
            }
        }
    });

    tokio::spawn(async move {
        loop {
            let event = event_rx.recv().await.unwrap();
            match event {
                SignalingClientEvent::Message(value) => {
                    println!("SignalingClientEvent::Message");
                }

                SignalingClientEvent::ConnectionReconnect => {
                    println!("SignalingClientEvent::ConnectionReconnect");
                }

                SignalingClientEvent::ConnectionStateChange(signaling_connection_state) => {
                    println!(
                        "SignalingClientEvent::ConnectionStateChange: {:?}",
                        signaling_connection_state
                    );
                    if signaling_connection_state == SignalingConnectionState::Connected {
                        // @TODO: should transport.start() be in here or should we try a different pattern?
                        // @TODO: Check this error
                        let err = transport.start().await;
                    }
                }

                SignalingClientEvent::ChannelStateChange(signaling_channel_state) => {
                    println!(
                        "SignalingClientEvent::ChannelStateChange {:?}",
                        signaling_channel_state
                    );
                }

                SignalingClientEvent::Error => {
                    println!("SignalingClientEvent::Error");
                }
            }
        }
    });

    client_task.await?;
    Ok(())
}
