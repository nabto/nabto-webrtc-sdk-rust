//! RTP to WebRTC example using Nabto signaling
//!
//! This example demonstrates how to consume RTP stream video from UDP and forward it
//! to a WebRTC client using Nabto WebRTC signaling.
//!
//! Usage:
//!   cargo run --example rtp_to_webrtc -- --product-id <ID> --device-id <ID> --private-key <FILE>
//!
//! Example:
//!   cargo run --example rtp_to_webrtc -- --product-id wp-abcdefghi --device-id wd-jklmnopqr --private-key device_key.pem
//!
//! After starting the example, you can send RTP video to localhost:5004 using GStreamer or ffmpeg:
//!
//! GStreamer:
//!   gst-launch-1.0 videotestsrc ! video/x-raw,width=640,height=480,format=I420 ! vp8enc error-resilient=partitions keyframe-max-dist=10 auto-alt-ref=true cpu-used=5 deadline=1 ! rtpvp8pay ! udpsink host=127.0.0.1 port=5004
//!
//! ffmpeg:
//!   ffmpeg -re -f lavfi -i testsrc=size=640x480:rate=30 -vcodec libvpx -cpu-used 5 -deadline 1 -g 10 -error-resilient 1 -auto-alt-ref 1 -f rtp rtp://127.0.0.1:5004

use anyhow::Result;
use clap::Parser;
use nabto_webrtc_sdk::device::{
    ChannelHandle, DeviceTokenGenerator, SignalingDevice, SignalingDeviceOptions,
};
use nabto_webrtc_sdk::util::{
    DeviceMessageTransport, DeviceMessageTransportOptions, DeviceTransportEvent, SecurityMode,
    WebrtcSignalingMessage,
};
use serde_json::Value as JsonValue;
use std::fs;
use std::future::Future;
use std::pin::Pin;
use std::process;
use std::sync::Arc;
use tokio::net::UdpSocket;
use tokio::sync::mpsc;
use webrtc::api::interceptor_registry::register_default_interceptors;
use webrtc::api::media_engine::{MediaEngine, MIME_TYPE_VP8};
use webrtc::api::{APIBuilder, API};
use webrtc::ice_transport::ice_server::RTCIceServer;
use webrtc::interceptor::registry::Registry;
use webrtc::peer_connection::configuration::RTCConfiguration;
use webrtc::peer_connection::peer_connection_state::RTCPeerConnectionState;
use webrtc::peer_connection::sdp::session_description::RTCSessionDescription;
use webrtc::peer_connection::RTCPeerConnection;
use webrtc::rtp_transceiver::rtp_codec::RTCRtpCodecCapability;
use webrtc::track::track_local::track_local_static_rtp::TrackLocalStaticRTP;
use webrtc::track::track_local::{TrackLocal, TrackLocalWriter};

/// Handles a single WebRTC connection with signaling
struct RtcConnectionHandler {
    handle: ChannelHandle,
    transport: DeviceMessageTransport,
    api: Arc<API>,
    video_track: Arc<TrackLocalStaticRTP>,
    peer_connection: Option<Arc<RTCPeerConnection>>,
}

impl RtcConnectionHandler {
    /// Create a new RTC connection handler
    fn new(
        handle: ChannelHandle,
        message_rx: mpsc::Receiver<JsonValue>,
        options: DeviceMessageTransportOptions,
        api: Arc<API>,
        video_track: Arc<TrackLocalStaticRTP>,
    ) -> Self {
        let channel_id = handle.channel_id().to_string();
        println!("[{}] Creating RTC connection handler", channel_id);

        let transport = DeviceMessageTransport::new(handle.clone(), message_rx, options);

        Self {
            handle,
            transport,
            api,
            video_track,
            peer_connection: None,
        }
    }

    /// Create the peer connection with ICE servers
    async fn create_peer_connection(
        &mut self,
        ice_servers: Option<Vec<nabto_webrtc_sdk::util::IceServer>>,
    ) -> Result<()> {
        let channel_id = self.handle.channel_id();

        // Build ICE server configuration
        let mut rtc_ice_servers = vec![RTCIceServer {
            urls: vec!["stun:stun.l.google.com:19302".to_owned()],
            ..Default::default()
        }];

        // Add ICE servers from the signaling service if provided
        if let Some(servers) = ice_servers {
            for server in servers {
                rtc_ice_servers.push(RTCIceServer {
                    urls: server.urls,
                    username: server.username.unwrap_or_default(),
                    credential: server.credential.unwrap_or_default(),
                    ..Default::default()
                });
            }
        }

        let config = RTCConfiguration {
            ice_servers: rtc_ice_servers,
            ..Default::default()
        };

        let peer_connection = Arc::new(self.api.new_peer_connection(config).await?);
        println!("[{}] Created peer connection", channel_id);

        // Add video track to peer connection
        let _rtp_sender = peer_connection
            .add_track(Arc::clone(&self.video_track) as Arc<dyn TrackLocal + Send + Sync>)
            .await?;
        println!("[{}] Added video track to peer connection", channel_id);

        // Set up peer connection state change handler
        let channel_id_for_state = channel_id.to_string();
        peer_connection.on_peer_connection_state_change(Box::new(
            move |state: RTCPeerConnectionState| {
                println!(
                    "[{}] Peer connection state changed: {}",
                    channel_id_for_state, state
                );
                Box::pin(async {})
            },
        ));

        self.peer_connection = Some(peer_connection);
        Ok(())
    }

    /// Run the connection handler
    async fn run(mut self) -> Result<()> {
        let channel_id = self.handle.channel_id().to_string();

        let mut event_rx = self
            .transport
            .take_event_receiver()
            .ok_or_else(|| anyhow::anyhow!("Failed to get event receiver"))?;

        println!("[{}] Connection handler running", channel_id);

        // Handle transport events
        while let Some(event) = event_rx.recv().await {
            match event {
                DeviceTransportEvent::SetupDone(ice_servers) => {
                    println!(
                        "[{}] Setup completed, ICE servers: {:?}",
                        channel_id, ice_servers
                    );

                    // Create peer connection now that we have ICE servers
                    self.create_peer_connection(ice_servers).await?;
                    println!("[{}] Peer connection ready", channel_id);
                }
                DeviceTransportEvent::WebrtcSignalingMessage(msg) => {
                    println!("[{}] Received WebRTC signaling message", channel_id);
                    self.handle_webrtc_message(msg).await?;
                }
                DeviceTransportEvent::Error(err) => {
                    eprintln!("[{}] Transport error: {}", channel_id, err);
                    return Err(anyhow::anyhow!("Transport error: {}", err));
                }
            }
        }

        println!("[{}] Connection handler stopped", channel_id);
        Ok(())
    }

    /// Handle WebRTC signaling messages (SDP offer/answer, ICE candidates)
    async fn handle_webrtc_message(&mut self, msg: WebrtcSignalingMessage) -> Result<()> {
        let channel_id = self.handle.channel_id();

        // Check if peer connection is ready
        let peer_connection = self
            .peer_connection
            .as_ref()
            .ok_or_else(|| anyhow::anyhow!("Peer connection not yet created"))?;

        match msg {
            WebrtcSignalingMessage::Description { description } => {
                println!(
                    "[{}] Received SDP description: {:?}",
                    channel_id, description.desc_type
                );

                // Set remote description (client's offer)
                let remote_desc = RTCSessionDescription::offer(description.sdp)?;
                peer_connection.set_remote_description(remote_desc).await?;

                // Create answer
                let answer = peer_connection.create_answer(None).await?;

                // Set local description
                peer_connection
                    .set_local_description(answer.clone())
                    .await?;

                println!("[{}] Created and set SDP answer", channel_id);

                // TODO: Send answer back through transport
                println!("[{}] TODO: Send SDP answer to client", channel_id);
            }
            WebrtcSignalingMessage::Candidate { candidate } => {
                println!(
                    "[{}] Received ICE candidate: {:?}",
                    channel_id, candidate.candidate
                );
                // TODO: Add ICE candidate to peer connection
                println!(
                    "[{}] TODO: Add ICE candidate to peer connection",
                    channel_id
                );
            }
        }

        Ok(())
    }
}

/// RTP to WebRTC forwarding using Nabto signaling
#[derive(Parser, Debug)]
#[command(name = "rtp_to_webrtc")]
#[command(author, version, about, long_about = None)]
struct Args {
    /// Nabto product ID (e.g., wp-abcdefghi)
    #[arg(short, long, value_name = "PRODUCT_ID")]
    product_id: String,

    /// Nabto device ID (e.g., wd-jklmnopqr)
    #[arg(short, long, value_name = "DEVICE_ID")]
    device_id: String,

    /// Path to the private key file (PEM format)
    #[arg(short = 'k', long, value_name = "FILE")]
    private_key: String,

    /// Optional endpoint URL (defaults to https://<product-id>.webrtc.nabto.net)
    #[arg(short, long, value_name = "URL")]
    endpoint: Option<String>,

    /// UDP port to listen for RTP packets (default: 5004)
    #[arg(short = 'r', long, value_name = "PORT", default_value = "5004")]
    rtp_port: u16,

    /// Optional shared secret for client authentication
    #[arg(short = 's', long, value_name = "SECRET")]
    shared_secret: Option<String>,
}

#[tokio::main]
async fn main() -> Result<()> {
    // Parse command-line arguments
    let args = Args::parse();

    let product_id = args.product_id;
    let device_id = args.device_id;
    let private_key_file = args.private_key;
    let rtp_port = args.rtp_port;
    let shared_secret = args.shared_secret;

    // Read the private key from file
    let private_key = fs::read_to_string(&private_key_file).map_err(|e| {
        anyhow::anyhow!(
            "Failed to read private key file '{}': {}",
            private_key_file,
            e
        )
    })?;

    println!("=== Nabto WebRTC RTP to WebRTC Forwarder ===");
    println!();
    println!("Product ID: {}", product_id);
    println!("Device ID: {}", device_id);
    println!("Private key: {}", private_key_file);
    println!("RTP listen port: {}", rtp_port);
    if let Some(ref endpoint) = args.endpoint {
        println!("Endpoint: {}", endpoint);
    }
    if let Some(ref secret) = shared_secret {
        println!("Shared secret: {}", secret);
    }
    println!();

    // Initialize WebRTC API
    println!("Setting up WebRTC...");
    let mut media_engine = MediaEngine::default();
    media_engine.register_default_codecs()?;

    let mut registry = Registry::new();
    registry = register_default_interceptors(registry, &mut media_engine)?;

    let api = Arc::new(
        APIBuilder::new()
            .with_media_engine(media_engine)
            .with_interceptor_registry(registry)
            .build(),
    );

    println!("WebRTC API initialized");

    // Create shared video track for VP8
    let video_track = Arc::new(TrackLocalStaticRTP::new(
        RTCRtpCodecCapability {
            mime_type: MIME_TYPE_VP8.to_owned(),
            ..Default::default()
        },
        "video".to_owned(),
        "webrtc-rs".to_owned(),
    ));

    println!("Created VP8 video track");
    println!();

    // Clone values for use in closure
    let product_id_for_token = product_id.clone();
    let device_id_for_token = device_id.clone();

    // Create a token generator that uses the private key
    let token_generator = Box::new(move || {
        let generator = DeviceTokenGenerator::new(
            product_id_for_token.clone(),
            device_id_for_token.clone(),
            private_key.clone(),
        );

        Box::pin(async move {
            // Generate JWT token with the private key
            generator.generate_token()
        }) as Pin<Box<dyn Future<Output = Result<String, nabto_webrtc_sdk::Error>> + Send>>
    });

    // Clone for later use in URL printing
    let product_id_for_url = product_id.clone();
    let device_id_for_url = device_id.clone();

    // Create signaling device options
    let options = SignalingDeviceOptions {
        endpoint_url: args.endpoint,
        product_id,
        device_id,
        token_generator,
    };

    // Create the signaling device
    let (mut device, mut event_rx, _command_tx) = SignalingDevice::new(options);

    println!("SignalingDevice created");
    println!("Connection state: {:?}", device.connection_state());
    println!();

    // Spawn a task to handle device events
    let shared_secret_for_task = shared_secret.clone();
    let api_for_task = api.clone();
    let video_track_for_task = Arc::clone(&video_track);
    tokio::spawn(async move {
        while let Some(event) = event_rx.recv().await {
            match event {
                nabto_webrtc_sdk::device::DeviceEvent::NewChannel {
                    handle,
                    message_rx,
                    authorized,
                } => {
                    println!("New signaling channel received!");
                    println!("  Channel ID: {}", handle.channel_id());
                    println!("  Authorized: {}", authorized);

                    // Accept connection if:
                    // 1. Centrally authorized (authorized=true), OR
                    // 2. Shared secret is configured (will validate JWT)
                    if !authorized && shared_secret_for_task.is_none() {
                        println!(
                            "  Rejecting unauthorized connection (no shared secret configured)"
                        );
                        println!();
                        continue;
                    }

                    if shared_secret_for_task.is_some() && !authorized {
                        println!("  Using shared secret authentication (JWT)");
                    }

                    println!();

                    // Create and spawn RTC connection handler
                    let api_clone = api_for_task.clone();
                    let video_track_clone = Arc::clone(&video_track_for_task);
                    let shared_secret_clone = shared_secret_for_task.clone();

                    tokio::spawn(async move {
                        // Create message transport with security mode
                        let security_mode = if let Some(secret) = shared_secret_clone {
                            SecurityMode::SharedSecret {
                                callback: Arc::new(move |_key_id| Ok(secret.clone())),
                            }
                        } else {
                            SecurityMode::None
                        };

                        let transport_options = DeviceMessageTransportOptions { security_mode };

                        // Create RTC connection handler with the transport
                        let handler = RtcConnectionHandler::new(
                            handle,
                            message_rx,
                            transport_options,
                            api_clone,
                            video_track_clone,
                        );
                        println!("RTC connection handler created");

                        if let Err(e) = handler.run().await {
                            eprintln!("Connection handler error: {}", e);
                        }
                    });
                }
                nabto_webrtc_sdk::device::DeviceEvent::StateChanged {
                    old_state,
                    new_state,
                } => {
                    println!(
                        "Connection state changed: {:?} -> {:?}",
                        old_state, new_state
                    );
                }
            }
        }
    });

    // Spawn UDP listener for RTP packets
    println!("Starting UDP listener on port {}...", rtp_port);
    let udp_socket = UdpSocket::bind(format!("0.0.0.0:{}", rtp_port)).await?;
    println!("UDP listener ready on 0.0.0.0:{}", rtp_port);
    println!();

    let video_track_for_rtp = Arc::clone(&video_track);
    tokio::spawn(async move {
        let mut buf = vec![0u8; 1500];
        loop {
            match udp_socket.recv_from(&mut buf).await {
                Ok((n, _addr)) => {
                    // Forward RTP packet to all connected WebRTC peers
                    if let Err(e) = video_track_for_rtp.write(&buf[..n]).await {
                        eprintln!("Error writing RTP packet to track: {}", e);
                    }
                }
                Err(e) => {
                    eprintln!("Error receiving RTP packet: {}", e);
                }
            }
        }
    });

    // Start the signaling device
    println!("Starting Nabto WebRTC Signaling Device...");
    println!();

    // Spawn the device run loop
    let device_task = tokio::spawn(async move {
        if let Err(e) = device.run().await {
            eprintln!("Device error: {:?}", e);
            process::exit(1);
        }
    });

    println!("Device is running!");
    println!();

    // Show video stream link if all required parameters are available
    if let Some(ref secret) = shared_secret {
        println!("Video Stream Link:");
        println!("  https://nabto.github.io/nabto-webrtc-sdk-js?mode=client&productId={}&deviceId={}&sharedSecret={}",
                 product_id_for_url, device_id_for_url, secret);
        println!();
    } else {
        println!("Video Stream Link: (incomplete - missing sharedSecret)");
        println!();
    }

    println!("Waiting for client connections...");
    println!();
    println!("You can now:");
    println!("  1. Open the video stream link above in a web browser");
    println!("  2. Send RTP video stream to localhost:{}", rtp_port);
    println!();
    println!("Example GStreamer command:");
    println!("  gst-launch-1.0 videotestsrc ! video/x-raw,width=640,height=480,format=I420 ! \\");
    println!("    vp8enc error-resilient=partitions keyframe-max-dist=10 auto-alt-ref=true cpu-used=5 deadline=1 ! \\");
    println!("    rtpvp8pay ! udpsink host=127.0.0.1 port={}", rtp_port);
    println!();
    println!("Press Ctrl+C to stop...");
    println!();

    // Wait for the device task (or until interrupted)
    device_task.await?;

    Ok(())
}
