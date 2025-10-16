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
use nabto_webrtc_sdk::device::{DeviceTokenGenerator, SignalingDevice, SignalingDeviceOptions};
use std::fs;
use std::future::Future;
use std::pin::Pin;
use std::process;
use tokio::net::UdpSocket;
use webrtc::api::interceptor_registry::register_default_interceptors;
use webrtc::api::media_engine::MediaEngine;
use webrtc::api::APIBuilder;
use webrtc::interceptor::registry::Registry;

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

    let _api = APIBuilder::new()
        .with_media_engine(media_engine)
        .with_interceptor_registry(registry)
        .build();

    println!("WebRTC API initialized");
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

    // Create signaling device options
    let options = SignalingDeviceOptions {
        endpoint_url: args.endpoint,
        product_id,
        device_id,
        token_generator,
    };

    // Create the signaling device
    let (mut device, mut event_rx) = SignalingDevice::new(options);

    println!("SignalingDevice created");
    println!("Connection state: {:?}", device.connection_state());
    println!();

    // Spawn a task to handle device events
    let shared_secret_for_task = shared_secret.clone();
    tokio::spawn(async move {
        while let Some(event) = event_rx.recv().await {
            match event {
                nabto_webrtc_sdk::device::DeviceEvent::NewChannel {
                    channel,
                    authorized,
                } => {
                    println!("New signaling channel received!");
                    println!("  Channel ID: {}", channel.channel_id());
                    println!("  Authorized: {}", authorized);

                    // If shared secret is configured, check authorization
                    if shared_secret_for_task.is_some() && !authorized {
                        println!("  Rejecting unauthorized connection (shared secret required)");
                        println!();
                        continue;
                    }

                    println!();
                    println!("TODO: Handle WebRTC negotiation for this channel");
                    // TODO: Create peer connection and handle SDP exchange
                }
                nabto_webrtc_sdk::device::DeviceEvent::StateChanged {
                    old_state,
                    new_state,
                } => {
                    println!("Connection state changed: {:?} -> {:?}", old_state, new_state);
                }
            }
        }
    });

    // Spawn UDP listener for RTP packets
    println!("Starting UDP listener on port {}...", rtp_port);
    let udp_socket = UdpSocket::bind(format!("0.0.0.0:{}", rtp_port)).await?;
    println!("UDP listener ready on 0.0.0.0:{}", rtp_port);
    println!();

    tokio::spawn(async move {
        let mut buf = vec![0u8; 1500];
        loop {
            match udp_socket.recv_from(&mut buf).await {
                Ok((n, _addr)) => {
                    // TODO: Forward RTP packet to WebRTC track
                    println!("Received RTP packet: {} bytes", n);
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
    println!("Waiting for client connections...");
    println!();
    println!("You can now:");
    println!("  1. Connect a WebRTC client using Nabto signaling");
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
