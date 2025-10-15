//! Signaling Device Connect example
//!
//! This example demonstrates how to start a SignalingDevice and connect to the
//! Nabto WebRTC Signaling Service.
//!
//! Usage:
//!   cargo run --example signaling_device_connect -- --product-id <ID> --device-id <ID> --private-key <FILE>
//!
//! Example:
//!   cargo run --example signaling_device_connect -- --product-id wp-abcdefghi --device-id wd-jklmnopqr --private-key device_key.pem

use clap::Parser;
use nabto_webrtc_sdk::{DeviceTokenGenerator, SignalingDevice, SignalingDeviceOptions};
use std::fs;
use std::future::Future;
use std::pin::Pin;
use std::process;

/// Start a signaling device and connect to the Nabto WebRTC Signaling Service
#[derive(Parser, Debug)]
#[command(name = "signaling_device_connect")]
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
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Parse command-line arguments
    let args = Args::parse();

    let product_id = args.product_id;
    let device_id = args.device_id;
    let private_key_file = args.private_key;

    // Read the private key from file
    let private_key = fs::read_to_string(&private_key_file)
        .map_err(|e| format!("Failed to read private key file '{}': {}", private_key_file, e))?;

    println!("=== Nabto WebRTC Signaling Device ===");
    println!();
    println!("Product ID: {}", product_id);
    println!("Device ID: {}", device_id);
    println!("Private key: {}", private_key_file);
    if let Some(ref endpoint) = args.endpoint {
        println!("Endpoint: {}", endpoint);
    }
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

    println!("📱 SignalingDevice created");
    println!("   Connection state: {:?}", device.connection_state());
    println!();

    // Spawn a task to handle device events
    tokio::spawn(async move {
        while let Some(event) = event_rx.recv().await {
            match event {
                nabto_webrtc_sdk::DeviceEvent::NewChannel { channel, authorized } => {
                    println!("📡 New channel received!");
                    println!("   Channel ID: {:?}", channel);
                    println!("   Authorized: {}", authorized);
                }
                nabto_webrtc_sdk::DeviceEvent::StateChanged { old_state, new_state } => {
                    println!("🔄 State changed: {:?} -> {:?}", old_state, new_state);
                }
            }
        }
    });

    // Start the device - this will:
    // 1. Make device connect HTTP request
    // 2. Get signaling URL
    // 3. Open WebSocket connection
    // 4. Handle reconnection logic
    println!("🔌 Connecting to Nabto WebRTC Signaling Service...");
    println!("   Connection state: {:?}", device.connection_state());

    match device.start().await {
        Ok(_) => {
            println!("✓ Connected successfully!");
            println!("   Connection state: {:?}", device.connection_state());
            println!();
            println!("Device is now ready to accept WebRTC signaling channels.");
            println!();

            // TODO: The device would now:
            // - Listen for new signaling channels from clients
            // - Handle WebRTC negotiation messages
            // - Manage multiple concurrent connections
            // - Automatically reconnect if connection drops

            println!("Note: WebSocket connection, channel handling, and message");
            println!("      processing will be implemented in future updates.");
        }
        Err(e) => {
            eprintln!("✗ Failed to connect: {:?}", e);
            println!("   Connection state: {:?}", device.connection_state());
            process::exit(1);
        }
    }

    Ok(())
}
