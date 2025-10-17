//! Request ICE servers example for the Nabto WebRTC SDK
//!
//! This example demonstrates how to request ICE servers (STUN/TURN credentials)
//! from the Nabto WebRTC Signaling Service.
//!
//! Usage:
//!   cargo run --example request_ice_servers -- --product-id <ID> --device-id <ID> --private-key <FILE>
//!
//! Example:
//!   cargo run --example request_ice_servers -- --product-id wp-abcdefghi --device-id wd-jklmnopqr --private-key device_key.pem

use clap::Parser;
use nabto_webrtc_sdk::device::{DeviceTokenGenerator, SignalingDevice, SignalingDeviceOptions};
use std::fs;
use std::future::Future;
use std::pin::Pin;
use std::process;

/// Request ICE servers from the Nabto WebRTC Signaling Service
#[derive(Parser, Debug)]
#[command(name = "request_ice_servers")]
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
    let private_key = fs::read_to_string(&private_key_file).map_err(|e| {
        format!(
            "Failed to read private key file '{}': {}",
            private_key_file, e
        )
    })?;

    println!("Product ID: {}", product_id);
    println!("Device ID: {}", device_id);
    println!("Private key loaded from: {}", private_key_file);
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
        endpoint_url: args.endpoint, // Use custom endpoint if provided
        product_id,
        device_id,
        token_generator,
    };

    // Create the signaling device
    let (device, _event_rx, _command_tx) = SignalingDevice::new(options);

    println!("SignalingDevice created successfully");
    println!("Connection state: {:?}", device.connection_state());
    println!();

    // Request ICE servers
    println!("Requesting ICE servers...");
    match device.request_ice_servers().await {
        Ok(ice_servers) => {
            println!(
                "✓ Successfully retrieved {} ICE servers:",
                ice_servers.len()
            );
            println!();
            for (i, server) in ice_servers.iter().enumerate() {
                println!("Server {}:", i + 1);
                println!("  URLs: {:?}", server.urls);
                if let Some(username) = &server.username {
                    println!("  Username: {}", username);
                }
                if let Some(credential) = &server.credential {
                    println!(
                        "  Credential: {}...",
                        &credential[..credential.len().min(20)]
                    );
                }
                println!();
            }
        }
        Err(e) => {
            eprintln!("✗ Failed to retrieve ICE servers: {:?}", e);
            process::exit(1);
        }
    }

    Ok(())
}
