//! Basic usage example for the Nabto WebRTC SDK
//!
//! This example demonstrates how to create a SignalingDevice and request ICE servers.

use nabto_webrtc_sdk::{SignalingDevice, SignalingDeviceOptions};
use std::future::Future;
use std::pin::Pin;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a token generator
    let token_generator = Box::new(|| {
        Box::pin(async {
            // In a real application, generate a JWT token here
            Ok("your-jwt-token-here".to_string())
        }) as Pin<Box<dyn Future<Output = Result<String, nabto_webrtc_sdk::Error>> + Send>>
    });

    // Create signaling device options
    let options = SignalingDeviceOptions {
        endpoint_url: None, // Will use default: https://{product_id}.webrtc.nabto.net
        product_id: "wp-example".to_string(),
        device_id: "wd-example".to_string(),
        token_generator,
    };

    // Create the signaling device
    let device = SignalingDevice::new(options);

    println!("SignalingDevice created successfully");
    println!("Connection state: {:?}", device.connection_state());

    // Example: Request ICE servers
    // Note: This will fail without a valid token, but demonstrates the API
    match device.request_ice_servers().await {
        Ok(ice_servers) => {
            println!("Retrieved {} ICE servers:", ice_servers.len());
            for server in ice_servers {
                println!("  URLs: {:?}", server.urls);
                if let Some(username) = server.username {
                    println!("  Username: {}", username);
                }
            }
        }
        Err(e) => {
            println!("Failed to retrieve ICE servers: {:?}", e);
        }
    }

    Ok(())
}
