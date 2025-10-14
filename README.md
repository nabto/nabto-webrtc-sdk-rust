# Nabto WebRTC SDK for Rust

A Rust SDK for Nabto WebRTC signaling, enabling peer-to-peer WebRTC connections through the Nabto platform.

## Overview

This SDK provides a Rust interface for establishing WebRTC connections using Nabto's signaling infrastructure. It handles the signaling process required to set up peer-to-peer WebRTC connections.

## Features

- Device-side WebRTC signaling through Nabto
- HTTP API for device connect and ICE server requests
- Async/await support with Tokio
- Type-safe API with Rust types
- WebSocket connection management (in progress)
- Reliability layer for ordered message delivery (in progress)
- Cross-platform compatibility

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
nabto-webrtc-sdk = "0.1.0"
tokio = { version = "1.0", features = ["full"] }
```

## Usage

### Basic Example

```rust
use nabto_webrtc_sdk::{SignalingDevice, SignalingDeviceOptions};
use std::future::Future;
use std::pin::Pin;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a token generator
    let token_generator = Box::new(|| {
        Box::pin(async {
            // Generate your JWT token here
            Ok("your-jwt-token".to_string())
        }) as Pin<Box<dyn Future<Output = Result<String, nabto_webrtc_sdk::Error>> + Send>>
    });

    // Create signaling device
    let options = SignalingDeviceOptions {
        endpoint_url: None, // Uses default: https://{product_id}.webrtc.nabto.net
        product_id: "wp-your-product".to_string(),
        device_id: "wd-your-device".to_string(),
        token_generator,
    };

    let device = SignalingDevice::new(options);

    // Request ICE servers
    let ice_servers = device.request_ice_servers().await?;
    println!("Retrieved {} ICE servers", ice_servers.len());

    Ok(())
}
```

### Running the Example

Request ICE servers using the provided example:

```bash
cargo run --example request_ice_servers -- \
  --product-id wp-your-product \
  --device-id wd-your-device \
  --private-key device_key.pem
```

To see all available options:
```bash
cargo run --example request_ice_servers -- --help
```

### HTTP API

The SDK implements two HTTP endpoints:

#### 1. Device Connect
```rust
// POST /v1/device/connect
// Returns signaling URL for WebSocket connection
let signaling_url = device.device_connect().await?;
```

#### 2. ICE Servers (TURN Credentials)
```rust
// POST /v1/ice-servers
// Returns STUN and TURN server configurations
let ice_servers = device.request_ice_servers().await?;
for server in ice_servers {
    println!("URLs: {:?}", server.urls);
    println!("Username: {:?}", server.username);
    println!("Credential: {:?}", server.credential);
}
```

## Development Status

This SDK is currently in early development.

## License

TBD

## Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.
