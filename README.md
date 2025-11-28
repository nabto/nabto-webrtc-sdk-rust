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
use nabto_webrtc::{SignalingDevice, SignalingDeviceOptions};
use std::future::Future;
use std::pin::Pin;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a token generator
    let token_generator = Box::new(|| {
        Box::pin(async {
            // Generate your JWT token here
            Ok("your-jwt-token".to_string())
        }) as Pin<Box<dyn Future<Output = Result<String, nabto_webrtc::Error>> + Send>>
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

## Development

### Running CI Checks Locally

Before pushing code, you can run the same checks that CI runs to catch issues early:

#### Shell script (recommended for pre-push):
```bash
./ci-check.sh                # Run all checks
./ci-check.sh --with-release # Include release build (slower)
```

#### Cargo aliases (recommended for quick iteration):
```bash
cargo quick-check      # Quick checks (fmt, clippy, test)
cargo fmt-check        # Check formatting
cargo clippy-ci        # Run clippy with CI settings
cargo build-all        # Build everything
cargo integration-test # Run integration tests
```

### Integration Tests

Integration tests require the integration test server:

1. Clone the JS SDK (done automatically in CI):
   ```bash
   git clone https://github.com/nabto/nabto-webrtc-sdk-js.git ../nabto-webrtc-sdk-js
   ```

2. Install and start the server:
   ```bash
   cd ../nabto-webrtc-sdk-js/integration_test_server
   bun install
   bun dev
   ```

3. Run integration tests:
   ```bash
   cargo test -- --ignored --test-threads=1
   # Or use: cargo integration-test
   ```

## Development Status

This SDK is currently in early development.

## License

TBD

## Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.

Before submitting a PR, please run `./ci-check.sh` to ensure all checks pass.
