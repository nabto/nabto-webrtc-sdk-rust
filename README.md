# Nabto WebRTC SDK for Rust

> **⚠️ Unstable API:** This SDK is under active development. The API is not stable and may change without notice. Use at your own risk.

A Rust SDK for Nabto WebRTC signaling, enabling peer-to-peer WebRTC connections through the Nabto platform.

## Overview

This SDK provides a Rust interface for establishing WebRTC connections using Nabto's signaling infrastructure. It handles the signaling process required to set up peer-to-peer WebRTC connections.

## Project Structure

This is a Cargo workspace with two crates:

- **`nabto_webrtc`**: Core signaling SDK for both device and client implementations
- **`nabto_webrtc_perfect_negotiation`**: Perfect negotiation pattern implementation for WebRTC

## Features

- **Device-side signaling**: Full device signaling implementation with connection management
- **Client-side signaling**: Client connection and channel handling
- **Perfect negotiation**: Helper library for WebRTC perfect negotiation pattern
- **Message transport**: Reliable message delivery with ACK/sequence handling
- **HTTP API**: Device connect and ICE server requests
- **WebSocket**: Full WebSocket connection management with automatic reconnection
- **Reliability layer**: Ordered message delivery with acknowledgments
- **Async/await**: Built on Tokio for async operations
- **Type-safe API**: Strongly typed Rust interfaces
- **Cross-platform compatibility**

## Installation

Add this to your `Cargo.toml`:

```toml
[dependencies]
nabto_webrtc = "0.1.0"
tokio = { version = "1.0", features = ["full"] }

# Optional: for perfect negotiation support
nabto_webrtc_perfect_negotiation = "0.1.0"
webrtc = "0.14"
```

## Usage

### Device-Side Example

```rust
use nabto_webrtc::device::{
    DeviceEvent, DeviceTokenGenerator, SignalingDevice, SignalingDeviceOptions, TokenGenerator,
};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let private_key = std::fs::read_to_string("device_key.pem")?;

    // Build the token generator once and reuse it: it parses the private key
    // and derives the key id up front.
    let generator = Arc::new(DeviceTokenGenerator::new(
        "wp-your-product".to_string(),
        "wd-your-device".to_string(),
        private_key,
    )?);
    println!("Device key id: {}", generator.key_id());

    let token_generator: TokenGenerator = Arc::new(move || {
        let generator = generator.clone();
        Box::pin(async move { generator.generate_token() })
    });

    let options = SignalingDeviceOptions::builder(
        "wp-your-product".to_string(),
        "wd-your-device".to_string(),
        token_generator,
    )
    .build();

    let (mut device, mut event_rx, _command_tx) = SignalingDevice::new(options);

    tokio::spawn(async move {
        while let Some(event) = event_rx.recv().await {
            match event {
                // Each client gets its own channel, with its own message
                // receiver.
                DeviceEvent::NewChannel {
                    handle,
                    mut message_rx,
                    authorized,
                } => {
                    println!("New channel {} (authorized: {})", handle.channel_id(), authorized);
                    tokio::spawn(async move {
                        while let Some(message) = message_rx.recv().await {
                            println!("Received: {}", message);
                        }
                    });
                }
                DeviceEvent::StateChanged { old_state, new_state } => {
                    println!("Connection state: {:?} -> {:?}", old_state, new_state);
                }
            }
        }
    });

    // Connects, then handles reconnection and incoming channels until stopped.
    device.run().await?;
    Ok(())
}
```

### Client-Side Example

```rust
use nabto_webrtc::client::{SignalingClient, SignalingClientEvent, SignalingClientOptions};
use nabto_webrtc::SignalingConnectionState;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let options = SignalingClientOptions::builder(
        "wp-your-product".to_string(),
        "wd-your-device".to_string(),
    )
    // Fail here with Error::DeviceOffline rather than connecting to a device
    // that cannot answer.
    .require_online(true)
    .build();

    let (mut client, mut event_rx) = SignalingClient::new(options).await?;
    let handle = client.channel_handle.clone();

    let client_task = tokio::spawn(async move { client.run().await });

    while let Some(event) = event_rx.recv().await {
        match event {
            SignalingClientEvent::ConnectionStateChange(SignalingConnectionState::Connected) => {
                handle
                    .send_message(serde_json::json!({ "type": "SETUP_REQUEST" }))
                    .await?;
            }
            SignalingClientEvent::Message(message) => {
                println!("Received: {}", message);
                break;
            }
            SignalingClientEvent::Error(err) => {
                eprintln!("Signaling error: {}", err);
                break;
            }
            _ => {}
        }
    }

    client_task.abort();
    Ok(())
}
```

For WebRTC negotiation you normally want the higher level
`ClientMessageTransport` / `DeviceMessageTransport` instead of handling raw
signaling messages; see `nabto_webrtc_perfect_negotiation/examples/client.rs`.
Note that `ClientMessageTransport` takes over message delivery, so
`SignalingClientEvent::Message` is not emitted when one is attached.

### Available Examples

The repository includes several working examples:

```bash
# Device: Request ICE servers
cargo run --example request_ice_servers -- \
  --product-id wp-your-product \
  --device-id wd-your-device \
  --private-key device_key.pem

# Device: Connect and wait for clients
cargo run --example signaling_device_connect -- \
  --product-id wp-your-product \
  --device-id wd-your-device \
  --private-key device_key.pem

# Device: Full WebRTC example with media streaming
cargo run --example rtp_to_webrtc -- \
  --product-id wp-your-product \
  --device-id wd-your-device \
  --private-key device_key.pem

# Client: Perfect negotiation example
cargo run -p nabto_webrtc_perfect_negotiation --example client -- \
  --product-id wp-your-product \
  --device-id wd-your-device
```

To see all available options for any example:
```bash
cargo run --example <example_name> -- --help
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

Integration tests require the integration test server from the JS SDK:

```bash
# Automated: Run all integration tests (starts server automatically)
./run-integration-tests.sh

# Manual: Run specific test file
./run-integration-tests.sh device_connectivity

# Manual control:
# Terminal 1: Start server
cd ../nabto-webrtc-sdk-js/integration_test_server
bun dev

# Terminal 2: Run tests
cargo test -- --ignored --test-threads=1
# Or use: cargo integration-test
```

See [INTEGRATION_TESTS.md](INTEGRATION_TESTS.md) for detailed documentation.

## Module Structure

The `nabto_webrtc` crate is organized as follows:

```
nabto_webrtc/
├── device/          # Device-side signaling implementation
│   ├── SignalingDevice
│   ├── SignalingDeviceOptions
│   └── Token generation utilities
│
├── client/          # Client-side signaling implementation
│   ├── SignalingClient
│   └── SignalingClientOptions
│
├── common/          # Shared infrastructure (internal)
│   ├── Connection management
│   ├── WebSocket handling
│   ├── HTTP client
│   ├── Message routing
│   ├── Reliability layer (ACK/sequence)
│   └── Channel management
│
└── util/            # High-level message transport APIs
    ├── DeviceMessageTransport
    ├── ClientMessageTransport
    ├── MessageEncoder
    └── SecurityMode (JWT/None)
```

The `nabto_webrtc_perfect_negotiation` crate provides:
- Perfect negotiation pattern implementation
- WebRTC peer connection helpers
- Integration with the signaling layer

## Development Status

This SDK is experimental and under active development. The API is **not stable** and may undergo breaking changes without notice. It includes:
- 21 integration tests covering connectivity, reliability, and channel handling
- CI/CD pipeline with automated testing
- Full device and client implementation
- Working examples for common use cases

## License

TBD

## Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.

Before submitting a PR, please run `./ci-check.sh` to ensure all checks pass.
