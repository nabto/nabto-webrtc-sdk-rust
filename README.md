# Nabto WebRTC SDK for Rust

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
use nabto_webrtc::device::{SignalingDevice, SignalingDeviceOptions};
use nabto_webrtc::util::{DeviceMessageTransport, SecurityMode};
use nabto_webrtc::SignalingConnectionState;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    // Create a message transport with JWT signing
    let transport = DeviceMessageTransport::new(
        "wp-your-product".to_string(),
        "wd-your-device".to_string(),
        SecurityMode::jwt_from_file("device_key.pem")?,
        None, // Uses default endpoint
    );

    // Start the transport
    transport.start().await?;

    // Wait for connection
    while transport.connection_state() != SignalingConnectionState::Connected {
        tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    }

    println!("Device connected!");

    // Listen for incoming messages
    loop {
        if let Some(event) = transport.poll_event().await {
            match event {
                MessageTransportEvent::Message { message, channel_id } => {
                    println!("Received message on channel {}: {}", channel_id, message);
                }
                _ => {}
            }
        }
    }
}
```

### Client-Side Example

```rust
use nabto_webrtc::client::{SignalingClient, SignalingClientOptions};
use nabto_webrtc::util::{ClientMessageTransport, SecurityMode};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let transport = ClientMessageTransport::new(
        "wp-your-product".to_string(),
        "wd-your-device".to_string(),
        SecurityMode::None, // Or use JWT
        None,
    );

    transport.start().await?;

    // Connect and create a channel
    let channel_id = transport.create_channel().await?;

    // Send a message
    transport.send_message(channel_id, "Hello from client!".to_string()).await?;

    Ok(())
}
```

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

This SDK is production-ready with comprehensive test coverage:
- 21 integration tests covering connectivity, reliability, and channel handling
- CI/CD pipeline with automated testing
- Full device and client implementation
- Working examples for common use cases

## License

TBD

## Contributing

Contributions are welcome! Please feel free to submit issues or pull requests.

Before submitting a PR, please run `./ci-check.sh` to ensure all checks pass.
