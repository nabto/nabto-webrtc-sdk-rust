# Integration Tests

This directory contains integration tests for the Nabto WebRTC SDK for Rust.

## Overview

The integration tests verify the SDK's behavior against a mock Nabto WebRTC Signaling Service. The mock service is implemented as a Bun-based server located at `~/sandbox/nabto-webrtc-sdk-js/integration_test_server`.

## Architecture

```
┌─────────────────────┐         HTTP/WS          ┌──────────────────────┐
│  Rust SDK (Device)  │ ◄──────────────────────► │  Integration Test    │
│  Under Test         │                          │  Server (Bun/Elysia) │
└─────────────────────┘                          └──────────────────────┘
         ▲                                                  ▲
         │                                                  │
         │ Assertions                      Control API     │
         │                                                  │
    ┌────┴──────────┐                          ┌───────────┴──────────┐
    │  Integration   │  ◄───────────────────►  │  Test Client         │
    │  Test Cases    │      HTTP Requests      │  (Rust reqwest)      │
    └────────────────┘                          └──────────────────────┘
```

## Running Tests

### Quick Start

Use the provided test runner script:

```bash
./run-integration-tests.sh
```

This script will:
1. Start the integration test server automatically
2. Run all integration tests
3. Stop the server when done

### Run Specific Tests

```bash
# Run only device connectivity tests
./run-integration-tests.sh device_connectivity

# Run a specific test case
cargo test --test device_connectivity test_device_connect_ok -- --ignored --nocapture
```

### Manual Test Execution

If you prefer to manage the test server manually:

```bash
# Terminal 1: Start the test server
cd ~/sandbox/nabto-webrtc-sdk-js/integration_test_server
bun dev

# Terminal 2: Run the tests
cd ~/sandbox/nabto-webrtc-sdk-rust
cargo test --test '*' -- --ignored --nocapture
```

## Test Structure

### Directory Layout

```
tests/
├── common/
│   ├── mod.rs              # Common module exports
│   ├── test_client.rs      # HTTP client for test server API
│   └── test_instance.rs    # DeviceTestInstance helper
├── device_connectivity.rs  # Device connection tests
├── device_reliability.rs   # Reliability layer tests (future)
├── device_data_format.rs   # Protocol extensibility tests (future)
└── ice_servers.rs          # ICE servers tests (future)
```

### Test Categories

- **Device Connectivity Tests** (`device_connectivity.rs`)
  - Basic connection establishment
  - Connection closure
  - HTTP/WebSocket error handling
  - Automatic reconnection
  - Protocol extensibility

- **Reliability Tests** (planned)
  - Message ordering
  - ACK/DATA handling
  - Sequence number management

- **ICE Servers Tests** (planned)
  - TURN/STUN server retrieval
  - Token-based authentication

## Writing New Tests

### Basic Test Structure

```rust
#[tokio::test]
#[ignore] // Requires integration test server to be running
async fn test_my_feature() {
    // 1. Create test instance
    let test = DeviceTestInstance::create(DeviceTestOptions::default())
        .await
        .expect("Failed to create test instance");

    // 2. Create and configure device
    let mut device = test.create_signaling_device();

    // 3. Perform test actions
    device.start().await.expect("Failed to start device");

    // 4. Verify behavior
    test.wait_for_state(&device, ConnectionState::Connected, Duration::from_secs(5))
        .await
        .expect("Device did not reach Connected state");

    // 5. Cleanup
    device.close().await.expect("Failed to close device");
    test.destroy().await.expect("Failed to destroy test");
}
```

### Test Server Control API

The `DeviceTestInstance` provides methods to control the mock server:

```rust
// Create virtual clients
let client_id = test.create_client().await?;

// Connect/disconnect clients
test.connect_client(&client_id).await?;
test.disconnect_client(&client_id).await?;

// Send messages
test.client_send_messages(&client_id, vec!["message1".to_string()]).await?;

// Simulate server disconnection
test.disconnect_device().await?;

// Protocol extensibility testing
test.send_new_message_type().await?;
```

### DeviceTestOptions

Configure the mock server behavior:

```rust
let test = DeviceTestInstance::create(DeviceTestOptions {
    fail_http: Some(true),              // Fail HTTP requests
    fail_ws: Some(true),                // Fail WebSocket connections
    product_id_not_found: Some(true),   // Simulate unknown product
    device_id_not_found: Some(true),    // Simulate unknown device
    extra_device_connect_response_data: Some(true), // Test extensibility
    ..Default::default()
}).await?;
```

## Implementation Details

### Why `#[ignore]`?

All integration tests are marked with `#[ignore]` because they require the external test server to be running. This prevents them from running during normal `cargo test` execution.

To run ignored tests:
```bash
cargo test -- --ignored
```

### Simple HTTP Client vs OpenAPI

We use a simple manual HTTP client implementation (`test_client.rs`) instead of OpenAPI code generation for several reasons:

1. **Simplicity**: Only ~10 endpoints to implement
2. **No build complexity**: No need for OpenAPI tooling in the build pipeline
3. **Tight coupling**: We control both the client and server
4. **Development speed**: Easy to iterate without regenerating code

The test server provides a Swagger UI at `http://localhost:13745/swagger` for API exploration.

## CI/CD Integration

Example GitHub Actions workflow:

```yaml
name: Integration Tests
on: [push, pull_request]

jobs:
  integration-tests:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v3

      - name: Install Bun
        uses: oven-sh/setup-bun@v1

      - name: Run Integration Tests
        run: ./run-integration-tests.sh
```

## Troubleshooting

### Server won't start

Check if the server is already running:
```bash
curl http://localhost:13745/swagger
```

Check server logs:
```bash
tail -f /tmp/integration-test-server.log
```

### Tests timeout

Increase timeout in test code:
```rust
test.wait_for_state(&device, ConnectionState::Connected, Duration::from_secs(30))
```

### Port conflicts

The test server uses port 13745. If this conflicts, update `TEST_SERVER_PORT` in the test runner script and `BASE_URL` in `tests/common/test_client.rs`.

## Future Enhancements

- [ ] Add event emitter pattern to SDK for automatic state tracking
- [ ] Implement reliability layer tests
- [ ] Add client-device interaction tests
- [ ] Performance and stress tests
- [ ] WebRTC data channel tests (when implemented)
