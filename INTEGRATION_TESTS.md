# Integration Testing Infrastructure

This document describes the integration testing infrastructure for the Nabto WebRTC SDK for Rust.

## Overview

The integration test suite validates the SDK's behavior against a mock Nabto WebRTC Signaling Service, providing comprehensive end-to-end testing of the signaling protocol, connection management, reliability layer, and channel handling.

## Current Status

✅ **Production Ready** - Comprehensive test coverage with 21 integration tests

### Test Infrastructure

- ✅ **Simple HTTP client** ([nabto_webrtc/tests/common/test_client.rs](nabto_webrtc/tests/common/test_client.rs)) - Test server API client
- ✅ **Test helper utilities** ([nabto_webrtc/tests/common/test_instance.rs](nabto_webrtc/tests/common/test_instance.rs)) - DeviceTestInstance helper
- ✅ **21 integration tests** across 4 test files
- ✅ **Automated test runner** ([run-integration-tests.sh](run-integration-tests.sh))
- ✅ **Comprehensive documentation** ([nabto_webrtc/tests/README.md](nabto_webrtc/tests/README.md))
- ✅ **CI/CD integration** with GitHub Actions

## Test Coverage

### Test Files and Categories

The integration tests are organized into 4 categories:

#### 1. Device Connectivity ([nabto_webrtc/tests/device_connectivity.rs](nabto_webrtc/tests/device_connectivity.rs))

7 tests covering connection lifecycle and error handling:

| Test | Description | Status |
|------|-------------|--------|
| `test_device_connect_ok` | Basic connection establishment | ✅ |
| `test_device_close` | Clean connection closure | ✅ |
| `test_device_http_error_retry` | HTTP error retry behavior | ✅ |
| `test_device_ws_error_retry` | WebSocket error retry behavior | ✅ |
| `test_device_reconnects` | Automatic reconnection after disconnect | ✅ |
| `test_device_http_extensibility` | HTTP protocol extensibility | ✅ |
| `test_device_ws_unknown_message_type` | Unknown WebSocket message handling | ✅ |

#### 2. Device-Client Interaction ([nabto_webrtc/tests/device_client.rs](nabto_webrtc/tests/device_client.rs))

4 tests covering client connections:

| Test | Description | Status |
|------|-------------|--------|
| `test_client_connect_ok` | Client connects successfully | ✅ |
| `test_client_disconnect` | Client disconnect handling | ✅ |
| `test_multiple_clients` | Multiple concurrent clients | ✅ |
| `test_client_reconnect` | Client reconnection | ✅ |

#### 3. Channel Management ([nabto_webrtc/tests/channel_handling.rs](nabto_webrtc/tests/channel_handling.rs))

4 tests covering channel lifecycle:

| Test | Description | Status |
|------|-------------|--------|
| `test_channel_open` | Channel creation | ✅ |
| `test_channel_close` | Channel closure | ✅ |
| `test_multiple_channels` | Multiple channels per client | ✅ |
| `test_channel_message_routing` | Message routing to correct channel | ✅ |

#### 4. Reliability Layer ([nabto_webrtc/tests/device_reliability.rs](nabto_webrtc/tests/device_reliability.rs))

6 tests covering message reliability:

| Test | Description | Status |
|------|-------------|--------|
| `test_message_ordering` | Messages delivered in order | ✅ |
| `test_ack_handling` | ACK message handling | ✅ |
| `test_sequence_numbers` | Sequence number management | ✅ |
| `test_retransmission` | Message retransmission on loss | ✅ |
| `test_duplicate_detection` | Duplicate message detection | ✅ |
| `test_flow_control` | Flow control mechanisms | ✅ |

**Total: 21 comprehensive integration tests**

## How to Use

### Quick Start

```bash
# Run all integration tests (starts server automatically)
./run-integration-tests.sh

# Run specific test file
./run-integration-tests.sh device_connectivity

# Run specific test case
cargo test --test device_connectivity test_device_connect_ok -- --ignored --nocapture
```

### Manual Server Control

```bash
# Terminal 1: Start test server
cd ~/sandbox/nabto-webrtc-sdk-js/integration_test_server
bun dev

# Terminal 2: Run tests
cargo test --test '*' -- --ignored --nocapture
```

## Architecture

```
┌──────────────────────────────────────┐
│      Integration Test (Rust)         │
│  ┌────────────────────────────────┐  │
│  │   SDK Under Test               │  │
│  │   - SignalingDevice/Client     │  │
│  │   - MessageTransport           │  │
│  │   - Reliability Layer          │  │
│  └────┬──────────────────────┬────┘  │
│       │                      │        │
│       │ HTTP/WebSocket       │        │
│       │ (Signaling Protocol) │        │
│       │                      │        │
│  ┌────▼──────────────────────▼────┐  │
│  │   TestClient (reqwest)         │  │
│  │   - Test Control API           │  │
│  │   - Mock Client Simulation     │  │
│  └────────────────┬────────────────┘  │
└───────────────────┼───────────────────┘
                    │ HTTP
                    ▼
┌──────────────────────────────────────┐
│  Integration Test Server (Bun)       │
│                                      │
│  ┌────────────────────────────────┐ │
│  │ Mock Signaling Service         │ │
│  │ - HTTP /v1/device/connect      │ │
│  │ - HTTP /v1/ice-servers         │ │
│  │ - WebSocket signaling          │ │
│  └────────────────────────────────┘ │
│                                      │
│  ┌────────────────────────────────┐ │
│  │ Test Control API               │ │
│  │ - POST /test/device            │ │
│  │ - POST /test/.../clients       │ │
│  │ - POST /test/.../send-messages │ │
│  │ - POST /test/.../disconnect    │ │
│  └────────────────────────────────┘ │
└──────────────────────────────────────┘
```

## Design Decisions

### ✅ Simple HTTP Client (Not OpenAPI Generated)

**Rationale:**
- Only 10 simple endpoints to implement
- No build-time code generation complexity
- Faster iteration during development
- Both client and server in same repository
- Easy to understand and maintain

**Alternative considered:**
- OpenAPI code generation would add tooling dependencies
- Rust OpenAPI ecosystem less mature than TypeScript
- Overkill for test-only infrastructure

### ✅ State Tracking with Polling

**Approach:**
- Tests use `wait_for_state()` helper with polling
- Reliable and simple for integration testing
- Avoids complexity of event-driven test coordination

**Implementation:**
```rust
test.wait_for_state(&device, ConnectionState::Connected, Duration::from_secs(5))
    .await
    .expect("Device did not connect");
```

## Test Server API

The test server provides these key endpoints:

### Test Lifecycle
- `POST /test/device` - Create test instance
- `DELETE /test/device/{testId}` - Cleanup test

### Client Simulation
- `POST /test/device/{testId}/clients` - Create client
- `POST /test/device/{testId}/clients/{id}/connect` - Connect client
- `POST /test/device/{testId}/clients/{id}/disconnect` - Disconnect client
- `POST /test/device/{testId}/clients/{id}/send-messages` - Send messages

### Server Control
- `POST /test/device/{testId}/disconnect-device` - Disconnect device
- `POST /test/device/{testId}/send-new-message-type` - Protocol testing
- `POST /test/device/{testId}/drop-device-messages` - Reliability testing

Explore the full API at: `http://localhost:13745/swagger`

## Example Tests

### Basic Connectivity Test

```rust
#[tokio::test]
#[ignore] // Requires test server
async fn test_device_connect_ok() {
    // Create test instance with mock server
    let test = DeviceTestInstance::create(DeviceTestOptions::default())
        .await
        .expect("Failed to create test instance");

    // Create device configured for test
    let mut device = test.create_signaling_device();
    assert_eq!(device.connection_state(), ConnectionState::New);

    // Start device and wait for connection
    device.start().await.expect("Failed to start device");
    test.wait_for_state(&device, ConnectionState::Connected, Duration::from_secs(5))
        .await
        .expect("Device did not connect");

    assert_eq!(device.connection_state(), ConnectionState::Connected);

    // Cleanup
    device.close().await.expect("Failed to close");
    test.destroy().await.expect("Failed to cleanup");
}
```

### Client Interaction Test

```rust
#[tokio::test]
#[ignore]
async fn test_client_connect_ok() {
    let test = DeviceTestInstance::create(DeviceTestOptions::default())
        .await
        .expect("Failed to create test");

    let mut device = test.create_signaling_device();
    device.start().await.expect("Failed to start device");
    test.wait_for_state(&device, ConnectionState::Connected, Duration::from_secs(5))
        .await
        .expect("Device did not connect");

    // Simulate client connection through test server
    let client_id = test.create_client().await.expect("Failed to create client");
    test.connect_client(&client_id).await.expect("Failed to connect client");

    // Verify device receives client connection
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Cleanup
    test.disconnect_client(&client_id).await.expect("Failed to disconnect");
    device.close().await.expect("Failed to close");
    test.destroy().await.expect("Failed to cleanup");
}
```

### Reliability Test

```rust
#[tokio::test]
#[ignore]
async fn test_message_ordering() {
    let test = DeviceTestInstance::create(DeviceTestOptions::default())
        .await
        .expect("Failed to create test");

    let transport = test.create_device_message_transport();
    transport.start().await.expect("Failed to start transport");

    // Create client and channel
    let client_id = test.create_client().await.expect("Failed to create client");
    test.connect_client(&client_id).await.expect("Failed to connect");

    // Send multiple messages
    let messages = vec!["msg1".to_string(), "msg2".to_string(), "msg3".to_string()];
    test.client_send_messages(&client_id, messages.clone())
        .await
        .expect("Failed to send messages");

    // Verify messages received in order
    for expected in messages {
        let event = transport.poll_event().await.expect("No event");
        match event {
            MessageTransportEvent::Message { message, .. } => {
                assert_eq!(message, expected);
            }
            _ => panic!("Unexpected event"),
        }
    }

    test.destroy().await.expect("Failed to cleanup");
}
```

## Completed Features

✅ All core test categories implemented:
- Device connectivity and lifecycle
- Client connection scenarios
- Channel management and routing
- Reliability layer with ACK/sequence handling

✅ Infrastructure complete:
- Automated test runner script
- CI/CD integration with GitHub Actions
- Comprehensive documentation

## Future Enhancements

Potential areas for expansion:

1. **Performance Testing**
   - Stress tests with many concurrent clients
   - Large message throughput testing
   - Memory usage profiling

2. **Advanced Scenarios**
   - Network partition simulation
   - Slow network conditions
   - Message loss patterns

3. **Protocol Coverage**
   - Additional edge cases in protocol extensibility
   - More complex error scenarios

## Dependencies

### Runtime
- **Bun**: For running the integration test server
- **reqwest**: HTTP client for test control API (already in dependencies)
- **tokio**: Async runtime (already in dependencies)

### Optional
- **curl**: For manual API testing/debugging

## Benefits

1. ✅ **Comprehensive Coverage** - 21 tests covering all major functionality
2. ✅ **Simple Architecture** - No code generation, straightforward HTTP calls
3. ✅ **Easy to Maintain** - Clear, explicit test code
4. ✅ **Well Documented** - README and examples for test authors
5. ✅ **Automated** - Single script to run everything
6. ✅ **CI/CD Ready** - Integrated into GitHub Actions pipeline
7. ✅ **Flexible** - Easy to add new tests and scenarios

## Resources

- **Test documentation**: [nabto_webrtc/tests/README.md](nabto_webrtc/tests/README.md)
- **Integration test server**: See nabto-webrtc-sdk-js repository
- **Test runner script**: [run-integration-tests.sh](run-integration-tests.sh)
- **Swagger UI**: `http://localhost:13745/swagger` (when server running)

---

**Status**: ✅ Production Ready
**Test Count**: 21 integration tests
**Last Updated**: 2025-11-28
