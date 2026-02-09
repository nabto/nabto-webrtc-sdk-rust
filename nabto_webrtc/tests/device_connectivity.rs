//! Device Connectivity Integration Tests
//!
//! These tests verify the connection between a Nabto WebRTC Signaling Device
//! and the Nabto WebRTC Signaling Service (mocked by the integration test server).
//!
//! To run these tests:
//! 1. Start the integration test server:
//!    cd ~/sandbox/nabto-webrtc-sdk-js/integration_test_server && bun dev
//! 2. Run the tests:
//!    cargo test --test device_connectivity

mod common;

use common::{init_logger, DeviceTestInstance, DeviceTestOptions};
use nabto_webrtc::common::SignalingConnectionState;
use std::time::Duration;
use tokio::time::Instant;

/// Device Connectivity Test 1:
/// OK connection. This tests that the device can connect to the signaling service.
#[tokio::test]
#[ignore] // Requires integration test server to be running
async fn test_device_connect_ok() {
    init_logger();
    let test = DeviceTestInstance::create(DeviceTestOptions::default())
        .await
        .expect("Failed to create test instance");

    let (device, _event_rx) = test.start_signaling_device();

    // Device should start in New state
    assert_eq!(
        device.connection_state().await,
        SignalingConnectionState::New
    );

    // Start the device

    // Wait for device to reach Connected state
    device
        .wait_for_state(SignalingConnectionState::Connected, Duration::from_secs(5))
        .await
        .expect("Device did not reach Connected state");

    assert_eq!(
        device.connection_state().await,
        SignalingConnectionState::Connected
    );

    // Cleanup
    device.stop().await; // Stop device("Failed to close device");
    test.destroy().await.expect("Failed to destroy test");
}

/// Device Connectivity Test 2:
/// Close the connection. This tests that close() on a device closes the connection
/// to the signaling service.
#[tokio::test]
#[ignore] // Requires integration test server to be running
async fn test_device_close() {
    let test = DeviceTestInstance::create(DeviceTestOptions::default())
        .await
        .expect("Failed to create test instance");

    let (device, _event_rx) = test.start_signaling_device();

    // Start and wait for connection
    device
        .wait_for_state(SignalingConnectionState::Connected, Duration::from_secs(5))
        .await
        .expect("Device did not reach Connected state");

    // Close the device
    device.stop().await;

    // Wait for the device to reach Closed state
    device
        .wait_for_state(SignalingConnectionState::Closed, Duration::from_secs(5))
        .await
        .expect("Device did not reach Closed state");

    // Verify state is Closed
    assert_eq!(
        device.connection_state().await,
        SignalingConnectionState::Closed
    );

    // Cleanup
    test.destroy().await.expect("Failed to destroy test");
}

/// Device Connectivity Test 3:
/// Initial HTTP Error. This tests that the device retries connections to the
/// signaling service if the initial HTTP request fails.
#[tokio::test]
#[ignore] // Requires integration test server to be running
async fn test_device_http_error_retry() {
    let test = DeviceTestInstance::create(DeviceTestOptions {
        fail_http: Some(true),
        ..Default::default()
    })
    .await
    .expect("Failed to create test instance");

    let (device, _event_rx) = test.start_signaling_device();

    // Start the device

    // The device should go to WaitRetry state due to HTTP failure
    device
        .wait_for_state(SignalingConnectionState::WaitRetry, Duration::from_secs(5))
        .await
        .expect("Device did not reach WaitRetry state");

    assert_eq!(
        device.connection_state().await,
        SignalingConnectionState::WaitRetry
    );

    // Cleanup
    device.stop().await;
    test.destroy().await.expect("Failed to destroy test");
}

/// Device Connectivity Test 4:
/// Initial WebSocket connect error. This tests that the device retries to connect
/// to the signaling service if the initial WebSocket connection fails.
#[tokio::test]
#[ignore] // Requires integration test server to be running
async fn test_device_websocket_error_retry() {
    let test = DeviceTestInstance::create(DeviceTestOptions {
        fail_ws: Some(true),
        ..Default::default()
    })
    .await
    .expect("Failed to create test instance");

    let (device, _event_rx) = test.start_signaling_device();

    // Start the device

    // The device should go to WaitRetry state due to WebSocket failure
    device
        .wait_for_state(SignalingConnectionState::WaitRetry, Duration::from_secs(5))
        .await
        .expect("Device did not reach WaitRetry state");

    assert_eq!(
        device.connection_state().await,
        SignalingConnectionState::WaitRetry
    );

    // Cleanup
    device.stop().await;
    test.destroy().await.expect("Failed to destroy test");
}

/// Device Connectivity Test 5:
/// Device reconnects. Test that the device reconnects if a WebSocket connection
/// is terminated by the server.
#[tokio::test]
#[ignore] // Requires integration test server to be running
async fn test_device_reconnects_after_disconnect() {
    let test = DeviceTestInstance::create(DeviceTestOptions::default())
        .await
        .expect("Failed to create test instance");

    let (device, _event_rx) = test.start_signaling_device();

    // Start and wait for initial connection
    device
        .wait_for_state(SignalingConnectionState::Connected, Duration::from_secs(5))
        .await
        .expect("Device did not reach Connected state initially");

    println!("Device initially connected");

    // Disconnect the device from the server side
    test.disconnect_device()
        .await
        .expect("Failed to disconnect device");

    println!("Device disconnected by server");

    // Wait for device to detect disconnection and go to WaitRetry state
    device
        .wait_for_state(SignalingConnectionState::WaitRetry, Duration::from_secs(5))
        .await
        .expect("Device did not reach WaitRetry state after disconnect");

    // Device should now be in WaitRetry state
    assert_eq!(
        device.connection_state().await,
        SignalingConnectionState::WaitRetry
    );
    println!("Device is in WaitRetry state");

    // TODO: Automatic reconnection is not yet implemented
    // For now, we just verify the state transition works

    // Cleanup
    device.stop().await; // Stop device("Failed to close device");
    test.destroy().await.expect("Failed to destroy test");
}

/// Device Connectivity Test 6:
/// HTTP Protocol extensibility. Observe that the device accepts extra fields
/// in the JSON response.
#[tokio::test]
#[ignore] // Requires integration test server to be running
async fn test_device_http_extensibility() {
    let test = DeviceTestInstance::create(DeviceTestOptions {
        extra_device_connect_response_data: Some(true),
        ..Default::default()
    })
    .await
    .expect("Failed to create test instance");

    let (device, _event_rx) = test.start_signaling_device();

    // Device should still connect successfully even with extra fields
    device
        .wait_for_state(SignalingConnectionState::Connected, Duration::from_secs(5))
        .await
        .expect("Device did not reach Connected state");

    assert_eq!(
        device.connection_state().await,
        SignalingConnectionState::Connected
    );

    // Cleanup
    device.stop().await; // Stop device("Failed to close device");
    test.destroy().await.expect("Failed to destroy test");
}

/// Device Connectivity Test 7:
/// WebSocket Protocol type extensibility. Observe that the device discards
/// messages with an unknown type.
#[tokio::test]
#[ignore] // Requires integration test server to be running
async fn test_device_ws_unknown_message_type() {
    let test = DeviceTestInstance::create(DeviceTestOptions::default())
        .await
        .expect("Failed to create test instance");

    let (device, _event_rx) = test.start_signaling_device();

    // Start and connect
    device
        .wait_for_state(SignalingConnectionState::Connected, Duration::from_secs(5))
        .await
        .expect("Device did not reach Connected state");

    // Send unknown message type
    test.send_new_message_type()
        .await
        .expect("Failed to send new message type");

    // Device should still be connected (ignored the unknown message)
    // Wait a bit to ensure it remains in Connected state
    device
        .wait_for_state(
            SignalingConnectionState::Connected,
            Duration::from_millis(500),
        )
        .await
        .expect("Device should remain in Connected state");

    assert_eq!(
        device.connection_state().await,
        SignalingConnectionState::Connected
    );

    // Cleanup
    device.stop().await; // Stop device("Failed to close device");
    test.destroy().await.expect("Failed to destroy test");
}

/// Device Connectivity Test 9:
/// Heartbeat keeps connection alive. The server is configured with a short idle
/// timeout that would close the WebSocket if no messages arrive. The heartbeat
/// PINGs keep the connection alive by resetting the server's idle timer.
#[tokio::test]
#[ignore] // Requires integration test server to be running
async fn test_device_heartbeat_keeps_connection_alive() {
    init_logger();

    // Server will close the connection if no messages arrive within 300ms
    let mut test = DeviceTestInstance::create(DeviceTestOptions {
        idle_timeout_ms: Some(300),
        ..Default::default()
    })
    .await
    .expect("Failed to create test instance");

    // Heartbeat every 200ms keeps the server's 300ms idle timeout from firing
    test.heartbeat_interval = Some(Duration::from_millis(200));

    let (device, _event_rx) = test.start_signaling_device();

    device
        .wait_for_state(SignalingConnectionState::Connected, Duration::from_secs(5))
        .await
        .expect("Device did not reach Connected state");

    // Wait well beyond the server's idle timeout — heartbeat should keep it alive
    tokio::time::sleep(Duration::from_millis(1500)).await;

    assert_eq!(
        device.connection_state().await,
        SignalingConnectionState::Connected,
        "Device should remain connected when heartbeat keeps the idle timeout from firing"
    );

    // Cleanup
    device.stop().await;
    test.destroy().await.expect("Failed to destroy test");
}

/// Device Connectivity Test 10:
/// Server idle timeout fires before heartbeat. The server's idle timeout is
/// shorter than the device's heartbeat interval, so the server closes the
/// connection before a heartbeat PING arrives. The device should detect the
/// closure and transition to WaitRetry, then reconnect.
#[tokio::test]
#[ignore] // Requires integration test server to be running
async fn test_device_server_idle_timeout_disconnects() {
    init_logger();

    // Server closes after 200ms of inactivity, but heartbeat only fires every 500ms
    let mut test = DeviceTestInstance::create(DeviceTestOptions {
        idle_timeout_ms: Some(200),
        ..Default::default()
    })
    .await
    .expect("Failed to create test instance");

    test.heartbeat_interval = Some(Duration::from_millis(500));

    let (device, _event_rx) = test.start_signaling_device();

    device
        .wait_for_state(SignalingConnectionState::Connected, Duration::from_secs(5))
        .await
        .expect("Device did not reach Connected state");

    // Server should close the connection before the first heartbeat PING arrives
    device
        .wait_for_state(SignalingConnectionState::WaitRetry, Duration::from_secs(5))
        .await
        .expect("Device should have disconnected due to server idle timeout");

    // The device should automatically reconnect
    device
        .wait_for_state(SignalingConnectionState::Connected, Duration::from_secs(10))
        .await
        .expect("Device did not reconnect after server idle timeout");

    // Cleanup
    device.stop().await;
    test.destroy().await.expect("Failed to destroy test");
}

/// Device Connectivity Test 11:
/// Heartbeat detects dead connection. Drop device messages so the server never
/// sees PINGs and never responds with PONGs, then verify the device detects the
/// failure, transitions to WaitRetry, and reconnects back to Connected.
#[tokio::test]
#[ignore] // Requires integration test server to be running
async fn test_device_heartbeat_detects_dead_connection() {
    init_logger();
    let mut test = DeviceTestInstance::create(DeviceTestOptions::default())
        .await
        .expect("Failed to create test instance");

    // Use a short heartbeat so the test completes quickly
    test.heartbeat_interval = Some(Duration::from_millis(200));

    let (device, _event_rx) = test.start_signaling_device();

    device
        .wait_for_state(SignalingConnectionState::Connected, Duration::from_secs(5))
        .await
        .expect("Device did not reach Connected state");

    // Drop all messages from the device so PINGs never reach the server
    test.drop_device_messages()
        .await
        .expect("Failed to drop device messages");

    let start = Instant::now();

    // Device should detect the dead connection via heartbeat timeout
    // and transition to WaitRetry
    device
        .wait_for_state(SignalingConnectionState::WaitRetry, Duration::from_secs(5))
        .await
        .expect("Device did not detect dead connection via heartbeat");

    let elapsed = start.elapsed();
    println!(
        "Heartbeat detected dead connection in {:?} (expected ~400ms)",
        elapsed
    );

    // The device should automatically reconnect back to Connected
    device
        .wait_for_state(SignalingConnectionState::Connected, Duration::from_secs(10))
        .await
        .expect("Device did not reconnect after heartbeat timeout");

    // Cleanup
    device.stop().await;
    test.destroy().await.expect("Failed to destroy test");
}
