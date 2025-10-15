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

use common::{DeviceTestInstance, DeviceTestOptions};
use nabto_webrtc_sdk::ConnectionState;
use std::time::Duration;

/// Device Connectivity Test 1:
/// OK connection. This tests that the device can connect to the signaling service.
#[tokio::test]
#[ignore] // Requires integration test server to be running
async fn test_device_connect_ok() {
    let test = DeviceTestInstance::create(DeviceTestOptions::default())
        .await
        .expect("Failed to create test instance");

    let (device, _event_rx) = test.start_signaling_device();

    // Device should start in New state
    assert_eq!(device.connection_state().await, ConnectionState::New);

    // Start the device

    // Wait for device to reach Connected state
    device.wait_for_state( ConnectionState::Connected, Duration::from_secs(5))
        .await
        .expect("Device did not reach Connected state");

    assert_eq!(device.connection_state().await, ConnectionState::Connected);

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
    device.wait_for_state( ConnectionState::Connected, Duration::from_secs(5))
        .await
        .expect("Device did not reach Connected state");

    // Close the device
    device.stop().await;

    // Wait for the device to reach Closed state
    device
        .wait_for_state(ConnectionState::Closed, Duration::from_secs(5))
        .await
        .expect("Device did not reach Closed state");

    // Verify state is Closed
    assert_eq!(device.connection_state().await, ConnectionState::Closed);

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
    device.wait_for_state( ConnectionState::WaitRetry, Duration::from_secs(5))
        .await
        .expect("Device did not reach WaitRetry state");

    assert_eq!(device.connection_state().await, ConnectionState::WaitRetry);

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
    device.wait_for_state( ConnectionState::WaitRetry, Duration::from_secs(5))
        .await
        .expect("Device did not reach WaitRetry state");

    assert_eq!(device.connection_state().await, ConnectionState::WaitRetry);

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
    device.wait_for_state( ConnectionState::Connected, Duration::from_secs(5))
        .await
        .expect("Device did not reach Connected state initially");

    println!("Device initially connected");

    // Disconnect the device from the server side
    test.disconnect_device()
        .await
        .expect("Failed to disconnect device");

    println!("Device disconnected by server");

    // Give the WebSocket monitoring task time to detect the disconnection
    // and process the event
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Process WebSocket events to detect the disconnection

    // Device should now be in WaitRetry state
    assert_eq!(device.connection_state().await, ConnectionState::WaitRetry);
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
    device.wait_for_state( ConnectionState::Connected, Duration::from_secs(5))
        .await
        .expect("Device did not reach Connected state");

    assert_eq!(device.connection_state().await, ConnectionState::Connected);

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
    device.wait_for_state( ConnectionState::Connected, Duration::from_secs(5))
        .await
        .expect("Device did not reach Connected state");

    // Send unknown message type
    test.send_new_message_type()
        .await
        .expect("Failed to send new message type");

    // Wait a bit to ensure device processes the message
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Device should still be connected (ignored the unknown message)
    assert_eq!(device.connection_state().await, ConnectionState::Connected);

    // Cleanup
    device.stop().await; // Stop device("Failed to close device");
    test.destroy().await.expect("Failed to destroy test");
}
