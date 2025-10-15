//! Device Client Integration Tests
//!
//! These tests verify that clients can connect to a device and that
//! SignalingChannels are properly managed.
//!
//! To run these tests:
//! 1. Start the integration test server:
//!    cd ~/sandbox/nabto-webrtc-sdk-js/integration_test_server && bun dev
//! 2. Run the tests:
//!    cargo test --test device_client

mod common;

use common::{DeviceTestInstance, DeviceTestOptions};
use nabto_webrtc_sdk::{ChannelState, ConnectionState, DeviceEvent};
use std::time::Duration;

/// Device Client Test 1:
/// Success. Test that a client can connect to a device.
///
/// Steps:
/// 1. Start a device.
/// 2. Connect a client.
/// 3. Observe that the device has a signaling channel in the state CONNECTED.
#[tokio::test]
#[ignore] // Requires integration test server to be running
async fn test_device_client_success() {
    let test = DeviceTestInstance::create(DeviceTestOptions::default())
        .await
        .expect("Failed to create test instance");

    let (mut device, mut event_rx) = test.create_signaling_device();

    // Step 1: Start a device
    device.start().await.expect("Failed to start device");
    test.wait_for_state(&device, ConnectionState::Connected, Duration::from_secs(5))
        .await
        .expect("Device did not reach Connected state");

    println!("✓ Device started and connected");

    // Step 2: Connect a client
    let client_id = test
        .create_client()
        .await
        .expect("Failed to create client");

    test.connect_client(&client_id)
        .await
        .expect("Failed to connect client");

    println!("✓ Client connected: {}", client_id);

    // Give time for WebSocket messages to arrive and process them
    tokio::time::sleep(Duration::from_millis(1000)).await;

    // Process events to handle the incoming channel creation
    device.process_events().await;

    println!("Events processed, checking for NewChannel event...");

    // Step 3: Observe that the device has a signaling channel in the state CONNECTED
    let mut channel_found = false;
    let mut channel_state = None;

    // Check for NewChannel event
    let mut event_count = 0;
    while let Ok(event) = event_rx.try_recv() {
        event_count += 1;
        match event {
            DeviceEvent::NewChannel { channel, authorized } => {
                println!(
                    "✓ Received NewChannel event for channel {} (authorized: {})",
                    channel.channel_id(),
                    authorized
                );

                // Verify the channel is in CONNECTED state
                channel_state = Some(channel.state());
                channel_found = true;

                println!("  Channel state: {:?}", channel.state());
            }
            DeviceEvent::StateChanged { old_state, new_state } => {
                println!("  Device state changed: {:?} -> {:?}", old_state, new_state);
            }
        }
    }

    println!("Received {} events total", event_count);

    // Verify that we received a NewChannel event
    assert!(
        channel_found,
        "Expected to receive NewChannel event when client connects. Received {} events total",
        event_count
    );

    // Verify the channel is in CONNECTED state
    assert_eq!(
        channel_state,
        Some(ChannelState::Connected),
        "Expected channel to be in CONNECTED state"
    );

    println!("✓ Channel is in CONNECTED state");

    // Cleanup
    test.disconnect_client(&client_id)
        .await
        .expect("Failed to disconnect client");
    device.close().await.expect("Failed to close device");
    test.destroy().await.expect("Failed to destroy test");
}
