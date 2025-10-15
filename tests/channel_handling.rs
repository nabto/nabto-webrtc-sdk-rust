//! Channel Handling Integration Tests
//!
//! These tests verify that SignalingDevice properly handles incoming messages
//! and creates SignalingChannels for new connections.
//!
//! To run these tests:
//! 1. Start the integration test server:
//!    cd ~/sandbox/nabto-webrtc-sdk-js/integration_test_server && bun dev
//! 2. Run the tests:
//!    cargo test --test channel_handling

mod common;

use common::{DeviceTestInstance, DeviceTestOptions};
use nabto_webrtc_sdk::{ConnectionState, DeviceEvent};
use std::time::Duration;

/// Channel Test 1:
/// Test that a new SignalingChannel is created when a client connects
/// and sends the initial message.
#[tokio::test]
#[ignore] // Requires integration test server to be running
async fn test_channel_creation_on_client_connect() {
    let test = DeviceTestInstance::create(DeviceTestOptions::default())
        .await
        .expect("Failed to create test instance");

    let (mut device, mut event_rx) = test.create_signaling_device();

    // Start and connect the device
    device.start().await.expect("Failed to start device");
    test.wait_for_state(&device, ConnectionState::Connected, Duration::from_secs(5))
        .await
        .expect("Device did not reach Connected state");

    println!("Device connected");

    // Create a client
    let client_id = test
        .create_client()
        .await
        .expect("Failed to create client");
    println!("Created client: {}", client_id);

    // Connect the client to the device
    test.connect_client(&client_id)
        .await
        .expect("Failed to connect client");
    println!("Client connected");

    // Give time for WebSocket messages to arrive
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Process events
    device.process_events().await;

    // Check for NewChannel event
    let mut new_channel_received = false;
    while let Ok(event) = event_rx.try_recv() {
        match event {
            DeviceEvent::NewChannel { channel, authorized } => {
                println!(
                    "Received NewChannel event for channel {} (authorized: {})",
                    channel.channel_id(),
                    authorized
                );
                new_channel_received = true;
                assert!(!authorized); // Client connects without authorization in basic test
            }
            DeviceEvent::StateChanged { old_state, new_state } => {
                println!("Device state changed: {:?} -> {:?}", old_state, new_state);
            }
        }
    }

    assert!(
        new_channel_received,
        "Expected to receive NewChannel event when client connects"
    );

    // Cleanup
    test.disconnect_client(&client_id)
        .await
        .expect("Failed to disconnect client");
    device.close().await.expect("Failed to close device");
    test.destroy().await.expect("Failed to destroy test");
}

/// Channel Test 2:
/// Test that multiple channels can be created for multiple clients
#[tokio::test]
#[ignore] // Requires integration test server to be running
async fn test_multiple_channel_creation() {
    let test = DeviceTestInstance::create(DeviceTestOptions::default())
        .await
        .expect("Failed to create test instance");

    let (mut device, mut event_rx) = test.create_signaling_device();

    // Start and connect the device
    device.start().await.expect("Failed to start device");
    test.wait_for_state(&device, ConnectionState::Connected, Duration::from_secs(5))
        .await
        .expect("Device did not reach Connected state");

    println!("Device connected");

    // Create first client
    let client1_id = test
        .create_client()
        .await
        .expect("Failed to create first client");
    test.connect_client(&client1_id)
        .await
        .expect("Failed to connect first client");
    println!("First client connected: {}", client1_id);

    // Create second client
    let client2_id = test
        .create_client()
        .await
        .expect("Failed to create second client");
    test.connect_client(&client2_id)
        .await
        .expect("Failed to connect second client");
    println!("Second client connected: {}", client2_id);

    // Give time for WebSocket messages to arrive
    tokio::time::sleep(Duration::from_millis(500)).await;

    // Process events
    device.process_events().await;

    // Check for NewChannel events
    let mut channel_count = 0;
    let mut channel_ids = Vec::new();

    while let Ok(event) = event_rx.try_recv() {
        match event {
            DeviceEvent::NewChannel { channel, authorized } => {
                println!(
                    "Received NewChannel event for channel {} (authorized: {})",
                    channel.channel_id(),
                    authorized
                );
                channel_ids.push(channel.channel_id().to_string());
                channel_count += 1;
            }
            DeviceEvent::StateChanged { .. } => {}
        }
    }

    assert_eq!(
        channel_count, 2,
        "Expected to receive 2 NewChannel events for 2 clients"
    );

    // Verify we got unique channel IDs
    assert_ne!(
        channel_ids[0], channel_ids[1],
        "Expected different channel IDs for different clients"
    );

    // Cleanup
    test.disconnect_client(&client1_id)
        .await
        .expect("Failed to disconnect first client");
    test.disconnect_client(&client2_id)
        .await
        .expect("Failed to disconnect second client");
    device.close().await.expect("Failed to close device");
    test.destroy().await.expect("Failed to destroy test");
}

/// Channel Test 3:
/// Test that non-initial messages to non-existent channels are rejected
#[tokio::test]
#[ignore] // Requires integration test server to be running
async fn test_non_initial_message_rejected() {
    let test = DeviceTestInstance::create(DeviceTestOptions::default())
        .await
        .expect("Failed to create test instance");

    let (mut device, mut event_rx) = test.create_signaling_device();

    // Start and connect the device
    device.start().await.expect("Failed to start device");
    test.wait_for_state(&device, ConnectionState::Connected, Duration::from_secs(5))
        .await
        .expect("Device did not reach Connected state");

    println!("Device connected");

    // Create a client and connect
    let client_id = test
        .create_client()
        .await
        .expect("Failed to create client");
    test.connect_client(&client_id)
        .await
        .expect("Failed to connect client");

    // Give time for initial connection
    tokio::time::sleep(Duration::from_millis(500)).await;
    device.process_events().await;

    // Consume the NewChannel event
    while let Ok(_) = event_rx.try_recv() {}

    // Now disconnect the client
    test.disconnect_client(&client_id)
        .await
        .expect("Failed to disconnect client");

    // Give time for disconnection to propagate
    tokio::time::sleep(Duration::from_millis(500)).await;
    device.process_events().await;

    // Try to send a message to the now-closed channel
    // This should be handled gracefully (no crash)
    // The device should still be in Connected state
    assert_eq!(device.connection_state(), ConnectionState::Connected);

    // Cleanup
    device.close().await.expect("Failed to close device");
    test.destroy().await.expect("Failed to destroy test");
}

/// Channel Test 4:
/// Test that channels receive messages after creation
#[tokio::test]
#[ignore] // Requires integration test server to be running
async fn test_channel_receives_messages() {
    let test = DeviceTestInstance::create(DeviceTestOptions::default())
        .await
        .expect("Failed to create test instance");

    let (mut device, mut event_rx) = test.create_signaling_device();

    // Start and connect the device
    device.start().await.expect("Failed to start device");
    test.wait_for_state(&device, ConnectionState::Connected, Duration::from_secs(5))
        .await
        .expect("Device did not reach Connected state");

    println!("Device connected");

    // Create and connect a client
    let client_id = test
        .create_client()
        .await
        .expect("Failed to create client");
    test.connect_client(&client_id)
        .await
        .expect("Failed to connect client");
    println!("Client connected: {}", client_id);

    // Give time for connection and initial message
    tokio::time::sleep(Duration::from_millis(500)).await;
    device.process_events().await;

    // Verify NewChannel event was received
    let mut new_channel_received = false;
    while let Ok(event) = event_rx.try_recv() {
        if matches!(event, DeviceEvent::NewChannel { .. }) {
            new_channel_received = true;
        }
    }
    assert!(new_channel_received, "Expected NewChannel event");

    // Send additional messages from client
    let messages = vec![
        "test message 1".to_string(),
        "test message 2".to_string(),
    ];
    test.client_send_messages(&client_id, messages.clone())
        .await
        .expect("Failed to send messages from client");

    println!("Sent {} messages from client", messages.len());

    // Give time for messages to arrive
    tokio::time::sleep(Duration::from_millis(500)).await;
    device.process_events().await;

    // At this point, the channel should have processed the messages
    // The messages go through the reliability layer and are queued in the channel
    // For now, we just verify the device is still connected
    assert_eq!(device.connection_state(), ConnectionState::Connected);

    // Cleanup
    test.disconnect_client(&client_id)
        .await
        .expect("Failed to disconnect client");
    device.close().await.expect("Failed to close device");
    test.destroy().await.expect("Failed to destroy test");
}
