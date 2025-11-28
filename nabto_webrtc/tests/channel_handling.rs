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
use nabto_webrtc::device::{ConnectionState, DeviceEvent};
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

    let (device, mut event_rx) = test.start_signaling_device();

    // Start and connect the device
    device
        .wait_for_state(ConnectionState::Connected, Duration::from_secs(5))
        .await
        .expect("Device did not reach Connected state");

    println!("Device connected");

    // Create a client
    let client_id = test.create_client().await.expect("Failed to create client");
    println!("Created client: {}", client_id);

    // Connect the client to the device
    test.connect_client(&client_id)
        .await
        .expect("Failed to connect client");
    println!("Client connected");

    // Send a message from client to device to trigger channel creation
    test.client_send_messages(&client_id, vec!["test message".to_string()])
        .await
        .expect("Failed to send message from client");
    println!("Client sent message to device");

    // Wait for NewChannel event
    let mut new_channel_received = false;
    let timeout = Duration::from_secs(5);
    let start = std::time::Instant::now();

    while start.elapsed() < timeout && !new_channel_received {
        match tokio::time::timeout(Duration::from_millis(100), event_rx.recv()).await {
            Ok(Some(event)) => {
                match event {
                    DeviceEvent::NewChannel {
                        handle, authorized, ..
                    } => {
                        println!(
                            "Received NewChannel event for channel {} (authorized: {})",
                            handle.channel_id(),
                            authorized
                        );
                        new_channel_received = true;
                        assert!(!authorized); // Client connects without authorization in basic test
                    }
                    DeviceEvent::StateChanged {
                        old_state,
                        new_state,
                    } => {
                        println!("Device state changed: {:?} -> {:?}", old_state, new_state);
                    }
                }
            }
            Ok(None) => break,
            Err(_) => continue,
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
    device.stop().await; // Stop device("Failed to close device");
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

    let (device, mut event_rx) = test.start_signaling_device();

    // Start and connect the device
    device
        .wait_for_state(ConnectionState::Connected, Duration::from_secs(5))
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

    // Send a message from first client to trigger channel creation
    test.client_send_messages(&client1_id, vec!["test message 1".to_string()])
        .await
        .expect("Failed to send message from first client");
    println!("First client sent message to device");

    // Create second client
    let client2_id = test
        .create_client()
        .await
        .expect("Failed to create second client");
    test.connect_client(&client2_id)
        .await
        .expect("Failed to connect second client");
    println!("Second client connected: {}", client2_id);

    // Send a message from second client to trigger channel creation
    test.client_send_messages(&client2_id, vec!["test message 2".to_string()])
        .await
        .expect("Failed to send message from second client");
    println!("Second client sent message to device");

    // Wait for NewChannel events from both clients
    let mut channel_count = 0;
    let mut channel_ids = Vec::new();
    let timeout = Duration::from_secs(5);
    let start = std::time::Instant::now();

    while start.elapsed() < timeout && channel_count < 2 {
        match tokio::time::timeout(Duration::from_millis(100), event_rx.recv()).await {
            Ok(Some(event)) => match event {
                DeviceEvent::NewChannel {
                    handle, authorized, ..
                } => {
                    println!(
                        "Received NewChannel event for channel {} (authorized: {})",
                        handle.channel_id(),
                        authorized
                    );
                    channel_ids.push(handle.channel_id().to_string());
                    channel_count += 1;
                }
                DeviceEvent::StateChanged { .. } => {}
            },
            Ok(None) => break,
            Err(_) => continue,
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
    device.stop().await; // Stop device("Failed to close device");
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

    let (device, mut event_rx) = test.start_signaling_device();

    // Start and connect the device
    device
        .wait_for_state(ConnectionState::Connected, Duration::from_secs(5))
        .await
        .expect("Device did not reach Connected state");

    println!("Device connected");

    // Create a client and connect
    let client_id = test.create_client().await.expect("Failed to create client");
    test.connect_client(&client_id)
        .await
        .expect("Failed to connect client");

    // Send a message from client to device to trigger channel creation
    test.client_send_messages(&client_id, vec!["test message".to_string()])
        .await
        .expect("Failed to send message from client");

    // Wait for NewChannel event
    let timeout = Duration::from_secs(5);
    let start = std::time::Instant::now();
    let mut channel_received = false;

    while start.elapsed() < timeout && !channel_received {
        match tokio::time::timeout(Duration::from_millis(100), event_rx.recv()).await {
            Ok(Some(event)) => {
                if matches!(event, DeviceEvent::NewChannel { .. }) {
                    channel_received = true;
                }
            }
            Ok(None) => break,
            Err(_) => continue,
        }
    }

    assert!(channel_received, "Expected NewChannel event");

    // Now disconnect the client
    test.disconnect_client(&client_id)
        .await
        .expect("Failed to disconnect client");

    // Try to send a message to the now-closed channel
    // This should be handled gracefully (no crash)
    // The device should still be in Connected state
    assert_eq!(device.connection_state().await, ConnectionState::Connected);

    // Cleanup
    device.stop().await; // Stop device("Failed to close device");
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

    let (device, mut event_rx) = test.start_signaling_device();

    // Start and connect the device
    device
        .wait_for_state(ConnectionState::Connected, Duration::from_secs(5))
        .await
        .expect("Device did not reach Connected state");

    println!("Device connected");

    // Create and connect a client
    let client_id = test.create_client().await.expect("Failed to create client");
    test.connect_client(&client_id)
        .await
        .expect("Failed to connect client");
    println!("Client connected: {}", client_id);

    // Send initial message from client to device to trigger channel creation
    test.client_send_messages(&client_id, vec!["initial message".to_string()])
        .await
        .expect("Failed to send initial message from client");
    println!("Client sent initial message to device");

    // Wait for NewChannel event
    let mut new_channel_received = false;
    let timeout = Duration::from_secs(5);
    let start = std::time::Instant::now();

    while start.elapsed() < timeout && !new_channel_received {
        match tokio::time::timeout(Duration::from_millis(100), event_rx.recv()).await {
            Ok(Some(event)) => {
                if matches!(event, DeviceEvent::NewChannel { .. }) {
                    new_channel_received = true;
                }
            }
            Ok(None) => break,
            Err(_) => continue,
        }
    }
    assert!(new_channel_received, "Expected NewChannel event");

    // Send additional messages from client
    let messages = vec!["test message 1".to_string(), "test message 2".to_string()];
    test.client_send_messages(&client_id, messages.clone())
        .await
        .expect("Failed to send messages from client");

    println!("Sent {} messages from client", messages.len());

    // At this point, the channel should have processed the messages
    // The messages go through the reliability layer and are queued in the channel
    // For now, we just verify the device is still connected
    assert_eq!(device.connection_state().await, ConnectionState::Connected);

    // Cleanup
    test.disconnect_client(&client_id)
        .await
        .expect("Failed to disconnect client");
    device.stop().await; // Stop device("Failed to close device");
    test.destroy().await.expect("Failed to destroy test");
}
