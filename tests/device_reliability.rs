//! Device Reliability Integration Tests
//!
//! These tests verify that messages are reliably sent and received between peers,
//! including scenarios with disconnection and reconnection.
//!
//! To run these tests:
//! 1. Start the integration test server:
//!    cd ~/sandbox/nabto-webrtc-sdk-js/integration_test_server && bun dev
//! 2. Run the tests:
//!    cargo test --test device_reliability

mod common;

use common::{DeviceTestInstance, DeviceTestOptions};
use nabto_webrtc_sdk::device::{ChannelHandle, ConnectionState, DeviceEvent};
use serde_json::Value as JsonValue;
use std::time::Duration;
use tokio::sync::mpsc;

/// Message receiver helper that collects messages from a SignalingChannel.
/// Filters out the initial "hello" message automatically.
struct MessageReceiver {
    received_messages: Vec<String>,
    message_rx: mpsc::Receiver<JsonValue>,
}

impl MessageReceiver {
    /// Create a new MessageReceiver for a SignalingChannel
    fn new(message_rx: mpsc::Receiver<JsonValue>) -> Self {
        Self {
            received_messages: Vec::new(),
            message_rx,
        }
    }

    /// Wait for specific messages to be received
    /// Returns the received messages or an error on timeout
    async fn wait_for_messages(
        &mut self,
        expected_messages: Vec<String>,
        timeout_ms: u64,
    ) -> Result<Vec<String>, Box<dyn std::error::Error>> {
        let timeout = Duration::from_millis(timeout_ms);
        let start = std::time::Instant::now();

        loop {
            // Check if we already have the expected messages
            if self.received_messages == expected_messages {
                return Ok(self.received_messages.clone());
            }

            // Check for timeout
            if start.elapsed() > timeout {
                return Err(format!(
                    "Timeout waiting for messages. Expected: {:?}, Received: {:?}",
                    expected_messages, self.received_messages
                )
                .into());
            }

            // Try to receive a message with a short timeout
            match tokio::time::timeout(Duration::from_millis(100), self.message_rx.recv()).await {
                Ok(Some(msg)) => {
                    // Filter out "hello" messages
                    if let Some(s) = msg.as_str() {
                        if s != "hello" {
                            self.received_messages.push(s.to_string());
                        }
                    }
                }
                Ok(None) => {
                    return Err("Message channel closed".into());
                }
                Err(_) => {
                    // Timeout, continue loop
                }
            }
        }
    }

    /// Get all messages received so far
    #[allow(dead_code)]
    fn get_received_messages(&self) -> Vec<String> {
        self.received_messages.clone()
    }
}

/// Reliability Test 1:
/// Successfully sending messages. Test that messages can be sent by a peer.
#[tokio::test]
#[ignore] // Requires integration test server to be running
async fn test_reliability_1_successfully_sending_messages() {
    let test = DeviceTestInstance::create(DeviceTestOptions::default())
        .await
        .expect("Failed to create test instance");

    let (device_handle, mut event_rx) = test.start_signaling_device();

    // Wait for device to connect
    device_handle
        .wait_for_state(ConnectionState::Connected, Duration::from_secs(5))
        .await
        .expect("Device did not reach Connected state");

    println!("Device connected");

    // Create a client
    let client_id = test.create_client().await.expect("Failed to create client");

    // Send initial hello message from client to trigger channel creation
    test.client_send_messages(&client_id, vec!["hello".to_string()])
        .await
        .expect("Failed to send hello message");

    // Wait for NewChannel event
    let (signaling_channel, _message_rx) = wait_for_new_channel(&mut event_rx)
        .await
        .expect("Failed to get NewChannel event");

    println!("Got signaling channel: {}", signaling_channel.channel_id());

    // Send messages from device to client
    let messages = vec!["1".to_string(), "2".to_string(), "3".to_string()];
    for msg in &messages {
        signaling_channel
            .send_message(serde_json::json!(msg))
            .await
            .expect("Failed to send message");
    }

    // Wait for client to receive messages
    let received_messages = test
        .client_wait_for_messages(&client_id, messages.clone(), 5000)
        .await
        .expect("Failed to wait for messages");

    assert_eq!(received_messages, messages);

    // Cleanup
    test.disconnect_client(&client_id)
        .await
        .expect("Failed to disconnect client");
    device_handle.stop().await;
    test.destroy().await.expect("Failed to destroy test");
}

/// Reliability Test 2:
/// Successfully receiving messages. Test that messages can be received by a peer.
#[tokio::test]
#[ignore] // Requires integration test server to be running
async fn test_reliability_2_successfully_receiving_messages() {
    let test = DeviceTestInstance::create(DeviceTestOptions::default())
        .await
        .expect("Failed to create test instance");

    let (device_handle, mut event_rx) = test.start_signaling_device();

    // Wait for device to connect
    device_handle
        .wait_for_state(ConnectionState::Connected, Duration::from_secs(5))
        .await
        .expect("Device did not reach Connected state");

    // Create a client
    let client_id = test.create_client().await.expect("Failed to create client");

    // Send initial hello message from client to trigger channel creation
    test.client_send_messages(&client_id, vec!["hello".to_string()])
        .await
        .expect("Failed to send hello message");

    // Wait for NewChannel event and get message receiver
    let (_signaling_channel, message_rx) = wait_for_new_channel(&mut event_rx)
        .await
        .expect("Failed to get NewChannel event");

    let mut message_receiver = MessageReceiver::new(message_rx);

    // Send messages from client to device
    let messages = vec!["1".to_string(), "2".to_string(), "3".to_string()];
    test.client_send_messages(&client_id, messages.clone())
        .await
        .expect("Failed to send messages from client");

    // Wait for device to receive messages
    let received_messages = message_receiver
        .wait_for_messages(messages.clone(), 5000)
        .await
        .expect("Failed to receive messages");

    assert_eq!(received_messages, messages);

    // Cleanup
    test.disconnect_client(&client_id)
        .await
        .expect("Failed to disconnect client");
    device_handle.stop().await;
    test.destroy().await.expect("Failed to destroy test");
}

/// Reliability Test 3:
/// Resend messages when a peer becomes online. Test that messages which are sent to
/// a peer which is DISCONNECTED, become retransmitted when the peer becomes online.
#[tokio::test]
#[ignore] // Requires integration test server to be running
async fn test_reliability_3_resend_messages_when_peer_becomes_online() {
    let test = DeviceTestInstance::create(DeviceTestOptions::default())
        .await
        .expect("Failed to create test instance");

    let (device_handle, mut event_rx) = test.start_signaling_device();

    // Wait for device to connect
    device_handle
        .wait_for_state(ConnectionState::Connected, Duration::from_secs(5))
        .await
        .expect("Device did not reach Connected state");

    // Create a client
    let client_id = test.create_client().await.expect("Failed to create client");

    // Send initial hello message from client to trigger channel creation
    test.client_send_messages(&client_id, vec!["hello".to_string()])
        .await
        .expect("Failed to send hello message");

    // Wait for NewChannel event
    let (signaling_channel, _message_rx) = wait_for_new_channel(&mut event_rx)
        .await
        .expect("Failed to get NewChannel event");

    // Send initial messages
    let messages = vec!["1".to_string(), "2".to_string(), "3".to_string()];
    for msg in &messages {
        signaling_channel
            .send_message(serde_json::json!(msg))
            .await
            .expect("Failed to send message");
    }

    // Wait for client to receive initial messages
    let received_messages = test
        .client_wait_for_messages(&client_id, messages.clone(), 5000)
        .await
        .expect("Failed to wait for messages");
    assert_eq!(received_messages, messages);

    // Disconnect the device
    test.disconnect_device()
        .await
        .expect("Failed to disconnect device");

    // Send more messages while disconnected
    let messages2 = vec!["4".to_string(), "5".to_string(), "6".to_string()];
    for msg in &messages2 {
        signaling_channel
            .send_message(serde_json::json!(msg))
            .await
            .expect("Failed to send message");
    }

    // Wait for device to reconnect and messages to be retransmitted
    let expected_messages = [messages.clone(), messages2.clone()].concat();
    let received_messages2 = test
        .client_wait_for_messages(&client_id, expected_messages.clone(), 10000)
        .await
        .expect("Failed to wait for retransmitted messages");

    assert_eq!(received_messages2, expected_messages);

    // Cleanup
    test.disconnect_client(&client_id)
        .await
        .expect("Failed to disconnect client");
    device_handle.stop().await;
    test.destroy().await.expect("Failed to destroy test");
}

/// Reliability Test 4:
/// Resend messages which are lost on a stale websocket connection.
#[tokio::test]
#[ignore] // Requires integration test server to be running
async fn test_reliability_4_resend_messages_lost_on_stale_websocket() {
    println!("[TEST4] Starting test_reliability_4_resend_messages_lost_on_stale_websocket");

    let test = DeviceTestInstance::create(DeviceTestOptions::default())
        .await
        .expect("Failed to create test instance");
    println!("[TEST4] Test instance created");

    let (device_handle, mut event_rx) = test.start_signaling_device();
    println!("[TEST4] Signaling device started");

    // Wait for device to connect
    device_handle
        .wait_for_state(ConnectionState::Connected, Duration::from_secs(5))
        .await
        .expect("Device did not reach Connected state");
    println!("[TEST4] Device connected");

    // Create a client
    let client_id = test.create_client().await.expect("Failed to create client");
    println!("[TEST4] Client created: {}", client_id);

    // Send initial hello message from client to trigger channel creation
    test.client_send_messages(&client_id, vec!["hello".to_string()])
        .await
        .expect("Failed to send hello message");
    println!("[TEST4] Sent hello message from client");

    // Wait for NewChannel event
    let (signaling_channel, _message_rx) = wait_for_new_channel(&mut event_rx)
        .await
        .expect("Failed to get NewChannel event");
    println!(
        "[TEST4] Got NewChannel event, channel_id: {}",
        signaling_channel.channel_id()
    );

    // Send initial message
    let initial_message = "1".to_string();
    signaling_channel
        .send_message(serde_json::json!(&initial_message))
        .await
        .expect("Failed to send message");
    println!("[TEST4] Sent initial message: {}", initial_message);

    // Wait for client to receive initial message
    println!("[TEST4] Waiting for client to receive initial message...");
    let received_messages1 = test
        .client_wait_for_messages(&client_id, vec![initial_message.clone()], 5000)
        .await
        .expect("Failed to wait for initial message");
    assert_eq!(received_messages1, vec![initial_message.clone()]);
    println!(
        "[TEST4] Client received initial message: {:?}",
        received_messages1
    );

    // Let the websocket connection drop all further messages
    test.drop_device_messages()
        .await
        .expect("Failed to drop device messages");
    println!("[TEST4] Enabled drop_device_messages mode");

    // Send more messages (they will be lost on the stale connection)
    let messages2 = vec!["2".to_string(), "3".to_string(), "4".to_string()];
    for msg in &messages2 {
        signaling_channel
            .send_message(serde_json::json!(msg))
            .await
            .expect("Failed to send message");
        println!("[TEST4] Sent message (will be dropped): {}", msg);
    }

    // Trigger check_alive to detect the stale connection and force reconnection
    println!("[TEST4] Calling check_alive to detect stale connection...");
    device_handle
        .check_alive()
        .await
        .expect("Failed to call check_alive");
    println!("[TEST4] check_alive completed");

    // Wait for messages to be retransmitted after reconnection
    let all_expected_messages = [vec![initial_message], messages2].concat();
    println!(
        "[TEST4] Waiting for client to receive retransmitted messages: {:?}",
        all_expected_messages
    );
    let received_messages2 = test
        .client_wait_for_messages(&client_id, all_expected_messages.clone(), 10000)
        .await
        .expect("Failed to wait for retransmitted messages");
    println!(
        "[TEST4] Client received all messages: {:?}",
        received_messages2
    );

    assert_eq!(received_messages2, all_expected_messages);
    println!("[TEST4] Assertion passed - all messages received correctly");

    // Cleanup
    println!("[TEST4] Starting cleanup...");
    test.disconnect_client(&client_id)
        .await
        .expect("Failed to disconnect client");
    println!("[TEST4] Client disconnected");

    device_handle.stop().await;
    println!("[TEST4] Device stopped");

    test.destroy().await.expect("Failed to destroy test");
    println!("[TEST4] Test destroyed - test completed successfully");
}

/// Reliability Test 5:
/// Discard duplicate messages from the remote peer.
#[tokio::test]
#[ignore] // Requires integration test server to be running
async fn test_reliability_5_discard_duplicate_messages() {
    let test = DeviceTestInstance::create(DeviceTestOptions::default())
        .await
        .expect("Failed to create test instance");

    let (device_handle, mut event_rx) = test.start_signaling_device();

    // Wait for device to connect
    device_handle
        .wait_for_state(ConnectionState::Connected, Duration::from_secs(5))
        .await
        .expect("Device did not reach Connected state");

    // Create a client
    let client_id = test.create_client().await.expect("Failed to create client");

    // Send initial hello message from client to trigger channel creation
    test.client_send_messages(&client_id, vec!["hello".to_string()])
        .await
        .expect("Failed to send hello message");

    // Wait for NewChannel event and get message receiver
    let (_signaling_channel, message_rx) = wait_for_new_channel(&mut event_rx)
        .await
        .expect("Failed to get NewChannel event");

    let mut message_receiver = MessageReceiver::new(message_rx);

    // Let the signaling service drop further messages from the device
    test.drop_device_messages()
        .await
        .expect("Failed to drop device messages");

    // Send messages to the device from the client
    let messages1 = vec!["1".to_string(), "2".to_string(), "3".to_string()];
    test.client_send_messages(&client_id, messages1.clone())
        .await
        .expect("Failed to send messages from client");

    // ACKs from the device are lost (because dropDeviceMessages is active)

    // Trigger check_alive to detect the stale connection and force reconnection
    device_handle
        .check_alive()
        .await
        .expect("Failed to call check_alive");

    // Wait a bit for reconnection to occur
    tokio::time::sleep(Duration::from_millis(500)).await;

    // The client will resend messages1 because it didn't receive acks

    // Send an extra message from the client
    let extra_message = vec!["4".to_string()];
    test.client_send_messages(&client_id, extra_message.clone())
        .await
        .expect("Failed to send extra message");

    // Wait for all messages, duplicates should be removed
    let expected_messages = [messages1, extra_message].concat();
    let received_messages = message_receiver
        .wait_for_messages(expected_messages.clone(), 10000)
        .await
        .expect("Failed to receive messages");

    assert_eq!(received_messages, expected_messages);

    // Cleanup
    test.disconnect_client(&client_id)
        .await
        .expect("Failed to disconnect client");
    device_handle.stop().await;
    test.destroy().await.expect("Failed to destroy test");
}

/// Reliability Test 6:
/// Test that a peer resend unacked messages when a PEER_ONLINE event is received.
#[tokio::test]
#[ignore] // Requires integration test server to be running
async fn test_reliability_6_resend_unacked_messages_on_peer_online() {
    let test = DeviceTestInstance::create(DeviceTestOptions::default())
        .await
        .expect("Failed to create test instance");

    let (device_handle, mut event_rx) = test.start_signaling_device();

    // Wait for device to connect
    device_handle
        .wait_for_state(ConnectionState::Connected, Duration::from_secs(5))
        .await
        .expect("Device did not reach Connected state");

    // Create a client
    let client_id = test.create_client().await.expect("Failed to create client");

    // Send initial hello message from client to trigger channel creation
    test.client_send_messages(&client_id, vec!["hello".to_string()])
        .await
        .expect("Failed to send hello message");

    // Wait for NewChannel event
    let (signaling_channel, _message_rx) = wait_for_new_channel(&mut event_rx)
        .await
        .expect("Failed to get NewChannel event");

    // Drop client messages so acknowledgments won't be sent back to the device
    test.drop_client_messages(&client_id)
        .await
        .expect("Failed to drop client messages");

    // Send messages to the client
    let messages = vec!["1".to_string(), "2".to_string(), "3".to_string()];
    for msg in &messages {
        signaling_channel
            .send_message(serde_json::json!(msg))
            .await
            .expect("Failed to send message");
    }

    // Disconnect and reconnect the client to trigger a PEER_ONLINE event
    test.disconnect_client(&client_id)
        .await
        .expect("Failed to disconnect client");
    test.connect_client(&client_id)
        .await
        .expect("Failed to reconnect client");

    // Send a message to establish the connection
    test.client_send_messages(&client_id, vec!["reconnect".to_string()])
        .await
        .expect("Failed to send reconnect message");

    // The device should resend the unacknowledged messages when the client comes back online
    let received_messages = test
        .client_wait_for_messages(&client_id, messages.clone(), 10000)
        .await
        .expect("Failed to wait for retransmitted messages");

    assert_eq!(received_messages, messages);

    // Cleanup
    test.disconnect_client(&client_id)
        .await
        .expect("Failed to disconnect client");
    device_handle.stop().await;
    test.destroy().await.expect("Failed to destroy test");
}

/// Helper function to wait for a NewChannel event and extract the handle with message receiver
async fn wait_for_new_channel(
    event_rx: &mut mpsc::Receiver<DeviceEvent>,
) -> Result<(ChannelHandle, mpsc::Receiver<JsonValue>), Box<dyn std::error::Error>> {
    let timeout = Duration::from_secs(5);
    let start = std::time::Instant::now();

    while start.elapsed() < timeout {
        match tokio::time::timeout(Duration::from_millis(100), event_rx.recv()).await {
            Ok(Some(DeviceEvent::NewChannel {
                handle, message_rx, ..
            })) => {
                // Return the handle and message receiver directly from the event
                return Ok((handle, message_rx));
            }
            Ok(Some(DeviceEvent::StateChanged { .. })) => {
                // Ignore state changes
            }
            Ok(None) => {
                return Err("Event channel closed".into());
            }
            Err(_) => {
                // Timeout, continue loop
            }
        }
    }

    Err("Timeout waiting for NewChannel event".into())
}
