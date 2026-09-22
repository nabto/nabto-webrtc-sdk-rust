//! Head-of-Line Blocking Integration Tests
//!
//! These tests verify that the per-channel actor architecture eliminates
//! cross-channel head-of-line blocking. Each channel runs in its own tokio
//! task, so a slow or blocked channel cannot delay message delivery on
//! other channels.
//!
//! To run these tests:
//! 1. Start the integration test server:
//!    cd ~/sandbox/nabto-webrtc-sdk-js/integration_test_server && bun dev
//! 2. Run the tests:
//!    cargo test --test hol_blocking -- --ignored --test-threads=1

mod common;

use common::{DeviceTestInstance, DeviceTestOptions};
use nabto_webrtc::common::SignalingConnectionState;
use nabto_webrtc::device::DeviceEvent;
use serde_json::Value as JsonValue;
use std::collections::HashMap;
use std::time::Duration;
use tokio::sync::mpsc;

fn init_logger() {
    let _ = env_logger::builder().is_test(true).try_init();
}

/// Helper: wait for N NewChannel events and return (channel_id → message_rx) map.
async fn collect_channels(
    event_rx: &mut mpsc::Receiver<DeviceEvent>,
    count: usize,
) -> HashMap<String, mpsc::Receiver<JsonValue>> {
    let mut result = HashMap::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);

    while result.len() < count {
        match tokio::time::timeout_at(deadline, event_rx.recv()).await {
            Ok(Some(DeviceEvent::NewChannel {
                handle, message_rx, ..
            })) => {
                result.insert(handle.channel_id().to_string(), message_rx);
            }
            Ok(Some(DeviceEvent::StateChanged { .. })) => continue,
            Ok(None) => panic!("Event channel closed before receiving {count} NewChannel events"),
            Err(_) => panic!(
                "Timeout waiting for NewChannel events (got {}/{})",
                result.len(),
                count
            ),
        }
    }
    result
}

/// Helper: wait for N NewChannel events and key them by each client's first
/// message, consuming that message.
///
/// [`collect_channels`] keys by channel id, which a test has no way to map back
/// to the client that caused it. `HashMap` iteration order is arbitrary, so
/// indexing into the keys picks a channel at random: a test that floods client
/// A and then asserts on "the other" receiver silently asserts on A's own
/// receiver about half the time, and passes without testing anything. Each
/// client sends a distinct first message, so use that as the key instead.
async fn collect_channels_by_label(
    event_rx: &mut mpsc::Receiver<DeviceEvent>,
    count: usize,
) -> HashMap<String, mpsc::Receiver<JsonValue>> {
    let channels = collect_channels(event_rx, count).await;
    let mut result = HashMap::new();

    for (channel_id, mut message_rx) in channels {
        let init = tokio::time::timeout(Duration::from_secs(5), message_rx.recv())
            .await
            .unwrap_or_else(|_| {
                panic!("Timed out waiting for the initial message on channel {channel_id}")
            })
            .unwrap_or_else(|| panic!("Channel {channel_id} closed before its initial message"));

        let label = init
            .as_str()
            .unwrap_or_else(|| panic!("Expected a string initial message, got {init}"))
            .to_string();

        if let Some(previous) = result.insert(label.clone(), message_rx) {
            drop(previous);
            panic!("Two channels reported the same initial message {label:?}");
        }
    }

    result
}

/// Take the receiver a client's initial message identified.
fn take_labelled(
    channels: &mut HashMap<String, mpsc::Receiver<JsonValue>>,
    label: &str,
) -> mpsc::Receiver<JsonValue> {
    channels
        .remove(label)
        .unwrap_or_else(|| panic!("No channel delivered the initial message {label:?}"))
}

/// HOL Blocking Test 1:
/// Channel B receives messages promptly even when channel A has a high
/// volume of messages. Both channels process independently.
#[tokio::test]
#[ignore] // Requires integration test server
async fn test_no_hol_blocking_high_volume_on_one_channel() {
    init_logger();

    let test = DeviceTestInstance::create(DeviceTestOptions::default())
        .await
        .expect("Failed to create test instance");
    let (device, mut event_rx) = test.start_signaling_device();

    device
        .wait_for_state(SignalingConnectionState::Connected, Duration::from_secs(5))
        .await
        .expect("Device did not connect");

    // Create two clients
    let client_a = test.create_client().await.unwrap();
    let client_b = test.create_client().await.unwrap();
    test.connect_client(&client_a).await.unwrap();
    test.connect_client(&client_b).await.unwrap();

    // Trigger channel creation with initial messages
    test.client_send_messages(&client_a, vec!["init_a".to_string()])
        .await
        .unwrap();
    test.client_send_messages(&client_b, vec!["init_b".to_string()])
        .await
        .unwrap();

    // Wait for both NewChannel events. Identifying the receivers by their
    // initial message is what ties each one to the client that will be flooded.
    let mut channels = collect_channels_by_label(&mut event_rx, 2).await;
    let mut msg_rx_a = take_labelled(&mut channels, "init_a");
    let mut msg_rx_b = take_labelled(&mut channels, "init_b");

    // Flood channel A with many messages. Because client_send_messages sends
    // via HTTP to the test server, these get forwarded through the WebSocket
    // to the device.
    let flood: Vec<String> = (1..=50).map(|i| format!("flood_{}", i)).collect();
    test.client_send_messages(&client_a, flood).await.unwrap();

    // Send a single message on channel B
    test.client_send_messages(&client_b, vec!["important".to_string()])
        .await
        .unwrap();

    // Channel B should receive its message within a reasonable timeout.
    // With per-channel actors, channel B's actor processes independently of
    // channel A's high volume.
    let msg = tokio::time::timeout(Duration::from_secs(5), msg_rx_b.recv())
        .await
        .expect("Channel B should receive message without HOL blocking")
        .unwrap();

    // Channel B's own message got through, not one of A's flood.
    assert_eq!(msg, JsonValue::String("important".to_string()));
    println!("Channel B received: {:?}", msg);

    // Channel A should also eventually deliver its messages
    let msg = tokio::time::timeout(Duration::from_secs(5), msg_rx_a.recv())
        .await
        .expect("Channel A should eventually receive messages")
        .unwrap();
    println!("Channel A received: {:?}", msg);

    device.stop().await;
    test.destroy().await.unwrap();
}

/// HOL Blocking Test 2:
/// When one channel's message_rx is not consumed (simulating a slow
/// application consumer), messages on other channels are unaffected.
/// The per-channel actor uses try_send for message delivery, so a full
/// buffer on one channel doesn't block the actor from processing other
/// channels' messages.
#[tokio::test]
#[ignore] // Requires integration test server
async fn test_unconsumed_channel_does_not_block_others() {
    init_logger();

    let test = DeviceTestInstance::create(DeviceTestOptions::default())
        .await
        .expect("Failed to create test instance");
    let (device, mut event_rx) = test.start_signaling_device();

    device
        .wait_for_state(SignalingConnectionState::Connected, Duration::from_secs(5))
        .await
        .expect("Device did not connect");

    let client_a = test.create_client().await.unwrap();
    let client_b = test.create_client().await.unwrap();
    test.connect_client(&client_a).await.unwrap();
    test.connect_client(&client_b).await.unwrap();

    test.client_send_messages(&client_a, vec!["init_a".to_string()])
        .await
        .unwrap();
    test.client_send_messages(&client_b, vec!["init_b".to_string()])
        .await
        .unwrap();

    let mut channels = collect_channels_by_label(&mut event_rx, 2).await;

    // Deliberately drop channel A's message_rx, simulating an application that
    // never reads from this channel. This has to be A's receiver specifically:
    // A is the channel that gets flooded below.
    let dropped_rx = take_labelled(&mut channels, "init_a");
    let mut msg_rx_b = take_labelled(&mut channels, "init_b");
    drop(dropped_rx);

    // Send many messages on channel A (they'll fill the buffer and be dropped)
    let flood: Vec<String> = (1..=50).map(|i| format!("flood_{}", i)).collect();
    test.client_send_messages(&client_a, flood).await.unwrap();

    // Give time for channel A's messages to be processed
    tokio::time::sleep(Duration::from_millis(200)).await;

    // Send a message on channel B
    test.client_send_messages(&client_b, vec!["still_works".to_string()])
        .await
        .unwrap();

    // Channel B should receive its message even though channel A's buffer is full
    let msg = tokio::time::timeout(Duration::from_secs(5), msg_rx_b.recv())
        .await
        .expect("Channel B should receive message even when channel A is unconsumed")
        .unwrap();

    // It must be B's own message: receiving one of A's flood messages here
    // would mean the test picked the wrong receiver.
    assert_eq!(msg, JsonValue::String("still_works".to_string()));
    println!("Channel B received: {:?}", msg);

    device.stop().await;
    test.destroy().await.unwrap();
}

/// HOL Blocking Test 3:
/// Both channels receive interleaved messages concurrently. Verifies
/// that messages sent to different channels are all delivered.
#[tokio::test]
#[ignore] // Requires integration test server
async fn test_concurrent_interleaved_message_delivery() {
    init_logger();

    let test = DeviceTestInstance::create(DeviceTestOptions::default())
        .await
        .expect("Failed to create test instance");
    let (device, mut event_rx) = test.start_signaling_device();

    device
        .wait_for_state(SignalingConnectionState::Connected, Duration::from_secs(5))
        .await
        .expect("Device did not connect");

    let client_a = test.create_client().await.unwrap();
    let client_b = test.create_client().await.unwrap();
    test.connect_client(&client_a).await.unwrap();
    test.connect_client(&client_b).await.unwrap();

    test.client_send_messages(&client_a, vec!["init_a".to_string()])
        .await
        .unwrap();
    test.client_send_messages(&client_b, vec!["init_b".to_string()])
        .await
        .unwrap();

    let mut channels = collect_channels_by_label(&mut event_rx, 2).await;
    let mut msg_rx_a = take_labelled(&mut channels, "init_a");
    let mut msg_rx_b = take_labelled(&mut channels, "init_b");

    // Send messages on both channels in an interleaved pattern
    let n = 10;
    for i in 1..=n {
        test.client_send_messages(&client_a, vec![format!("a_{}", i)])
            .await
            .unwrap();
        test.client_send_messages(&client_b, vec![format!("b_{}", i)])
            .await
            .unwrap();
    }

    // Both channels should receive all N messages, and each channel should see
    // only its own: now that the receivers are identified, cross-delivery is
    // detectable rather than just being counted.
    let mut a_count = 0;
    let mut b_count = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);

    while a_count < n || b_count < n {
        tokio::select! {
            msg = msg_rx_a.recv() => {
                let msg = msg.expect("Channel closed unexpectedly");
                let msg = msg.as_str().expect("Expected a string message");
                assert!(
                    msg.starts_with("a_"),
                    "Channel A received {msg:?}, which was sent to channel B"
                );
                a_count += 1;
            }
            msg = msg_rx_b.recv() => {
                let msg = msg.expect("Channel closed unexpectedly");
                let msg = msg.as_str().expect("Expected a string message");
                assert!(
                    msg.starts_with("b_"),
                    "Channel B received {msg:?}, which was sent to channel A"
                );
                b_count += 1;
            }
            _ = tokio::time::sleep_until(deadline) => {
                panic!(
                    "Timeout: received {}/{} on channel A, {}/{} on channel B",
                    a_count, n, b_count, n
                );
            }
        }
    }

    println!("Both channels received all {} of their own messages", n);

    device.stop().await;
    test.destroy().await.unwrap();
}
