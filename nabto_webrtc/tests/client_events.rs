//! Integration tests for the `SignalingClient` event API.
//!
//! These cover the events a client hands back from [`SignalingClient::new`],
//! and the options that affect whether `new` succeeds at all. The mock server
//! plays the device.
//!
//! Mirrors `client_conect.test.ts` in the JS SDK.

mod common;

use common::init_logger;
use common::test_client::{ClientTestOptions, TestClient};
use nabto_webrtc::client::{SignalingClient, SignalingClientEvent, SignalingClientOptions};
use nabto_webrtc::{Error, SignalingChannelState, SignalingConnectionState};
use serde_json::json;
use std::time::Duration;
use tokio::sync::mpsc;

const EVENT_TIMEOUT: Duration = Duration::from_secs(10);

/// Wait for the first event matching `predicate`, failing the test on timeout.
async fn wait_for_event<T>(
    event_rx: &mut mpsc::Receiver<SignalingClientEvent>,
    what: &str,
    mut predicate: impl FnMut(SignalingClientEvent) -> Option<T>,
) -> T {
    let deadline = tokio::time::Instant::now() + EVENT_TIMEOUT;
    loop {
        let event = tokio::time::timeout_at(deadline, event_rx.recv())
            .await
            .unwrap_or_else(|_| panic!("timed out waiting for {}", what))
            .unwrap_or_else(|| panic!("event channel closed while waiting for {}", what));

        if let Some(value) = predicate(event) {
            return value;
        }
    }
}

/// Start a client against a test instance and run it in the background.
async fn start_client(
    test: &common::test_client::TestClientResponse,
    access_token: Option<String>,
    require_online: bool,
) -> Result<
    (
        nabto_webrtc::common::channel::ChannelHandle,
        mpsc::Receiver<SignalingClientEvent>,
        tokio::task::JoinHandle<()>,
    ),
    Error,
> {
    let mut builder =
        SignalingClientOptions::builder(test.product_id.clone(), test.device_id.clone())
            .endpoint_url(test.endpoint_url.clone())
            .require_online(require_online);

    if let Some(token) = access_token {
        builder = builder.access_token(token);
    }

    let (mut client, event_rx) = SignalingClient::new(builder.build()).await?;
    let handle = client.channel_handle.clone();

    let task = tokio::spawn(async move {
        let _ = client.run().await;
    });

    Ok((handle, event_rx, task))
}

/// The bug from the 2026-09-22 report: a message from the device must reach the
/// application as `SignalingClientEvent::Message`.
#[tokio::test]
#[ignore]
async fn test_client_receives_message_event() {
    init_logger();
    let api = TestClient::new();
    let test = api
        .create_client_test(ClientTestOptions::default())
        .await
        .expect("create client test");

    api.client_test_connect_device(&test.test_id)
        .await
        .expect("connect device");

    let (handle, mut event_rx, task) = start_client(&test, None, false).await.expect("new client");

    wait_for_event(&mut event_rx, "Connected", |event| match event {
        SignalingClientEvent::ConnectionStateChange(SignalingConnectionState::Connected) => {
            Some(())
        }
        _ => None,
    })
    .await;

    // Send a message so the device has a channel to answer on, and confirm it
    // arrived: if this fails the test is broken, not the receive path.
    handle
        .send_message(json!({ "type": "SETUP_REQUEST" }))
        .await
        .expect("send message");

    api.client_test_wait_for_device_messages(
        &test.test_id,
        vec![json!({ "type": "SETUP_REQUEST" })],
        5000,
    )
    .await
    .expect("device should receive the client's message");

    api.client_test_send_device_messages(&test.test_id, vec![json!({ "type": "SETUP_RESPONSE" })])
        .await
        .expect("device sends reply");

    let message = wait_for_event(&mut event_rx, "Message", |event| match event {
        SignalingClientEvent::Message(message) => Some(message),
        _ => None,
    })
    .await;

    assert_eq!(message, json!({ "type": "SETUP_RESPONSE" }));

    task.abort();
    api.delete_client_test(&test.test_id).await.ok();
}

/// Several messages in order, to show the forwarder is not a one-shot.
#[tokio::test]
#[ignore]
async fn test_client_receives_multiple_messages_in_order() {
    init_logger();
    let api = TestClient::new();
    let test = api
        .create_client_test(ClientTestOptions::default())
        .await
        .expect("create client test");

    api.client_test_connect_device(&test.test_id)
        .await
        .expect("connect device");

    let (handle, mut event_rx, task) = start_client(&test, None, false).await.expect("new client");

    wait_for_event(&mut event_rx, "Connected", |event| match event {
        SignalingClientEvent::ConnectionStateChange(SignalingConnectionState::Connected) => {
            Some(())
        }
        _ => None,
    })
    .await;

    handle
        .send_message(json!({ "type": "SETUP_REQUEST" }))
        .await
        .expect("send message");

    let sent: Vec<serde_json::Value> = (0..5).map(|i| json!({ "seq": i })).collect();
    api.client_test_send_device_messages(&test.test_id, sent.clone())
        .await
        .expect("device sends replies");

    let mut received = Vec::new();
    while received.len() < sent.len() {
        let message = wait_for_event(&mut event_rx, "Message", |event| match event {
            SignalingClientEvent::Message(message) => Some(message),
            _ => None,
        })
        .await;
        received.push(message);
    }

    assert_eq!(received, sent);

    task.abort();
    api.delete_client_test(&test.test_id).await.ok();
}

/// `ChannelStateChange` must be emitted, not just tracked internally.
#[tokio::test]
#[ignore]
async fn test_client_emits_channel_state_change() {
    init_logger();
    let api = TestClient::new();
    let test = api
        .create_client_test(ClientTestOptions::default())
        .await
        .expect("create client test");

    api.client_test_connect_device(&test.test_id)
        .await
        .expect("connect device");

    let (_handle, mut event_rx, task) = start_client(&test, None, false).await.expect("new client");

    let state = wait_for_event(&mut event_rx, "ChannelStateChange", |event| match event {
        SignalingClientEvent::ChannelStateChange(state) => Some(state),
        _ => None,
    })
    .await;

    assert_eq!(state, SignalingChannelState::Connected);

    task.abort();
    api.delete_client_test(&test.test_id).await.ok();
}

/// A channel error from the device must reach the application.
#[tokio::test]
#[ignore]
async fn test_client_emits_error_event() {
    init_logger();
    let api = TestClient::new();
    let test = api
        .create_client_test(ClientTestOptions::default())
        .await
        .expect("create client test");

    api.client_test_connect_device(&test.test_id)
        .await
        .expect("connect device");

    let (handle, mut event_rx, task) = start_client(&test, None, false).await.expect("new client");

    wait_for_event(&mut event_rx, "Connected", |event| match event {
        SignalingClientEvent::ConnectionStateChange(SignalingConnectionState::Connected) => {
            Some(())
        }
        _ => None,
    })
    .await;

    handle
        .send_message(json!({ "type": "SETUP_REQUEST" }))
        .await
        .expect("send message");

    api.client_test_send_device_error(&test.test_id, "TEST_ERROR", "something went wrong")
        .await
        .expect("device sends error");

    let error = wait_for_event(&mut event_rx, "Error", |event| match event {
        SignalingClientEvent::Error(error) => Some(error),
        _ => None,
    })
    .await;

    assert!(
        error.to_string().contains("TEST_ERROR"),
        "error should carry the code from the device, got: {}",
        error
    );

    task.abort();
    api.delete_client_test(&test.test_id).await.ok();
}

/// `require_online(true)` must fail when the device is offline.
///
/// Mirrors "Test connection to a device which is offline but is required to be
/// online" in the JS suite.
#[tokio::test]
#[ignore]
async fn test_require_online_fails_when_device_offline() {
    init_logger();
    let api = TestClient::new();
    let test = api
        .create_client_test(ClientTestOptions::default())
        .await
        .expect("create client test");

    // Deliberately do not connect the device.
    let result = start_client(&test, None, true).await;

    match result {
        Err(Error::DeviceOffline) => {}
        Err(other) => panic!("expected Error::DeviceOffline, got {:?}", other),
        Ok(_) => panic!("expected new() to fail when the device is offline"),
    }

    api.delete_client_test(&test.test_id).await.ok();
}

/// ...and must not fail when the device is online.
#[tokio::test]
#[ignore]
async fn test_require_online_succeeds_when_device_online() {
    init_logger();
    let api = TestClient::new();
    let test = api
        .create_client_test(ClientTestOptions::default())
        .await
        .expect("create client test");

    api.client_test_connect_device(&test.test_id)
        .await
        .expect("connect device");

    let (_handle, mut event_rx, task) = start_client(&test, None, true)
        .await
        .expect("new() should succeed when the device is online");

    wait_for_event(&mut event_rx, "Connected", |event| match event {
        SignalingClientEvent::ConnectionStateChange(SignalingConnectionState::Connected) => {
            Some(())
        }
        _ => None,
    })
    .await;

    task.abort();
    api.delete_client_test(&test.test_id).await.ok();
}

/// Without `require_online`, connecting to an offline device still succeeds.
#[tokio::test]
#[ignore]
async fn test_offline_device_is_allowed_by_default() {
    init_logger();
    let api = TestClient::new();
    let test = api
        .create_client_test(ClientTestOptions::default())
        .await
        .expect("create client test");

    let (_handle, _event_rx, task) = start_client(&test, None, false)
        .await
        .expect("new() should succeed when require_online is not set");

    task.abort();
    api.delete_client_test(&test.test_id).await.ok();
}

/// The configured access token must actually be sent on the connect request.
#[tokio::test]
#[ignore]
async fn test_access_token_is_used() {
    init_logger();
    let api = TestClient::new();
    let test = api
        .create_client_test(ClientTestOptions {
            require_access_token: Some(true),
            ..Default::default()
        })
        .await
        .expect("create client test");

    // Without the token the service rejects the connect request.
    let missing = start_client(&test, None, false).await;
    assert!(
        missing.is_err(),
        "connect should fail when the service requires an access token and none is given"
    );

    let (_handle, _event_rx, task) = start_client(&test, Some(test.access_token.clone()), false)
        .await
        .expect("connect should succeed once the access token is configured");

    task.abort();
    api.delete_client_test(&test.test_id).await.ok();
}
