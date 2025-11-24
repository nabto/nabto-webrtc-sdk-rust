use nabto_webrtc_sdk::client::{SignalingClient, SignalingClientOptions, SignalingClientEvent};
use nabto_webrtc_sdk::common::SignalingConnectionState;
use nabto_webrtc_sdk::util::{ClientSecurityMode, ClientMessageTransport};

#[tokio::main]
async fn main() {
    let options = SignalingClientOptions {
        product_id: "wp-ooraxfzr".to_string(),
        device_id: "wd-qpjx37pf9utuzwbq".to_string(),
        access_token: None,
        endpoint_url: None,
        require_online: None
    };

    let (mut client, mut event_rx) = SignalingClient::new(options).await.unwrap();

    let security_mode = ClientSecurityMode::SharedSecret {
        shared_secret: "foo".to_string(),
        key_id: None
    };

    let message_transport = ClientMessageTransport::new(&mut client, security_mode);

    let client_task = tokio::spawn(async move {
        if let Err(e) = client.run().await {
            eprintln!("Error: {:?}", e);
        }
    });

    tokio::spawn(async move {
        loop {
            let event = event_rx.recv().await.unwrap();
            match event {
                SignalingClientEvent::Message(value) => {
                    println!("SignalingClientEvent::Message");
                },

                SignalingClientEvent::ConnectionReconnect => {
                    println!("SignalingClientEvent::ConnectionReconnect");
                },

                SignalingClientEvent::ConnectionStateChange(signaling_connection_state) => {
                    println!("SignalingClientEvent::ConnectionStateChange: {:?}", signaling_connection_state);
                    if signaling_connection_state == SignalingConnectionState::Connected {
                        // @TODO: Check this error
                        let err = message_transport.start().await;
                    }
                },

                SignalingClientEvent::ChannelStateChange(signaling_channel_state) => {
                    println!("SignalingClientEvent::ChannelStateChange {:?}", signaling_channel_state);
                },

                SignalingClientEvent::Error => {
                    println!("SignalingClientEvent::Error");
                },
            }
        }
    });

    client_task.await;
}