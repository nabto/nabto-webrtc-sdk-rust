use nabto_webrtc_sdk::client::{SignalingClient, SignalingClientOptions};

#[tokio::main]
async fn main() {
    let options = SignalingClientOptions {
        product_id: "wp-ooraxfzr".to_string(),
        device_id: "wd-qpjx37pf9utuzwbq".to_string(),
        access_token: None,
        endpoint_url: None,
        require_online: None
    };

    let mut client = SignalingClient::new(options);

    let client_task = tokio::spawn(async move {
        if let Err(e) = client.run().await {
            eprintln!("Error: {:?}", e);
        }
    });

    client_task.await;
}