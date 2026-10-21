//! TLS configuration shared by the HTTP and WebSocket connections
//!
//! Both trust the bundled webpki-roots instead of the OS trust store, and use
//! ring explicitly. rustls only picks a provider automatically when exactly
//! one is enabled, and Cargo features are unified across the dependency
//! graph, so an application that enables aws-lc-rs would otherwise make
//! `ClientConfig::builder()` panic.

use std::sync::Arc;

use tokio::net::TcpStream;
use tokio_tungstenite::{
    connect_async_tls_with_config, tungstenite, Connector, MaybeTlsStream, WebSocketStream,
};

/// rustls client config with the bundled webpki-roots and the ring provider
pub(crate) fn client_config() -> rustls::ClientConfig {
    let roots = rustls::RootCertStore {
        roots: webpki_roots::TLS_SERVER_ROOTS.to_vec(),
    };
    rustls::ClientConfig::builder_with_provider(Arc::new(rustls::crypto::ring::default_provider()))
        .with_safe_default_protocol_versions()
        .expect("ring supports the default TLS protocol versions")
        .with_root_certificates(roots)
        .with_no_client_auth()
}

/// `connect_async` using [`client_config`] for `wss://` URLs
pub(crate) async fn connect_websocket(
    url: &str,
) -> Result<
    (
        WebSocketStream<MaybeTlsStream<TcpStream>>,
        tungstenite::handshake::client::Response,
    ),
    tungstenite::Error,
> {
    let connector = Connector::Rustls(Arc::new(client_config()));
    connect_async_tls_with_config(url, None, false, Some(connector)).await
}
