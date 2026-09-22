//! HTTP API implementation for the Nabto WebRTC Signaling Service
//!
//! Handles device connect and ICE servers requests.

#![allow(dead_code)]

use crate::{Error, Result};
use log::{debug, warn};
use serde::{Deserialize, Serialize};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

/// HTTP client for the Nabto WebRTC Signaling Service
#[derive(Clone)]
pub struct HttpApi {
    endpoint_url: String,
    product_id: String,
    device_id: String,
    client: reqwest::Client,
}

#[derive(Debug, Serialize)]
struct ClientConnectRequest {
    #[serde(rename = "deviceId")]
    device_id: String,
    #[serde(rename = "productId")]
    product_id: String,
}

#[derive(Debug, Deserialize)]
pub struct ClientConnectResponse {
    #[serde(rename = "signalingUrl")]
    pub signaling_url: String,
    #[serde(rename = "deviceOnline")]
    pub device_online: Option<bool>,
    #[serde(rename = "channelId")]
    pub channel_id: Option<String>,
    #[serde(rename = "reconnectToken")]
    pub reconnect_token: Option<String>,
}

/// Request body for device connect
#[derive(Debug, Serialize)]
struct DeviceConnectRequest {
    #[serde(rename = "deviceId")]
    device_id: String,
    #[serde(rename = "productId")]
    product_id: String,
}

/// Response from device connect
#[derive(Debug, Deserialize)]
pub struct DeviceConnectResponse {
    #[serde(rename = "signalingUrl")]
    pub signaling_url: String,
}

/// Request body for ICE servers
#[derive(Debug, Serialize)]
struct IceServersRequest {
    #[serde(rename = "deviceId")]
    device_id: String,
    #[serde(rename = "productId")]
    product_id: String,
}

/// ICE server configuration from response
#[derive(Debug, Deserialize, Clone)]
pub struct IceServer {
    pub urls: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub username: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub credential: Option<String>,
}

/// Response from ICE servers request
#[derive(Debug, Deserialize)]
struct IceServersResponse {
    #[serde(rename = "iceServers")]
    ice_servers: Vec<IceServer>,
}

/// HTTP error response body
#[derive(Debug, Deserialize)]
struct ErrorResponse {
    message: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    code: Option<String>,
}

impl HttpApi {
    /// Create a new HTTP API client
    pub fn new(endpoint_url: String, product_id: String, device_id: String) -> Self {
        Self {
            endpoint_url,
            product_id,
            device_id,
            client: reqwest::Client::builder()
                .connect_timeout(std::time::Duration::from_secs(10))
                .timeout(std::time::Duration::from_secs(30))
                .pool_max_idle_per_host(0)
                .build()
                .expect("Failed to build HTTP client"),
        }
    }

    pub async fn client_connect(&self, auth_token: Option<&str>) -> Result<ClientConnectResponse> {
        let url = format!("{}/v1/client/connect", self.endpoint_url);

        let request_body = ClientConnectRequest {
            product_id: self.product_id.clone(),
            device_id: self.device_id.clone(),
        };

        let mut request = self
            .client
            .post(&url)
            .header("Content-Type", "application/json")
            .json(&request_body);

        if let Some(token) = auth_token {
            request = request.header("Authorization", format!("Bearer {}", token));
        }

        debug!("Sending POST {}", url);
        let response = request.send().await.map_err(|e| {
            Error::Connection(format!("Failed to send client connect request: {}", e))
        })?;
        debug!("Received {} from POST {}", response.status(), url);

        self.handle_response(response).await
    }

    /// Make a device connect request
    ///
    /// POST /v1/device/connect
    /// Returns the signaling URL for WebSocket connection
    pub async fn device_connect(&self, access_token: &str) -> Result<DeviceConnectResponse> {
        let url = format!("{}/v1/device/connect", self.endpoint_url);

        let request_body = DeviceConnectRequest {
            device_id: self.device_id.clone(),
            product_id: self.product_id.clone(),
        };

        debug!("Sending POST {}", url);
        let response = self
            .client
            .post(&url)
            .header("Content-Type", "application/json")
            .header("Authorization", format!("Bearer {}", access_token))
            .json(&request_body)
            .send()
            .await
            .map_err(|e| {
                Error::Connection(format!("Failed to send device connect request: {}", e))
            })?;
        debug!("Received {} from POST {}", response.status(), url);

        self.handle_response(response).await
    }

    /// Request ICE servers from the signaling service
    ///
    /// POST /v1/ice-servers
    /// Returns STUN and TURN server configurations
    pub async fn request_ice_servers(&self, access_token: &str) -> Result<Vec<IceServer>> {
        let url = format!("{}/v1/ice-servers", self.endpoint_url);

        let request_body = IceServersRequest {
            device_id: self.device_id.clone(),
            product_id: self.product_id.clone(),
        };

        debug!("Sending POST {}", url);
        let response = self
            .client
            .post(&url)
            .header("Content-Type", "application/json")
            .header("Authorization", format!("Bearer {}", access_token))
            .json(&request_body)
            .send()
            .await
            .map_err(|e| Error::Connection(format!("Failed to send ICE servers request: {}", e)))?;
        debug!("Received {} from POST {}", response.status(), url);

        let ice_response: IceServersResponse = self.handle_response(response).await?;
        Ok(ice_response.ice_servers)
    }

    /// Handle HTTP response with error checking
    async fn handle_response<T: for<'de> Deserialize<'de>>(
        &self,
        response: reqwest::Response,
    ) -> Result<T> {
        let status = response.status();

        if status.is_success() {
            return response
                .json::<T>()
                .await
                .map_err(|e| Error::Other(format!("Failed to parse response: {}", e)));
        }

        // Read Retry-After before the body is consumed.
        let retry_after = parse_retry_after(
            response
                .headers()
                .get(reqwest::header::RETRY_AFTER)
                .and_then(|v| v.to_str().ok()),
        );

        let status = status.as_u16();

        // Try to parse error response
        let error_body = response.json::<ErrorResponse>().await.ok();

        let message = error_body
            .as_ref()
            .map(|e| e.message.clone())
            .unwrap_or_else(|| format!("HTTP error: {}", status));

        // A missing or unparseable Retry-After on a rate limit still means
        // "back off"; match the JS SDK and fall back to 5 minutes rather than
        // letting the caller retry immediately.
        let retry_after = match status {
            429 | 503 => Some(retry_after.unwrap_or(DEFAULT_RETRY_AFTER)),
            _ => None,
        };

        // Keep the two 404 codes as configuration errors: they are permanent
        // and say exactly what is misconfigured.
        if status == 404 {
            if let Some(code) = error_body.and_then(|e| e.code) {
                match code.as_str() {
                    "PRODUCT_ID_NOT_FOUND" => {
                        return Err(Error::Configuration("Product ID not found".to_string()))
                    }
                    "DEVICE_ID_NOT_FOUND" => {
                        return Err(Error::Configuration("Device ID not found".to_string()))
                    }
                    _ => {}
                }
            }
        }

        Err(Error::Http {
            status,
            retry_after,
            message,
        })
    }
}

/// Fallback wait when a rate-limited response carries no usable `Retry-After`.
const DEFAULT_RETRY_AFTER: Duration = Duration::from_secs(300);

/// Parse a `Retry-After` header value.
///
/// The header is either a number of seconds or an HTTP-date (RFC 9110).
/// Returns `None` if the header is absent or cannot be parsed, and for dates
/// already in the past.
fn parse_retry_after(value: Option<&str>) -> Option<Duration> {
    let value = value?.trim();

    if let Ok(seconds) = value.parse::<u64>() {
        return Some(Duration::from_secs(seconds));
    }

    let target = parse_http_date(value).or_else(|| {
        warn!("Could not parse the received Retry-After header: {}", value);
        None
    })?;

    let now = SystemTime::now().duration_since(UNIX_EPOCH).ok()?.as_secs();

    // A date in the past means "retry now".
    Some(Duration::from_secs(target.saturating_sub(now)))
}

/// Parse an IMF-fixdate (`Sun, 06 Nov 1994 08:49:37 GMT`) into a unix
/// timestamp.
///
/// This is the only date format a conforming server may send in `Retry-After`,
/// so the two obsolete formats in RFC 9110 are not accepted. Implemented here
/// rather than pulling in a date crate: the SDK targets embedded devices and
/// this is the only date it ever parses.
///
/// Every field is required to be exactly the width RFC 9110 gives it. That is
/// not pedantry: the value comes off the network, and an unbounded year would
/// overflow the arithmetic in [`days_from_civil`] rather than being rejected.
fn parse_http_date(value: &str) -> Option<u64> {
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];

    /// Parse a field of exactly `width` ASCII digits.
    ///
    /// `str::parse` alone would accept `+1`, `1_0`, unicode digits and a year
    /// of any length.
    fn digits(value: &str, width: usize) -> Option<u64> {
        if value.len() != width || !value.bytes().all(|b| b.is_ascii_digit()) {
            return None;
        }
        value.parse().ok()
    }

    // "Sun, 06 Nov 1994 08:49:37 GMT"
    let rest = value.split_once(", ")?.1;
    let mut parts = rest.split(' ');
    let day = digits(parts.next()?, 2)?;
    let month = parts.next()?;
    let year = digits(parts.next()?, 4)?;
    let time = parts.next()?;
    if parts.next()? != "GMT" || parts.next().is_some() {
        return None;
    }

    let month = MONTHS.iter().position(|m| *m == month)? as u64 + 1;

    let mut hms = time.split(':');
    let hour = digits(hms.next()?, 2)?;
    let minute = digits(hms.next()?, 2)?;
    let second = digits(hms.next()?, 2)?;
    if hms.next().is_some() || hour > 23 || minute > 59 || second > 60 || day == 0 || day > 31 {
        return None;
    }

    // Dates before the epoch are already in the past; the caller treats those
    // the same as "retry now".
    if year < 1970 {
        return Some(0);
    }

    // The four-digit year bounds this well inside u64, but keep the arithmetic
    // total so a future change to the parsing cannot turn into a panic.
    days_from_civil(year, month, day)
        .checked_mul(86400)?
        .checked_add(hour * 3600 + minute * 60 + second)
}

/// Days since the unix epoch for a Gregorian date at or after 1970-01-01.
///
/// Howard Hinnant's `days_from_civil`, which shifts the year to start in March
/// so the leap day lands at the end and the leap year rules fall out without
/// branching on February.
fn days_from_civil(year: u64, month: u64, day: u64) -> u64 {
    let year = if month <= 2 { year - 1 } else { year };
    let era = year / 400;
    let year_of_era = year - era * 400;
    let day_of_year = (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;
    (era * 146097 + day_of_era).saturating_sub(719468)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_http_api_creation() {
        let api = HttpApi::new(
            "https://example.nabto.net".to_string(),
            "wp-test".to_string(),
            "wd-test".to_string(),
        );
        assert_eq!(api.endpoint_url, "https://example.nabto.net");
        assert_eq!(api.product_id, "wp-test");
        assert_eq!(api.device_id, "wd-test");
    }

    #[test]
    fn test_retry_after_seconds() {
        assert_eq!(
            parse_retry_after(Some("120")),
            Some(Duration::from_secs(120))
        );
        assert_eq!(
            parse_retry_after(Some("  30  ")),
            Some(Duration::from_secs(30))
        );
        assert_eq!(parse_retry_after(Some("0")), Some(Duration::ZERO));
    }

    #[test]
    fn test_retry_after_absent_or_garbage() {
        assert_eq!(parse_retry_after(None), None);
        assert_eq!(parse_retry_after(Some("")), None);
        assert_eq!(parse_retry_after(Some("soon")), None);
        assert_eq!(parse_retry_after(Some("-5")), None);
        // Obsolete RFC 9110 date formats are not accepted.
        assert_eq!(
            parse_retry_after(Some("Sunday, 06-Nov-94 08:49:37 GMT")),
            None
        );
    }

    #[test]
    fn test_retry_after_http_date() {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        // A date well in the past yields a zero wait, not an underflow.
        assert_eq!(
            parse_retry_after(Some("Sun, 06 Nov 1994 08:49:37 GMT")),
            Some(Duration::ZERO)
        );

        // A date in the future yields roughly the remaining time.
        let future = parse_retry_after(Some("Wed, 21 Oct 2099 07:28:00 GMT")).unwrap();
        let expected = 4096250880u64.saturating_sub(now);
        assert!(
            future.as_secs().abs_diff(expected) <= 2,
            "got {}s, expected about {}s",
            future.as_secs(),
            expected
        );
    }

    #[test]
    fn test_http_date_reference_values() {
        // The RFC 9110 example date.
        assert_eq!(
            parse_http_date("Sun, 06 Nov 1994 08:49:37 GMT"),
            Some(784111777)
        );
        // The epoch itself.
        assert_eq!(parse_http_date("Thu, 01 Jan 1970 00:00:00 GMT"), Some(0));
        // Leap day, to exercise the March-based year shift.
        assert_eq!(
            parse_http_date("Mon, 29 Feb 2016 12:00:00 GMT"),
            Some(1456747200)
        );
        // Day after a century non-leap year boundary.
        assert_eq!(
            parse_http_date("Wed, 01 Mar 2000 00:00:00 GMT"),
            Some(951868800)
        );

        assert_eq!(parse_http_date("Sun, 06 Nov 1994 08:49:37 UTC"), None);
        assert_eq!(parse_http_date("Sun, 06 Foo 1994 08:49:37 GMT"), None);
        assert_eq!(parse_http_date("Sun, 06 Nov 1994 25:49:37 GMT"), None);
        assert_eq!(parse_http_date("Sun, 32 Nov 1994 08:49:37 GMT"), None);
        assert_eq!(parse_http_date("garbage"), None);
    }

    /// The year comes off the network, so an out-of-range one must be rejected
    /// rather than overflowing the day arithmetic.
    #[test]
    fn test_http_date_rejects_out_of_range_year() {
        // 19 digits: parses as u64, overflows days_from_civil if not rejected.
        assert_eq!(
            parse_http_date("Sun, 06 Nov 1844674407370955161 08:49:37 GMT"),
            None
        );
        // 20 digits: beyond u64 entirely.
        assert_eq!(
            parse_http_date("Sun, 06 Nov 99999999999999999999 08:49:37 GMT"),
            None
        );
        // Anything that is not exactly four digits.
        assert_eq!(parse_http_date("Sun, 06 Nov 99999 08:49:37 GMT"), None);
        assert_eq!(parse_http_date("Sun, 06 Nov 199 08:49:37 GMT"), None);
        assert_eq!(parse_http_date("Sun, 06 Nov  1994 08:49:37 GMT"), None);
    }

    /// Every field is fixed width, and `str::parse` alone would not enforce it.
    #[test]
    fn test_http_date_rejects_malformed_fields() {
        assert_eq!(parse_http_date("Sun, 6 Nov 1994 08:49:37 GMT"), None);
        assert_eq!(parse_http_date("Sun, 06 Nov 1994 8:49:37 GMT"), None);
        assert_eq!(parse_http_date("Sun, +6 Nov 1994 08:49:37 GMT"), None);
        assert_eq!(parse_http_date("Sun, 06 Nov +994 08:49:37 GMT"), None);
        assert_eq!(parse_http_date("Sun, 06 Nov 1994 08:49:37 GMT extra"), None);
        // Still accepts the canonical form.
        assert_eq!(
            parse_http_date("Sun, 06 Nov 1994 08:49:37 GMT"),
            Some(784111777)
        );
    }

    /// The whole point of the header: a hostile or buggy value must not take
    /// the process down.
    #[test]
    fn test_retry_after_never_panics() {
        for value in [
            "Sun, 06 Nov 1844674407370955161 08:49:37 GMT",
            "Sun, 06 Nov 9999999999999999999999 99:99:99 GMT",
            "Mon, 99 Zzz 0000 00:00:00 GMT",
            ", , , ",
            "18446744073709551615",
            "-1",
            "",
            "   ",
        ] {
            let _ = parse_retry_after(Some(value));
        }
    }

    /// Serve one canned HTTP response and return the endpoint url.
    async fn serve_once(
        status_line: &'static str,
        headers: &'static str,
        body: &'static str,
    ) -> String {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();

        tokio::spawn(async move {
            let (mut stream, _) = listener.accept().await.unwrap();
            // Read the request headers so the client is not writing into a
            // closed socket.
            let mut buf = [0u8; 4096];
            let _ = tokio::io::AsyncReadExt::read(&mut stream, &mut buf).await;

            let response = format!(
                "HTTP/1.1 {}\r\n{}Content-Length: {}\r\nConnection: close\r\n\r\n{}",
                status_line,
                headers,
                body.len(),
                body
            );
            let _ = tokio::io::AsyncWriteExt::write_all(&mut stream, response.as_bytes()).await;
            let _ = tokio::io::AsyncWriteExt::shutdown(&mut stream).await;
        });

        format!("http://{}", addr)
    }

    async fn client_connect_error(
        status_line: &'static str,
        headers: &'static str,
        body: &'static str,
    ) -> Error {
        let endpoint = serve_once(status_line, headers, body).await;
        let api = HttpApi::new(endpoint, "wp-test".to_string(), "wd-test".to_string());
        api.client_connect(None)
            .await
            .expect_err("a non-2xx response should be an error")
    }

    /// The header has to be read before the body is consumed; this is what
    /// catches getting that ordering wrong.
    #[tokio::test]
    async fn test_429_carries_status_and_retry_after() {
        let err = client_connect_error(
            "429 Too Many Requests",
            "Retry-After: 120\r\nContent-Type: application/json\r\n",
            r#"{"message":"slow down"}"#,
        )
        .await;

        assert_eq!(err.status(), Some(429));
        assert_eq!(err.retry_after(), Some(Duration::from_secs(120)));
        assert!(err.to_string().contains("slow down"), "got: {}", err);
    }

    #[tokio::test]
    async fn test_429_without_retry_after_falls_back() {
        let err = client_connect_error(
            "429 Too Many Requests",
            "Content-Type: application/json\r\n",
            r#"{"message":"slow down"}"#,
        )
        .await;

        assert_eq!(err.status(), Some(429));
        assert_eq!(err.retry_after(), Some(DEFAULT_RETRY_AFTER));
    }

    #[tokio::test]
    async fn test_503_carries_retry_after() {
        let err = client_connect_error(
            "503 Service Unavailable",
            "Retry-After: 30\r\nContent-Type: application/json\r\n",
            r#"{"message":"maintenance"}"#,
        )
        .await;

        assert_eq!(err.status(), Some(503));
        assert_eq!(err.retry_after(), Some(Duration::from_secs(30)));
    }

    /// Other statuses keep the code but must not invent a retry delay.
    #[tokio::test]
    async fn test_other_status_has_no_retry_after() {
        let err = client_connect_error(
            "401 Unauthorized",
            "Content-Type: application/json\r\n",
            r#"{"message":"bad kid"}"#,
        )
        .await;

        assert_eq!(err.status(), Some(401));
        assert_eq!(err.retry_after(), None);
        assert!(err.to_string().contains("bad kid"), "got: {}", err);
    }

    /// The two permanent 404 codes stay configuration errors.
    #[tokio::test]
    async fn test_product_id_not_found_stays_configuration_error() {
        let err = client_connect_error(
            "404 Not Found",
            "Content-Type: application/json\r\n",
            r#"{"message":"nope","code":"PRODUCT_ID_NOT_FOUND"}"#,
        )
        .await;

        assert!(
            matches!(err, Error::Configuration(_)),
            "expected Configuration, got {:?}",
            err
        );
        assert_eq!(err.status(), None);
    }
}
