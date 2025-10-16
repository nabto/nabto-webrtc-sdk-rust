//! Nabto WebRTC SDK for Rust
//!
//! This library provides a Rust interface for WebRTC signaling using the Nabto platform.
//!
//! # Module Structure
//!
//! - `device`: Core device-side signaling implementation
//! - `util`: Message transport utilities built on top of device
//! - `error`: Error types used throughout the SDK
//!
//! # Example
//!
//! ```no_run
//! use nabto_webrtc_sdk::device::{SignalingDevice, SignalingDeviceOptions};
//! use nabto_webrtc_sdk::util::{DeviceMessageTransport, SecurityMode};
//!
//! # async fn example() {
//! // Create a device...
//! # }
//! ```

pub mod device;
pub mod error;
pub mod types;
pub mod util;

pub use error::{Error, Result};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lib_imports() {
        // Verify that core types are accessible
        let _: Option<Error> = None;
        let _: Option<device::ConnectionState> = None;
    }
}
