//! Nabto WebRTC SDK for Rust
//!
//! This library provides a Rust interface for WebRTC signaling using the Nabto platform.
//! This SDK focuses on device-side implementation.

pub mod error;
pub mod signaling_device;
pub mod types;

pub use error::{Error, Result};
pub use signaling_device::{
    ConnectionState, DeviceEvent, DeviceTokenGenerator, IceServer, SignalingDevice,
    SignalingDeviceOptions,
};

#[cfg(test)]
mod tests {
    #[test]
    fn test_basic() {
        // Basic test placeholder
        assert!(true);
    }
}
