//! Nabto WebRTC SDK for Rust
//!
//! This library provides a Rust interface for WebRTC signaling using the Nabto platform.
//! This SDK focuses on device-side implementation.

pub mod error;
pub mod signaling_device;
pub mod types;

pub use error::{Error, Result};
pub use signaling_device::{
    ChannelState, ConnectionState, DeviceEvent, DeviceTokenGenerator, IceServer, SignalingDevice,
    SignalingDeviceOptions,
};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_lib_imports() {
        // Verify that core types are accessible
        let _: Option<Error> = None;
        let _: Option<ConnectionState> = None;
    }
}
