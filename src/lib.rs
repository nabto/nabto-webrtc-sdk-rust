//! Nabto WebRTC SDK for Rust
//!
//! This library provides a Rust interface for WebRTC signaling using the Nabto platform.

pub mod client;
pub mod error;
pub mod signaling;
pub mod types;

pub use error::{Error, Result};

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_basic() {
        // Basic test placeholder
        assert!(true);
    }
}
