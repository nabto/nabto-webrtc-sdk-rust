//! Common utilities for integration tests

#![allow(dead_code)] // Test utilities may not all be used yet
#![allow(unused_imports)]

use std::sync::Once;

pub mod test_client;
pub mod test_instance;

pub use test_client::{ClientTestOptions, DeviceTestOptions};
pub use test_instance::{DeviceHandle, DeviceTestInstance};

static INIT: Once = Once::new();

/// Initialize the logger for tests. Call this at the start of each test
/// to enable log output with RUST_LOG environment variable.
/// Safe to call multiple times - only initializes once.
pub fn init_logger() {
    INIT.call_once(|| {
        env_logger::builder().is_test(true).try_init().ok();
    });
}
