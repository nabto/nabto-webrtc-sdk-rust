# Development Guide

This guide covers development workflows and tools for the nabto-webrtc-sdk-rust project.

## Quick Start

Before pushing code, run CI checks locally:

```bash
./ci-check.sh
```

This runs the same checks that GitHub Actions CI runs, catching issues early.

## Available Tools

### 1. Shell Script (Recommended for Pre-Push Validation)

The `ci-check.sh` script provides colored output and detailed feedback. Run this before pushing code to catch issues early:

```bash
./ci-check.sh                # Run all checks
./ci-check.sh --with-release # Include release build (slower)
```

**Checks performed:**
- ✓ Code formatting (`cargo fmt --check`)
- ✓ Clippy lints (`cargo clippy`)
- ✓ Build (`cargo build`)
- ✓ Unit tests (`cargo test`)
- ✓ Build examples
- ✓ Doc tests
- ✓ Release build (with `--with-release`)

### 2. Cargo Aliases (Recommended for Quick Iteration)

For faster development cycles, use the cargo aliases defined in `.cargo/config.toml`:

```bash
cargo quick-check      # Quick checks (fmt, clippy, test)
cargo fmt-check        # Check formatting
cargo clippy-ci        # Run clippy with CI settings
cargo build-all        # Build everything
cargo integration-test # Run integration tests
```

## Development Workflow

### Typical Development Cycle

1. **Make changes** to the code

2. **Run quick checks** during development:
   ```bash
   cargo quick-check
   ```

3. **Before committing**, run full CI check:
   ```bash
   ./ci-check.sh
   ```

4. **Fix any issues** reported

5. **Commit and push** with confidence!

### Recommended Workflow

- **During active development**: Use `cargo quick-check` for rapid feedback
- **Before pushing to GitHub**: Use `./ci-check.sh` to catch all CI issues locally
- **For specific tasks**: Use individual cargo commands as needed

### Formatting Code

Auto-format all code:
```bash
cargo fmt --all
```

Check formatting without modifying files:
```bash
cargo fmt --all -- --check
# or
cargo fmt-check
```

### Running Lints

Run clippy with CI settings:
```bash
cargo clippy --all-targets --all-features -- -D warnings
# or
cargo clippy-ci
```

### Running Tests

Unit tests only:
```bash
cargo test
```

Integration tests (requires integration test server):
```bash
cargo test -- --ignored --test-threads=1
# or
cargo integration-test
```

All tests:
```bash
cargo test --all
```

## Integration Tests

Integration tests require the integration test server from the JS SDK.

### Setup

1. Clone the JS SDK repository:
   ```bash
   git clone https://github.com/nabto/nabto-webrtc-sdk-js.git ../nabto-webrtc-sdk-js
   ```

2. Install dependencies (requires Bun):
   ```bash
   cd ../nabto-webrtc-sdk-js/integration_test_server
   bun install
   ```

3. Start the server:
   ```bash
   bun dev
   # Server runs on http://localhost:13745
   ```

4. In another terminal, run integration tests:
   ```bash
   cd /path/to/nabto-webrtc-sdk-rust
   cargo test -- --ignored --test-threads=1
   ```

### Integration Test Structure

- `tests/channel_handling.rs` - Channel creation and message handling (4 tests)
- `tests/device_client.rs` - Client connection scenarios (4 tests)
- `tests/device_connectivity.rs` - Connection and reconnection (7 tests)

Total: 15 integration tests

## CI Pipeline

The GitHub Actions CI runs three jobs:

1. **test** - Formatting, clippy, build, unit tests, examples, doc tests
2. **integration-test** - Full integration test suite against test server
3. **build-release** - Release build verification

All checks in the `test` job can be run locally with `./ci-check.sh`.

## Troubleshooting

### Formatting Issues

If `cargo fmt --check` fails:
```bash
cargo fmt --all  # Auto-fix formatting
```

### Clippy Warnings

If clippy fails, fix warnings or add `#[allow(...)]` attributes if justified:
```rust
#[allow(dead_code)]  // Reason why this is needed
```

### Test Failures

For unit test failures:
```bash
cargo test -- --nocapture  # Show println! output
RUST_LOG=debug cargo test  # Enable logging
```

For integration test failures:
1. Ensure integration test server is running
2. Check server logs for errors
3. Run tests with `--test-threads=1` to avoid race conditions

## Module Structure

```
nabto_webrtc_sdk/
├── device/          # Core device-side signaling
│   ├── SignalingDevice
│   ├── SignalingChannel
│   ├── ConnectionState, ChannelState
│   └── Internal: routing, reliability, connection, http
│
└── util/            # Message transport utilities
    ├── DeviceMessageTransport
    ├── MessageEncoder
    ├── MessageSigner (JWT & None)
    └── WebRTC message types
```

## Best Practices

1. **Always run `./ci-check.sh` before pushing**
2. **Write tests** for new functionality
3. **Document public APIs** with doc comments
4. **Keep commits atomic** and well-described
5. **Run integration tests** when changing protocol implementation

## Getting Help

- Check GitHub Issues for known problems
- Review the protocol documentation in `~/sandbox/documentation/webrtc/protocol/`
- Look at the JS implementation for reference: `~/sandbox/nabto-webrtc-sdk-js/`
