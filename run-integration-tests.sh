#!/bin/bash
# Integration Test Runner
#
# This script starts the integration test server and runs the Rust integration tests.
#
# Usage:
#   ./run-integration-tests.sh [test-name]
#
# Examples:
#   ./run-integration-tests.sh                    # Run all integration tests
#   ./run-integration-tests.sh device_connectivity # Run only device_connectivity tests

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Configuration
TEST_SERVER_DIR="${HOME}/sandbox/nabto-webrtc-sdk-js/integration_test_server"
TEST_SERVER_PORT=13745
PID_FILE="/tmp/nabto-integration-test-server.pid"

# Function to check if server is running
check_server() {
    curl -s "http://localhost:${TEST_SERVER_PORT}/swagger" > /dev/null 2>&1
    return $?
}

# Function to start the test server
start_server() {
    echo -e "${YELLOW}Starting integration test server...${NC}"

    if [ ! -d "$TEST_SERVER_DIR" ]; then
        echo -e "${RED}Error: Test server directory not found at ${TEST_SERVER_DIR}${NC}"
        echo "Please update TEST_SERVER_DIR in this script to point to the integration_test_server directory"
        exit 1
    fi

    # Start the server in the background (from the server directory)
    (cd "$TEST_SERVER_DIR" && bun run src/index.ts > /tmp/integration-test-server.log 2>&1) &
    SERVER_PID=$!
    echo $SERVER_PID > "$PID_FILE"

    echo "Server started with PID: $SERVER_PID"
    echo "Waiting for server to be ready..."

    # Wait for server to be ready (max 10 seconds)
    for i in {1..20}; do
        if check_server; then
            echo -e "${GREEN}✓ Test server is ready${NC}"
            return 0
        fi
        sleep 0.5
    done

    echo -e "${RED}Error: Test server failed to start within 10 seconds${NC}"
    echo "Check the logs at /tmp/integration-test-server.log"
    stop_server
    exit 1
}

# Function to stop the test server
stop_server() {
    if [ -f "$PID_FILE" ]; then
        SERVER_PID=$(cat "$PID_FILE")
        echo -e "${YELLOW}Stopping test server (PID: $SERVER_PID)...${NC}"
        kill $SERVER_PID 2>/dev/null || true
        rm -f "$PID_FILE"
        echo -e "${GREEN}✓ Test server stopped${NC}"
    fi
}

# Cleanup on exit
cleanup() {
    stop_server
}
trap cleanup EXIT INT TERM

# Main script
echo "================================================"
echo "Nabto WebRTC SDK - Integration Test Runner"
echo "================================================"
echo ""

# Check if server is already running
if check_server; then
    echo -e "${YELLOW}Test server is already running${NC}"
    EXTERNAL_SERVER=true
else
    start_server
    EXTERNAL_SERVER=false
fi

echo ""
echo -e "${YELLOW}Running integration tests...${NC}"
echo ""

# Run the tests
cd "$(dirname "$0")"

if [ -z "$1" ]; then
    # Run all integration tests
    cargo test --test '*' -- --ignored --nocapture
else
    # Run specific test file
    cargo test --test "$1" -- --ignored --nocapture
fi

TEST_EXIT_CODE=$?

echo ""
if [ $TEST_EXIT_CODE -eq 0 ]; then
    echo -e "${GREEN}✓ All tests passed!${NC}"
else
    echo -e "${RED}✗ Some tests failed${NC}"
fi

# Don't stop the server if it was already running
if [ "$EXTERNAL_SERVER" = true ]; then
    echo ""
    echo -e "${YELLOW}Note: Test server was already running and has been left running${NC}"
    trap - EXIT INT TERM  # Remove the cleanup trap
fi

exit $TEST_EXIT_CODE
