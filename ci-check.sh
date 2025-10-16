#!/bin/bash
# CI Check Script - Run all CI checks locally before pushing
#
# This script runs the same checks that CI runs, allowing you to catch
# issues locally before they fail in CI.

set -e  # Exit on first error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

echo -e "${YELLOW}======================================${NC}"
echo -e "${YELLOW}Running CI checks locally...${NC}"
echo -e "${YELLOW}======================================${NC}"
echo ""

# Function to print status
print_step() {
    echo -e "${YELLOW}➜ $1${NC}"
}

print_success() {
    echo -e "${GREEN}✓ $1${NC}"
}

print_error() {
    echo -e "${RED}✗ $1${NC}"
}

# Track if any step failed
FAILED=0

# 1. Check formatting
print_step "Checking code formatting..."
if cargo fmt --all -- --check; then
    print_success "Code formatting passed"
else
    print_error "Code formatting failed. Run: cargo fmt --all"
    FAILED=1
fi
echo ""

# 2. Run clippy
print_step "Running clippy..."
if cargo clippy --all-targets --all-features -- -D warnings; then
    print_success "Clippy passed"
else
    print_error "Clippy failed"
    FAILED=1
fi
echo ""

# 3. Build
print_step "Building project..."
if cargo build --verbose; then
    print_success "Build passed"
else
    print_error "Build failed"
    FAILED=1
fi
echo ""

# 4. Run tests
print_step "Running unit tests..."
if cargo test --verbose; then
    print_success "Unit tests passed"
else
    print_error "Unit tests failed"
    FAILED=1
fi
echo ""

# 5. Build examples
print_step "Building examples..."
if cargo build --examples --verbose; then
    print_success "Examples build passed"
else
    print_error "Examples build failed"
    FAILED=1
fi
echo ""

# 6. Run doc tests
print_step "Running doc tests..."
if cargo test --doc --verbose; then
    print_success "Doc tests passed"
else
    print_error "Doc tests failed"
    FAILED=1
fi
echo ""

# 7. Build release (optional - can be slow)
if [ "$1" == "--with-release" ]; then
    print_step "Building release..."
    if cargo build --release --verbose; then
        print_success "Release build passed"
    else
        print_error "Release build failed"
        FAILED=1
    fi
    echo ""
fi

# Summary
echo -e "${YELLOW}======================================${NC}"
if [ $FAILED -eq 0 ]; then
    echo -e "${GREEN}✓ All CI checks passed!${NC}"
    echo -e "${GREEN}  Your code is ready to push.${NC}"
    exit 0
else
    echo -e "${RED}✗ Some CI checks failed!${NC}"
    echo -e "${RED}  Please fix the issues above before pushing.${NC}"
    exit 1
fi
