#!/usr/bin/env bash
set -euo pipefail

# Clean build script for VibeSQL
# This script helps resolve stale build cache issues by cleaning and rebuilding the project
# Usage: ./scripts/clean-build.sh [OPTIONS]

# Color output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Default options
BUILD_RELEASE=false
RUN_TESTS=false
VERBOSE=false

# Parse command line arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -r|--release)
            BUILD_RELEASE=true
            shift
            ;;
        -t|--test)
            RUN_TESTS=true
            shift
            ;;
        -v|--verbose)
            VERBOSE=true
            shift
            ;;
        -h|--help)
            echo "Usage: $0 [OPTIONS]"
            echo ""
            echo "Options:"
            echo "  -r, --release    Build in release mode (default: debug)"
            echo "  -t, --test       Run tests after building"
            echo "  -v, --verbose    Show verbose cargo output"
            echo "  -h, --help       Show this help message"
            echo ""
            echo "Examples:"
            echo "  $0                      # Clean and build debug"
            echo "  $0 --release            # Clean and build release"
            echo "  $0 --release --test     # Clean, build release, and run tests"
            exit 0
            ;;
        *)
            echo -e "${RED}Error: Unknown option: $1${NC}"
            echo "Use --help for usage information"
            exit 1
            ;;
    esac
done

# Print header
echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}VibeSQL Clean Build Script${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""

# Step 1: Clean build artifacts
echo -e "${YELLOW}[1/3] Cleaning build artifacts...${NC}"
if [ "$VERBOSE" = true ]; then
    cargo clean
else
    cargo clean > /dev/null 2>&1
fi
echo -e "${GREEN}✓ Build artifacts cleaned${NC}"
echo ""

# Step 2: Build project
if [ "$BUILD_RELEASE" = true ]; then
    echo -e "${YELLOW}[2/3] Building project (release mode)...${NC}"
    if [ "$VERBOSE" = true ]; then
        cargo build --release
    else
        echo "This may take several minutes..."
        cargo build --release 2>&1 | grep -E "(Compiling|Finished|error|warning:)" || true
    fi
    BUILD_PATH="target/release"
else
    echo -e "${YELLOW}[2/3] Building project (debug mode)...${NC}"
    if [ "$VERBOSE" = true ]; then
        cargo build
    else
        echo "This may take several minutes..."
        cargo build 2>&1 | grep -E "(Compiling|Finished|error|warning:)" || true
    fi
    BUILD_PATH="target/debug"
fi

# Check if build succeeded
if [ $? -eq 0 ]; then
    echo -e "${GREEN}✓ Build completed successfully${NC}"
else
    echo -e "${RED}✗ Build failed${NC}"
    exit 1
fi
echo ""

# Step 3: Run tests (optional)
if [ "$RUN_TESTS" = true ]; then
    echo -e "${YELLOW}[3/3] Running tests...${NC}"
    if [ "$BUILD_RELEASE" = true ]; then
        cargo test --release
    else
        cargo test
    fi

    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ All tests passed${NC}"
    else
        echo -e "${RED}✗ Some tests failed${NC}"
        exit 1
    fi
else
    echo -e "${BLUE}[3/3] Skipping tests (use --test to run tests)${NC}"
fi
echo ""

# Print summary
echo -e "${BLUE}========================================${NC}"
echo -e "${GREEN}Clean build completed successfully!${NC}"
echo -e "${BLUE}========================================${NC}"
echo ""
echo "Build artifacts location: $BUILD_PATH"
echo ""
echo "Next steps:"
if [ "$RUN_TESTS" = false ]; then
    echo "  - Run tests: cargo test"
fi
echo "  - Run vibesql: $BUILD_PATH/vibesql"
echo ""
