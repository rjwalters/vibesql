#!/usr/bin/env bash
# Build script for VibeSQL Python bindings
# This script builds the Python wheel using maturin

set -e  # Exit on error

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PYTHON_BINDINGS_DIR="$REPO_ROOT/crates/vibesql-python-bindings"

echo "VibeSQL Python Bindings Builder"
echo "================================"
echo ""

# Check if Python is available
if ! command -v python3 &> /dev/null; then
    echo "❌ Error: python3 is not installed"
    echo "Please install Python 3.8 or newer"
    exit 1
fi

PYTHON_VERSION=$(python3 --version | cut -d' ' -f2)
echo "✓ Found Python $PYTHON_VERSION"

# Check if maturin is available (either as command or via python -m)
if ! python3 -m maturin --version &> /dev/null; then
    echo ""
    echo "⚠️  maturin is not installed"
    echo "Installing maturin..."
    pip3 install maturin
    echo "✓ maturin installed"
else
    echo "✓ Found maturin $(python3 -m maturin --version 2>&1 | head -1)"
fi

echo ""
echo "Building Python wheel..."
echo "------------------------"

# Navigate to Python bindings directory
cd "$PYTHON_BINDINGS_DIR"

# Build the wheel in release mode (use python3 -m to ensure we find maturin)
python3 -m maturin build --release

echo ""
echo "✓ Build complete!"
echo ""
echo "Wheel location:"
find "$REPO_ROOT/target/wheels" -name "vibesql-*.whl" -type f -exec ls -lh {} \;

echo ""
echo "To install the wheel:"
echo "  pip install target/wheels/vibesql-*.whl"
echo ""
echo "Or for development (editable install):"
echo "  cd crates/vibesql-python-bindings"
echo "  maturin develop"
