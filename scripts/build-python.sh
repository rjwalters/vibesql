#!/usr/bin/env bash
# Build script for VibeSQL Python bindings
# This script builds the Python wheel using maturin

set -e  # Exit on error

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "$SCRIPT_DIR/.." && pwd)"
PYTHON_BINDINGS_DIR="$REPO_ROOT/crates/vibesql-python-bindings"

# Strip macOS quarantine attribute from built binaries
# macOS Gatekeeper can flag freshly-compiled binaries as 'malware'
strip_quarantine() {
    if [[ "$(uname)" == "Darwin" ]] && [[ -d "$REPO_ROOT/target" ]]; then
        find "$REPO_ROOT/target" -type f -perm +111 -exec xattr -d com.apple.quarantine {} \; 2>/dev/null || true
    fi
}

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

# Ensure pipx bin directory is in PATH (common issue after pipx install)
if [[ -d "$HOME/.local/bin" ]] && [[ ":$PATH:" != *":$HOME/.local/bin:"* ]]; then
    export PATH="$HOME/.local/bin:$PATH"
fi

# Check if maturin is available
if ! command -v maturin &> /dev/null; then
    echo ""
    echo "⚠️  maturin is not installed or not in PATH"
    echo "Installing maturin via pipx..."
    if ! command -v pipx &> /dev/null; then
        echo "❌ Error: pipx is not installed"
        echo "Please install pipx first: brew install pipx && pipx ensurepath"
        exit 1
    fi
    pipx install maturin --force
    # Ensure PATH includes pipx bin dir after install
    export PATH="$HOME/.local/bin:$PATH"
    if ! command -v maturin &> /dev/null; then
        echo "❌ Error: maturin installed but not found in PATH"
        echo "Try running: pipx ensurepath"
        echo "Then restart your shell and try again"
        exit 1
    fi
    echo "✓ maturin installed"
else
    MATURIN_VERSION=$(maturin --version 2>&1 | head -1)
    echo "✓ Found $MATURIN_VERSION"
fi

echo ""
echo "Building Python wheel..."
echo "------------------------"

# Navigate to Python bindings directory
cd "$PYTHON_BINDINGS_DIR"

# Build the wheel in release mode
maturin build --release

# Strip quarantine on macOS
strip_quarantine

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
