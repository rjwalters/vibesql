#!/bin/bash
#
# VibeSQL Development Environment Setup for macOS
#
# This script installs all prerequisites needed to run "make all"
#
# Prerequisites installed:
#   - Homebrew (if not installed)
#   - Rust toolchain (via rustup)
#   - wasm-pack (for WebAssembly builds)
#   - Python 3 (usually pre-installed on macOS)
#   - maturin (Python bindings build tool)
#   - jq (JSON processing for test analysis)
#   - pnpm (for web-demo, optional)
#
# Usage:
#   ./install.sh           # Install all prerequisites
#   ./install.sh --check   # Check what's missing without installing
#

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

print_status() { echo -e "${BLUE}==>${NC} $1"; }
print_success() { echo -e "${GREEN}✓${NC} $1"; }
print_warning() { echo -e "${YELLOW}⚠${NC} $1"; }
print_error() { echo -e "${RED}✗${NC} $1"; }

# Check if running on macOS
check_macos() {
    if [[ "$(uname)" != "Darwin" ]]; then
        print_error "This script is designed for macOS only"
        exit 1
    fi
}

# Check if a command exists
command_exists() {
    command -v "$1" &> /dev/null
}

# Check mode - just report what's missing
check_only=false
if [[ "$1" == "--check" ]]; then
    check_only=true
fi

echo ""
echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║           VibeSQL Development Environment Setup               ║"
echo "╚═══════════════════════════════════════════════════════════════╝"
echo ""

check_macos

missing=()

# Check Homebrew
print_status "Checking Homebrew..."
if command_exists brew; then
    print_success "Homebrew is installed ($(brew --version | head -1))"
else
    print_warning "Homebrew is not installed"
    missing+=("homebrew")
fi

# Check Rust
print_status "Checking Rust toolchain..."
if command_exists rustc; then
    print_success "Rust is installed ($(rustc --version))"
    if command_exists cargo; then
        print_success "Cargo is installed ($(cargo --version))"
    else
        print_warning "Cargo is not installed"
        missing+=("cargo")
    fi
else
    print_warning "Rust is not installed"
    missing+=("rust")
fi

# Check wasm-pack
print_status "Checking wasm-pack..."
if command_exists wasm-pack; then
    print_success "wasm-pack is installed ($(wasm-pack --version))"
else
    print_warning "wasm-pack is not installed (needed for: make build-wasm)"
    missing+=("wasm-pack")
fi

# Check wasm32 target
print_status "Checking wasm32 target..."
if command_exists rustup; then
    if rustup target list --installed | grep -q "wasm32-unknown-unknown"; then
        print_success "wasm32-unknown-unknown target is installed"
    else
        print_warning "wasm32-unknown-unknown target is not installed"
        missing+=("wasm32-target")
    fi
else
    print_warning "rustup not found, cannot check wasm32 target"
fi

# Check Python 3
print_status "Checking Python 3..."
if command_exists python3; then
    print_success "Python 3 is installed ($(python3 --version))"
else
    print_warning "Python 3 is not installed"
    missing+=("python3")
fi

# Check pip
print_status "Checking pip..."
if python3 -m pip --version &> /dev/null; then
    print_success "pip is available"
else
    print_warning "pip is not available"
    missing+=("pip")
fi

# Check maturin
print_status "Checking maturin..."
if command_exists maturin; then
    print_success "maturin is installed ($(maturin --version))"
else
    print_warning "maturin is not installed (needed for: make build-python)"
    missing+=("maturin")
fi

# Check jq
print_status "Checking jq..."
if command_exists jq; then
    print_success "jq is installed ($(jq --version))"
else
    print_warning "jq is not installed (needed for: test analysis)"
    missing+=("jq")
fi

# Check git submodules
print_status "Checking git submodules..."
if [[ -d "third_party/sqllogictest/test" ]] && [[ -n "$(ls -A third_party/sqllogictest/test 2>/dev/null)" ]]; then
    print_success "SQLLogicTest submodule is initialized"
else
    print_warning "SQLLogicTest submodule is not initialized"
    missing+=("submodules")
fi

# Optional: Check pnpm (for web-demo)
print_status "Checking pnpm (optional, for web-demo)..."
if command_exists pnpm; then
    print_success "pnpm is installed ($(pnpm --version))"
else
    print_warning "pnpm is not installed (optional, for web-demo)"
    # Don't add to missing - it's optional
fi

echo ""

# If check only mode, exit here
if $check_only; then
    if [[ ${#missing[@]} -eq 0 ]]; then
        echo -e "${GREEN}All prerequisites are installed!${NC}"
        echo "You can run: make all"
    else
        echo -e "${YELLOW}Missing prerequisites:${NC} ${missing[*]}"
        echo "Run ./install.sh to install them"
    fi
    exit 0
fi

# Install missing prerequisites
if [[ ${#missing[@]} -eq 0 ]]; then
    print_success "All prerequisites are already installed!"
    echo ""
    echo "You can now run:"
    echo "  make all        # Build, test, and benchmark"
    echo "  make help       # See all available targets"
    exit 0
fi

echo -e "${BLUE}Installing missing prerequisites: ${missing[*]}${NC}"
echo ""

# Install Homebrew if needed
if [[ " ${missing[*]} " =~ " homebrew " ]]; then
    print_status "Installing Homebrew..."
    /bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/Homebrew/install/HEAD/install.sh)"

    # Add Homebrew to PATH for this session (Apple Silicon)
    if [[ -f "/opt/homebrew/bin/brew" ]]; then
        eval "$(/opt/homebrew/bin/brew shellenv)"
    fi
    print_success "Homebrew installed"
fi

# Install Rust if needed
if [[ " ${missing[*]} " =~ " rust " ]]; then
    print_status "Installing Rust via rustup..."
    curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y
    source "$HOME/.cargo/env"
    print_success "Rust installed"
fi

# Install wasm-pack if needed
if [[ " ${missing[*]} " =~ " wasm-pack " ]]; then
    print_status "Installing wasm-pack..."
    curl https://rustwasm.github.io/wasm-pack/installer/init.sh -sSf | sh
    print_success "wasm-pack installed"
fi

# Install wasm32 target if needed
if [[ " ${missing[*]} " =~ " wasm32-target " ]]; then
    print_status "Installing wasm32-unknown-unknown target..."
    rustup target add wasm32-unknown-unknown
    print_success "wasm32 target installed"
fi

# Install Python 3 if needed (via Homebrew)
if [[ " ${missing[*]} " =~ " python3 " ]]; then
    print_status "Installing Python 3 via Homebrew..."
    brew install python3
    print_success "Python 3 installed"
fi

# Install maturin if needed
if [[ " ${missing[*]} " =~ " maturin " ]]; then
    print_status "Installing maturin..."
    pip3 install maturin
    print_success "maturin installed"
fi

# Install jq if needed
if [[ " ${missing[*]} " =~ " jq " ]]; then
    print_status "Installing jq via Homebrew..."
    brew install jq
    print_success "jq installed"
fi

# Initialize submodules if needed
if [[ " ${missing[*]} " =~ " submodules " ]]; then
    print_status "Initializing git submodules..."
    git submodule update --init --recursive
    print_success "Git submodules initialized"
fi

echo ""
echo "╔═══════════════════════════════════════════════════════════════╗"
echo "║                    Installation Complete!                      ║"
echo "╚═══════════════════════════════════════════════════════════════╝"
echo ""
echo "You can now run:"
echo "  make all        # Build, test, and benchmark everything"
echo "  make build      # Build all Rust crates"
echo "  make test       # Run all tests"
echo "  make help       # See all available targets"
echo ""

# Check if shell needs to be reloaded for Rust
if [[ " ${missing[*]} " =~ " rust " ]]; then
    echo -e "${YELLOW}Note:${NC} You may need to restart your terminal or run:"
    echo "  source ~/.cargo/env"
    echo ""
fi
