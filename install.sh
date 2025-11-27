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
#   - gh (GitHub CLI for issue/PR management)
#   - sccache (compilation cache for faster builds)
#   - coreutils (GNU timeout for benchmarks)
#   - node (for web-demo)
#   - pnpm (for web-demo)
#   - Docker Desktop (for MySQL/PostgreSQL compatibility testing)
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

# Check gh (GitHub CLI)
print_status "Checking gh (GitHub CLI)..."
if command_exists gh; then
    print_success "gh is installed ($(gh --version | head -1))"
else
    print_warning "gh is not installed (needed for: issue/PR management)"
    missing+=("gh")
fi

# Check sccache
print_status "Checking sccache..."
if command_exists sccache; then
    print_success "sccache is installed ($(sccache --version))"
    # Check if sccache is configured as rustc wrapper
    if [[ -f "$HOME/.cargo/config.toml" ]] && grep -q "sccache" "$HOME/.cargo/config.toml" 2>/dev/null; then
        print_success "sccache is configured as rustc wrapper"
    else
        print_warning "sccache is not configured as rustc wrapper"
        missing+=("sccache-config")
    fi
else
    print_warning "sccache is not installed (speeds up builds)"
    missing+=("sccache")
fi

# Check coreutils (provides GNU timeout for benchmarks)
print_status "Checking coreutils (GNU timeout)..."
if command_exists gtimeout; then
    print_success "coreutils is installed (gtimeout available)"
elif command_exists timeout; then
    print_success "timeout is available"
else
    print_warning "coreutils is not installed (needed for: make benchmark)"
    missing+=("coreutils")
fi

# Check git submodules
print_status "Checking git submodules..."
if [[ -d "third_party/sqllogictest/test" ]] && [[ -n "$(ls -A third_party/sqllogictest/test 2>/dev/null)" ]]; then
    print_success "SQLLogicTest submodule is initialized"
else
    print_warning "SQLLogicTest submodule is not initialized"
    missing+=("submodules")
fi

# Check Node.js (for web-demo)
print_status "Checking Node.js..."
if command_exists node; then
    print_success "Node.js is installed ($(node --version))"
else
    print_warning "Node.js is not installed (needed for: web-demo)"
    missing+=("node")
fi

# Check pnpm (for web-demo)
print_status "Checking pnpm..."
if command_exists pnpm; then
    print_success "pnpm is installed ($(pnpm --version))"
else
    print_warning "pnpm is not installed (needed for: web-demo)"
    missing+=("pnpm")
fi

# Check Docker (for MySQL/PostgreSQL compatibility testing)
print_status "Checking Docker..."
if command_exists docker; then
    if docker info &> /dev/null; then
        print_success "Docker is installed and running ($(docker --version | cut -d' ' -f3 | tr -d ','))"
    else
        print_warning "Docker is installed but not running"
        missing+=("docker-running")
    fi
else
    print_warning "Docker is not installed (needed for: MySQL/PostgreSQL compatibility testing)"
    missing+=("docker")
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
    # Use cargo install instead of the shell installer which has Apple Silicon issues
    cargo install wasm-pack
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

# Install gh if needed
if [[ " ${missing[*]} " =~ " gh " ]]; then
    print_status "Installing gh (GitHub CLI) via Homebrew..."
    brew install gh
    print_success "gh installed"
    echo ""
    print_warning "Run 'gh auth login' to authenticate with GitHub"
fi

# Install sccache if needed
if [[ " ${missing[*]} " =~ " sccache " ]]; then
    print_status "Installing sccache via Homebrew..."
    brew install sccache
    print_success "sccache installed"
fi

# Install coreutils if needed
if [[ " ${missing[*]} " =~ " coreutils " ]]; then
    print_status "Installing coreutils via Homebrew..."
    brew install coreutils
    print_success "coreutils installed (gtimeout now available)"
fi

# Install Node.js if needed
if [[ " ${missing[*]} " =~ " node " ]]; then
    print_status "Installing Node.js via Homebrew..."
    brew install node
    print_success "Node.js installed"
fi

# Install pnpm if needed
if [[ " ${missing[*]} " =~ " pnpm " ]]; then
    print_status "Installing pnpm via Homebrew..."
    brew install pnpm
    print_success "pnpm installed"
fi

# Install Docker if needed
if [[ " ${missing[*]} " =~ " docker " ]]; then
    print_status "Installing Docker Desktop via Homebrew..."
    brew install --cask docker
    print_success "Docker Desktop installed"
    echo ""
    print_warning "Please open Docker Desktop from Applications to complete setup"
    print_warning "Docker needs to be running for MySQL/PostgreSQL compatibility testing"
fi

# Remind user to start Docker if it's installed but not running
if [[ " ${missing[*]} " =~ " docker-running " ]]; then
    print_warning "Docker is installed but not running"
    print_warning "Please start Docker Desktop from Applications"
fi

# Configure sccache as rustc wrapper if needed
if [[ " ${missing[*]} " =~ " sccache " ]] || [[ " ${missing[*]} " =~ " sccache-config " ]]; then
    print_status "Configuring sccache as rustc wrapper..."
    mkdir -p "$HOME/.cargo"
    # Create or update cargo config
    if [[ -f "$HOME/.cargo/config.toml" ]]; then
        # Check if [build] section exists
        if grep -q "^\[build\]" "$HOME/.cargo/config.toml"; then
            # Check if rustc-wrapper is already set
            if ! grep -q "rustc-wrapper" "$HOME/.cargo/config.toml"; then
                # Add rustc-wrapper under existing [build] section
                sed -i '' '/^\[build\]/a\
rustc-wrapper = "sccache"
' "$HOME/.cargo/config.toml"
            fi
        else
            # Add [build] section with rustc-wrapper
            echo "" >> "$HOME/.cargo/config.toml"
            echo "[build]" >> "$HOME/.cargo/config.toml"
            echo 'rustc-wrapper = "sccache"' >> "$HOME/.cargo/config.toml"
        fi
    else
        # Create new config file
        cat > "$HOME/.cargo/config.toml" << 'EOF'
[build]
rustc-wrapper = "sccache"
EOF
    fi
    print_success "sccache configured as rustc wrapper"
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
