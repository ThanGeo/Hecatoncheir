#!/bin/bash

set -e  # Exit on any error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m' # No Color

# Print colored output
print_status() { echo -e "${GREEN}[✓]${NC} $1"; }
print_error() { echo -e "${RED}[✗]${NC} $1"; }
print_warning() { echo -e "${YELLOW}[!]${NC} $1"; }
print_info() { echo -e "${BLUE}[i]${NC} $1"; }
print_section() { echo -e "${CYAN}==>${NC} $1"; }

# Detect OS
detect_os() {
    if [[ "$OSTYPE" == "linux-gnu"* ]]; then
        OS="linux"
        print_info "Detected Linux"
    elif [[ "$OSTYPE" == "darwin"* ]]; then
        OS="macos"
        print_info "Detected macOS"
    else
        print_error "Unsupported OS: $OSTYPE"
        exit 1
    fi
}

# Check if command exists
command_exists() { command -v "$1" >/dev/null 2>&1; }

# Check Node.js version
check_nodejs_version() {
    local required_major=16
    local current_version=$(node -v | cut -d'v' -f2 | cut -d'.' -f1)
    if [ "$current_version" -lt "$required_major" ]; then
        print_warning "Node.js version $current_version detected. Version $required_major or higher recommended."
        return 1
    fi
    return 0
}

# Install Node.js and npm
install_nodejs() {
    print_section "Checking Node.js installation..."
    if ! command_exists node; then
        print_warning "Node.js not found. Installing Node.js..."
        if [ "$OS" == "linux" ]; then
            if command_exists apt-get; then
                print_info "Installing Node.js via NodeSource repository..."
                curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
                sudo apt-get install -y nodejs
            elif command_exists yum; then
                curl -fsSL https://rpm.nodesource.com/setup_20.x | sudo bash -
                sudo yum install -y nodejs
            elif command_exists dnf; then
                curl -fsSL https://rpm.nodesource.com/setup_20.x | sudo bash -
                sudo dnf install -y nodejs
            else
                print_error "No supported package manager found. Please install Node.js manually."
                exit 1
            fi
        elif [ "$OS" == "macos" ]; then
            if command_exists brew; then
                brew install node
            else
                print_error "Homebrew not found. Install Node.js manually."
                exit 1
            fi
        fi
    else
        NODE_VERSION=$(node -v)
        print_status "Node.js $NODE_VERSION is already installed"
        check_nodejs_version
    fi

    if ! command_exists npm; then
        print_error "npm is not installed"
        exit 1
    fi
    print_status "npm $(npm -v) is available"
}

# Build C++ server using existing build.sh
build_cpp_server() {
    print_section "Building C++ server..."
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    if [ ! -f "$SCRIPT_DIR/build.sh" ]; then
        print_error "C++ build script not found at $SCRIPT_DIR/build.sh"
        exit 1
    fi
    chmod +x "$SCRIPT_DIR/build.sh"
    "$SCRIPT_DIR/build.sh"
    print_status "C++ server built successfully"
}

# Install UI root dependencies
install_ui_dependencies() {
    print_section "Checking UI root dependencies..."
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    if [ -d "$SCRIPT_DIR/Hecatoncheir/UI" ]; then
        cd "$SCRIPT_DIR/Hecatoncheir/UI"
        if [ -f "package.json" ]; then
            npm install
            print_status "UI root dependencies installed"
        fi
        cd - > /dev/null
    fi
}

# Install Frontend dependencies
install_frontend() {
    print_section "Installing Frontend dependencies..."
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    cd "$SCRIPT_DIR/Hecatoncheir/UI/frontend"
    npm install
    print_status "Frontend dependencies installed successfully"
    cd - > /dev/null
}

# Install Backend dependencies
install_backend() {
    print_section "Installing Backend dependencies..."
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    cd "$SCRIPT_DIR/Hecatoncheir/UI/backend"
    npm install
    print_status "Backend dependencies installed successfully"
    cd - > /dev/null
}

# Build Frontend for production
build_frontend() {
    print_section "Building Frontend for production..."
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    cd "$SCRIPT_DIR/Hecatoncheir/UI/frontend"
    npm run build
    cd - > /dev/null
    print_status "Frontend production build complete"
}

# Create startup scripts
create_startup_scripts() {
    print_section "Creating startup scripts..."
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

    # Development start script
    cat > "$SCRIPT_DIR/start-dev.sh" << 'EOF'
#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Starting Hecatoncheir Application in Development Mode..."
echo ""

# Start C++ server
echo "[C++ Server] Starting Hecatoncheir engine..."
CPP_SERVER_PATH="$SCRIPT_DIR/build/Hecatoncheir/UI/hec_server"

if [[ -x "$CPP_SERVER_PATH" ]]; then
  "$CPP_SERVER_PATH" &
  CPP_PID=$!
  echo "[C++ Server] Started (PID: $CPP_PID)"
else
  echo "[C++ Server] Skipped — binary not found at $CPP_SERVER_PATH"
  CPP_PID=0
fi

# Wait for C++ server to initialize
sleep 2

# Start Backend
echo "[Backend] Starting Node.js server on port 5000..."
node "$SCRIPT_DIR/Hecatoncheir/UI/backend/server.js" &
BE_PID=$!

sleep 2

# Start Frontend
echo "[Frontend] Starting React development server on port 3000..."
cd "$SCRIPT_DIR/Hecatoncheir/UI/frontend"
npm start &
FE_PID=$!
cd - > /dev/null

echo ""
echo "✓ All services started!"
echo "  - C++ Server PID: $CPP_PID"
echo "  - Backend PID: $BE_PID"
echo "  - Frontend PID: $FE_PID"
echo ""
echo "Access the app at: http://localhost:3000"
echo ""
echo "Press Ctrl+C to stop all services"

trap "echo ''; echo 'Stopping services...'; [[ $CPP_PID -ne 0 ]] && kill $CPP_PID; kill $BE_PID $FE_PID 2>/dev/null; exit" INT TERM
wait
EOF

    chmod +x "$SCRIPT_DIR/start-dev.sh"

    # Production start script
    cat > "$SCRIPT_DIR/start-prod.sh" << 'EOF'
#!/bin/bash
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Starting Hecatoncheir Application in Production Mode..."
echo ""

CPP_SERVER_PATH="$SCRIPT_DIR/build/Hecatoncheir/UI/hec_server"

if [[ -x "$CPP_SERVER_PATH" ]]; then
  "$CPP_SERVER_PATH" &
  CPP_PID=$!
  echo "[C++ Server] Started (PID: $CPP_PID)"
else
  echo "[C++ Server] Skipped — binary not found at $CPP_SERVER_PATH"
  CPP_PID=0
fi

sleep 2

echo "[Backend] Starting Node.js server..."
NODE_ENV=production node "$SCRIPT_DIR/Hecatoncheir/UI/backend/server.js" &
BE_PID=$!

echo ""
echo "✓ All services started!"
echo "  - C++ Server PID: $CPP_PID"
echo "  - Backend PID: $BE_PID"
echo "Press Ctrl+C to stop all services"

trap "echo ''; echo 'Stopping services...'; [[ $CPP_PID -ne 0 ]] && kill $CPP_PID; kill $BE_PID 2>/dev/null; exit" INT TERM
wait
EOF

    chmod +x "$SCRIPT_DIR/start-prod.sh"

    # Stop script
    cat > "$SCRIPT_DIR/stop.sh" << 'EOF'
#!/bin/bash
echo "Stopping all Hecatoncheir services..."
pkill -f "hec_server" && echo "✓ C++ server stopped"
pkill -f "node.*server.js" && echo "✓ Backend stopped"
pkill -f "react-scripts" && echo "✓ Frontend stopped"
echo "All services stopped"
EOF

    chmod +x "$SCRIPT_DIR/stop.sh"

    print_status "Startup scripts created:"
    print_info "  - start-dev.sh   (development mode)"
    print_info "  - start-prod.sh  (production mode)"
    print_info "  - stop.sh        (stop all services)"
}

# Verify installation
verify_installation() {
    print_section "Verifying installation..."
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    local all_good=true

    command_exists node && print_status "Node.js $(node -v)" || { print_error "Node.js missing"; all_good=false; }
    command_exists npm && print_status "npm $(npm -v)" || { print_error "npm missing"; all_good=false; }

    if [ -f "$SCRIPT_DIR/build/Hecatoncheir/UI/hec_server" ]; then
        print_status "C++ server binary found"
    else
        print_warning "C++ binary not found at build/Hecatoncheir/UI/hec_server"
    fi

    [ -d "$SCRIPT_DIR/Hecatoncheir/UI/frontend/node_modules" ] && print_status "Frontend deps OK" || { print_error "Frontend deps missing"; all_good=false; }
    [ -d "$SCRIPT_DIR/Hecatoncheir/UI/backend/node_modules" ] && print_status "Backend deps OK" || { print_error "Backend deps missing"; all_good=false; }

    $all_good && print_status "All checks passed!" || print_error "Some verifications failed"
}

# Print usage
print_usage() {
    echo ""
    echo "================================================================"
    echo " Hecatoncheir Setup Complete!"
    echo "================================================================"
    echo ""
    echo "Development Mode:   ./start-dev.sh"
    echo "Production Mode:    ./start-prod.sh"
    echo "Stop All Services:  ./stop.sh"
    echo ""
    echo "Default Ports:"
    echo "  Frontend: http://localhost:3000"
    echo "  Backend:  http://localhost:5000"
    echo "================================================================"
}

# Main
main() {
    echo ""
    echo "================================================================"
    echo "   Hecatoncheir Complete Build & Setup Script"
    echo "================================================================"
    echo ""

    detect_os
    install_nodejs
    build_cpp_server
    install_ui_dependencies
    install_frontend
    install_backend
    create_startup_scripts
    verify_installation
    print_usage
}

main "$@"
