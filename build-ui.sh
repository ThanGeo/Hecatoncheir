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
print_status() {
    echo -e "${GREEN}[✓]${NC} $1"
}

print_error() {
    echo -e "${RED}[✗]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[!]${NC} $1"
}

print_info() {
    echo -e "${BLUE}[i]${NC} $1"
}

print_section() {
    echo -e "${CYAN}==>${NC} $1"
}

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
command_exists() {
    command -v "$1" >/dev/null 2>&1
}

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
            # Check for different package managers
            if command_exists apt-get; then
                print_info "Installing Node.js via NodeSource repository..."
                curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
                sudo apt-get install -y nodejs
            elif command_exists yum; then
                print_info "Installing Node.js via NodeSource repository..."
                curl -fsSL https://rpm.nodesource.com/setup_20.x | sudo bash -
                sudo yum install -y nodejs
            elif command_exists dnf; then
                print_info "Installing Node.js via NodeSource repository..."
                curl -fsSL https://rpm.nodesource.com/setup_20.x | sudo bash -
                sudo dnf install -y nodejs
            else
                print_error "No supported package manager found. Please install Node.js manually from https://nodejs.org"
                exit 1
            fi
        elif [ "$OS" == "macos" ]; then
            if command_exists brew; then
                brew install node
            else
                print_error "Homebrew not found. Please install Node.js manually from https://nodejs.org"
                exit 1
            fi
        fi
    else
        NODE_VERSION=$(node -v)
        print_status "Node.js $NODE_VERSION is already installed"
        check_nodejs_version
    fi
    
    # Verify npm is installed
    if ! command_exists npm; then
        print_error "npm is not installed"
        exit 1
    fi
    
    NPM_VERSION=$(npm -v)
    print_status "npm $NPM_VERSION is available"
}

# Build C++ server using existing build.sh
build_cpp_server() {
    print_section "Building C++ server..."
    
    # Get the directory where this script is located
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    
    if [ ! -f "$SCRIPT_DIR/build.sh" ]; then
        print_error "C++ build script not found at $SCRIPT_DIR/build.sh"
        exit 1
    fi
    
    print_info "Running existing C++ build script..."
    chmod +x "$SCRIPT_DIR/build.sh"
    "$SCRIPT_DIR/build.sh"
    
    print_status "C++ server built successfully"
}

# Install UI root dependencies (if any)
install_ui_dependencies() {
    print_section "Checking UI root dependencies..."
    
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    
    if [ ! -d "$SCRIPT_DIR/Hecatoncheir/UI" ]; then
        print_error "UI directory not found at $SCRIPT_DIR/Hecatoncheir/UI"
        exit 1
    fi
    
    cd "$SCRIPT_DIR/Hecatoncheir/UI"
    
    if [ -f "package.json" ]; then
        print_info "Installing UI root dependencies..."
        npm install
        print_status "UI root dependencies installed"
    else
        print_info "No package.json in UI root, skipping..."
    fi
    
    cd - > /dev/null
}

# Install Frontend dependencies
install_frontend() {
    print_section "Installing Frontend dependencies..."
    
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    
    if [ ! -d "$SCRIPT_DIR/Hecatoncheir/UI/frontend" ]; then
        print_error "Frontend directory not found at $SCRIPT_DIR/Hecatoncheir/UI/frontend"
        exit 1
    fi
    
    cd "$SCRIPT_DIR/Hecatoncheir/UI/frontend"
    
    if [ ! -f "package.json" ]; then
        print_error "package.json not found in $SCRIPT_DIR/Hecatoncheir/UI/frontend"
        exit 1
    fi
    
    print_info "Installing React, TypeScript, and Ant Design dependencies..."
    npm install
    
    print_status "Frontend dependencies installed successfully"
    print_info "Installed packages:"
    print_info "  - React 19.1.0"
    print_info "  - TypeScript 4.9.5"
    print_info "  - Ant Design 5.25.4"
    print_info "  - Testing libraries"
    
    cd - > /dev/null
}

# Install Backend dependencies
install_backend() {
    print_section "Installing Backend dependencies..."
    
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    
    if [ ! -d "$SCRIPT_DIR/Hecatoncheir/UI/backend" ]; then
        print_error "Backend directory not found at $SCRIPT_DIR/Hecatoncheir/UI/backend"
        exit 1
    fi
    
    cd "$SCRIPT_DIR/Hecatoncheir/UI/backend"
    
    if [ ! -f "package.json" ]; then
        print_error "package.json not found in $SCRIPT_DIR/Hecatoncheir/UI/backend"
        exit 1
    fi
    
    print_info "Installing Express, CORS, and Multer dependencies..."
    npm install
    
    print_status "Backend dependencies installed successfully"
    print_info "Installed packages:"
    print_info "  - Express 5.1.0"
    print_info "  - CORS 2.8.5"
    print_info "  - Multer 2.0.1"
    
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
    print_info "Build output: $SCRIPT_DIR/Hecatoncheir/UI/frontend/build"
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
cd "$SCRIPT_DIR/Hecatoncheir"
./hec_server &
CPP_PID=$!
cd - > /dev/null

# Wait for C++ server to initialize
sleep 2

# Start Backend
echo "[Backend] Starting Node.js server on port 5000..."
node "$SCRIPT_DIR/Hecatoncheir/UI/backend/server.js" &
BE_PID=$!

# Wait a bit for backend to start
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
echo "Access the application at: http://localhost:3000"
echo ""
echo "Press Ctrl+C to stop all services"

# Trap Ctrl+C and kill all processes
trap "echo ''; echo 'Stopping services...'; kill $CPP_PID $BE_PID $FE_PID 2>/dev/null; exit" INT TERM

wait
EOF
    
    chmod +x "$SCRIPT_DIR/start-dev.sh"
    
    # Production start script
    cat > "$SCRIPT_DIR/start-prod.sh" << 'EOF'
#!/bin/bash

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "Starting Hecatoncheir Application in Production Mode..."
echo ""

# Check if frontend is built
if [ ! -d "$SCRIPT_DIR/Hecatoncheir/UI/frontend/build" ]; then
    echo "Error: Frontend not built. Run './build-ui.sh --production' first."
    exit 1
fi

# Start C++ server
echo "[C++ Server] Starting Hecatoncheir engine..."
cd "$SCRIPT_DIR/Hecatoncheir"
./hec_server &
CPP_PID=$!
cd - > /dev/null

# Wait for C++ server to initialize
sleep 2

# Start Backend
echo "[Backend] Starting Node.js server on port 5000..."
NODE_ENV=production node "$SCRIPT_DIR/Hecatoncheir/UI/backend/server.js" &
BE_PID=$!

echo ""
echo "✓ All services started!"
echo "  - C++ Server PID: $CPP_PID"
echo "  - Backend PID: $BE_PID"
echo ""
echo "Note: Serve the frontend build with your preferred static file server"
echo "Frontend build location: $SCRIPT_DIR/Hecatoncheir/UI/frontend/build"
echo ""
echo "Press Ctrl+C to stop all services"

# Trap Ctrl+C and kill all processes
trap "echo ''; echo 'Stopping services...'; kill $CPP_PID $BE_PID 2>/dev/null; exit" INT TERM

wait
EOF
    
    chmod +x "$SCRIPT_DIR/start-prod.sh"
    
    # Stop script
    cat > "$SCRIPT_DIR/stop.sh" << 'EOF'
#!/bin/bash

echo "Stopping all Hecatoncheir services..."

# Kill C++ server
pkill -f "hec_server" && echo "✓ C++ server stopped"

# Kill Node.js backend processes
pkill -f "node.*server.js" && echo "✓ Backend stopped"

# Kill React development server
pkill -f "react-scripts" && echo "✓ Frontend development server stopped"

echo "All services stopped"
EOF
    
    chmod +x "$SCRIPT_DIR/stop.sh"
    
    print_status "Startup scripts created:"
    print_info "  - start-dev.sh   (development mode with hot reload)"
    print_info "  - start-prod.sh  (production mode)"
    print_info "  - stop.sh        (stop all services)"
}

# Verify installation
verify_installation() {
    print_section "Verifying installation..."
    
    SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
    
    local all_good=true
    
    # Check Node.js
    if command_exists node; then
        print_status "Node.js $(node -v) installed"
    else
        print_error "Node.js not found"
        all_good=false
    fi
    
    # Check npm
    if command_exists npm; then
        print_status "npm $(npm -v) installed"
    else
        print_error "npm not found"
        all_good=false
    fi
    
    # Check C++ server binary
    if [ -f "$SCRIPT_DIR/Hecatoncheir/hec_server" ]; then
        print_status "C++ server binary found"
    else
        print_warning "C++ server binary not found at $SCRIPT_DIR/Hecatoncheir/hec_server"
        print_info "This might be expected if the binary is in a different location"
    fi
    
    # Check Frontend dependencies
    if [ -d "$SCRIPT_DIR/Hecatoncheir/UI/frontend/node_modules" ]; then
        print_status "Frontend dependencies installed"
    else
        print_error "Frontend dependencies missing"
        all_good=false
    fi
    
    # Check Backend dependencies
    if [ -d "$SCRIPT_DIR/Hecatoncheir/UI/backend/node_modules" ]; then
        print_status "Backend dependencies installed"
    else
        print_error "Backend dependencies missing"
        all_good=false
    fi
    
    if [ "$all_good" = true ]; then
        print_status "All verifications passed!"
        return 0
    else
        print_error "Some verifications failed"
        return 1
    fi
}

# Print usage instructions
print_usage() {
    echo ""
    echo "================================================================"
    echo "          Hecatoncheir Complete Setup Finished!                 "
    echo "================================================================"
    echo ""
    echo "Development Mode (with hot reload):"
    echo "  ./start-dev.sh"
    echo ""
    echo "Production Mode (optimized build):"
    echo "  ./build-ui.sh --production    # Build frontend for production"
    echo "  ./start-prod.sh               # Start all services"
    echo ""
    echo "Stop All Services:"
    echo "  ./stop.sh"
    echo ""
    echo "Manual Commands:"
    echo "  C++ Server: cd Hecatoncheir && ./hec_server"
    echo "  Backend:    node Hecatoncheir/UI/backend/server.js"
    echo "  Frontend:   cd Hecatoncheir/UI/frontend && npm start"
    echo "  Build frontend:   cd Hecatoncheir/UI/frontend && npm run build"
    echo ""
    echo "Default Ports:"
    echo "  Frontend: http://localhost:3000"
    echo "  Backend:  http://localhost:5000"
    echo ""
    echo "================================================================"
}

# Main installation function
main() {
    echo ""
    echo "================================================================"
    echo "       Hecatoncheir Complete Build & Setup Script              "
    echo "================================================================"
    echo ""
    
    # Parse arguments
    BUILD_PROD=false
    SKIP_CPP=false
    
    while [[ $# -gt 0 ]]; do
        case $1 in
            --production|-p)
                BUILD_PROD=true
                shift
                ;;
            --skip-cpp)
                SKIP_CPP=true
                shift
                ;;
            --help|-h)
                echo "Usage: ./build-ui.sh [OPTIONS]"
                echo ""
                echo "Options:"
                echo "  --production, -p    Build frontend for production"
                echo "  --skip-cpp          Skip C++ server build"
                echo "  --help, -h          Show this help message"
                echo ""
                exit 0
                ;;
            *)
                print_error "Unknown option: $1"
                echo "Use --help for usage information"
                exit 1
                ;;
        esac
    done
    
    # Detect operating system
    detect_os
    echo ""
    
    # Step 1: Install Node.js if needed
    print_info "Step 1/7: Checking Node.js installation..."
    install_nodejs
    echo ""
    
    # Step 2: Build C++ server
    if [ "$SKIP_CPP" = false ]; then
        print_info "Step 2/7: Building C++ server..."
        build_cpp_server
        echo ""
    else
        print_warning "Step 2/7: Skipping C++ server build (--skip-cpp flag)"
        echo ""
    fi
    
    # Step 3: Install UI root dependencies
    print_info "Step 3/7: Setting up UI root..."
    install_ui_dependencies
    echo ""
    
    # Step 4: Install Frontend dependencies
    print_info "Step 4/7: Setting up Frontend (React + TypeScript)..."
    install_frontend
    echo ""
    
    # Step 5: Install Backend dependencies
    print_info "Step 5/7: Setting up Backend (Express + Node.js)..."
    install_backend
    echo ""
    
    # Step 6: Build production if requested
    if [ "$BUILD_PROD" = true ]; then
        print_info "Step 6/7: Building Frontend for production..."
        build_frontend
        echo ""
    else
        print_info "Step 6/7: Skipping production build (use --production flag to build)"
        echo ""
    fi
    
    # Step 7: Create startup scripts
    print_info "Step 7/7: Creating convenience scripts..."
    create_startup_scripts
    echo ""
    
    # Verify installation
    verify_installation
    echo ""
    
    # Print usage instructions
    print_usage
}

# Run main function
main "$@"