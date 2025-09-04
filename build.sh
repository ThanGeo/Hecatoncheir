bash#!/bin/bash

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\e[0;33m'
NC='\033[0m'

# Check if TAU compilation is requested
USE_TAU=false
TAU_COMPILER_FLAGS="-fopenmp"

# Parse arguments
while [[ "$#" -gt 0 ]]; do
    case $1 in
        --use-tau) USE_TAU=true ;;
        --c-compiler) C_COMPILER="$2"; shift ;;
        --cxx-compiler) CXX_COMPILER="$2"; shift ;;
        *) echo -e "${RED}Unknown parameter passed: $1${NC}"; exit 1 ;;
    esac
    shift
done

# Default compiler paths
DEFAULT_C_COMPILER="mpicc.mpich"
DEFAULT_CXX_COMPILER="mpicxx.mpich"

# Use TAU wrapper compilers if requested
if [ "$USE_TAU" = true ]; then
    echo -e "${GREEN}Using TAU wrapper compilers${NC}"
    DEFAULT_C_COMPILER="tau_cc.sh"
    DEFAULT_CXX_COMPILER="tau_cxx.sh"
    # Make sure TAU_OPTIONS includes MPI tracking
    TAU_COMPILER_FLAGS="-tau_options=-optVerbose -optCompInst -optOpenMP -optMPI -optTRACE -optPROFILE"
fi

# Use environment variables if set, otherwise fall back to defaults
C_COMPILER=${C_COMPILER:-${DEFAULT_C_COMPILER}}
CXX_COMPILER=${CXX_COMPILER:-${DEFAULT_CXX_COMPILER}}

# Rest of your script remains the same...
echo -e "-- ${GREEN}Using C compiler: ${NC}${C_COMPILER}"
echo -e "-- ${GREEN}Using C++ compiler: ${NC}${CXX_COMPILER}"

# Check for available build systems
if command -v ninja &> /dev/null; then
    GENERATOR="Ninja"
    MAKE_PROGRAM=$(which ninja)
    echo -e "${GREEN}Ninja build system detected${NC}"
else
    GENERATOR="Unix Makefiles"
    MAKE_PROGRAM=$(which make)
    echo -e "${YELLOW}Ninja not found - falling back to Make${NC}"
fi

# Create build directory if it doesn't exist
mkdir -p build
cd build

# Run CMake only if cache doesn't exist or compiler changed
if [ ! -f "CMakeCache.txt" ] || \
   ! grep -q "CMAKE_C_COMPILER:FILEPATH=.*$C_COMPILER" CMakeCache.txt || \
   ! grep -q "CMAKE_CXX_COMPILER:FILEPATH=.*$CXX_COMPILER" CMakeCache.txt; then
    echo "-- Running cmake configuration..."
    
    # Clean previous build if compiler changed
    if [ -f "CMakeCache.txt" ]; then
        rm -f CMakeCache.txt
        rm -rf CMakeFiles
    fi
    
    cmake -G "$GENERATOR" \
        -D CMAKE_C_COMPILER="$C_COMPILER" \
        -D CMAKE_CXX_COMPILER="$CXX_COMPILER" \
        -D CMAKE_C_FLAGS="$TAU_COMPILER_FLAGS" \
        -D CMAKE_CXX_FLAGS="$TAU_COMPILER_FLAGS" \
        -D CMAKE_BUILD_TYPE=Release \
        -D CMAKE_MAKE_PROGRAM="$MAKE_PROGRAM" ..
else
    echo "-- CMake already configured. Skipping reconfiguration."
fi

# Build the project (incremental)
echo "-- Starting build..."
cmake --build . -- -j $(nproc) || {
    echo -e "${RED}Build failed${NC}"
    exit 1
}
cd ..

# the nodes(addresses) are stored in the file "nodes"
nodefile="nodes"
mapfile -t NODES < "$nodefile"

# get current directory in host machine (only the build directory)
SOURCE_DIR=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )/build

# directory where the code should be placed on each node
DEST_DIR="$SOURCE_DIR"
echo -e "-- ${GREEN}Host directory: ${NC} $SOURCE_DIR"
echo -e "-- ${GREEN}Dest directory: ${NC} $DEST_DIR"

# make the same directory in the nodes (if it does not exist)
for node in "${NODES[@]}"; do
  ssh "$node" "mkdir -p '$SOURCE_DIR'"
done

# sync function
sync_code() {
  local node=$1
  rsync -q -avz -e "ssh -i ~/.ssh/id_rsa -o StrictHostKeyChecking=no" --delete $SOURCE_DIR/ --exclude={'data/','docs/'} ${node}:${DEST_DIR}/
}

# sync for each node
echo "-- Syncing code across machines..."
for node in "${NODES[@]}"; do
  sync_code $node
done
echo -e "-- ${GREEN}Code sync across machines complete.${NC}"

# generate doxygen
DOC_SCRIPT="documentation.sh"
if [ -f "$DOC_SCRIPT" ] && [ -x "$DOC_SCRIPT" ]; then
    ./"$DOC_SCRIPT"
else
    echo "Error: $DOC_SCRIPT does not exist or is not executable."
    exit 1
fi
