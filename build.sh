#!/bin/bash

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\e[0;33m'
NC='\033[0m'


# create build directory
mkdir -p build

# build the code. It's the same for both local/cluster, but with different build types
cd build && cmake -D CMAKE_C_COMPILER=mpicc.mpich -D CMAKE_CXX_COMPILER=mpicxx.mpich -DCMAKE_BUILD_TYPE=$buildtype .. && cmake --build .
cd ..

# the nodes(addresses) are stored in the file "nodes"
nodefile="nodes"
mapfile -t NODES < "$nodefile"
# get current directory in host machine
SOURCE_DIR=$( cd "$(dirname "${BASH_SOURCE[0]}")" ; pwd -P )
# directory where the code should be placed on each node
DEST_DIR="$SOURCE_DIR"
echo -e "-- ${GREEN}Host directory: ${NC}" $SOURCE_DIR
echo -e "-- ${GREEN}Dest directory: ${NC}" $DEST_DIR
# make the same directory in the nodes (if it does not exist)
for node in "${NODES[@]}"; do
  ssh "$node" "mkdir -p '$SOURCE_DIR'"
done
# sync function
sync_code() {
  local node=$1
  rsync -q -avz -e "ssh -i ~/.ssh/id_rsa -o StrictHostKeyChecking=no" --delete --exclude="data/" $SOURCE_DIR/ ${node}:${DEST_DIR}/
}
echo "-- Syncing code across machines..."
# sync for each node and append to hostfile
hostfile="hostfile"
> $hostfile
for node in "${NODES[@]}"; do
  sync_code $node
  # append to hostfile
  echo "$node:1" >> $hostfile
done
echo -e "-- ${GREEN}Code sync across machines complete.${NC}"

# generate doxygen
DOC_SCRIPT="documentation.sh"
# Check if the documentation script exists and is executable
if [ -f "$DOC_SCRIPT" ] && [ -x "$DOC_SCRIPT" ]; then
    # execute
    ./"$DOC_SCRIPT"
else
    echo "Error: $DOC_SCRIPT does not exist or is not executable."
    exit 1
fi