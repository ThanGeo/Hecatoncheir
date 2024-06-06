#! /bin/bash

# build
if [ ! -d "build" ]; then
    mkdir -p build
fi
cd build && cmake -D CMAKE_C_COMPILER=mpicc.mpich -D CMAKE_CXX_COMPILER=mpicxx.mpich -DCMAKE_BUILD_TYPE=Release .. && cmake --build .

# and sync to all workers
# list of nodes (may skip host node)
NODES=("node1" "node2" "node3")

# directory containing the code on the master node
SOURCE_DIR="/home/hec/thanasis/Hecatoncheir"

# directory where the code should be placed on each node
DEST_DIR="/home/hec/thanasis/Hecatoncheir"

# sync fynction
sync_code() {
  local node=$1
  rsync -q -avz -e "ssh -i ~/.ssh/id_rsa -o StrictHostKeyChecking=no" --delete $SOURCE_DIR/ ${node}:${DEST_DIR}/
}

# sync for each node
for node in "${NODES[@]}"; do
  # echo "Syncing code to $node..."
  sync_code $node
done

echo "Code sync complete across machines."