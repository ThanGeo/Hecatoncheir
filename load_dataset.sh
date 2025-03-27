#!/bin/bash

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\e[0;33m'
NC='\033[0m'

# Read file path from user input
if [[ -z "$1" ]]; then
  echo -e "${RED}Error: No file path provided.${NC}"
  echo "Usage: $0 <absolute_file_path> [destination_path]"
  exit 1
fi

FILE_PATH="$1"

# Check if FILE_PATH is absolute
if [[ "$FILE_PATH" != /* ]]; then
  echo -e "${RED}Error: Please provide an absolute file path.${NC}"
  exit 1
fi

# Extract directory from given file path
SRC_DIR=$(dirname "$FILE_PATH")
FILE_NAME=$(basename "$FILE_PATH")
DEST_PATH="$SRC_DIR"

# Read nodes from file
NODE_FILE="nodes"
if [[ ! -f "$NODE_FILE" ]]; then
  echo -e "${RED}Error: Node file '$NODE_FILE' not found.${NC}"
  exit 1
fi

mapfile -t NODES < "$NODE_FILE"

echo -e "-- ${GREEN}Source file: ${NC}$FILE_PATH"
echo -e "-- ${GREEN}Destination path: ${NC}$DEST_PATH"

# Ensure directories exist on nodes
for node in "${NODES[@]}"; do
  # echo -e "${YELLOW}Ensuring directory exists on ${node}:${DEST_PATH}${NC}"
  ssh "$node" "mkdir -p '$DEST_PATH'"
done

# Rsync file to each node
sync_file() {
  local node=$1
  echo -e "${YELLOW}Syncing file to ${node}:${DEST_PATH}/${FILE_NAME}${NC}"
  rsync -q -avz -e "ssh -i ~/.ssh/id_rsa -o StrictHostKeyChecking=no" "$FILE_PATH" "${node}:${DEST_PATH}/"
}

echo "-- Starting file sync..."
for node in "${NODES[@]}"; do
  sync_file "$node"
done
echo -e "-- ${GREEN}File loading across machines complete.${NC}"
