#!/bin/bash
# Gather TAU profiles from multiple VMs, merge them, and prepare for analysis

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m'

# === CONFIG ===
# Read nodes from nodes.txt (one per line)
SCRIPT_DIR=$( cd "$(dirname "${BASH_SOURCE[0]}")" && pwd )
mkdir -p $SCRIPT_DIR

NODES_FILE="$SCRIPT_DIR/nodes"
if [[ ! -f "$NODES_FILE" ]]; then
    echo -e "${RED}ERROR:${NC} nodes not found in $SCRIPT_DIR"
    exit 1
fi
mapfile -t NODES < "$NODES_FILE"

# Directories
SOURCE_DIR=$SCRIPT_DIR
PROFILING_DIR="$SCRIPT_DIR/profiling"
REMOTE_DIR="$SOURCE_DIR/spawn-1"

LOCAL_DIR="$PROFILING_DIR/all_profiles"
MERGED_DIR="$PROFILING_DIR/merged_profiles"

# SSH options
SSH_OPTS="-o StrictHostKeyChecking=no"

# === SCRIPT ===
echo -e "${GREEN}>> Collecting TAU profiles from nodes...${NC}"
mkdir -p "$LOCAL_DIR"

for node in "${NODES[@]}"; do
    echo -e "${YELLOW}Syncing from $node...${NC}"
    NODE_DIR="$LOCAL_DIR/$node"
    mkdir -p "$NODE_DIR"
    rsync -avz -e "ssh $SSH_OPTS" "${node}:${REMOTE_DIR}/*" "$NODE_DIR"/ 2>/dev/null
done

echo -e "${GREEN}>> Profiles collected in ${LOCAL_DIR}${NC}"

# Merge profiles
# echo -e "${GREEN}>> Merging profiles with tau_merge...${NC}"
# rm -rf "$MERGED_DIR"
# mkdir -p "$MERGED_DIR"
# tau_merge "$LOCAL_DIR" "$MERGED_DIR"

# # Pack into .ppk for paraprof
# echo -e "${GREEN}>> Creating portable pack file (merged.ppk)...${NC}"
# paraprof --pack=merged.ppk "$MERGED_DIR"

# echo -e "${GREEN}All done!${NC}"
# echo -e " - Collected profiles: ${LOCAL_DIR}"
# echo -e " - Merged profiles:   ${MERGED_DIR}"
# echo -e " - Portable profile:  merged.ppk"
# echo -e "\nRun ${YELLOW}paraprof merged.ppk${NC} to analyze."
