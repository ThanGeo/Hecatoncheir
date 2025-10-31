#!/bin/bash
# Gather TAU profiles from multiple VMs, merge them, and prepare for analysis

RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[0;33m'
NC='\033[0m'

# === CONFIG ===
# Read nodes from nodes.txt (one per line)
SCRIPT_DIR=$( cd "$(dirname "${BASH_SOURCE[0]}")" && pwd )

NODES_FILE="$SCRIPT_DIR/nodes"
if [[ ! -f "$NODES_FILE" ]]; then
    echo -e "${RED}ERROR:${NC} nodes not found in $SCRIPT_DIR"
    exit 1
fi
mapfile -t NODES < "$NODES_FILE"

# Directories
SOURCE_DIR="$SCRIPT_DIR/spawn-1"
PROFILING_DIR="$SCRIPT_DIR/profiling"
LOCAL_DIR="$PROFILING_DIR/all_profiles"
REPORT_DIR="$PROFILING_DIR/merged_reports"

# SSH options
SSH_OPTS="-o StrictHostKeyChecking=no"

# === SCRIPT ===
echo -e "${GREEN}>> Collecting TAU profiles from nodes...${NC}"
mkdir -p "$LOCAL_DIR"

for node in "${NODES[@]}"; do
    echo -e "${YELLOW}Syncing from $node...${NC}"
    # Sync directly to the all_profiles directory without subdirectories
    rsync -avz -e "ssh $SSH_OPTS" "${node}:${SOURCE_DIR}/*" "$LOCAL_DIR"/ 2>/dev/null
done

echo -e "${GREEN}>> Profiles collected in ${LOCAL_DIR}${NC}"

# === Merge profiles ===
# echo -e "${GREEN}>> Merging TAU profiles...${NC}"
# cd "$LOCAL_DIR" || exit 1

# Merge profiles into a single database
# tau_treemerge.pl

# echo -e "${GREEN}>> Profiles merged.${NC}"

# # === Generate reports ===
# echo -e "${GREEN}>> Generating summary reports...${NC}"
# mkdir -p "$REPORT_DIR"

# # CPU & memory summary
# pprof -s > "$REPORT_DIR/summary.txt"

# # Callpath report
# pprof -c > "$REPORT_DIR/callpath.txt"

# # All functions report (optional)
# pprof -a > "$REPORT_DIR/all_functions.txt"

# Memory-specific lines
# grep '\[Memory\]' "$REPORT_DIR/all_functions.txt" > "$REPORT_DIR/memory.txt"

# echo -e "${GREEN}>> Reports generated in ${REPORT_DIR}${NC}"
echo -e "${GREEN}All done!${NC}"
