#!/bin/bash

MACHINES_FILE="nodes"
SRC_DIR="/home/hec/tau-2.34.1"
TARGET_DIR="/home/hec/"

# Get the hostname or IP of the current machine
CURRENT_HOST=$(hostname)

# Read the machines file into an array, removing empty lines and leading/trailing whitespace
mapfile -t MACHINES < "$MACHINES_FILE"

# Iterate over the array of machines
for MACHINE in "${MACHINES[@]}"; do
    # Skip syncing/deleting on the current host
    if [ "$MACHINE" == "$CURRENT_HOST" ]; then
        echo "Skipping current machine: $MACHINE"
        continue
    fi

    # Skip empty lines that might have been left in the file
    if [ -z "$MACHINE" ]; then
        continue
    fi

    echo "=== Processing $MACHINE ==="

    # Delete the specific directory on the remote machine
    ssh "$MACHINE" "rm -rf $TARGET_DIR/$(basename "$SRC_DIR")"

    # Synchronize the directory using rsync
    rsync -avz --progress "$SRC_DIR" "$MACHINE:$TARGET_DIR"

    echo "=== $MACHINE done ==="
done

echo "All remote machines are synchronized."