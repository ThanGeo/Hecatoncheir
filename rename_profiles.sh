#!/bin/bash
# Normalize TAU profiles per VM directory based on reference file ending in .0.0 but not starting with 0

PROFILING_DIR=$( cd "$(dirname "${BASH_SOURCE[0]}")" && pwd )/profiling
BASE_DIR="$PROFILING_DIR/all_profiles"

cd "$BASE_DIR" || { echo "Directory not found: $BASE_DIR"; exit 1; }

for VM_DIR in */; do
    VM_DIR="${VM_DIR%/}"
    echo "Processing VM directory: $VM_DIR"

    cd "$VM_DIR" || { echo "Cannot enter $VM_DIR"; continue; }

    # Find the reference profile (ends in .0.0) but doesn't start with 0
    # Look for all profile.*.0.0 files and exclude those starting with profile.0.
    REF_FILE=$(ls profile.*.0.0 2>/dev/null | grep -v "^profile\\.0\\." | head -n1)
    
    if [[ -z "$REF_FILE" ]]; then
        echo "  No non-zero reference profile (profile.[1-9]*.0.0) found, skipping."
        cd ..
        continue
    fi

    # Extract the reference rank (first number after profile.)
    REF_RANK=$(echo "$REF_FILE" | awk -F. '{print $2}')
    echo "  Reference rank: $REF_RANK (from $REF_FILE)"
    
    # Calculate the new base rank by appending 0
    NEW_BASE_RANK="${REF_RANK}0"
    echo "  New base rank: $NEW_BASE_RANK"

    # Process all files
    for f in profile.*; do
        # Skip directories
        if [[ -d "$f" ]]; then
            continue
        fi

        # Extract all parts of the filename
        # Format: profile.X.Y.Z
        RANK=$(echo "$f" | awk -F. '{print $2}')
        MIDDLE=$(echo "$f" | awk -F. '{print $3}')
        SUFFIX=$(echo "$f" | awk -F. '{print $4}')
        
        # If this file already has the reference rank pattern, skip it
        if [[ "$RANK" == "$REF_RANK" ]]; then
            echo "  Keeping (already has reference rank): $f"
            continue
        fi
        
        # For files that don't have the reference rank, rename them using the new base rank
        NEW_NAME="profile.${NEW_BASE_RANK}.${MIDDLE}.${SUFFIX}"

        echo "  Renaming $f -> $NEW_NAME"
        mv -n "$f" "$NEW_NAME"
    done

    cd ..
done

echo "All VM directories normalized."