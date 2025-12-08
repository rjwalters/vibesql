#!/usr/bin/env bash
# Script to publish vibesql crates to crates.io in correct dependency order
# Must be run from repository root

# Note: We don't use 'set -e' because we handle errors manually
# (e.g., "already published" is not a fatal error)

DRY_RUN="${1:---dry-run}"  # Default to dry-run for safety

echo "Publishing vibesql crates to crates.io..."
echo "Mode: $DRY_RUN"
echo

# Array of crates in dependency order (bottom-up)
# Note: vibesql-sqllogictest has publish = false (test harness only)
CRATES=(
    "vibesql-types"
    "vibesql-ast"
    "vibesql-catalog"
    "vibesql-parser"
    "vibesql-storage"
    "vibesql-executor"
    "vibesql"
    "vibesql-cli"
    "vibesql-server"
    "vibesql-wasm-bindings"
    "vibesql-python-bindings"
)

CARGO_TOML="Cargo.toml"
PATCH_BACKUP=""

# Function to disable the [patch.crates-io] section for publishing
# This is needed because our vendored sqllogictest has publish = false
disable_patch() {
    if grep -q '^\[patch\.crates-io\]' "$CARGO_TOML"; then
        echo "Temporarily disabling [patch.crates-io] section for publishing..."
        # Save original before any modifications
        cp "$CARGO_TOML" "${CARGO_TOML}.bak"
        # Comment out the patch section (2 lines: header and the dependency)
        # Use sed with temp file for cross-platform compatibility
        sed 's/^\[patch\.crates-io\]/# [patch.crates-io]/' "$CARGO_TOML" > "${CARGO_TOML}.tmp" && mv "${CARGO_TOML}.tmp" "$CARGO_TOML"
        sed 's/^sqllogictest = { path/# sqllogictest = { path/' "$CARGO_TOML" > "${CARGO_TOML}.tmp" && mv "${CARGO_TOML}.tmp" "$CARGO_TOML"
        PATCH_BACKUP="1"
        # Update Cargo.lock to use crates.io version
        cargo update -p sqllogictest 2>/dev/null || true
    fi
}

# Function to restore the [patch.crates-io] section
restore_patch() {
    if [ -n "$PATCH_BACKUP" ] && [ -f "${CARGO_TOML}.bak" ]; then
        echo "Restoring [patch.crates-io] section..."
        mv "${CARGO_TOML}.bak" "$CARGO_TOML"
        # Restore Cargo.lock to use local version
        cargo update -p sqllogictest 2>/dev/null || true
    fi
}

# Ensure patch is restored even if script fails
trap restore_patch EXIT

# Function to publish a single crate
publish_crate() {
    local crate_name=$1
    local crate_path=$2

    echo "================================"
    echo "Publishing: $crate_name"
    echo "Path: $crate_path"
    echo "================================"

    cd "$crate_path"

    # Run cargo publish with or without --dry-run
    # --no-verify skips build verification which can fail due to workspace patches
    if [ "$DRY_RUN" = "--dry-run" ]; then
        cargo publish --dry-run --allow-dirty
    else
        # Capture output to check for "already uploaded" error
        local output
        local exit_code
        output=$(cargo publish --no-verify --allow-dirty 2>&1) && exit_code=0 || exit_code=$?
        echo "$output"

        if [ $exit_code -ne 0 ]; then
            if echo "$output" | grep -q "already uploaded\|already exists"; then
                echo ">>> Crate $crate_name already published, continuing..."
            else
                echo ">>> ERROR: Failed to publish $crate_name"
                cd - > /dev/null
                return 1
            fi
        else
            # Wait a bit for crates.io to register the crate before publishing dependents
            echo "Waiting 10 seconds for crates.io to index $crate_name..."
            sleep 10
        fi
    fi

    cd - > /dev/null
    echo
}

# Disable patch section before publishing
disable_patch

# Publish each crate in order
for crate in "${CRATES[@]}"; do
    if [ "$crate" = "vibesql" ]; then
        publish_crate "$crate" "."
    else
        publish_crate "$crate" "crates/$crate"
    fi
done

echo "================================"
echo "Publishing complete!"
echo "================================"

if [ "$DRY_RUN" = "--dry-run" ]; then
    echo
    echo "This was a DRY RUN. To actually publish, run:"
    echo "  $0 --publish"
fi
