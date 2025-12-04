#!/bin/bash
# Script to publish vibesql crates to crates.io in correct dependency order
# Must be run from repository root

set -e  # Exit on error

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
    if [ "$DRY_RUN" = "--dry-run" ]; then
        cargo publish --dry-run
    else
        cargo publish
        # Wait a bit for crates.io to register the crate before publishing dependents
        echo "Waiting 10 seconds for crates.io to index $crate_name..."
        sleep 10
    fi

    cd - > /dev/null
    echo
}

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
