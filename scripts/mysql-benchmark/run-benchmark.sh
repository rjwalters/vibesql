#!/bin/bash
# MySQL TPC-H Benchmark Runner
#
# This script starts MySQL via Docker, loads TPC-H data, and runs benchmarks.
#
# Usage:
#   ./scripts/mysql-benchmark/run-benchmark.sh [--scale-factor 0.01] [--iterations 3]
#
# Prerequisites:
#   - Docker installed and running
#   - Python 3 with mysql-connector-python: pip install mysql-connector-python

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PROJECT_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

# Default values
SCALE_FACTOR=0.01
ITERATIONS=3
SKIP_DOCKER=false
OUTPUT_FILE=""

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        --scale-factor)
            SCALE_FACTOR="$2"
            shift 2
            ;;
        --iterations)
            ITERATIONS="$2"
            shift 2
            ;;
        --skip-docker)
            SKIP_DOCKER=true
            shift
            ;;
        --output)
            OUTPUT_FILE="$2"
            shift 2
            ;;
        -h|--help)
            echo "Usage: $0 [options]"
            echo ""
            echo "Options:"
            echo "  --scale-factor SF   TPC-H scale factor (default: 0.01)"
            echo "  --iterations N      Benchmark iterations per query (default: 3)"
            echo "  --skip-docker       Skip Docker setup (use existing MySQL)"
            echo "  --output FILE       Save results to JSON file"
            echo "  -h, --help          Show this help"
            exit 0
            ;;
        *)
            echo "Unknown option: $1"
            exit 1
            ;;
    esac
done

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}=== MySQL TPC-H Benchmark ===${NC}"
echo "Scale factor: $SCALE_FACTOR"
echo "Iterations: $ITERATIONS"
echo ""

# Check for Python and mysql-connector
if ! command -v python3 &> /dev/null; then
    echo "Error: python3 not found"
    exit 1
fi

python3 -c "import mysql.connector" 2>/dev/null || {
    echo -e "${YELLOW}Installing mysql-connector-python...${NC}"
    pip3 install mysql-connector-python
}

# Start MySQL via Docker
if [ "$SKIP_DOCKER" = false ]; then
    echo -e "${YELLOW}Starting MySQL via Docker...${NC}"

    # Check if container exists
    if docker ps -a --format '{{.Names}}' | grep -q 'vibesql-mysql-tpch'; then
        # Check if it's running
        if docker ps --format '{{.Names}}' | grep -q 'vibesql-mysql-tpch'; then
            echo "MySQL container already running"
        else
            echo "Starting existing MySQL container..."
            docker start vibesql-mysql-tpch
        fi
    else
        echo "Creating new MySQL container..."
        cd "$SCRIPT_DIR"
        docker compose up -d
    fi

    # Wait for MySQL to be ready
    echo -n "Waiting for MySQL to be ready"
    for i in {1..30}; do
        if docker exec vibesql-mysql-tpch mysqladmin ping -h localhost --silent 2>/dev/null; then
            echo ""
            echo -e "${GREEN}MySQL is ready!${NC}"
            break
        fi
        echo -n "."
        sleep 1
    done

    if [ $i -eq 30 ]; then
        echo ""
        echo "Error: MySQL failed to start"
        exit 1
    fi
fi

# Build output args
OUTPUT_ARGS=""
if [ -n "$OUTPUT_FILE" ]; then
    OUTPUT_ARGS="--output $OUTPUT_FILE"
fi

# Run benchmark
echo ""
echo -e "${YELLOW}Running TPC-H benchmarks...${NC}"
cd "$PROJECT_ROOT"
python3 scripts/mysql-benchmark/bench_mysql_tpch.py \
    --scale-factor "$SCALE_FACTOR" \
    --iterations "$ITERATIONS" \
    $OUTPUT_ARGS

echo ""
echo -e "${GREEN}Benchmark complete!${NC}"

# Print comparison hint
echo ""
echo "To compare with VibeSQL, run:"
echo "  ./scripts/bench-tpch-isolated.sh 30 /tmp/vibesql_results.txt"
