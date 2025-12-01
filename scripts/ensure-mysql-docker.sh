#!/bin/bash
# Ensure MySQL Docker container is running for benchmarks
#
# This script starts the MySQL Docker container if it's not already running
# and waits for it to be ready. It's designed to be sourced or called before
# running benchmarks to automatically enable MySQL comparisons.
#
# Usage:
#   ./scripts/ensure-mysql-docker.sh          # Start MySQL and print MYSQL_URL
#   eval $(./scripts/ensure-mysql-docker.sh)  # Start MySQL and export MYSQL_URL
#   source <(./scripts/ensure-mysql-docker.sh --export)  # Same, more explicit
#
# Exit codes:
#   0 - MySQL is ready, MYSQL_URL printed
#   1 - Docker not available or MySQL failed to start

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/mysql-benchmark/docker-compose.yml"
CONTAINER_NAME="vibesql-mysql-tpch"
MYSQL_PORT=3306
MAX_WAIT=60

# Colors (only if terminal)
if [ -t 2 ]; then
    GREEN='\033[0;32m'
    YELLOW='\033[1;33m'
    RED='\033[0;31m'
    NC='\033[0m'
else
    GREEN=''
    YELLOW=''
    RED=''
    NC=''
fi

log() {
    echo -e "$1" >&2
}

# Check if Docker is available
if ! command -v docker &> /dev/null; then
    log "${RED}Error: Docker is not installed or not in PATH${NC}"
    log "Install Docker to enable MySQL benchmarks: https://docs.docker.com/get-docker/"
    exit 1
fi

# Check if Docker daemon is running
if ! docker info &> /dev/null; then
    log "${RED}Error: Docker daemon is not running${NC}"
    log "Start Docker Desktop or the Docker daemon to enable MySQL benchmarks"
    exit 1
fi

# Check if container exists and is running
if docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
    log "${GREEN}MySQL container already running${NC}"
else
    # Check if container exists but is stopped
    if docker ps -a --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        log "${YELLOW}Starting existing MySQL container...${NC}"
        docker start "$CONTAINER_NAME" > /dev/null
    else
        log "${YELLOW}Creating new MySQL container...${NC}"
        if [ ! -f "$COMPOSE_FILE" ]; then
            log "${RED}Error: docker-compose.yml not found at $COMPOSE_FILE${NC}"
            exit 1
        fi
        docker compose -f "$COMPOSE_FILE" up -d
    fi
fi

# Wait for MySQL to be ready
log "Waiting for MySQL to be ready..."
waited=0
while [ $waited -lt $MAX_WAIT ]; do
    if docker exec "$CONTAINER_NAME" mysqladmin ping -h localhost --silent &>/dev/null; then
        break
    fi
    sleep 1
    waited=$((waited + 1))
    # Progress indicator every 5 seconds
    if [ $((waited % 5)) -eq 0 ]; then
        log "  Still waiting... (${waited}s)"
    fi
done

if [ $waited -ge $MAX_WAIT ]; then
    log "${RED}Error: MySQL failed to start within ${MAX_WAIT}s${NC}"
    log "Check Docker logs: docker logs $CONTAINER_NAME"
    exit 1
fi

log "${GREEN}MySQL is ready!${NC}"

# Create benchmark database if it doesn't exist
log "Ensuring benchmark database exists..."
docker exec "$CONTAINER_NAME" mysql -uroot -e "CREATE DATABASE IF NOT EXISTS sysbench" 2>/dev/null || true
docker exec "$CONTAINER_NAME" mysql -uroot -e "CREATE DATABASE IF NOT EXISTS tpch" 2>/dev/null || true
docker exec "$CONTAINER_NAME" mysql -uroot -e "CREATE DATABASE IF NOT EXISTS tpcc" 2>/dev/null || true

# Output the MYSQL_URL
# Using 127.0.0.1 instead of localhost to force TCP connection (avoids socket issues)
MYSQL_URL="mysql://root@127.0.0.1:${MYSQL_PORT}/sysbench"

# Check for --export flag to output in sourceable format
if [ "${1:-}" = "--export" ]; then
    echo "export MYSQL_URL='$MYSQL_URL'"
else
    echo "$MYSQL_URL"
fi

log "${GREEN}MySQL URL: $MYSQL_URL${NC}"
