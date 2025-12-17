#!/bin/bash
# Start Amp and its dependencies

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_DIR="$(dirname "$SCRIPT_DIR")"

echo "Starting PostgreSQL..."
cd "$PROJECT_DIR"
docker-compose up -d

echo "Waiting for PostgreSQL to be ready..."
sleep 3

echo "Starting Amp server..."
ampd server --config "$PROJECT_DIR/amp.toml"
