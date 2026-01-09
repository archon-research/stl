#!/bin/bash
set -e

echo "==> Installing Go tools..."

# Install Go development tools
make tools

echo "==> Installing project dependencies..."
cd /workspaces/stl/stl-verify
go mod download

echo "==> Done!"
