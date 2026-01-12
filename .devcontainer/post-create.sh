#!/bin/bash
set -e

echo "==> Installing Go tools..."

# Install Go development tools
cd /workspaces/stl/stl-verify
make tools

echo "==> Installing project dependencies..."
go mod download

echo "==> Done!"
