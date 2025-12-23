#!/bin/bash
set -e

echo "==> Installing Go tools..."

# Install Go development tools
go install golang.org/x/tools/gopls@latest
go install golang.org/x/tools/cmd/goimports@latest
go install github.com/golangci/golangci-lint/cmd/golangci-lint@latest
go install honnef.co/go/tools/cmd/staticcheck@latest
go install golang.org/x/vuln/cmd/govulncheck@latest
go install github.com/go-delve/delve/cmd/dlv@latest

echo "==> Installing project dependencies..."
cd /workspaces/stl/stl-verify
go mod download

echo "==> Done!"
