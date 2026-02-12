#!/bin/bash
set -e

echo "==> Setting up bare repo for git worktree..."
# This repo uses a bare-repo + worktrees layout (see https://nicknisi.com/posts/git-worktrees/).
# Only the worktree directory is mounted, so we recreate the bare repo and worktree linkage
# so that git commands work inside the sandbox.
BARE_DIR="/Users/knaekbroed/projects/worktrees/stl/.bare"
WORKTREE_DIR="$BARE_DIR/worktrees/watcher-250-ms"
WORKTREE_ROOT="/Users/knaekbroed/projects/worktrees/stl/watcher-250-ms"

if [ ! -d "$BARE_DIR" ]; then
    sudo git init --bare "$BARE_DIR"
    sudo mkdir -p "$WORKTREE_DIR"
    echo "$WORKTREE_ROOT/.git" | sudo tee "$WORKTREE_DIR/gitdir" > /dev/null
    echo "ref: refs/heads/watcher-250-ms" | sudo tee "$WORKTREE_DIR/HEAD" > /dev/null
    echo "../.." | sudo tee "$WORKTREE_DIR/commondir" > /dev/null
    sudo chown -R "$(id -u):$(id -g)" "$BARE_DIR"
    git -C "$WORKTREE_ROOT" config user.email "agent@sandbox"
    git -C "$WORKTREE_ROOT" config user.name "Agent"
    git -C "$WORKTREE_ROOT" add -A
    git -C "$WORKTREE_ROOT" commit -m "Initial commit from worktree snapshot"
    echo "==> Bare repo created and initial commit made."
else
    echo "==> Bare repo already exists, skipping."
fi

echo "==> Installing Go tools..."

# Install Go development tools
cd /workspaces/stl/stl-verify
make tools

echo "==> Installing project dependencies..."
go mod download

echo "==> Done!"
