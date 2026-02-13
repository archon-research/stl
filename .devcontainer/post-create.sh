#!/bin/bash
set -e

echo "==> Setting up bare repo for git worktree..."
# This repo uses a bare-repo + worktrees layout (see https://nicknisi.com/posts/git-worktrees/).
# Only the worktree directory is mounted, so we recreate the bare repo and worktree linkage
# so that git commands work inside the sandbox.
WORKTREE_ROOT="${DEVCONTAINER_WORKSPACE_FOLDER:-$(pwd)}"
WORKTREE_NAME=$(basename "$WORKTREE_ROOT")
BARE_DIR="$(dirname "$WORKTREE_ROOT")/.bare"
WORKTREE_DIR="$BARE_DIR/worktrees/$WORKTREE_NAME"

validate_worktree() {
    [ -f "$WORKTREE_DIR/gitdir" ] && [ "$(cat "$WORKTREE_DIR/gitdir")" = "$WORKTREE_ROOT/.git" ] &&
    [ -f "$WORKTREE_DIR/HEAD" ] &&
    [ -f "$WORKTREE_DIR/commondir" ] && [ "$(cat "$WORKTREE_DIR/commondir")" = "../.." ] &&
    [ -f "$WORKTREE_ROOT/.git" ] && [ "$(cat "$WORKTREE_ROOT/.git")" = "gitdir: $WORKTREE_DIR" ]
}

setup_worktree_metadata() {
    mkdir -p "$WORKTREE_DIR"
    echo "$WORKTREE_ROOT/.git" > "$WORKTREE_DIR/gitdir"
    echo "ref: refs/heads/$WORKTREE_NAME" > "$WORKTREE_DIR/HEAD"
    echo "../.." > "$WORKTREE_DIR/commondir"
    echo "gitdir: $WORKTREE_DIR" > "$WORKTREE_ROOT/.git"
}

if [ ! -d "$BARE_DIR" ]; then
    git init --bare "$BARE_DIR"
    setup_worktree_metadata
    echo "==> Bare repo created."
elif ! validate_worktree; then
    echo "==> Bare repo exists but worktree metadata is inconsistent, repairing..."
    setup_worktree_metadata
    echo "==> Worktree metadata repaired."
else
    echo "==> Bare repo and worktree metadata already consistent."
fi

# Create initial commit only if repo has no commits yet.
if ! git -C "$WORKTREE_ROOT" rev-parse --quiet --verify HEAD > /dev/null 2>&1; then
    git -C "$WORKTREE_ROOT" config user.email "agent@sandbox"
    git -C "$WORKTREE_ROOT" config user.name "Agent"
    git -C "$WORKTREE_ROOT" add -A
    git -C "$WORKTREE_ROOT" commit -m "Initial commit from worktree snapshot"
    echo "==> Initial commit created."
fi

echo "==> Installing Go tools..."

# Install Go development tools
cd /workspaces/stl/stl-verify
make tools

echo "==> Installing project dependencies..."
go mod download

echo "==> Done!"
