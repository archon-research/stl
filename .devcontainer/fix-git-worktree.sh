#!/bin/bash
# Fix broken git worktree linkage in Docker Sandbox.
#
# When a git worktree directory is mounted into a sandbox, the .git file
# contains a gitdir pointer to the bare repo on the host, which doesn't
# exist inside the sandbox. This recreates the bare repo structure so
# git operations work.
#
# Used as a Claude Code SessionStart hook.

# Don't block on stdin — redirect from /dev/null
exec < /dev/null

_dir="$PWD"
while [ "$_dir" != "/" ]; do
    if [ -f "$_dir/.git" ]; then
        _target=$(sed -n 's/^gitdir: //p' "$_dir/.git")
        if [ -z "$_target" ] || [ -d "$_target" ]; then
            exit 0  # not a worktree pointer, or target exists — nothing to fix
        fi

        _name=$(basename "$_dir")
        _bare="/tmp/.git-bare"
        _wt="$_bare/worktrees/$_name"

        git init --bare "$_bare" >/dev/null 2>&1
        mkdir -p "$_wt"
        printf '%s\n' "$_dir/.git" > "$_wt/gitdir"
        printf '%s\n' "ref: refs/heads/$_name" > "$_wt/HEAD"
        printf '%s\n' "../.." > "$_wt/commondir"
        printf '%s\n' "gitdir: $_wt" > "$_dir/.git"

        git -C "$_dir" config user.email "agent@sandbox"
        git -C "$_dir" config user.name "Agent"
        git -C "$_dir" add -A >/dev/null 2>&1
        git -C "$_dir" commit -m "Initial commit from worktree snapshot" >/dev/null 2>&1
        exit 0
    fi
    _dir=$(dirname "$_dir")
done
