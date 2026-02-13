#!/bin/bash
# Fix broken git worktree linkage in Docker Sandbox.
#
# When a git worktree directory is mounted into a sandbox, the .git file
# contains a gitdir pointer to the bare repo on the host, which doesn't
# exist inside the sandbox.
#
# Instead of modifying .git (which is a bind mount to the host), this script
# creates the expected directory structure at the exact path the .git file
# already points to, using sudo for parent directory permissions.
#
# Used as a Claude Code SessionStart hook.

exec < /dev/null

_dir="$PWD"
while [ "$_dir" != "/" ]; do
    if [ -f "$_dir/.git" ]; then
        _target=$(sed -n 's/^gitdir: //p' "$_dir/.git")
        if [ -z "$_target" ] || [ -d "$_target" ]; then
            exit 0  # not a worktree pointer, or target already exists
        fi

        _name=$(basename "$_dir")
        _bare=$(cd "$_dir" && dirname "$(dirname "$_target")")

        sudo git init --bare "$_bare" >/dev/null 2>&1
        sudo mkdir -p "$_target"
        printf '%s\n' "$_dir/.git" | sudo tee "$_target/gitdir" > /dev/null
        printf '%s\n' "ref: refs/heads/$_name" | sudo tee "$_target/HEAD" > /dev/null
        printf '%s\n' "../.." | sudo tee "$_target/commondir" > /dev/null
        sudo chown -R "$(id -u):$(id -g)" "$_bare"

        git -C "$_dir" config user.email "agent@sandbox"
        git -C "$_dir" config user.name "Agent"
        git -C "$_dir" add -A >/dev/null 2>&1
        git -C "$_dir" commit -m "Initial commit from worktree snapshot" >/dev/null 2>&1
        exit 0
    fi
    _dir=$(dirname "$_dir")
done
