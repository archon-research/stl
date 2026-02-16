#!/bin/bash
# Fix broken git worktree linkage in Docker Sandbox.
#
# When a git worktree directory is mounted into a sandbox, the .git file
# contains a gitdir pointer to the bare repo on the host, which doesn't
# exist inside the sandbox.
#
# Instead of modifying .git (which is a bind mount to the host), this script
# creates the minimal directory structure at the exact path the .git file
# already points to. Uses sudo only for `install -d` to create directories
# with correct ownership — no recursive chown, no running git as root.
#
# Used as a Claude Code SessionStart hook.

exec < /dev/null

_dir="$PWD"
while [ "$_dir" != "/" ]; do
    if [ -f "$_dir/.git" ]; then
        _target=$(sed -n 's/^gitdir: //p' "$_dir/.git")
        [ -n "$_target" ] || exit 0  # not a worktree pointer

        _name=$(basename "$_dir")
        _bare=$(cd "$_dir" && dirname "$(dirname "$_target")")

        # Validate worktree metadata — repair if anything is missing or inconsistent.
        _needs_repair=false
        [ -d "$_bare/objects" ]                                        || _needs_repair=true
        [ -d "$_bare/refs" ]                                           || _needs_repair=true
        [ -f "$_bare/HEAD" ]                                           || _needs_repair=true
        [ -f "$_target/gitdir" ] && [ "$(cat "$_target/gitdir")" = "$_dir/.git" ] || _needs_repair=true
        [ -f "$_target/HEAD" ]                                         || _needs_repair=true
        [ -f "$_target/commondir" ] && [ "$(cat "$_target/commondir")" = "../.." ] || _needs_repair=true

        [ "$_needs_repair" = true ] || exit 0

        # Create minimal bare repo + worktree metadata dirs with correct ownership.
        sudo install -d -o "$(id -u)" -g "$(id -g)" \
            "$_bare/objects/pack" \
            "$_bare/refs/heads" \
            "$_bare/refs/tags" \
            "$_target"

        # Write metadata as current user (dirs are already owned by us).
        printf '%s\n' "ref: refs/heads/main"   > "$_bare/HEAD"
        printf '%s\n' "$_dir/.git"             > "$_target/gitdir"
        printf '%s\n' "ref: refs/heads/$_name" > "$_target/HEAD"
        printf '%s\n' "../.."                  > "$_target/commondir"
        exit 0
    fi
    _dir=$(dirname "$_dir")
done
