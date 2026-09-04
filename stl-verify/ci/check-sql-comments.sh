#!/usr/bin/env bash
#
# check-sql-comments.sh — fail when a SQL `--` comment block runs longer than
# SQL_COMMENT_MAX_LINES (default 3).
#
# Why: comment blocks in db/migrations/ had grown to 100 lines, and no linter
# reaches them — golangci-lint's funlen counts lines inside Go functions, so it
# sees neither SQL nor a file header.
#
# Scope is deliberately files ADDED in this branch, not the tree: 107 of the 125
# migrations already on main carry a longer block, and they cannot be corrected.
# The migrator sha256s the whole file (verifyChecksum), so editing a comment in an
# applied migration makes every database that has it refuse to migrate.
#
# Usage:
#   check-sql-comments.sh [file ...]     check exactly these files
#   check-sql-comments.sh                check *.sql added vs BASE (default origin/main)
#
# Exempt one block with `-- lint:allow-long-comment` on the line directly above it.
set -euo pipefail

# git reports repo-root-relative paths, and this runs from stl-verify/ via the
# Makefile, so resolve everything from the root.
cd "$(git rev-parse --show-toplevel)"

MAX="${SQL_COMMENT_MAX_LINES:-3}"
BASE="${BASE:-origin/main}"
EXEMPT='-- lint:allow-long-comment'

files=()
if [ "$#" -gt 0 ]; then
  files=("$@")
else
  if ! git rev-parse --verify --quiet "$BASE" >/dev/null; then
    echo "check-sql-comments: $BASE not found; fetch it or pass files explicitly" >&2
    exit 2
  fi
  base_sha=$(git merge-base "$BASE" HEAD)
  # Added only. A modified migration is a separate (and worse) problem, which the
  # migrator's checksum already blocks. `read` loop rather than mapfile: bash 3.2
  # ships without mapfile, and its absence made this exit 0 with no files checked.
  while IFS= read -r path; do
    [ -n "$path" ] && files+=("$path")
  done < <(git diff --name-only --diff-filter=A "${base_sha}...HEAD" -- '*.sql')
fi

if [ "${#files[@]}" -eq 0 ]; then
  exit 0
fi

status=0
for f in "${files[@]}"; do
  [ -n "$f" ] || continue
  [ -f "$f" ] || continue
  case "$f" in *.sql) ;; *) continue ;; esac

  # awk reports: start-line, run-length, first line of the run — for runs over MAX
  # that are not directly preceded by the exempt directive.
  while IFS='|' read -r start len text; do
    [ -n "$start" ] || continue
    printf '%s:%s: comment block of %s lines exceeds %s\n    %s\n' "$f" "$start" "$len" "$MAX" "$text" >&2
    status=1
  done < <(awk -v max="$MAX" -v exempt="$EXEMPT" '
    function flush() {
      if (n > max && !exempted) printf "%d|%d|%s\n", start, n, first
      n = 0; exempted = 0
    }
    {
      line = $0
      sub(/^[ \t]+/, "", line)
      if (line ~ /^--/) {
        if (n == 0) { start = NR; first = substr(line, 1, 90); exempted = (prev == exempt) }
        n++
        # the directive itself is not part of the block it exempts
        if (line == exempt) { n = 0; exempted = 0; prev = line; next }
      } else {
        flush()
      }
      prev = line
    }
    END { flush() }
  ' "$f")
done

if [ "$status" -ne 0 ]; then
  cat >&2 <<'MSG'

SQL comment blocks are capped at 3 lines, and a comment should say something the
code cannot. Put measurements, counts and rationale in the PR or the ticket: a
migration is immutable once applied, so a figure recorded in one can never be
corrected.

To keep a longer block, put this on the line directly above it:
    -- lint:allow-long-comment
MSG
fi
exit "$status"
