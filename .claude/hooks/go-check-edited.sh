#!/usr/bin/env bash
# PostToolUse (Write|Edit) — typecheck the package a just-edited .go file belongs to,
# via `make check-file`, and record the file for the Stop gate.
#
# Non-blocking: mid-refactor a package is legitimately broken between edits, so the
# result is injected as context rather than an error. go-check-session.sh is the gate
# that actually holds.
set -uo pipefail
export PATH="$(go env GOPATH 2>/dev/null)/bin:$PATH"

payload=$(cat)
file=$(printf '%s' "$payload" | jq -r '.tool_response.filePath // .tool_input.file_path // empty' 2>/dev/null)
[ -n "$file" ] || exit 0
case "$file" in *.go) ;; *) exit 0 ;; esac
[ -f "$file" ] || exit 0

dir=$(cd "$(dirname "$file")" 2>/dev/null && pwd) || exit 0
root=$dir
while [ "$root" != "/" ] && [ ! -f "$root/go.mod" ]; do root=$(dirname "$root"); done
[ -f "$root/go.mod" ] || exit 0
[ -f "$root/Makefile" ] || exit 0

# Remember the file so the Stop gate knows which packages this session touched.
session=$(printf '%s' "$payload" | jq -r '.session_id // "unknown"' 2>/dev/null)
state_dir="${TMPDIR:-/tmp}/stl-go-touched"
mkdir -p "$state_dir"
printf '%s\n' "$file" >> "$state_dir/$session"

if out=$(make -C "$root" --no-print-directory check-file FILE="$file" 2>&1); then
  exit 0
fi

jq -n --arg o "$out" --arg f "$file" '{
  hookSpecificOutput: {
    hookEventName: "PostToolUse",
    additionalContext: ("make check-file failed for " + $f + ":\n" + $o + "\nFix this before moving on.")
  }
}'
exit 0
