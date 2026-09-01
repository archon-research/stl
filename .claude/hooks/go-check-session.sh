#!/usr/bin/env bash
# Stop — hold the end of the turn until the Go files touched this session pass
# `make check-pkgs` (gofmt -s + golangci-lint, the same checks ci-checks runs).
#
# Blocks at most MAX_BLOCKS times per session, so a finding that cannot be satisfied
# never traps the session in a loop.
set -uo pipefail
export PATH="$(go env GOPATH 2>/dev/null)/bin:$PATH"
MAX_BLOCKS=2

payload=$(cat)
session=$(printf '%s' "$payload" | jq -r '.session_id // "unknown"' 2>/dev/null)
state_dir="${TMPDIR:-/tmp}/stl-go-touched"
list="$state_dir/$session"
count_file="$state_dir/$session.blocks"
[ -s "$list" ] || exit 0

files=$(sort -u "$list" | while IFS= read -r f; do [ -f "$f" ] && printf '%s\n' "$f"; done)
if [ -z "$files" ]; then rm -f "$list" "$count_file"; exit 0; fi

pairs=$(printf '%s\n' "$files" | while IFS= read -r f; do
  d=$(cd "$(dirname "$f")" 2>/dev/null && pwd) || continue
  r=$d
  while [ "$r" != "/" ] && [ ! -f "$r/go.mod" ]; do r=$(dirname "$r"); done
  [ -f "$r/go.mod" ] && [ -f "$r/Makefile" ] || continue
  printf '%s\t.%s\n' "$r" "${d#$r}"
done | sort -u)

problems=""
if [ -n "$pairs" ]; then
  roots=$(printf '%s\n' "$pairs" | cut -f1 | sort -u)
  while IFS= read -r root; do
    [ -n "$root" ] || continue
    rels=$(printf '%s\n' "$pairs" | awk -F'\t' -v r="$root" '$1==r{printf "%s ", $2}')
    if ! out=$(make -C "$root" --no-print-directory check-pkgs PKGS="$rels" 2>&1); then
      problems="${problems}make check-pkgs failed in ${root}:
${out}

"
    fi
  done <<< "$roots"
fi

if [ -z "$problems" ]; then
  rm -f "$list" "$count_file"
  exit 0
fi

blocks=$(cat "$count_file" 2>/dev/null || echo 0)
blocks=$((blocks + 1))
printf '%s' "$blocks" > "$count_file"

if [ "$blocks" -gt "$MAX_BLOCKS" ]; then
  jq -n --arg p "$problems" --arg n "$MAX_BLOCKS" \
    '{systemMessage: ("Go checks still failing after " + $n + " attempts — not blocking again:\n" + $p)}'
  rm -f "$list" "$count_file"
  exit 0
fi

printf 'Go checks failed on files edited this session. Fix these before finishing:\n\n%s' "$problems" >&2
exit 2
