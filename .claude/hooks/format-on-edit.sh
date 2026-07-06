#!/usr/bin/env bash
# PostToolUse hook: run fast, single-file formatters/linters on the file Claude
# just edited, keyed by extension. Advisory only — never blocks the edit
# (always exits 0). Heavy checks (go vet/staticcheck/golangci-lint/vulncheck,
# tsgo -b, pytest) stay in lefthook + CI, which remain the source of truth.
#
# Per-language set is limited to tools that run sub-second on one file:
#   Go     → gofmt -s -w, goimports -w        (no fast single-file lint/typecheck exists)
#   Python → ruff format, ruff check --fix, ty check   (compiled Rust, all fast)
#   TS/JS  → oxfmt --write, oxlint --fix       (tsgo -b needs whole-project graph → CI only)

set -uo pipefail

file="$(python3 -c 'import json,sys; d=json.load(sys.stdin); print((d.get("tool_input") or {}).get("file_path",""))' 2>/dev/null)"
[ -z "$file" ] && exit 0
[ -f "$file" ] || exit 0

has() { command -v "$1" >/dev/null 2>&1; }
run() { "$@" >/dev/null 2>&1 || true; }

case "$file" in
  *.go)
    has gofmt     && run gofmt -s -w "$file"
    has goimports && run goimports -w "$file"
    ;;
  *.py)
    has ruff && run ruff format "$file"
    has ruff && run ruff check --fix "$file"
    has ty   && run ty check "$file"
    ;;
  *.ts|*.tsx|*.js|*.jsx)
    has oxfmt  && run oxfmt --write "$file"
    has oxlint && run oxlint --fix "$file"
    ;;
esac

exit 0
