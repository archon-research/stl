#!/usr/bin/env bash

# Every cmd/**/main.go hand-wires three *string out-params into
# buildinfo.Populate(commit, branch, buildTime *string). Because all three
# parameters share the same type, an argument-order slip (e.g. swapping
# GitCommit and GitBranch) compiles cleanly and go vet/staticcheck say
# nothing. Three of these files feed the local var straight into telemetry
# as ServiceVersion: GitCommit, so a slip silently reports a branch name as
# the service version. This asserts every call site uses the exact same
# argument order instead of trusting each file to get it right.

set -euo pipefail

cd "$(dirname "$0")/.."

expected='buildinfo.Populate(&GitCommit, &GitBranch, &BuildTime)'

# Scoped to cmd/ (not repo-wide '.'): .gopath/ (module cache) and
# .claude/worktrees/ (sibling worktree checkouts) both sit outside cmd/, so
# this can't pick up vendored or duplicate copies of the same main.go files.
files="$(grep -rl 'buildinfo\.Populate(' cmd --include='main.go' | sort)"

if [[ -z "$files" ]]; then
  echo "ERROR: no cmd/**/main.go calls buildinfo.Populate -- check the grep pattern" >&2
  exit 1
fi

count=$(printf '%s\n' "$files" | grep -c .)

offenders=""
while IFS= read -r f; do
  # || true: grep exits 1 when the call is reflowed across lines, and under
  # `set -e` that would abort here -- failing closed, but silently, with no
  # diagnostic. An empty match is itself a finding, so report it as one.
  call="$(grep -o 'buildinfo\.Populate([^)]*)' "$f" || true)"
  if [[ -z "$call" ]]; then
    offenders+="  $f: no single-line buildinfo.Populate(...) found (reflowed across lines?)"$'\n'
  elif [[ "$call" != "$expected" ]]; then
    offenders+="  $f: $call"$'\n'
  fi
done <<< "$files"

if [[ -n "$offenders" ]]; then
  echo "ERROR: these main.go files call buildinfo.Populate with the wrong argument order:" >&2
  printf '%s' "$offenders" >&2
  echo "All call sites must read exactly: $expected" >&2
  exit 1
fi

echo "All $count cmd/**/main.go call sites use the canonical buildinfo.Populate argument order."
