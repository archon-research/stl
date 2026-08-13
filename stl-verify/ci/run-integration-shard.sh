#!/usr/bin/env bash

set -euo pipefail

cd "$(dirname "$0")/.."

test_names() {
  awk '
    /^func Test[[:alnum:]_]+\(/ {
      name = $2
      sub(/\(.*/, "", name)
      if (name != "TestMain") print name
    }
  ' "$@" | sort -u
}

run_package() {
  local package="$1"
  local file
  local integration_names
  local ordinary_names
  local duplicates
  local pattern
  local -a integration_files=()
  local -a ordinary_files=()

  while IFS= read -r -d '' file; do
    if grep -qE '^//go:build .*integration' "$file"; then
      integration_files+=("$file")
    else
      ordinary_files+=("$file")
    fi
  done < <(find "$package" -maxdepth 1 -type f -name '*_test.go' -print0)

  if [[ "${#integration_files[@]}" -eq 0 ]]; then
    echo "ERROR: no integration test files found in $package" >&2
    return 1
  fi

  integration_names="$(test_names "${integration_files[@]}")"
  if [[ -z "$integration_names" ]]; then
    echo "ERROR: no integration tests found in $package" >&2
    return 1
  fi

  ordinary_names=""
  if [[ "${#ordinary_files[@]}" -gt 0 ]]; then
    ordinary_names="$(test_names "${ordinary_files[@]}")"
  fi
  duplicates="$(comm -12 \
    <(printf '%s\n' "$integration_names") \
    <(printf '%s\n' "$ordinary_names"))"
  if [[ -n "$duplicates" ]]; then
    echo "ERROR: integration and ordinary tests share names in $package:" >&2
    printf '  %s\n' "$duplicates" >&2
    return 1
  fi

  pattern="$(printf '%s\n' "$integration_names" | paste -sd '|' -)"
  go test -tags=integration -v -timeout=10m -run "^($pattern)$" "$package"
}

if [[ "${1:-}" == "--package" ]]; then
  if [[ "$#" -ne 2 ]]; then
    echo "usage: $0 --package PACKAGE" >&2
    exit 2
  fi
  run_package "$2"
  exit
fi

if [[ "$#" -ne 1 ]]; then
  echo "usage: $0 MANIFEST" >&2
  exit 2
fi

manifest="$1"
if [[ ! -f "$manifest" ]]; then
  echo "ERROR: integration shard manifest not found: $manifest" >&2
  exit 1
fi

export GOEXPERIMENT=goroutineleakprofile
grep -Ev '^[[:space:]]*(#|$)' "$manifest" \
  | xargs -P 4 -n 1 bash "$0" --package
