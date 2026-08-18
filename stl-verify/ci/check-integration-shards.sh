#!/usr/bin/env bash

set -euo pipefail

cd "$(dirname "$0")/.."

expected_manifests=(
  ci/integration-shards/1.txt
  ci/integration-shards/2.txt
)
actual_manifests=(ci/integration-shards/*.txt)

if [[ "${actual_manifests[*]}" != "${expected_manifests[*]}" ]]; then
  echo "ERROR: integration shard manifests must be exactly 1.txt and 2.txt" >&2
  printf '  %s\n' "${actual_manifests[@]}" >&2
  exit 1
fi

for manifest in "${expected_manifests[@]}"; do
  packages="$(sed '/^[[:space:]]*#/d; /^[[:space:]]*$/d' "$manifest")"
  if [[ -z "$packages" ]]; then
    echo "ERROR: integration shard manifest is empty: $manifest" >&2
    exit 1
  fi
done

configured="$({
  sed '/^[[:space:]]*#/d; /^[[:space:]]*$/d' "${expected_manifests[@]}"
} | sort)"
duplicates="$(printf '%s\n' "$configured" | uniq -d)"

if [[ -n "$duplicates" ]]; then
  echo "ERROR: integration packages assigned to more than one shard:" >&2
  printf '  %s\n' "$duplicates" >&2
  exit 1
fi

actual="$(go list -tags=integration ./... \
  | sed 's#^github.com/archon-research/stl/stl-verify#.#' \
  | sort)"

if ! diff -u \
  <(printf '%s\n' "$actual") \
  <(printf '%s\n' "$configured"); then
  echo "ERROR: integration shard manifests must contain every package exactly once" >&2
  exit 1
fi

echo "Integration shard manifests cover every package exactly once."
