#!/usr/bin/env bash

# The integration job runs its services from tags written in the workflow, while
# local runs take them from images.go. Nothing forces the two to agree, so a tag
# bump in one place would silently test a different version in the other.

set -euo pipefail

cd "$(dirname "$0")/.."

images_file=internal/testutil/images.go
workflow=../.github/workflows/go-ci.yml

declared="$(grep -oE 'Image[A-Za-z0-9_]+[[:space:]]*=[[:space:]]*"[^"]+"' "$images_file" \
  | grep -oE '"[^"]+"' | tr -d '"' | sort -u)"
if [[ -z "$declared" ]]; then
  echo "ERROR: no image constants found in $images_file -- check the grep pattern" >&2
  exit 1
fi

configured="$(sed -n 's/^[[:space:]]*image:[[:space:]]*\([^[:space:]#]*\).*/\1/p' "$workflow" | sort -u)"
if [[ -z "$configured" ]]; then
  echo "ERROR: no service images found in $workflow -- check the sed pattern" >&2
  exit 1
fi

if ! diff -u \
  <(printf '%s\n' "$declared") \
  <(printf '%s\n' "$configured"); then
  echo "ERROR: service images in $workflow must match the constants in $images_file" >&2
  exit 1
fi

echo "Service images match internal/testutil/images.go."
