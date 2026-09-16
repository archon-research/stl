#!/usr/bin/env bash
#
# check-base-image-version-consistency.test.sh — drive
# check-base-image-version-consistency.sh over fixture trees.
#
# The guard's dangerous failure is not a wrong complaint, it is a reassuring
# silence: report "pinned identically" for a Dockerfile it never opened, or for
# one whose FROM line lost its digest while an old digest survived in a comment.
# Every arm below is a tree the guard must reject, built here because a real
# checkout only ever exercises the passing one.
#
# Usage: check-base-image-version-consistency.test.sh
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
SUBJECT="${HERE}/check-base-image-version-consistency.sh"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

GO_PIN="golang:1.26.6-alpine@sha256:3889b425f035be855a72fb4755265311293b6d414521f0a519d819df32222d83"
ALPINE_PIN="alpine:3.24@sha256:28bd5fe8b56d1bd048e5babf5b10710ebe0bae67db86916198a6eec434943f8b"
PY_PIN="python:3.12.14-slim@sha256:78387bc3881b8273120a12ebe6c1ab22b018ccc2c9adf565ae1ac9b536e184ea"
NODE_PIN="node:24.20.0-alpine@sha256:e67514e5d0f6c46656005e1b693b2ec9d52e80b641307de684d4a015ba7a4eaf"

PASSED=0
FAILED=0

# tree <dir>: a minimal repo the guard accepts, for a case to then break.
tree() {
  local d="$1"
  rm -rf "$d"
  mkdir -p "$d/stl-verify/python"
  printf '1.26.6\n' > "$d/.go-version"
  printf '3.12.14\n' > "$d/.python-version"
  printf '24.20.0\n' > "$d/.node-version"
  printf 'FROM --platform=$BUILDPLATFORM %s AS builder\nFROM %s\n' "$GO_PIN" "$ALPINE_PIN" \
    > "$d/stl-verify/Dockerfile.common"
  printf 'FROM --platform=$BUILDPLATFORM %s AS builder\nFROM %s\n' "$GO_PIN" "$ALPINE_PIN" \
    > "$d/stl-verify/Dockerfile.migrate"
  printf 'FROM --platform=$BUILDPLATFORM %s AS ui-builder\nFROM %s AS py-base\n' "$NODE_PIN" "$PY_PIN" \
    > "$d/stl-verify/python/Dockerfile"
  printf 'module github.com/archon-research/stl/stl-verify\n\ngo 1.26.6\n' > "$d/stl-verify/go.mod"
}

# check <name> <expected-exit> <expected-substring> <dir>
check() {
  local name="$1" want_exit="$2" want_text="$3" dir="$4"
  local out status
  set +e
  out="$(cd "$dir" && bash "$SUBJECT" 2>&1)"
  status=$?
  set -e
  if [ "$status" != "$want_exit" ]; then
    FAILED=$((FAILED + 1))
    echo "  FAIL ${name}: exit ${status}, want ${want_exit}"
    printf '%s\n' "$out" | sed 's/^/         /'
    return
  fi
  if ! printf '%s' "$out" | grep -q -- "$want_text"; then
    FAILED=$((FAILED + 1))
    echo "  FAIL ${name}: output missing '${want_text}'"
    printf '%s\n' "$out" | sed 's/^/         /'
    return
  fi
  PASSED=$((PASSED + 1))
  echo "  ok   ${name}"
}

D="${WORK}/clean"; tree "$D"
check "a fully pinned tree passes" 0 "are identical across Dockerfiles" "$D"

# The arm that matters most: one file stops pinning while the other still does.
# A single grep across both files sees the survivor and calls the pair agreed.
D="${WORK}/partial"; tree "$D"
printf 'FROM --platform=$BUILDPLATFORM %s AS builder\nFROM alpine:3.24\n' "$GO_PIN" \
  > "$D/stl-verify/Dockerfile.migrate"
check "one file dropping its alpine digest is caught" 1 "no pinned 'alpine' FROM line found" "$D"

# An old digest left behind in a comment must not stand in for the instruction.
D="${WORK}/comment"; tree "$D"
printf 'FROM --platform=$BUILDPLATFORM %s AS builder\n# previously: %s\nFROM alpine:3.24\n' \
  "$GO_PIN" "$ALPINE_PIN" > "$D/stl-verify/Dockerfile.migrate"
check "a digest quoted in a comment does not satisfy the guard" 1 "no pinned 'alpine' FROM line found" "$D"

# Same tag either side, different image. check_tag cannot see this: it compares
# the tag and discards the digest.
D="${WORK}/digest-split"; tree "$D"
printf 'FROM --platform=$BUILDPLATFORM %s AS builder\nFROM alpine:3.24@sha256:%s\n' \
  "$GO_PIN" "0000000000000000000000000000000000000000000000000000000000000000" \
  > "$D/stl-verify/Dockerfile.migrate"
check "same tag against different digests is caught" 1 "alpine is pinned 2 different ways" "$D"

D="${WORK}/tag-split"; tree "$D"
printf 'FROM --platform=$BUILDPLATFORM golang:1.26.8-alpine@sha256:%s AS builder\nFROM %s\n' \
  "3889b425f035be855a72fb4755265311293b6d414521f0a519d819df32222d83" "$ALPINE_PIN" \
  > "$D/stl-verify/Dockerfile.migrate"
check "a golang tag divergence is caught" 1 "does not match expected" "$D"

D="${WORK}/node-drift"; tree "$D"
printf '24.21.0\n' > "$D/.node-version"
check "node disagreeing with .node-version is caught" 1 "node:24.20.0-alpine' does not match" "$D"

D="${WORK}/node-major-tag"; tree "$D"
printf 'FROM --platform=$BUILDPLATFORM node:24-alpine@sha256:%s AS ui-builder\nFROM %s AS py-base\n' \
  "e67514e5d0f6c46656005e1b693b2ec9d52e80b641307de684d4a015ba7a4eaf" "$PY_PIN" \
  > "$D/stl-verify/python/Dockerfile"
check "a major-only node tag is caught" 1 "node:24-alpine' does not match" "$D"

D="${WORK}/go-drift"; tree "$D"
printf '1.27.1\n' > "$D/.go-version"
check "go disagreeing with .go-version is caught" 1 "does not match expected" "$D"

D="${WORK}/missing"; tree "$D"
rm "$D/stl-verify/Dockerfile.migrate"
check "a guarded Dockerfile that is gone is caught" 1 "Dockerfile.migrate" "$D"

# python/Dockerfile carries one pin because the py-base stage collapsed two.
# Re-inlining a second FROM is the way that property gets lost, so it is the
# arm that has to bite -- check_tag cannot see it, both tags being equal.
D="${WORK}/py-second-pin"; tree "$D"
printf 'FROM --platform=$BUILDPLATFORM %s AS ui-builder\nFROM %s AS py-base\nFROM python:3.12.14-slim@sha256:%s AS builder\n' \
  "$NODE_PIN" "$PY_PIN" "1111111111111111111111111111111111111111111111111111111111111111" \
  > "$D/stl-verify/python/Dockerfile"
check "a reintroduced second python pin is caught" 1 "python is pinned 2 different ways" "$D"

D="${WORK}/gomod-drift"; tree "$D"
printf 'module github.com/archon-research/stl/stl-verify\n\ngo 1.27.1\n' > "$D/stl-verify/go.mod"
check "go.mod disagreeing with .go-version is caught" 1 "does not match .go-version" "$D"

D="${WORK}/gomod-missing"; tree "$D"
rm "$D/stl-verify/go.mod"
check "a missing go.mod is caught" 1 "go.mod" "$D"

echo "${PASSED} passed, ${FAILED} failed"
[ "$FAILED" -eq 0 ] || exit 1
