#!/usr/bin/env bash
#
# check-base-image-version-consistency.sh — fail if a Dockerfile's pinned
# golang/python/node base-image tag disagrees with the matching version file,
# or if two Dockerfiles pin the same base image differently.
#
# VEC-783 pinned stl-verify/Dockerfile.common, Dockerfile.migrate and
# python/Dockerfile to a hardcoded tag+digest (FROM golang:X-alpine@sha256:...,
# FROM python:X-slim@sha256:...) instead of interpolating the tag from an ARG,
# because an ARG-driven tag next to a fixed digest drifts from it silently:
# Docker pulls by digest and never re-checks that the tag still names it. That
# closed one drift path but opened another -- .go-version and .python-version
# still drive setup-go/setup-python in CI and mise/uv locally, so nothing
# stopped a version-file bump from silently building a different toolchain than
# the one CI compiles and tests against. This check is that stop.
#
# VEC-818 added two things. node is now guarded too: python/Dockerfile used to
# pin node:24-alpine, a major-only tag with no patch for this check to compare,
# and its digest had already drifted to 24.21.0 while .node-version said
# 24.20.0 with nothing to notice. And check_pins_agree covers what check_tag
# cannot — alpine has no version file at all, and check_tag compares only the
# tag half, so two Dockerfiles could carry the same tag against different
# digests and pass.
#
# Usage:
#   check-base-image-version-consistency.sh
set -euo pipefail

GO_VERSION="$(cat .go-version)"
PYTHON_VERSION="$(cat .python-version)"
NODE_VERSION="$(cat .node-version)"

# The Go images share a base; they are listed once here so the checks below and
# the agreement check cannot fall out of step with each other.
GO_DOCKERFILES="stl-verify/Dockerfile.common stl-verify/Dockerfile.migrate"

FAILED=0

# check_tag <file> <image> <expected-tag>: assert every pinned FROM line naming
# <image> in <file> carries exactly <expected-tag>. A file that pins <image> zero
# times is itself a failure -- the check must not pass by finding nothing.
check_tag() {
  local file="$1" image="$2" expected="$3"
  local pattern="FROM[[:space:]]+(--platform=[^[:space:]]+[[:space:]]+)?${image}:[^@[:space:]]+@sha256:[0-9a-f]{64}"
  local matches status=0
  matches="$(grep -noE "$pattern" "$file")" || status=$?
  if [ "$status" -ne 0 ] || [ -z "$matches" ]; then
    echo "  BAD  ${file}: no pinned '${image}' FROM line found"
    FAILED=1
    return
  fi

  while IFS=: read -r lineno rest; do
    local tag
    tag="$(printf '%s' "$rest" | sed -E "s#^FROM[[:space:]]+(--platform=[^[:space:]]+[[:space:]]+)?${image}:([^@]+)@sha256:.*#\\2#")"
    if [ "$tag" = "$expected" ]; then
      echo "  ok   ${file}:${lineno}: ${image}:${tag}"
    else
      echo "  BAD  ${file}:${lineno}: pinned tag '${image}:${tag}' does not match expected '${image}:${expected}'"
      FAILED=1
    fi
  done <<<"$matches"
}

# check_pins_agree <image> <file>...: every pinned FROM line naming <image>,
# across all the given files, must carry the same tag AND the same digest. This
# is the only guard on alpine, which has no version file to check against, and
# it is what catches two Dockerfiles agreeing on a tag while pointing at
# different images -- the exact drift VEC-783 hardcoded the tag to prevent.
check_pins_agree() {
  local image="$1"
  shift
  local pattern="${image}:[^@[:space:]]+@sha256:[0-9a-f]{64}"
  local refs distinct count
  refs="$(grep -hoE "$pattern" "$@" || true)"
  if [ -z "$refs" ]; then
    echo "  BAD  no pinned '${image}' FROM line found in: $*"
    FAILED=1
    return
  fi
  distinct="$(printf '%s\n' "$refs" | sort -u)"
  count="$(printf '%s\n' "$distinct" | wc -l | tr -d ' ')"
  if [ "$count" -eq 1 ]; then
    echo "  ok   ${image} pinned identically across $# file(s): ${distinct}"
  else
    echo "  BAD  ${image} is pinned ${count} different ways across: $*"
    printf '%s\n' "$distinct" | sed 's/^/         /'
    FAILED=1
  fi
}

for f in $GO_DOCKERFILES; do
  check_tag "$f" golang "${GO_VERSION}-alpine"
done
check_tag stl-verify/python/Dockerfile python "${PYTHON_VERSION}-slim"
check_tag stl-verify/python/Dockerfile node "${NODE_VERSION}-alpine"

# shellcheck disable=SC2086 # deliberate word-splitting: the list is a filename list
check_pins_agree golang $GO_DOCKERFILES
# shellcheck disable=SC2086
check_pins_agree alpine $GO_DOCKERFILES

if [ "$FAILED" -ne 0 ]; then
  cat >&2 <<MSG
::error::A Dockerfile's pinned base-image tag does not match .go-version
::error::(${GO_VERSION}), .python-version (${PYTHON_VERSION}) or .node-version
::error::(${NODE_VERSION}), or two Dockerfiles pin the same base image differently.
::error::VEC-783 hardcoded the tag next to its digest deliberately, so bumping the
::error::version file does not by itself change what gets built -- update the FROM
::error::line's tag and digest together with the version file, in the same PR, and
::error::keep every Dockerfile pinning a given base image on the same ref.
MSG
  exit 1
fi

echo "Base-image pins agree with .go-version (${GO_VERSION}) / .python-version (${PYTHON_VERSION}) / .node-version (${NODE_VERSION}), and are identical across Dockerfiles."
