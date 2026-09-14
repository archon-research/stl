#!/usr/bin/env bash
#
# check-base-image-version-consistency.sh — fail if a Dockerfile's pinned
# golang/python base-image tag disagrees with .go-version/.python-version.
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
# Usage:
#   check-base-image-version-consistency.sh
set -euo pipefail

GO_VERSION="$(cat .go-version)"
PYTHON_VERSION="$(cat .python-version)"

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

check_tag stl-verify/Dockerfile.common golang "${GO_VERSION}-alpine"
check_tag stl-verify/Dockerfile.migrate golang "${GO_VERSION}-alpine"
check_tag stl-verify/python/Dockerfile python "${PYTHON_VERSION}-slim"

if [ "$FAILED" -ne 0 ]; then
  cat >&2 <<MSG
::error::A Dockerfile's pinned base-image tag does not match .go-version
::error::(${GO_VERSION}) or .python-version (${PYTHON_VERSION}). VEC-783 hardcoded
::error::the tag next to its digest deliberately, so bumping the version file does
::error::not by itself change what gets built -- update the FROM line's tag and
::error::digest together with the version file, in the same PR.
MSG
  exit 1
fi

echo "Base-image tags in all guarded Dockerfiles match .go-version (${GO_VERSION}) / .python-version (${PYTHON_VERSION})."
