#!/usr/bin/env bash
#
# check-base-image-version-consistency.sh — hold each base image to one pin,
# and that pin's tag to the matching version file.
#
# VEC-783 pinned the Dockerfiles to a hardcoded tag+digest
# (FROM golang:X-alpine@sha256:...) rather than interpolating the tag from an
# ARG, because Docker pulls by digest and never re-checks that the tag still
# names it. The version files remain what setup-go/setup-python/setup-node and
# mise read, so this is what keeps the two in step.
#
# Paths resolve against the working directory, which is what lets
# check-base-image-version-consistency.test.sh drive this over fixture trees.
#
# Usage:
#   check-base-image-version-consistency.sh
set -euo pipefail

GO_VERSION="$(cat .go-version)"
PYTHON_VERSION="$(cat .python-version)"
NODE_VERSION="$(cat .node-version)"

GO_DOCKERFILE=stl-verify/Dockerfile.common
PYTHON_DOCKERFILE=stl-verify/python/Dockerfile

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

# check_pins_agree <image> <file>...: assert every file pins <image> on a FROM
# line, and that every such line names the same tag and the same digest. alpine
# has no version file, so this is the whole of its guard, and it is what holds
# each of the others to a single pin -- check_tag compares the tag and discards
# the digest, so it cannot see two pins of one image diverge.
#
# Each file is read on its own, and the pattern is anchored on FROM.
check_pins_agree() {
  local image="$1"
  shift
  if [ "$#" -eq 0 ]; then
    echo "  BAD  check_pins_agree ${image}: called with no files"
    FAILED=1
    return
  fi
  local from_pattern="FROM[[:space:]]+(--platform=[^[:space:]]+[[:space:]]+)?${image}:[^@[:space:]]+@sha256:[0-9a-f]{64}"
  local ref_pattern="${image}:[^@[:space:]]+@sha256:[0-9a-f]{64}"
  local refs="" file hits status distinct count
  for file in "$@"; do
    if [ ! -f "$file" ]; then
      echo "  BAD  ${file}: not a readable file"
      FAILED=1
      return
    fi
    status=0
    hits="$(grep -hoE "^${from_pattern}" "$file" | grep -oE "$ref_pattern")" || status=$?
    if [ "$status" -ne 0 ] || [ -z "$hits" ]; then
      echo "  BAD  ${file}: no pinned '${image}' FROM line found"
      FAILED=1
      return
    fi
    refs="${refs}${hits}"$'\n'
  done
  distinct="$(printf '%s' "$refs" | sort -u)"
  count="$(printf '%s\n' "$distinct" | wc -l | tr -d ' ')"
  if [ "$count" -eq 1 ]; then
    echo "  ok   ${image}: one pin — ${distinct}"
  else
    echo "  BAD  ${image} is pinned ${count} different ways across: $*"
    printf '%s\n' "$distinct" | sed 's/^/         /'
    FAILED=1
  fi
}

# check_go_directive <file> <expected>: go.mod states the toolchain version too,
# outside the set renovate.json's go toolchain group moves.
check_go_directive() {
  local file="$1" expected="$2" found status=0
  if [ ! -f "$file" ]; then
    echo "  BAD  ${file}: not a readable file"
    FAILED=1
    return
  fi
  found="$(grep -oE "^go[[:space:]]+[0-9]+\.[0-9]+(\.[0-9]+)?" "$file")" || status=$?
  if [ "$status" -ne 0 ] || [ -z "$found" ]; then
    echo "  BAD  ${file}: no 'go' directive found"
    FAILED=1
    return
  fi
  found="${found#go}"
  found="${found# }"
  found="$(printf '%s' "$found" | tr -d '[:space:]')"
  if [ "$found" = "$expected" ]; then
    echo "  ok   ${file}: go ${found}"
  else
    echo "  BAD  ${file}: 'go ${found}' does not match .go-version (${expected})"
    FAILED=1
  fi
}

check_tag "$GO_DOCKERFILE" golang "${GO_VERSION}-alpine"
check_tag "$PYTHON_DOCKERFILE" python "${PYTHON_VERSION}-slim"
check_tag "$PYTHON_DOCKERFILE" node "${NODE_VERSION}-alpine"
check_go_directive stl-verify/go.mod "$GO_VERSION"

check_pins_agree golang "$GO_DOCKERFILE"
check_pins_agree alpine "$GO_DOCKERFILE"
check_pins_agree python "$PYTHON_DOCKERFILE"
check_pins_agree node "$PYTHON_DOCKERFILE"

if [ "$FAILED" -ne 0 ]; then
  cat >&2 <<MSG
::error::A Dockerfile's pinned base-image tag does not match .go-version
::error::(${GO_VERSION}), .python-version (${PYTHON_VERSION}) or .node-version
::error::(${NODE_VERSION}), or a base image is pinned more than one way.
::error::VEC-783 hardcoded the tag next to its digest deliberately, so bumping the
::error::version file does not by itself change what gets built -- update the FROM
::error::line's tag and digest together with the version file, in the same PR, and
::error::keep every Dockerfile pinning a given base image on the same ref.
MSG
  exit 1
fi

echo "Each base image carries one pin, matching .go-version (${GO_VERSION}) / .python-version (${PYTHON_VERSION}) / .node-version (${NODE_VERSION})."
