#!/usr/bin/env bash
#
# check-build-reproducible.sh — assert that an image's layers are a function of
# its source, and nothing else.
#
# ORB-366 rests on one property: two builds of the same source produce the same
# layer digests, so a deploy can tell an unchanged service from a changed one
# and leave the unchanged one's pods alone. That property is easy to lose by
# accident — a build timestamp linked into a binary, a per-commit ARG above a
# RUN, a `date` in a Dockerfile — and losing it is invisible. Nothing fails; the
# deploy simply goes back to rolling everything. So it is asserted here rather
# than believed.
#
# Three builds per image, and all three verdicts matter:
#   same        identical source, different build metadata  -> layers must MATCH
#   docs-only   a markdown edit, as a new commit would be   -> layers must MATCH
#               (python's build never reaches README.md — see ADR-0007 — so
#               this leg is reported skipped-as-vacuous for --image python
#               instead of run; a passing leg that lies is worse than one that
#               says it was skipped)
#   code        a real change to compiled source            -> layers must DIFFER
#
# The third is not decoration. Without it a comparison that always reports
# "identical" — the wrong path compared, an empty layer list, a silently reused
# tag — reads as a pass, and this script would certify the very thing it exists
# to catch. A green run means the comparison is sensitive AND the build is
# stable; a green run without the `code` case means neither. The go docs-only
# leg carries an analogous tripwire: it asserts the `COPY . .` step that ships
# README.md into the builder was not itself cache-hit, so a future Dockerfile
# change that stops the perturbation from reaching the build context fails
# loudly instead of passing vacuously the way python's currently does.
#
# REQUIRES BuildKit (`docker/setup-buildx-action`). Layer identity across two
# builds comes from BuildKit reusing a cached layer when the content feeding it
# is unchanged: the binary is rebuilt, its bytes are identical, so the COPY that
# ships it hits cache and the previous layer blob is reused. buildah/podman does
# not reproduce that — it can report differing layers for identical content, or
# reuse a layer despite a different --build-arg — so it is not a substitute for
# running this in CI. require_buildkit() below refuses to run against anything
# but a real buildx/BuildKit toolchain rather than silently producing a
# meaningless result.
#
# CACHE, WARM ON PURPOSE, WITH ONE DELIBERATE EXCEPTION: every build below runs
# against the same warm BuildKit cache within this one script invocation, so a
# "layers match" verdict means the content hashed the same — not merely that
# nothing reran. That is sound for the layers each leg is designed to perturb.
# But no leg ever perturbs the inputs to python's `ui-builder` stage (the
# `ts/` -> static-assets build), so across a whole run that stage is never
# rebuilt — it is cache-hit every time, and a cache hit looks identical
# regardless of whether ui-builder's own output is actually reproducible. The
# python `same-source` leg forces that one stage cold (--no-cache-filter
# ui-builder) so its output is independently recomputed and compared for real
# at least once, without paying for a fully cold run — which would also
# rebuild the QEMU-emulated arm64 stages, at several times the cost, for no
# added coverage of ui-builder.
#
# Usage:
#   check-build-reproducible.sh --image go|python [--keep]
#
#   --image go      build a Go service from Dockerfile.common (all Go services
#                   share it, so one stands in for every one of them)
#   --image python  build the python-api image from python/Dockerfile
#   --keep          leave the built images behind for inspection
#
# Deliberately bash 3.2 + BSD awk compatible, like the other scripts here.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
BUILD_DIR="${REPO_ROOT}/stl-verify"
IMAGE=""
KEEP=0
# Set (and unset) by callers around a build() call; deliberately plain strings,
# not arrays — an empty bash-3.2 array under `set -u` word-splits as an
# unbound variable, and every value used here is a bare flag with no spaces or
# quoting to preserve.
DOCS_BACKUP=""
BUILD_EXTRA_ARGS=""
BUILD_LOG=""

die() { echo "::error::$*" >&2; exit 1; }

while [ $# -gt 0 ]; do
  case "$1" in
    --image) IMAGE="${2:-}"; shift 2 ;;
    --keep)  KEEP=1; shift ;;
    -h|--help) sed -n '/^# Usage:/,/^# Deliberately/p' "$0" | sed '$d' | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) die "unknown argument: $1" ;;
  esac
done

# The compiled file each build perturbs must be reachable from the service
# being built, or the `code` case would prove nothing. The docs file is not
# assumed reachable — python's isn't (ADR-0007) — so each image handles it on
# its own terms below rather than pretending the perturbation always lands.
DOCS_FILE="${BUILD_DIR}/README.md"
case "$IMAGE" in
  go)
    SERVICE_CMD_PATH="cmd/workers/psm3-indexer"
    SERVICE_BIN="psm3-indexer"
    CODE_FILE="${BUILD_DIR}/${SERVICE_CMD_PATH}/zz_reproducibility_control.go"
    ;;
  python)
    CODE_FILE="${BUILD_DIR}/python/app/zz_reproducibility_control.py"
    ;;
  *) die "--image must be go or python (got '${IMAGE:-}')" ;;
esac

[ -f "$DOCS_FILE" ] || die "docs file not found, cannot run the docs-only case: ${DOCS_FILE}"

TAG_PREFIX="stl-repro-check/${IMAGE}"
# Fixed, because build() runs inside a command substitution: a tag list it
# appended to would die with the subshell and cleanup would remove nothing.
BUILD_SUFFIXES="baseline same-source docs-only code-change"

# Restore the tree whatever happens: this script edits real files in the
# checkout, and leaving a control file behind would be committed by the next
# careless `git add -A`.
cleanup() {
  rm -f "$CODE_FILE"
  # Restore README.md only if this run actually perturbed it (DOCS_BACKUP is
  # only ever set around that one edit), and restore the exact bytes captured
  # right before the edit rather than `git checkout`ing it back to HEAD — the
  # latter would silently discard any real uncommitted edit that was already
  # sitting in the working tree when the script started.
  if [ -n "$DOCS_BACKUP" ] && [ -f "$DOCS_BACKUP" ]; then
    cp "$DOCS_BACKUP" "$DOCS_FILE"
    rm -f "$DOCS_BACKUP"
  fi
  rm -f "$BUILD_LOG"
  if [ "$KEEP" -eq 0 ]; then
    for suffix in $BUILD_SUFFIXES; do
      docker rmi -f "${TAG_PREFIX}:${suffix}" >/dev/null 2>&1 || true
    done
  fi
}
trap cleanup EXIT

# require_buildkit: verify a real buildx/BuildKit toolchain rather than assume
# one just because a `docker buildx` subcommand exists. On this stack's dev
# machines `docker` shims to podman, and podman's `buildx` stub does not fail —
# it reports buildah's own version string instead — so a bare
# command-exists check would pass while every comparison below is meaningless
# (buildah reuses layers across different --build-arg values and reports
# differing layers for identical content; see the header).
require_buildkit() {
  local version
  version="$(docker buildx version 2>&1)" || die "docker buildx is unavailable, and BuildKit is required (see this script's header): ${version}"
  case "$version" in
    *buildah*) die "docker buildx resolved to buildah/podman (\"${version}\"), not BuildKit. Layer-digest comparisons are meaningless under it (see header) — run this in CI (docker/setup-buildx-action) or against a real Docker/BuildKit daemon." ;;
  esac
}

# run_build: wraps `docker buildx build`, optionally teeing its plain-progress
# output to BUILD_LOG so a leg can inspect which steps actually ran (used by
# check_docs_reached_go_context). Plain progress only when logging — it is
# noisier, and only that tripwire needs to parse it.
run_build() {
  if [ -n "$BUILD_LOG" ]; then
    docker buildx build --progress=plain "$@" 2>&1 | tee "$BUILD_LOG" >&2
  else
    docker buildx build "$@" >&2
  fi
}

# build <tag-suffix> <git-commit> <build-time>: build the image and echo its
# layer digests as one space-separated line.
build() {
  local suffix="$1" commit="$2" build_time="$3"
  local tag="${TAG_PREFIX}:${suffix}"
  local go_version python_version

  if [ "$IMAGE" = "go" ]; then
    go_version="$(cat "${REPO_ROOT}/.go-version")"
    run_build --platform linux/arm64 \
      --build-arg GO_VERSION="$go_version" \
      --build-arg CMD_PATH="$SERVICE_CMD_PATH" \
      --build-arg BIN="$SERVICE_BIN" \
      --build-arg GIT_COMMIT="$commit" \
      --build-arg GIT_BRANCH="repro-check-${suffix}" \
      --build-arg BUILD_TIME="$build_time" \
      $BUILD_EXTRA_ARGS \
      -f "${BUILD_DIR}/Dockerfile.common" -t "$tag" --load "$BUILD_DIR"
  else
    python_version="$(cat "${REPO_ROOT}/.python-version")"
    run_build --platform linux/arm64 \
      --build-arg PYTHON_VERSION="$python_version" \
      --build-arg GIT_COMMIT="$commit" \
      $BUILD_EXTRA_ARGS \
      -f "${BUILD_DIR}/python/Dockerfile" -t "$tag" --load "$BUILD_DIR"
  fi

  local layers
  layers="$(docker image inspect "$tag" --format '{{range .RootFS.Layers}}{{.}} {{end}}')"
  # An empty layer list would make every comparison trivially equal, which is
  # the shape of a pass that means nothing.
  [ -n "${layers// /}" ] || die "no layers reported for ${tag}; the comparison would be meaningless"
  printf '%s' "$layers"
}

# check_docs_reached_go_context <build-log>: the go docs-only leg is only a
# real test if the README.md edit actually reached the builder. If a future
# Dockerfile change stopped `COPY . .` from seeing it, this leg would silently
# become as vacuous as python's (ADR-0007) and still report "match". Confirm
# from the build's own plain-progress log that the COPY step was not cache-hit
# — i.e. that BuildKit saw different content this time than the baseline build.
check_docs_reached_go_context() {
  local log="$1" step
  step="$(awk '/\[builder [0-9]+\/[0-9]+\] COPY \. \./ { match($0, /^#[0-9]+/); print substr($0, RSTART, RLENGTH); exit }' "$log")"
  [ -n "$step" ] || die "could not find the 'COPY . .' step in the build log; cannot confirm the docs-only edit reached the go build context"
  if grep -qF "${step} CACHED" "$log"; then
    die "docs-only leg is vacuous: 'COPY . .' was cache-hit, so the README.md edit never reached the go build context. Check .dockerignore and Dockerfile.common's COPY paths (ORB-366)."
  fi
}

# compare <case-name> <expectation: match|differ> <layers-a> <layers-b>
FAILED=0
compare() {
  local name="$1" expectation="$2" a="$3" b="$4"
  # An empty or missing layer list must never read as "identical" — that is
  # exactly the silent-pass failure mode this function exists to avoid (see
  # the self-test below).
  if [ -z "${a// /}" ] || [ -z "${b// /}" ]; then
    FAILED=1
    echo "  BAD  ${name}: empty/missing layer list (a='${a}' b='${b}'); cannot be treated as identical"
    return
  fi
  if [ "$a" = "$b" ]; then
    if [ "$expectation" = "match" ]; then
      echo "  ok   ${name}: layers identical, as required"
    else
      FAILED=1
      echo "  BAD  ${name}: layers identical, but this case changed compiled source"
      echo "       Either the change did not reach the image, or the comparison is not sensitive"
      echo "       to content — in both cases the matching verdicts above prove nothing."
    fi
  else
    if [ "$expectation" = "differ" ]; then
      echo "  ok   ${name}: layers differ, as required"
    else
      FAILED=1
      echo "  BAD  ${name}: layers differ for source that should produce identical layers"
      echo "       a: ${a}"
      echo "       b: ${b}"
      echo "       Something per-build is reaching a layer again. Check for a value that varies"
      echo "       per build or per commit declared above the last COPY, or linked into the"
      echo "       binary with -ldflags -X (ORB-366)."
    fi
  fi
}

# self_test_compare: compare() decides pass/fail for the whole script, so it
# gets its own smoke test before anything else trusts it. Runs unconditionally
# on every invocation (no docker involved, so it costs nothing) rather than
# only in some separate test suite.
self_test_compare() {
  local saved_failed="$FAILED"

  FAILED=0; compare t match  x x  >/dev/null; [ "$FAILED" -eq 0 ] || die "compare() self-test failed: identical inputs + match expectation should pass"
  FAILED=0; compare t differ x x  >/dev/null; [ "$FAILED" -eq 1 ] || die "compare() self-test failed: identical inputs + differ expectation should fail"
  FAILED=0; compare t differ x y  >/dev/null; [ "$FAILED" -eq 0 ] || die "compare() self-test failed: differing inputs + differ expectation should pass"
  FAILED=0; compare t match  x y  >/dev/null; [ "$FAILED" -eq 1 ] || die "compare() self-test failed: differing inputs + match expectation should fail"
  FAILED=0; compare t match  "" ""  >/dev/null; [ "$FAILED" -eq 1 ] || die "compare() self-test failed: empty/missing layers must not read as identical"
  FAILED=0; compare t differ "" ""  >/dev/null; [ "$FAILED" -eq 1 ] || die "compare() self-test failed: empty/missing layers must not read as identical either way"

  FAILED="$saved_failed"
  echo "==> compare() self-test passed"
}
self_test_compare

require_buildkit

COMMIT_A="1111111111111111111111111111111111111111"
COMMIT_B="2222222222222222222222222222222222222222"
COMMIT_C="3333333333333333333333333333333333333333"

echo "==> Reproducibility check for the ${IMAGE} image"

echo "--> baseline build"
LAYERS_A="$(build baseline "$COMMIT_A" "2020-01-01T00:00:00Z")"

echo "--> rebuild with different build metadata, same source"
if [ "$IMAGE" = "python" ]; then
  # Force the otherwise-never-perturbed ui-builder stage cold so its output is
  # independently recomputed at least once in this run, instead of only ever
  # being cache-hit (see the CACHE header note).
  BUILD_EXTRA_ARGS="--no-cache-filter ui-builder"
fi
LAYERS_SAME="$(build same-source "$COMMIT_B" "2021-06-15T12:34:56Z")"
BUILD_EXTRA_ARGS=""
compare "same source, different build metadata" match "$LAYERS_A" "$LAYERS_SAME"

echo "--> rebuild after a docs-only edit"
if [ "$IMAGE" = "python" ]; then
  echo "  skip: python/Dockerfile has no COPY reaching ${DOCS_FILE#"${REPO_ROOT}/"} (ADR-0007);"
  echo "        perturbing it would rebuild byte-identical instructions to the"
  echo "        same-source case above and prove nothing."
else
  DOCS_BACKUP="$(mktemp)"
  cp "$DOCS_FILE" "$DOCS_BACKUP"
  echo "" >> "$DOCS_FILE"
  echo "<!-- reproducibility check: transient edit, reverted by the script -->" >> "$DOCS_FILE"
  BUILD_LOG="$(mktemp)"
  LAYERS_DOCS="$(build docs-only "$COMMIT_C" "2022-02-02T02:02:02Z")"
  check_docs_reached_go_context "$BUILD_LOG"
  rm -f "$BUILD_LOG"
  BUILD_LOG=""
  compare "docs-only change" match "$LAYERS_A" "$LAYERS_DOCS"
  cp "$DOCS_BACKUP" "$DOCS_FILE"
  rm -f "$DOCS_BACKUP"
  DOCS_BACKUP=""
fi

echo "--> rebuild after a real source change (control: this one must differ)"
if [ "$IMAGE" = "go" ]; then
  # An init() cannot be dropped by the linker, so this reliably changes the
  # binary. A comment or an unused declaration would not, and a control that
  # silently changes nothing is worse than no control.
  cat > "$CODE_FILE" <<'CONTROL'
package main

import "os"

// Written and removed by scripts/deploy/check-build-reproducible.sh.
func init() { _ = os.Setenv("STL_REPRODUCIBILITY_CONTROL", "control") }
CONTROL
else
  echo '"""Written and removed by scripts/deploy/check-build-reproducible.sh."""' > "$CODE_FILE"
  echo 'STL_REPRODUCIBILITY_CONTROL = "control"' >> "$CODE_FILE"
fi
LAYERS_CODE="$(build code-change "$COMMIT_A" "2020-01-01T00:00:00Z")"
compare "real source change" differ "$LAYERS_A" "$LAYERS_CODE"
rm -f "$CODE_FILE"

if [ "$FAILED" -ne 0 ]; then
  die "the ${IMAGE} image's layers are not a function of its source alone; ORB-366's deploy comparison cannot be trusted on it."
fi

echo "The ${IMAGE} image's layers depend on its source and nothing else."
