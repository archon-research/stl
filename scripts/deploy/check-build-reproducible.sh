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
#   code        a real change to compiled source            -> layers must DIFFER
#
# The third is not decoration. Without it a comparison that always reports
# "identical" — the wrong path compared, an empty layer list, a silently reused
# tag — reads as a pass, and this script would certify the very thing it exists
# to catch. A green run means the comparison is sensitive AND the build is
# stable; a green run without the `code` case means neither.
#
# REQUIRES BuildKit (`docker/setup-buildx-action`). Layer identity across two
# builds comes from BuildKit reusing a cached layer when the content feeding it
# is unchanged: the binary is rebuilt, its bytes are identical, so the COPY that
# ships it hits cache and the previous layer blob is reused. buildah/podman does
# not reproduce that, and will report differing layers for identical content —
# it is not a substitute for running this in CI, which is why nothing here tries
# to accommodate it.
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

die() { echo "::error::$*" >&2; exit 1; }

while [ $# -gt 0 ]; do
  case "$1" in
    --image) IMAGE="${2:-}"; shift 2 ;;
    --keep)  KEEP=1; shift ;;
    -h|--help) sed -n '/^# Usage:/,/^# Deliberately/p' "$0" | sed '$d' | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) die "unknown argument: $1" ;;
  esac
done

# The docs file and the compiled file each build perturbs. Both are inside the
# build context; the compiled one must be reachable from the service being
# built, or the `code` case would prove nothing.
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
  git -C "$REPO_ROOT" checkout -- "${DOCS_FILE#"${REPO_ROOT}/"}" 2>/dev/null || true
  if [ "$KEEP" -eq 0 ]; then
    for suffix in $BUILD_SUFFIXES; do
      docker rmi -f "${TAG_PREFIX}:${suffix}" >/dev/null 2>&1 || true
    done
  fi
}
trap cleanup EXIT

# build <tag-suffix> <git-commit> <build-time>: build the image and echo its
# layer digests as one space-separated line.
build() {
  local suffix="$1" commit="$2" build_time="$3"
  local tag="${TAG_PREFIX}:${suffix}"
  local go_version python_version

  if [ "$IMAGE" = "go" ]; then
    go_version="$(cat "${REPO_ROOT}/.go-version")"
    docker buildx build --platform linux/arm64 \
      --build-arg GO_VERSION="$go_version" \
      --build-arg CMD_PATH="$SERVICE_CMD_PATH" \
      --build-arg BIN="$SERVICE_BIN" \
      --build-arg GIT_COMMIT="$commit" \
      --build-arg GIT_BRANCH="repro-check-${suffix}" \
      --build-arg BUILD_TIME="$build_time" \
      -f "${BUILD_DIR}/Dockerfile.common" -t "$tag" --load "$BUILD_DIR" >&2
  else
    python_version="$(cat "${REPO_ROOT}/.python-version")"
    docker buildx build --platform linux/arm64 \
      --build-arg PYTHON_VERSION="$python_version" \
      --build-arg GIT_COMMIT="$commit" \
      -f "${BUILD_DIR}/python/Dockerfile" -t "$tag" --load "$BUILD_DIR" >&2
  fi

  local layers
  layers="$(docker image inspect "$tag" --format '{{range .RootFS.Layers}}{{.}} {{end}}')"
  # An empty layer list would make every comparison trivially equal, which is
  # the shape of a pass that means nothing.
  [ -n "${layers// /}" ] || die "no layers reported for ${tag}; the comparison would be meaningless"
  printf '%s' "$layers"
}

# compare <case-name> <expectation: match|differ> <layers-a> <layers-b>
FAILED=0
compare() {
  local name="$1" expectation="$2" a="$3" b="$4"
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

COMMIT_A="1111111111111111111111111111111111111111"
COMMIT_B="2222222222222222222222222222222222222222"
COMMIT_C="3333333333333333333333333333333333333333"

echo "==> Reproducibility check for the ${IMAGE} image"

echo "--> baseline build"
LAYERS_A="$(build baseline "$COMMIT_A" "2020-01-01T00:00:00Z")"

echo "--> rebuild with different build metadata, same source"
LAYERS_SAME="$(build same-source "$COMMIT_B" "2021-06-15T12:34:56Z")"
compare "same source, different build metadata" match "$LAYERS_A" "$LAYERS_SAME"

echo "--> rebuild after a docs-only edit"
echo "" >> "$DOCS_FILE"
echo "<!-- reproducibility check: transient edit, reverted by the script -->" >> "$DOCS_FILE"
LAYERS_DOCS="$(build docs-only "$COMMIT_C" "2022-02-02T02:02:02Z")"
compare "docs-only change" match "$LAYERS_A" "$LAYERS_DOCS"
git -C "$REPO_ROOT" checkout -- "${DOCS_FILE#"${REPO_ROOT}/"}"

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
