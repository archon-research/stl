#!/usr/bin/env bash
#
# compare-image-layers.test.sh — exercise compare-image-layers.sh against a stub
# ECR, with no AWS credentials.
#
# The comparator's failure mode is not a wrong verdict, it is a reassuring one:
# answer "unchanged" when a credential expired or a manifest could not be
# parsed, and once it drives the deploy it pins an old image forever while
# reporting success. That path cannot be exercised against a real registry —
# you cannot ask ECR to fail on demand — so every branch is driven here through
# a stub `aws` on PATH.
#
# Usage: compare-image-layers.test.sh
set -euo pipefail

HERE="$(cd "$(dirname "$0")" && pwd)"
SUBJECT="${HERE}/compare-image-layers.sh"
WORK="$(mktemp -d)"
trap 'rm -rf "$WORK"' EXIT

DEPLOY_SHA="bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
PINNED_SHA="aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"

PASSED=0
FAILED=0

# The stub reads $WORK/responses/<repo>__<tag> and prints it as the manifest, or
# exits non-zero when that file is absent — which is how ECR behaves for a tag
# that does not exist, and how an AWS failure reaches the comparator.
install_stub_aws() {
  mkdir -p "${WORK}/bin"
  cat > "${WORK}/bin/aws" <<'STUB'
#!/usr/bin/env bash
set -euo pipefail
repo=""; tag=""
while [ $# -gt 0 ]; do
  case "$1" in
    --repository-name) repo="$2"; shift 2 ;;
    --image-ids) tag="${2#imageTag=}"; shift 2 ;;
    *) shift ;;
  esac
done
file="${STUB_RESPONSES}/${repo}__${tag}"
# A genuine call failure: throttle, credentials, network.
[ -f "${file}.awsfail" ] && exit 254
# Real batch-get-image exits 0 for a tag that does not exist and puts the miss
# in failures[], so --query 'images[0].imageManifest' renders "None". Modelling
# a missing tag as an error is what let PINNED_GONE and NOT_BUILT be verified
# against behaviour ECR does not have.
[ -f "$file" ] || { echo "None"; exit 0; }
cat "$file"
STUB
  chmod +x "${WORK}/bin/aws"
}

# respond <repo> <tag> <manifest-json|EMPTY|NONE|AWSFAIL>
respond() {
  mkdir -p "${WORK}/responses"
  case "$3" in
    EMPTY)   printf '' > "${WORK}/responses/${1}__${2}" ;;
    NONE)    printf 'None\n' > "${WORK}/responses/${1}__${2}" ;;
    AWSFAIL) : > "${WORK}/responses/${1}__${2}.awsfail" ;;
    *)       printf '%s\n' "$3" > "${WORK}/responses/${1}__${2}" ;;
  esac
}

manifest_with_layers() {
  local digests="" d
  for d in "$@"; do digests="${digests}{\"digest\":\"${d}\"},"; done
  printf '{"mediaType":"application/vnd.docker.distribution.manifest.v2+json","layers":[%s]}' "${digests%,}"
}

manifest_no_digest() {
  printf '%s' '{"mediaType":"application/vnd.docker.distribution.manifest.v2+json","layers":[{"size":1},{"size":2}]}'
}

manifest_list() {
  printf '{"mediaType":"application/vnd.docker.distribution.manifest.list.v2+json","manifests":[{"digest":"sha256:child"}]}'
}

# overlay <repo-suffix>...: write a kustomization pinning each repo at PINNED_SHA
overlay() {
  local file="${WORK}/kustomization.yaml" suffix
  {
    echo "resources:"
    echo "  - ../../base/watcher"
    echo "images:"
    for suffix in "$@"; do
      echo "  - name: ${suffix}"
      echo "    newName: 579039992622.dkr.ecr.eu-west-1.amazonaws.com/${suffix}"
      echo "    newTag: ${PINNED_SHA}"
    done
  } > "$file"
  printf '%s' "$file"
}

# check <name> <expected-exit> <expected-substring> -- <args...>
check() {
  local name="$1" want_exit="$2" want_text="$3"; shift 4
  local out status
  set +e
  out="$(STUB_RESPONSES="${WORK}/responses" PATH="${WORK}/bin:$PATH" bash "$SUBJECT" "$@" 2>&1)"
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
    echo "  FAIL ${name}: output does not contain '${want_text}'"
    printf '%s\n' "$out" | sed 's/^/         /'
    return
  fi
  PASSED=$((PASSED + 1))
  echo "  ok   ${name}"
}

install_stub_aws
echo "==> compare-image-layers.sh"

# Identical layers is the verdict the whole ticket turns on.
rm -rf "${WORK}/responses"
respond stl-sentinelstaging-watcher "$PINNED_SHA" "$(manifest_with_layers sha256:one sha256:two)"
respond stl-sentinelstaging-watcher "$DEPLOY_SHA" "$(manifest_with_layers sha256:one sha256:two)"
check "identical layers report UNCHANGED" 0 "UNCHANGED   stl-sentinelstaging-watcher" -- \
  --kustomization "$(overlay stl-sentinelstaging-watcher)" --tag "$DEPLOY_SHA"

# Layer order is part of the identity: same digests, different order is a
# different image, and treating it as unchanged would pin the wrong one.
rm -rf "${WORK}/responses"
respond stl-sentinelstaging-watcher "$PINNED_SHA" "$(manifest_with_layers sha256:one sha256:two)"
respond stl-sentinelstaging-watcher "$DEPLOY_SHA" "$(manifest_with_layers sha256:two sha256:one)"
check "reordered layers report CHANGED" 0 "CHANGED     stl-sentinelstaging-watcher" -- \
  --kustomization "$(overlay stl-sentinelstaging-watcher)" --tag "$DEPLOY_SHA"

rm -rf "${WORK}/responses"
respond stl-sentinelstaging-watcher "$PINNED_SHA" "$(manifest_with_layers sha256:one)"
respond stl-sentinelstaging-watcher "$DEPLOY_SHA" "$(manifest_with_layers sha256:one sha256:two)"
check "differing layers report CHANGED" 0 "CHANGED     stl-sentinelstaging-watcher" -- \
  --kustomization "$(overlay stl-sentinelstaging-watcher)" --tag "$DEPLOY_SHA"

# The ARCT-436 failure: the running tag has been expired out of the registry.
rm -rf "${WORK}/responses"
respond stl-sentinelstaging-watcher "$DEPLOY_SHA" "$(manifest_with_layers sha256:one)"
check "a pinned tag missing from ECR reports PINNED_GONE" 0 "PINNED_GONE" -- \
  --kustomization "$(overlay stl-sentinelstaging-watcher)" --tag "$DEPLOY_SHA"

rm -rf "${WORK}/responses"
respond stl-sentinelstaging-watcher "$PINNED_SHA" "$(manifest_with_layers sha256:one)"
check "an unbuilt candidate reports NOT_BUILT" 0 "NOT_BUILT" -- \
  --kustomization "$(overlay stl-sentinelstaging-watcher)" --tag "$DEPLOY_SHA"

# The failures that must never read as UNCHANGED.
rm -rf "${WORK}/responses"
respond stl-sentinelstaging-watcher "$PINNED_SHA" "not json at all"
respond stl-sentinelstaging-watcher "$DEPLOY_SHA" "not json at all"
check "an unparseable manifest reports UNKNOWN, not UNCHANGED" 1 "UNKNOWN" -- \
  --kustomization "$(overlay stl-sentinelstaging-watcher)" --tag "$DEPLOY_SHA"

rm -rf "${WORK}/responses"
respond stl-sentinelstaging-watcher "$PINNED_SHA" "$(manifest_list)"
respond stl-sentinelstaging-watcher "$DEPLOY_SHA" "$(manifest_list)"
check "a manifest list reports UNKNOWN rather than guessing a platform" 1 "UNKNOWN" -- \
  --kustomization "$(overlay stl-sentinelstaging-watcher)" --tag "$DEPLOY_SHA"

rm -rf "${WORK}/responses"
respond stl-sentinelstaging-watcher "$PINNED_SHA" "$(manifest_with_layers)"
respond stl-sentinelstaging-watcher "$DEPLOY_SHA" "$(manifest_with_layers)"
check "an empty layer list is UNKNOWN, not two images that match" 1 "UNKNOWN" -- \
  --kustomization "$(overlay stl-sentinelstaging-watcher)" --tag "$DEPLOY_SHA"

# A total failure must not print a clean summary: it evaluated nothing.
rm -rf "${WORK}/responses"
check "determining nothing exits non-zero" 1 "could not determine anything about a single one" -- \
  --kustomization "$(overlay stl-sentinelstaging-watcher stl-sentinelstaging-migrate)" --tag "$DEPLOY_SHA"

# One readable image is enough for the run to be healthy; the other's problem is
# reported without condemning the run.
rm -rf "${WORK}/responses"
respond stl-sentinelstaging-watcher "$PINNED_SHA" "$(manifest_with_layers sha256:one)"
respond stl-sentinelstaging-watcher "$DEPLOY_SHA" "$(manifest_with_layers sha256:one)"
check "a partial failure still exits zero when something was determined" 0 "1 unchanged, 0 changed, 0 missing from ECR, 1 not compared" -- \
  --kustomization "$(overlay stl-sentinelstaging-watcher stl-sentinelstaging-migrate)" --tag "$DEPLOY_SHA"

# Running after the deploy rewrote the block: both sides name the same tag, so
# there is nothing to compare and UNCHANGED would be a lie.
rm -rf "${WORK}/responses"
respond stl-sentinelstaging-watcher "$DEPLOY_SHA" "$(manifest_with_layers sha256:one)"
OVERLAY_AT_DEPLOY="${WORK}/kustomization-at-deploy.yaml"
sed "s/${PINNED_SHA}/${DEPLOY_SHA}/" "$(overlay stl-sentinelstaging-watcher)" > "$OVERLAY_AT_DEPLOY"
check "an already-rewritten overlay reports UNKNOWN" 1 "run this before the deploy rewrites" -- \
  --kustomization "$OVERLAY_AT_DEPLOY" --tag "$DEPLOY_SHA"

# Cronjobs share one repo and carry a name prefix; the prefix must survive.
rm -rf "${WORK}/responses"
CRONJOB_OVERLAY="${WORK}/kustomization-cronjob.yaml"
{
  echo "images:"
  echo "  - name: transform-worker"
  echo "    newName: 579039992622.dkr.ecr.eu-west-1.amazonaws.com/stl-sentinelstaging-cronjob"
  echo "    newTag: transform-worker-${PINNED_SHA}"
} > "$CRONJOB_OVERLAY"
respond stl-sentinelstaging-cronjob "transform-worker-${PINNED_SHA}" "$(manifest_with_layers sha256:one)"
respond stl-sentinelstaging-cronjob "transform-worker-${DEPLOY_SHA}" "$(manifest_with_layers sha256:one)"
check "a cronjob tag keeps its name prefix" 0 "UNCHANGED   stl-sentinelstaging-cronjob" -- \
  --kustomization "$CRONJOB_OVERLAY" --tag "$DEPLOY_SHA"

# Bad invocations fail loudly rather than comparing something arbitrary.
check "a short SHA is rejected" 2 "40-char lowercase git SHA" -- \
  --kustomization "$(overlay stl-sentinelstaging-watcher)" --tag deadbeef
check "a missing overlay is rejected" 2 "must point to an existing file" -- \
  --kustomization "${WORK}/nope.yaml" --tag "$DEPLOY_SHA"

# The JSON output is what a week of shadow verdicts gets reconciled from.
rm -rf "${WORK}/responses"
respond stl-sentinelstaging-watcher "$PINNED_SHA" "$(manifest_with_layers sha256:one)"
respond stl-sentinelstaging-watcher "$DEPLOY_SHA" "$(manifest_with_layers sha256:one)"
JSON="${WORK}/verdicts.json"
check "verdicts are written as JSON" 0 "Verdicts written" -- \
  --kustomization "$(overlay stl-sentinelstaging-watcher)" --tag "$DEPLOY_SHA" --json "$JSON"
if [ "$(jq -r '.results[0].verdict' "$JSON" 2>/dev/null)" = "UNCHANGED" ] &&
   [ "$(jq -r '.deploySha' "$JSON" 2>/dev/null)" = "$DEPLOY_SHA" ]; then
  PASSED=$((PASSED + 1)); echo "  ok   the JSON carries the verdict and the deploy SHA"
else
  FAILED=$((FAILED + 1)); echo "  FAIL the JSON does not carry the verdict and the deploy SHA"
  cat "$JSON" 2>/dev/null | sed 's/^/         /'
fi

# A one-sided call failure must never become a claim about the registry. This is
# the realistic failure — 2 calls per image, no retry config — and after the
# cutover it is the one that inverts the required response: a throttle reading
# as NOT_BUILT or PINNED_GONE at exit 0.
rm -rf "${WORK}/responses"
respond stl-sentinelstaging-watcher "$PINNED_SHA" "$(manifest_with_layers sha256:one)"
respond stl-sentinelstaging-watcher "$DEPLOY_SHA" AWSFAIL
check "a throttled candidate call is UNKNOWN, not NOT_BUILT" 1 "an ECR call failed" -- \
  --kustomization "$(overlay stl-sentinelstaging-watcher)" --tag "$DEPLOY_SHA"

rm -rf "${WORK}/responses"
respond stl-sentinelstaging-watcher "$PINNED_SHA" AWSFAIL
respond stl-sentinelstaging-watcher "$DEPLOY_SHA" "$(manifest_with_layers sha256:one)"
check "a throttled pinned call is UNKNOWN, not PINNED_GONE" 1 "an ECR call failed" -- \
  --kustomization "$(overlay stl-sentinelstaging-watcher)" --tag "$DEPLOY_SHA"

# The realistic missing-tag shape: the call succeeds and renders None.
rm -rf "${WORK}/responses"
respond stl-sentinelstaging-watcher "$PINNED_SHA" NONE
respond stl-sentinelstaging-watcher "$DEPLOY_SHA" "$(manifest_with_layers sha256:one)"
check "a pinned tag ECR reports as None is PINNED_GONE" 0 "PINNED_GONE" -- \
  --kustomization "$(overlay stl-sentinelstaging-watcher)" --tag "$DEPLOY_SHA"

# Layers present but digest-less compared equal and reported "0 layer(s)
# identical" — a self-contradictory line at exit 0.
rm -rf "${WORK}/responses"
respond stl-sentinelstaging-watcher "$PINNED_SHA" "$(manifest_no_digest)"
respond stl-sentinelstaging-watcher "$DEPLOY_SHA" "$(manifest_no_digest)"
check "digest-less layers are UNKNOWN, not 0-layers-identical" 1 "no usable layer digests" -- \
  --kustomization "$(overlay stl-sentinelstaging-watcher)" --tag "$DEPLOY_SHA"

# The non-ECR parse branch was reachable, correct, and completely unpinned.
rm -rf "${WORK}/responses"
NON_ECR_OVERLAY="${WORK}/kustomization-nonecr.yaml"
{
  echo "images:"
  echo "  - name: postgres"
  echo "    newName: docker.io/library/postgres"
  echo "    newTag: ${PINNED_SHA}"
} > "$NON_ECR_OVERLAY"
check "a non-ECR newName is UNKNOWN" 1 "could not parse ECR account/region" -- \
  --kustomization "$NON_ECR_OVERLAY" --tag "$DEPLOY_SHA"

# A quote in a value used to produce invalid JSON and kill the script at exit 5.
rm -rf "${WORK}/responses"
respond stl-sentinelstaging-watcher "$PINNED_SHA" "$(manifest_with_layers sha256:one)"
respond stl-sentinelstaging-watcher "$DEPLOY_SHA" "$(manifest_with_layers sha256:one)"
QUOTE_OVERLAY="${WORK}/kustomization-quote.yaml"
{
  echo "images:"
  echo "  - name: watcher"
  echo "    newName: 579039992622.dkr.ecr.eu-west-1.amazonaws.com/stl-sentinelstaging-watcher"
  echo "    newTag: ${PINNED_SHA}"
} > "$QUOTE_OVERLAY"
JSON_Q="${WORK}/quote.json"
check "the JSON survives a quote in a value" 0 "Verdicts written" -- \
  --kustomization "$QUOTE_OVERLAY" --tag "$DEPLOY_SHA" --json "$JSON_Q"
if jq -e . "$JSON_Q" >/dev/null 2>&1; then
  PASSED=$((PASSED + 1)); echo "  ok   the JSON output parses"
else
  FAILED=$((FAILED + 1)); echo "  FAIL the JSON output does not parse"
fi

echo "${PASSED} passed, ${FAILED} failed"
[ "$FAILED" -eq 0 ] || exit 1
