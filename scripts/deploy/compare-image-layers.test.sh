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
  # `shift 4` below assumes exactly this shape. Under `set -e`, a call site one
  # argument short (a missing "--", a forgotten want_text) would make `shift 4`
  # fail with "shift count out of range" and abort the whole test run right
  # there, mid-suite, with no indication of which check() call was short — the
  # remaining checks simply never run. Fail closed instead: report which named
  # check was malformed and keep going, so one bad call site costs one FAIL, not
  # the rest of the suite.
  if [ "$#" -lt 4 ]; then
    FAILED=$((FAILED + 1))
    echo "  FAIL ${1:-<unnamed check>}: check() called with $# argument(s), need at least 4: <name> <exit> <text> -- <args...>"
    return
  fi
  local name="$1" want_exit="$2" want_text="$3" sep="$4"
  if [ "$sep" != "--" ]; then
    FAILED=$((FAILED + 1))
    echo "  FAIL ${name}: check()'s 4th argument must be '--', got '${sep}'"
    return
  fi
  shift 4
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

# expect_verdict <name> <expected-exit> <expected-verdict-text> <pinned-manifest>
# <candidate-manifest>: the verdict-assertion preamble repeated throughout this
# suite — reset the stub's responses, program the pinned and candidate
# manifests for the one repo most cases exercise
# (stl-sentinelstaging-watcher), and check() the result. <pinned-manifest> and
# <candidate-manifest> are anything respond()'s third argument accepts: a
# manifest JSON string, or EMPTY/NONE/AWSFAIL. Cases needing more than one repo
# or a non-default overlay still call rm -rf/respond/check directly below.
expect_verdict() {
  local name="$1" want_exit="$2" want_text="$3" pinned="$4" candidate="$5"
  rm -rf "${WORK}/responses"
  respond stl-sentinelstaging-watcher "$PINNED_SHA" "$pinned"
  respond stl-sentinelstaging-watcher "$DEPLOY_SHA" "$candidate"
  check "$name" "$want_exit" "$want_text" -- \
    --kustomization "$(overlay stl-sentinelstaging-watcher)" --tag "$DEPLOY_SHA"
}

install_stub_aws
echo "==> compare-image-layers.sh"

# Identical layers is the verdict the whole ticket turns on.
expect_verdict "identical layers report UNCHANGED" 0 "UNCHANGED   stl-sentinelstaging-watcher" \
  "$(manifest_with_layers sha256:one sha256:two)" "$(manifest_with_layers sha256:one sha256:two)"

# Layer order is part of the identity: same digests, different order is a
# different image, and treating it as unchanged would pin the wrong one.
expect_verdict "reordered layers report CHANGED" 0 "CHANGED     stl-sentinelstaging-watcher" \
  "$(manifest_with_layers sha256:one sha256:two)" "$(manifest_with_layers sha256:two sha256:one)"

expect_verdict "differing layers report CHANGED" 0 "CHANGED     stl-sentinelstaging-watcher" \
  "$(manifest_with_layers sha256:one)" "$(manifest_with_layers sha256:one sha256:two)"

# The ARCT-436 failure: the running tag has been expired out of the registry.
expect_verdict "a pinned tag missing from ECR reports PINNED_GONE" 0 "PINNED_GONE" \
  NONE "$(manifest_with_layers sha256:one)"

expect_verdict "an unbuilt candidate reports NOT_BUILT" 0 "NOT_BUILT" \
  "$(manifest_with_layers sha256:one)" NONE

# The failures that must never read as UNCHANGED.
expect_verdict "an unparseable manifest reports UNKNOWN, not UNCHANGED" 1 "UNKNOWN" \
  "not json at all" "not json at all"

expect_verdict "a manifest list reports UNKNOWN rather than guessing a platform" 1 "UNKNOWN" \
  "$(manifest_list)" "$(manifest_list)"

expect_verdict "an empty layer list is UNKNOWN, not two images that match" 1 "UNKNOWN" \
  "$(manifest_with_layers)" "$(manifest_with_layers)"

# A total failure must not print a clean summary: it evaluated nothing.
rm -rf "${WORK}/responses"
check "determining nothing exits non-zero" 1 "could not determine anything about a single one" -- \
  --kustomization "$(overlay stl-sentinelstaging-watcher stl-sentinelstaging-migrate)" --tag "$DEPLOY_SHA"

# One readable image is enough for the run to be healthy; the other's problem is
# reported without condemning the run.
rm -rf "${WORK}/responses"
respond stl-sentinelstaging-watcher "$PINNED_SHA" "$(manifest_with_layers sha256:one)"
respond stl-sentinelstaging-watcher "$DEPLOY_SHA" "$(manifest_with_layers sha256:one)"
check "a partial failure still exits zero when something was determined" 0 \
  "1 unchanged, 0 changed, 0 pinned tag(s) gone from ECR (retention risk on a running image), 0 candidate(s) not yet built, 0 skipped (overlay already at the deploy tag), 1 not compared" -- \
  --kustomization "$(overlay stl-sentinelstaging-watcher stl-sentinelstaging-migrate)" --tag "$DEPLOY_SHA"

# PINNED_GONE (a retention incident on a running image) and NOT_BUILT (a build
# that has not landed yet) are different operational problems and must not be
# folded into one undifferentiated "registry problems" count in the summary.
rm -rf "${WORK}/responses"
respond stl-sentinelstaging-watcher "$DEPLOY_SHA" "$(manifest_with_layers sha256:one)"
check "PINNED_GONE is counted separately from NOT_BUILT in the summary" 0 \
  "0 unchanged, 0 changed, 1 pinned tag(s) gone from ECR (retention risk on a running image), 0 candidate(s) not yet built, 0 skipped (overlay already at the deploy tag), 0 not compared" -- \
  --kustomization "$(overlay stl-sentinelstaging-watcher)" --tag "$DEPLOY_SHA"

rm -rf "${WORK}/responses"
respond stl-sentinelstaging-watcher "$PINNED_SHA" "$(manifest_with_layers sha256:one)"
check "NOT_BUILT is counted separately from PINNED_GONE in the summary" 0 \
  "0 unchanged, 0 changed, 0 pinned tag(s) gone from ECR (retention risk on a running image), 1 candidate(s) not yet built, 0 skipped (overlay already at the deploy tag), 0 not compared" -- \
  --kustomization "$(overlay stl-sentinelstaging-watcher)" --tag "$DEPLOY_SHA"

# Running after the deploy rewrote the block: both sides name the same tag, so
# there is nothing to compare and UNCHANGED would be a lie.
rm -rf "${WORK}/responses"
respond stl-sentinelstaging-watcher "$DEPLOY_SHA" "$(manifest_with_layers sha256:one)"
OVERLAY_AT_DEPLOY="${WORK}/kustomization-at-deploy.yaml"
sed "s/${PINNED_SHA}/${DEPLOY_SHA}/" "$(overlay stl-sentinelstaging-watcher)" > "$OVERLAY_AT_DEPLOY"
check "an already-rewritten overlay reports SKIPPED, not a failure" 0 "SKIPPED     stl-sentinelstaging-watcher" -- \
  --kustomization "$OVERLAY_AT_DEPLOY" --tag "$DEPLOY_SHA"
check "a fully skipped run says so plainly" 0 "there was nothing to compare. Not a failure." -- \
  --kustomization "$OVERLAY_AT_DEPLOY" --tag "$DEPLOY_SHA"
# The alarm exists to separate a real failure from a clean week; a benign
# re-deploy must not trip it, or readers learn to ignore it.
OUT_SKIP="${WORK}/skip.out"
STUB_RESPONSES="${WORK}/responses" PATH="${WORK}/bin:$PATH" \
  bash "$SUBJECT" --kustomization "$OVERLAY_AT_DEPLOY" --tag "$DEPLOY_SHA" >"$OUT_SKIP" 2>&1 || true
if grep -q 'The comparison is broken' "$OUT_SKIP"; then
  FAILED=$((FAILED + 1)); echo "  FAIL a skipped run must not raise the 'comparison is broken' alarm"
else
  PASSED=$((PASSED + 1)); echo "  ok   a skipped run does not raise the 'comparison is broken' alarm"
fi

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

# --help is sliced out of the header comment between two marker lines
# ("# Usage:" and "# End of --help output."). This guards against that
# extraction silently breaking (truncating to nothing, or running past its
# intended end) if either marker is ever edited without updating the other.
check "--help produces non-empty output containing the usage line" 0 \
  "compare-image-layers.sh --kustomization <file> --tag <40-hex-sha>" -- --help

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
expect_verdict "a throttled candidate call is UNKNOWN, not NOT_BUILT" 1 "an ECR call failed" \
  "$(manifest_with_layers sha256:one)" AWSFAIL

expect_verdict "a throttled pinned call is UNKNOWN, not PINNED_GONE" 1 "an ECR call failed" \
  AWSFAIL "$(manifest_with_layers sha256:one)"

# The realistic missing-tag shape: the call succeeds and renders None.
expect_verdict "a pinned tag ECR reports as None is PINNED_GONE" 0 "PINNED_GONE" \
  NONE "$(manifest_with_layers sha256:one)"

# Layers present but digest-less compared equal and reported "0 layer(s)
# identical" — a self-contradictory line at exit 0.
expect_verdict "digest-less layers are UNKNOWN, not 0-layers-identical" 1 "no usable layer digests" \
  "$(manifest_no_digest)" "$(manifest_no_digest)"

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

# A quote or backslash in a value used to produce invalid JSON and kill the
# script at exit 5 (outside its documented 0/1/2 contract). This must be a
# genuinely adversarial value: an earlier version of this test built
# QUOTE_OVERLAY with `newName: ...stl-sentinelstaging-watcher` -- containing no
# quote character anywhere -- so both checks below passed whether or not the
# jq -nc --arg fix was actually in place. This one carries a real double quote
# and a real backslash into two JSON string fields (image and detail).
rm -rf "${WORK}/responses"
respond stl-sentinelstaging-watcher "$PINNED_SHA" "$(manifest_with_layers sha256:one)"
respond stl-sentinelstaging-watcher "$DEPLOY_SHA" "$(manifest_with_layers sha256:one)"
QUOTE_OVERLAY="${WORK}/kustomization-quote.yaml"
ADVERSARIAL_NAME='not-an-ecr-host"quoted\repo'
{
  printf 'images:\n'
  printf '  - name: watcher\n'
  printf '    newName: 579039992622.dkr.ecr.eu-west-1.amazonaws.com/stl-sentinelstaging-watcher\n'
  printf '    newTag: %s\n' "$PINNED_SHA"
  # A second, unresolvable entry so the adversarial quote+backslash value
  # itself flows into the JSON (as `image` and inside `detail`), while the
  # watcher entry above keeps DETERMINED > 0 so the run still exits 0.
  printf '  - name: quoted\n'
  printf '    newName: %s\n' "$ADVERSARIAL_NAME"
  printf '    newTag: %s\n' "$PINNED_SHA"
} > "$QUOTE_OVERLAY"
JSON_Q="${WORK}/quote.json"
check "the JSON survives a quote and a backslash in a value" 0 "Verdicts written" -- \
  --kustomization "$QUOTE_OVERLAY" --tag "$DEPLOY_SHA" --json "$JSON_Q"
if jq -e . "$JSON_Q" >/dev/null 2>&1; then
  PASSED=$((PASSED + 1)); echo "  ok   the JSON output parses"
else
  FAILED=$((FAILED + 1)); echo "  FAIL the JSON output does not parse"
fi
if [ "$(jq -r --arg n "$ADVERSARIAL_NAME" '.results[] | select(.image == $n) | .image' "$JSON_Q" 2>/dev/null)" = "$ADVERSARIAL_NAME" ]; then
  PASSED=$((PASSED + 1)); echo "  ok   the adversarial quote+backslash value round-trips through the JSON intact"
else
  FAILED=$((FAILED + 1)); echo "  FAIL the adversarial value did not round-trip through the JSON"
  cat "$JSON_Q" 2>/dev/null | sed 's/^/         /'
fi

# classify_pair_status's fail-closed arm (finding #3): layers_of's contract is
# exactly {0, 2, 3, 4} and every value in that set already has a branch above
# it, so a stub `aws` can never actually drive this function to the fail-closed
# arm -- there is no way to make the real call site produce a fifth status.
# Extract just this function from the subject script and call it directly with
# one layers_of cannot currently produce, so the arm that matters most (the one
# standing between an unrecognized status and a false UNCHANGED) is still
# exercised end to end rather than only reasoned about.
echo "==> classify_pair_status (unit)"
CLASSIFY_SRC="$(sed -n '/^classify_pair_status()/,/^}/p' "$SUBJECT")"
if [ -z "$CLASSIFY_SRC" ]; then
  FAILED=$((FAILED + 1))
  echo "  FAIL could not extract classify_pair_status() from ${SUBJECT}"
else
  (
    eval "$CLASSIFY_SRC"
    classify_pair_status 9 0 "sha256:one" "sha256:one" "some-repo" "pinned-tag" "candidate-tag"
    printf '%s\t%s\n' "$verdict" "$detail"
  ) > "${WORK}/classify-out"
  CLASSIFY_VERDICT="$(cut -f1 "${WORK}/classify-out")"
  CLASSIFY_DETAIL="$(cut -f2- "${WORK}/classify-out")"
  if [ "$CLASSIFY_VERDICT" = "UNKNOWN" ] && printf '%s' "$CLASSIFY_DETAIL" | grep -q "unrecognized status"; then
    PASSED=$((PASSED + 1))
    echo "  ok   an unrecognized status (9) is UNKNOWN, not a fallthrough content comparison"
  else
    FAILED=$((FAILED + 1))
    echo "  FAIL an unrecognized status did not report UNKNOWN (got verdict='${CLASSIFY_VERDICT}' detail='${CLASSIFY_DETAIL}')"
  fi

  # Same call but with identical layers: proves the fail-closed arm is checked
  # BEFORE the content comparison, not after -- otherwise an unrecognized
  # status paired with equal layer strings would still report UNCHANGED, which
  # is exactly the dangerous wrong answer this arm exists to prevent.
  (
    eval "$CLASSIFY_SRC"
    classify_pair_status 0 9 "sha256:one" "sha256:one" "some-repo" "pinned-tag" "candidate-tag"
    printf '%s\n' "$verdict"
  ) > "${WORK}/classify-out2"
  if [ "$(cat "${WORK}/classify-out2")" = "UNKNOWN" ]; then
    PASSED=$((PASSED + 1))
    echo "  ok   an unrecognized status wins over an equal-layers comparison, never UNCHANGED"
  else
    FAILED=$((FAILED + 1))
    echo "  FAIL an unrecognized status with equal layers reported '$(cat "${WORK}/classify-out2")', not UNKNOWN"
  fi
fi

# check()'s own `shift 4` (finding #4): a call site short of arguments must not
# abort the whole suite under set -e with "shift: shift count out of range" --
# it should report one FAIL for that call and let the rest of the suite run.
# Exercised in a subshell so the deliberately-malformed call below does not
# corrupt this run's own PASSED/FAILED.
echo "==> check() hardening (unit)"
if (
  PASSED=0; FAILED=0
  check "deliberately short call" 0 "irrelevant"
  [ "$FAILED" -eq 1 ] && [ "$PASSED" -eq 0 ]
) >/dev/null 2>&1; then
  PASSED=$((PASSED + 1))
  echo "  ok   check() reports a clean FAIL on a short call instead of aborting the suite"
else
  FAILED=$((FAILED + 1))
  echo "  FAIL check() did not handle a short call as expected"
fi

# The JSON envelope carries generatedAt so a week of verdicts can be ordered and
# correlated with deploys.
rm -rf "${WORK}/responses"
respond stl-sentinelstaging-watcher "$PINNED_SHA" "$(manifest_with_layers sha256:one)"
respond stl-sentinelstaging-watcher "$DEPLOY_SHA" "$(manifest_with_layers sha256:two)"
JSON_G="${WORK}/generated.json"
check "the JSON envelope is written" 0 "Verdicts written" -- \
  --kustomization "$(overlay stl-sentinelstaging-watcher)" --tag "$DEPLOY_SHA" --json "$JSON_G"
if [ -n "$(jq -r '.generatedAt // empty' "$JSON_G" 2>/dev/null)" ]; then
  PASSED=$((PASSED + 1)); echo "  ok   generatedAt is recorded in the JSON envelope"
else
  FAILED=$((FAILED + 1)); echo "  FAIL generatedAt missing from the JSON envelope"
  cat "$JSON_G" 2>/dev/null | sed 's/^/         /'
fi

echo "${PASSED} passed, ${FAILED} failed"
[ "$FAILED" -eq 0 ] || exit 1
