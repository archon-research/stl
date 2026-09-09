#!/usr/bin/env bash
#
# compare-image-layers.sh — for each image an overlay pins, report whether the
# newly built image for a deploy SHA has the same layers as the one already
# running. Reports; never acts.
#
# This is ORB-366's measurement step. The deploy currently rewrites every tag in
# the overlay to the deploy SHA, so every service's pods roll on every deploy,
# including the services whose code did not change — a docs edit anywhere under
# stl-verify/ sets `go` in .github/changed-files.yml and rolls all of them. The
# proposal is to leave an unchanged service's tag alone. Before any deploy
# behaviour changes, this runs alongside the existing file-list logic and says
# what it *would* have decided, so the two can be reconciled against real
# deploys rather than argued about.
#
# Nothing here writes a tag, a file, or an image. Its exit code says whether the
# comparison itself worked, never what the deploy should do.
#
# Why layer lists and not manifest digests: the commit now rides in the image
# config as ENV (ORB-366), so an unchanged service's manifest digest still
# differs between two commits while its layers do not. Comparing manifests would
# report everything as changed and quietly reproduce today's behaviour.
#
# Both tags are read from ECR after the push, so the two sides are the same kind
# of thing — registry layer digests. A locally built image reports uncompressed
# diff IDs instead, which are not comparable with what a registry stores, and
# comparing the two would report every service as changed.
#
# Expect one all-CHANGED day per week, and do not read it as a fault. The
# weekly security refresh (.github/workflows/image-security-refresh.yaml)
# rebuilds every image with the layer cache disabled, so the OS package layers
# move and the first deploy after it legitimately differs from every pinned
# image. That is the deploy this whole mechanism is meant to allow — patches
# landing — but it means a week of shadow verdicts contains one day where
# essentially every image reports CHANGED. Anyone reconciling those verdicts to
# justify the cutover should exclude that day, or read it as the refresh rather
# than as churn the comparison failed to suppress.
#
# Verdicts, one per image:
#   UNCHANGED  both resolve, layers identical -> the deploy could keep the pinned
#              tag and leave these pods alone
#   CHANGED    both resolve, layers differ -> the deploy must bump this tag
#   NOT_BUILT  the candidate tag is absent from ECR, though by this point in the
#              deploy it should exist. That is a finding about the deploy, not a
#              failure of the comparison: it is reported loudly, counts as
#              determined, and does not affect the exit code — see the note on
#              UNKNOWN below for what the exit code does mean
#   PINNED_GONE the tag the overlay pins is absent from ECR. This is the
#              openmetadata-ingestion failure (ARCT-436, 2026-08-31): the image
#              is gone but nothing notices until a node rotates and the kubelet
#              can no longer pull from its local cache. The mechanism this
#              script measures for (UNCHANGED keeping the pinned tag instead of
#              bumping it) leaves that pinned tag referenced for longer than it
#              would otherwise be, without extending its retention in ECR by a
#              single day — so the same lifecycle-policy expiry now lands on a
#              tag pods are still depending on. Called out loudly here for that
#              reason
#   UNKNOWN    the comparison could not be made (AWS error, unparseable
#              manifest, a manifest list where a single image was expected)
#
# UNKNOWN exists because the dangerous failure is not a wrong verdict, it is a
# comforting one: a comparator that answers "unchanged" because a credential
# expired would, once this drives the deploy, pin an old image forever and
# report success. Nothing here defaults to UNCHANGED, and a run that could not
# evaluate a single image exits non-zero rather than printing an empty summary.
#
# Usage:
#   compare-image-layers.sh --kustomization <file> --tag <40-hex-sha> [--json <path>] [--weekly-refresh]
#
#   --kustomization  the overlay whose pinned tags are the "already running"
#                    side. Read as it stands on disk, so run this BEFORE the
#                    deploy rewrites it — afterwards both sides name the same
#                    tag and every verdict is trivially UNCHANGED.
#   --tag            the deploy SHA, giving the candidate tag for each image
#                    (the trailing 40-hex is replaced, any cronjob "name-"
#                    prefix kept — the same rule verify-ecr-images.sh uses)
#   --json           also write the verdicts as JSON, for collecting a week of
#                    them and reconciling every disagreement
#   --weekly-refresh tell this run it follows the Monday 04:00 UTC
#                    image-security-refresh, so a wave of CHANGED verdicts is
#                    the refresh landing, not churn. This script has no
#                    reliable way to infer that itself (the caller knows when
#                    it is running; guessing from the current date here would
#                    just move the unreliable inference inside the thing nobody
#                    can double-check), so it is a flag the caller sets, not a
#                    computation done here. Recorded in the JSON as
#                    weeklyRefresh and noted in the human summary; changes no
#                    verdict.
#
# Requires AWS credentials for the account the overlay's images name, with
# ecr:BatchGetImage — the same permission verify-ecr-images.sh needs and the
# deploy job already holds. Read-only.
#
# Deliberately bash 3.2 + BSD awk compatible, like the other scripts here.
set -euo pipefail

KUSTOMIZATION=""
TAG=""
JSON_OUT=""
WEEKLY_REFRESH=0

die() { echo "::error::$*" >&2; exit 2; }

while [ $# -gt 0 ]; do
  case "$1" in
    --kustomization)  KUSTOMIZATION="${2:-}"; shift 2 ;;
    --tag)             TAG="${2:-}"; shift 2 ;;
    --json)            JSON_OUT="${2:-}"; shift 2 ;;
    --weekly-refresh)  WEEKLY_REFRESH=1; shift ;;
    -h|--help) sed -n '/^# Usage:/,/^# Requires AWS/p' "$0" | sed '$d' | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) die "unknown argument: $1" ;;
  esac
done

[ -n "$KUSTOMIZATION" ] && [ -f "$KUSTOMIZATION" ] || die "--kustomization must point to an existing file (got: '${KUSTOMIZATION}')"
[[ "$TAG" =~ ^[a-f0-9]{40}$ ]] || die "--tag must be a 40-char lowercase git SHA (got: '${TAG}')"

command -v jq >/dev/null || die "jq is required to read ECR image manifests"
command -v aws >/dev/null || die "the aws CLI is required to read ECR image manifests"

# (newName, newTag) pairs from the overlay's images block. Same shape as
# verify-ecr-images.sh: kustomize emits `- name:` / `newName:` / `newTag:`, each
# newName pairs with the newTag that follows it, and several bases can share one
# image so the list is deduped.
#
# This is the THIRD independent parser of this block (render-overlay-images.sh,
# check-overlay-tag-consistency.sh, and this one), each reading the YAML its own
# way. They are not unified here — out of scope for ORB-366 — but
# render-overlay-images.sh is authoritative: k8s/AGENTS.md says the block is
# generated from k8s/image-roster.txt by that script, and its --check mode is
# what actually gates deploy-prod. Known divergence: this parser strips only a
# leading/trailing quote char from a value and does not strip a trailing `#
# comment`, while both other parsers do: render-overlay-images.sh anchors its
# newTag sed with `"?[[:space:]]*(#.*)?$`, and check-overlay-tag-consistency.sh
# strips with `s/[[:space:]]+#.*$//`. A hand-pinned entry like
# `newTag: "<sha>"  # pinned per ARCT-436` parses clean in both of those and
# comes out here as the sha with `"  # pinned per ARCT-436` glued on. Verified
# by running all three against that exact line.
#
# It fails safe: the mangled tag does not resolve in ECR, so this script says
# UNKNOWN rather than giving a wrong verdict, and the bot-written block carries
# no comments today. Tracked as VEC-754 (not fixed here -- unifying three
# parsers touches the deploy path, which a log-only measurement should not).
PAIRS_FILE="$(mktemp)"
ROWS_FILE="$(mktemp)"
trap 'rm -f "$PAIRS_FILE" "$ROWS_FILE"' EXIT
awk '
    /^images:/            { in_images = 1; next }
    in_images && /^[^[:space:]-]/ { in_images = 0 }
    !in_images            { next }
    /^[[:space:]]*-?[[:space:]]*newName:/ {
      v = $0; sub(/^[^:]*:[[:space:]]*/, "", v); gsub(/^"|"$/, "", v); name = v; next
    }
    /^[[:space:]]*newTag:/ {
      v = $0; sub(/^[^:]*:[[:space:]]*/, "", v); gsub(/^"|"$/, "", v)
      if (name != "") { print name "\t" v; name = "" }
    }
  ' "$KUSTOMIZATION" | sort -u > "$PAIRS_FILE"

PAIR_COUNT="$(wc -l < "$PAIRS_FILE" | tr -d ' ')"
[ "$PAIR_COUNT" -gt 0 ] || die "no images: entries found in ${KUSTOMIZATION}"

# layers_of <account> <region> <repo> <tag>: echo the image's layer digests,
# space-separated and newline-free, and report WHY it could not through the exit
# code. The distinction is the whole safety property of this script:
#
#   0  read successfully; digests on stdout
#   2  the ECR call itself failed (throttle, credentials, network) -> UNDETERMINED
#   3  the call succeeded and the tag is genuinely absent          -> a finding
#   4  the manifest exists but is unusable (list, unparseable, no digests)
#
# Collapsing 2 into 3 is the failure that matters. A throttled call on the
# candidate would read as NOT_BUILT and one on the pinned tag as PINNED_GONE --
# both confident claims about the registry, both counted as determined, both
# exiting 0. This run makes 2 calls per image with no retry config, so a
# throttle is the realistic failure, and after the cutover it is the one that
# inverts the required response.
#
# `batch-get-image` exits 0 for a tag that does not exist and puts the miss in
# failures[], so `images[0].imageManifest` renders "None". A non-zero exit is
# therefore the call failing, never a missing tag.
#
# The `|| status=$?` form on the assignment below is load-bearing, not style.
# Every call site happens to invoke this as an `if` condition, and under `set
# -e` that context suppresses errexit for everything evaluated inside it,
# including a failing command substitution deep in this function -- which is
# the only reason a plain `status=$?` on the next line ever gets to run.
# Called any other way (a future call site, a helper that isn't an `if`), the
# assignment's failure would abort the whole script right there under `set
# -e`, before `status=$?` executes, collapsing the 2/3/4 distinction into a
# bare `set -e` exit. Attaching `|| status=$?` directly to the failing command
# is exempt from errexit unconditionally -- it is the command before the final
# `||` in an OR list -- so the distinction survives regardless of how this
# function is called.
layers_of() {
  local account="$1" region="$2" repo="$3" tag="$4" manifest media status=0

  manifest="$(aws ecr batch-get-image \
      --region "$region" \
      --registry-id "$account" \
      --repository-name "$repo" \
      --image-ids "imageTag=${tag}" \
      --query 'images[0].imageManifest' --output text 2>/dev/null)" || status=$?
  [ "$status" -eq 0 ] || return 2
  [ -n "$manifest" ] && [ "$manifest" != "None" ] || return 3

  # A manifest list has no layers of its own. Guessing a platform out of one
  # would compare an arbitrary child image, so refuse instead.
  media="$(printf '%s' "$manifest" | jq -r '.mediaType // ""' 2>/dev/null)" || return 4
  case "$media" in
    *manifest.list*|*image.index*) return 4 ;;
  esac

  # Every layer must carry a non-empty digest, not just the array be non-empty:
  # [null,null] | join(" ") is " ", which is non-empty, so two digest-less
  # layers used to compare equal and report "0 layer(s) identical".
  printf '%s' "$manifest" | jq -er '
      if (.layers | type) == "array"
         and (.layers | length) > 0
         and ([.layers[].digest] | map(select(type == "string" and length > 0)) | length) == (.layers | length)
      then [.layers[].digest] | join(" ")
      else error("manifest has no usable layer digests") end' 2>/dev/null || return 4
}

# classify_pair_status <pinnedStatus> <candidateStatus> <pinnedLayers>
# <candidateLayers> <repo> <pinnedTag> <candidateTag>: set $verdict and
# $detail from one pair of layers_of results.
#
# Split out from the main loop so its last arm can be unit-tested directly:
# layers_of's contract is exactly {0, 2, 3, 4}, and a stub `aws` can only ever
# drive this function through statuses in that set, so the fail-closed arm
# below -- the one that matters most, since it is the only thing standing
# between an unrecognized status and a content comparison run on
# possibly-empty strings -- could never be exercised end-to-end. As its own
# function, a test can call it with a status layers_of cannot currently
# produce and check that it still refuses to guess (see
# compare-image-layers.test.sh).
classify_pair_status() {
  local pinnedStatus="$1" candidateStatus="$2" pinnedLayers="$3" candidateLayers="$4" repo="$5" pinnedTag="$6" candidateTag="$7"

  if [ "$pinnedStatus" -eq 2 ] || [ "$candidateStatus" -eq 2 ]; then
    verdict="UNKNOWN"
    detail="an ECR call failed for ${repo} (throttle, credentials or network); absent and unreadable are not distinguishable here, so neither is claimed"
  elif [ "$pinnedStatus" -eq 4 ] || [ "$candidateStatus" -eq 4 ]; then
    verdict="UNKNOWN"; detail="${repo} returned a manifest with no usable layer digests"
  elif [ "$pinnedStatus" -eq 3 ] && [ "$candidateStatus" -eq 3 ]; then
    verdict="UNKNOWN"; detail="neither ${pinnedTag} nor ${candidateTag} exists in ${repo}"
  elif [ "$pinnedStatus" -eq 3 ]; then
    verdict="PINNED_GONE"; detail="${repo}:${pinnedTag} is pinned by the overlay but absent from ECR"
  elif [ "$candidateStatus" -eq 3 ]; then
    verdict="NOT_BUILT"; detail="${repo}:${candidateTag} was not found; expected it to exist by this point in the deploy"
  elif [ "$pinnedStatus" -ne 0 ] || [ "$candidateStatus" -ne 0 ]; then
    # Fail closed. layers_of promises only {0, 2, 3, 4} and every value in
    # that set is handled above, so reaching here means either side returned
    # something this function does not recognize. The one thing that must
    # never happen next is falling into the content comparison below with
    # possibly-empty strings and risking UNCHANGED -- the most dangerous wrong
    # answer, since it means "do not redeploy".
    verdict="UNKNOWN"
    detail="${repo} returned an unrecognized status from layers_of (pinned=${pinnedStatus}, candidate=${candidateStatus})"
  elif [ "$pinnedLayers" = "$candidateLayers" ]; then
    verdict="UNCHANGED"; detail="$(printf '%s' "$pinnedLayers" | wc -w | tr -d ' ') layer(s) identical to ${pinnedTag}"
  else
    verdict="CHANGED"; detail="layers differ from ${pinnedTag}"
  fi
}

UNCHANGED=0; CHANGED=0; PINNED_GONE_COUNT=0; NOT_BUILT_COUNT=0; UNDETERMINED=0; DETERMINED=0

echo "Comparing ${PAIR_COUNT} image(s) pinned by ${KUSTOMIZATION} against their build at ${TAG:0:12}"

while IFS=$'\t' read -r newName pinnedTag; do
  [ -z "$newName" ] && continue

  candidateTag="$(printf '%s' "$pinnedTag" | sed -E "s/[a-f0-9]{40}/${TAG}/")"

  host="${newName%%/*}"
  repo="${newName#*/}"
  account="${host%%.*}"
  region="$(printf '%s' "$host" | sed -E 's/^[0-9]+\.dkr\.ecr\.([a-z0-9-]+)\.amazonaws\.com$/\1/')"
  if [ "$region" = "$host" ] || ! [[ "$account" =~ ^[0-9]{12}$ ]]; then
    verdict="UNKNOWN"; detail="could not parse ECR account/region from ${newName}"
  elif [ "$pinnedTag" = "$candidateTag" ]; then
    # The overlay already names the deploy SHA, so there is no "already running"
    # side left to compare against. Reporting UNCHANGED here would be the
    # comforting answer, and wrong: it means this ran after the rewrite.
    verdict="UNKNOWN"
    detail="overlay already pins ${TAG:0:12}; run this before the deploy rewrites the block"
  else
    if pinnedLayers="$(layers_of "$account" "$region" "$repo" "$pinnedTag")"; then
      pinnedStatus=0
    else
      pinnedStatus=$?
    fi
    if candidateLayers="$(layers_of "$account" "$region" "$repo" "$candidateTag")"; then
      candidateStatus=0
    else
      candidateStatus=$?
    fi

    classify_pair_status "$pinnedStatus" "$candidateStatus" "$pinnedLayers" "$candidateLayers" "$repo" "$pinnedTag" "$candidateTag"
  fi

  # PINNED_GONE and NOT_BUILT are findings, not failures: the comparison worked
  # and found something true about the registry. Only UNKNOWN means the
  # comparator could not do its job, which is the one thing its exit code
  # reports. Both findings stay warnings here because this run must never
  # affect a deploy — escalating a missing pinned tag to a hard failure belongs
  # to the registry conformance check (ARCT-436), not to a shadow comparison.
  #
  # PINNED_GONE and NOT_BUILT are counted separately (not folded into one
  # "registry problems" number): a pinned tag disappearing from ECR is a
  # retention incident on an image already running, while a candidate tag
  # missing is a build that has not landed yet by this point in the deploy.
  # Different operational problems, different responses, so the summary below
  # names them separately rather than as one undifferentiated count.
  case "$verdict" in
    UNCHANGED)   UNCHANGED=$((UNCHANGED + 1)); DETERMINED=$((DETERMINED + 1)); echo "  UNCHANGED   ${repo}: ${detail}" ;;
    CHANGED)     CHANGED=$((CHANGED + 1));     DETERMINED=$((DETERMINED + 1)); echo "  CHANGED     ${repo}: ${detail}" ;;
    UNKNOWN)     UNDETERMINED=$((UNDETERMINED + 1)); echo "::warning::UNKNOWN ${repo}: ${detail}" ;;
    PINNED_GONE) PINNED_GONE_COUNT=$((PINNED_GONE_COUNT + 1)); DETERMINED=$((DETERMINED + 1)); echo "::warning::PINNED_GONE ${repo}: ${detail}" ;;
    NOT_BUILT)   NOT_BUILT_COUNT=$((NOT_BUILT_COUNT + 1));     DETERMINED=$((DETERMINED + 1)); echo "::warning::NOT_BUILT ${repo}: ${detail}" ;;
    *)           echo "::error::${repo} produced an unrecognized verdict '${verdict}': ${detail}" >&2; exit 1 ;;
  esac

  # Built by jq, not by interpolation: a quote anywhere in newName or detail
  # used to produce invalid JSON, and `| jq .` then failed under pipefail with
  # exit 5 — outside this script's documented 0/1/2 contract, with no
  # ::error::, and swallowed by continue-on-error. The measurement simply
  # vanished.
  jq -nc \
    --arg image "$newName" \
    --arg repository "$repo" \
    --arg pinnedTag "$pinnedTag" \
    --arg candidateTag "$candidateTag" \
    --arg verdict "$verdict" \
    --arg detail "$detail" \
    '{image: $image, repository: $repository, pinnedTag: $pinnedTag, candidateTag: $candidateTag, verdict: $verdict, detail: $detail}' \
    >> "$ROWS_FILE"
done < "$PAIRS_FILE"

if [ -n "$JSON_OUT" ]; then
  # generatedAt is recorded so a week of verdicts can be reconciled against the
  # refresh workflow's actual run history, which is authoritative. weeklyRefresh
  # stays caller-supplied and defaults to false: a reconciler should cross-check
  # the timestamp rather than trust a flag nobody could verify.
  jq -s \
    --arg deploySha "$TAG" \
    --arg kustomization "$KUSTOMIZATION" \
    --arg generatedAt "$(date -u +%Y-%m-%dT%H:%M:%SZ)" \
    --argjson weeklyRefresh "$([ "$WEEKLY_REFRESH" -eq 1 ] && echo true || echo false)" \
    '{deploySha: $deploySha, kustomization: $kustomization, generatedAt: $generatedAt, weeklyRefresh: $weeklyRefresh, results: .}' \
    "$ROWS_FILE" > "$JSON_OUT"
  echo "Verdicts written to ${JSON_OUT}"
fi

cat <<SUMMARY
Summary: ${UNCHANGED} unchanged, ${CHANGED} changed, ${PINNED_GONE_COUNT} pinned tag(s) gone from ECR (retention risk on a running image), ${NOT_BUILT_COUNT} candidate(s) not yet built, ${UNDETERMINED} not compared (of ${PAIR_COUNT}).
This run changed nothing. Today's deploy rewrites every newTag in the block regardless
of this verdict — more entries than the ${PAIR_COUNT} images counted here, since several
bases share one image — so any image reported unchanged above names pods that rolled
for no reason.
SUMMARY

if [ "$WEEKLY_REFRESH" -eq 1 ]; then
  cat <<REFRESH
This run is flagged --weekly-refresh: it follows the Monday 04:00 UTC
image-security-refresh, so a wave of CHANGED verdicts above is that refresh
landing, not churn the comparison failed to suppress.
REFRESH
fi

# The exit code reports whether the comparison worked, never what the deploy
# should do. A run that determined nothing has told us nothing, and the one
# thing it must not do is read as clean.
if [ "$DETERMINED" -eq 0 ]; then
  echo "::error::Examined ${PAIR_COUNT} image(s) and could not determine anything about a single one. The comparison is broken, not the deploy — do not read this as 'nothing changed'." >&2
  exit 1
fi
exit 0
