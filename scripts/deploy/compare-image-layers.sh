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
#              can no longer pull from its local cache. Keeping older tags for
#              longer makes it likelier, so it is called out loudly here
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
#   compare-image-layers.sh --kustomization <file> --tag <40-hex-sha> [--json <path>]
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

die() { echo "::error::$*" >&2; exit 2; }

while [ $# -gt 0 ]; do
  case "$1" in
    --kustomization) KUSTOMIZATION="${2:-}"; shift 2 ;;
    --tag)           TAG="${2:-}"; shift 2 ;;
    --json)          JSON_OUT="${2:-}"; shift 2 ;;
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
layers_of() {
  local account="$1" region="$2" repo="$3" tag="$4" manifest media status

  manifest="$(aws ecr batch-get-image \
      --region "$region" \
      --registry-id "$account" \
      --repository-name "$repo" \
      --image-ids "imageTag=${tag}" \
      --query 'images[0].imageManifest' --output text 2>/dev/null)"
  status=$?
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

UNCHANGED=0; CHANGED=0; REGISTRY_PROBLEMS=0; UNDETERMINED=0; DETERMINED=0

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
    elif [ "$pinnedLayers" = "$candidateLayers" ]; then
      verdict="UNCHANGED"; detail="$(printf '%s' "$pinnedLayers" | wc -w | tr -d ' ') layer(s) identical to ${pinnedTag}"
    else
      verdict="CHANGED"; detail="layers differ from ${pinnedTag}"
    fi
  fi

  # PINNED_GONE and NOT_BUILT are findings, not failures: the comparison worked
  # and found something true about the registry. Only UNKNOWN means the
  # comparator could not do its job, which is the one thing its exit code
  # reports. Both findings stay warnings here because this run must never
  # affect a deploy — escalating a missing pinned tag to a hard failure belongs
  # to the registry conformance check (ARCT-436), not to a shadow comparison.
  case "$verdict" in
    UNCHANGED) UNCHANGED=$((UNCHANGED + 1)); DETERMINED=$((DETERMINED + 1)); echo "  UNCHANGED   ${repo}: ${detail}" ;;
    CHANGED)   CHANGED=$((CHANGED + 1));     DETERMINED=$((DETERMINED + 1)); echo "  CHANGED     ${repo}: ${detail}" ;;
    UNKNOWN)   UNDETERMINED=$((UNDETERMINED + 1)); echo "::warning::UNKNOWN ${repo}: ${detail}" ;;
    *)         REGISTRY_PROBLEMS=$((REGISTRY_PROBLEMS + 1)); DETERMINED=$((DETERMINED + 1)); echo "::warning::${verdict} ${repo}: ${detail}" ;;
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
  jq -s \
    --arg deploySha "$TAG" \
    --arg kustomization "$KUSTOMIZATION" \
    '{deploySha: $deploySha, kustomization: $kustomization, results: .}' \
    "$ROWS_FILE" > "$JSON_OUT"
  echo "Verdicts written to ${JSON_OUT}"
fi

cat <<SUMMARY
Summary: ${UNCHANGED} unchanged, ${CHANGED} changed, ${REGISTRY_PROBLEMS} missing from ECR, ${UNDETERMINED} not compared (of ${PAIR_COUNT}).
This run changed nothing. Today's deploy rewrites every newTag in the block regardless
of this verdict — more entries than the ${PAIR_COUNT} images counted here, since several
bases share one image — so any image reported unchanged above names pods that rolled
for no reason.
SUMMARY

# The exit code reports whether the comparison worked, never what the deploy
# should do. A run that determined nothing has told us nothing, and the one
# thing it must not do is read as clean.
if [ "$DETERMINED" -eq 0 ]; then
  echo "::error::Examined ${PAIR_COUNT} image(s) and could not determine anything about a single one. The comparison is broken, not the deploy — do not read this as 'nothing changed'." >&2
  exit 1
fi
exit 0
