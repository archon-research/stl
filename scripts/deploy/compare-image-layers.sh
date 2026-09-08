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
# Verdicts, one per image:
#   UNCHANGED  both resolve, layers identical -> the deploy could keep the pinned
#              tag and leave these pods alone
#   CHANGED    both resolve, layers differ -> the deploy must bump this tag
#   NOT_BUILT  the candidate tag is absent from ECR. Not a comparison result: at
#              this point in the deploy it should exist, so it is reported as a
#              failure of the run, not as a service that happens to be unchanged
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
trap 'rm -f "$PAIRS_FILE"' EXIT
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
# space-separated and newline-free, or nothing at all if it could not be read.
# Silence here always means "could not read", never "no layers".
layers_of() {
  local account="$1" region="$2" repo="$3" tag="$4" manifest media

  manifest="$(aws ecr batch-get-image \
      --region "$region" \
      --registry-id "$account" \
      --repository-name "$repo" \
      --image-ids "imageTag=${tag}" \
      --query 'images[0].imageManifest' --output text 2>/dev/null)" || return 1
  [ -n "$manifest" ] && [ "$manifest" != "None" ] || return 1

  # A manifest list has no layers of its own. Guessing a platform out of one
  # would compare an arbitrary child image, so refuse instead.
  media="$(printf '%s' "$manifest" | jq -r '.mediaType // ""' 2>/dev/null)" || return 1
  case "$media" in
    *manifest.list*|*image.index*) return 1 ;;
  esac

  printf '%s' "$manifest" | jq -er '
      if (.layers | type) == "array" and (.layers | length) > 0
      then [.layers[].digest] | join(" ")
      else error("no layers in manifest") end' 2>/dev/null || return 1
}

UNCHANGED=0; CHANGED=0; REGISTRY_PROBLEMS=0; UNDETERMINED=0; DETERMINED=0
ROWS=""

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
    pinnedLayers="$(layers_of "$account" "$region" "$repo" "$pinnedTag" || true)"
    candidateLayers="$(layers_of "$account" "$region" "$repo" "$candidateTag" || true)"

    if [ -z "$pinnedLayers" ] && [ -z "$candidateLayers" ]; then
      verdict="UNKNOWN"; detail="neither ${pinnedTag} nor ${candidateTag} could be read from ${repo}"
    elif [ -z "$pinnedLayers" ]; then
      verdict="PINNED_GONE"; detail="${repo}:${pinnedTag} is pinned by the overlay but absent from ECR"
    elif [ -z "$candidateLayers" ]; then
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

  ROWS="${ROWS}$(printf '{"image":"%s","repository":"%s","pinnedTag":"%s","candidateTag":"%s","verdict":"%s","detail":"%s"}' \
    "$newName" "$repo" "$pinnedTag" "$candidateTag" "$verdict" "$detail"),"
done < "$PAIRS_FILE"

if [ -n "$JSON_OUT" ]; then
  printf '{"deploySha":"%s","kustomization":"%s","results":[%s]}\n' \
    "$TAG" "$KUSTOMIZATION" "${ROWS%,}" | jq . > "$JSON_OUT"
  echo "Verdicts written to ${JSON_OUT}"
fi

cat <<SUMMARY
Summary: ${UNCHANGED} unchanged, ${CHANGED} changed, ${REGISTRY_PROBLEMS} missing from ECR, ${UNDETERMINED} not compared (of ${PAIR_COUNT}).
This run changed nothing. Today's deploy rewrites all ${PAIR_COUNT} tags regardless of
this verdict, so any image reported unchanged above names pods that rolled for no reason.
SUMMARY

# The exit code reports whether the comparison worked, never what the deploy
# should do. A run that determined nothing has told us nothing, and the one
# thing it must not do is read as clean.
if [ "$DETERMINED" -eq 0 ]; then
  echo "::error::Examined ${PAIR_COUNT} image(s) and could not determine anything about a single one. The comparison is broken, not the deploy — do not read this as 'nothing changed'." >&2
  exit 1
fi
exit 0
