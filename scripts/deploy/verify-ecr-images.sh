#!/usr/bin/env bash
#
# verify-ecr-images.sh — fail if any image a Kustomize overlay references is
# absent from its ECR repository.
#
# Closes the gap behind two incidents:
#   * ORB-299: the prod promotion list drifted, so the bot stamped a prod
#     manifest with an image:SHA that had never been pushed -> ImagePullBackOff.
#   * ORB-313: a brand-new service was added with a placeholder tag; ArgoCD
#     synced it before the image existed -> ImagePullBackOff race.
#
# The overlay's `images:` block is the source of truth for what will actually
# run, so we validate against it directly rather than any hand-maintained list.
#
# Usage:
#   verify-ecr-images.sh --kustomization k8s/overlays/prod/kustomization.yaml [--tag <40-hex-sha>]
#
#   --tag SHA   Resolve every newTag to this deploy SHA before checking
#               (mirrors the sed the deploy bot uses to stamp tags: the trailing
#               40-hex is replaced, any cronjob "name-" prefix is preserved).
#               Omit to check the tags exactly as written in the overlay
#               (PR-time / pre-sync mode).
#
# Requires AWS credentials for the account that owns the overlay's ECR registry
# (the caller configures the right role; a single overlay only references one
# account). Uses `aws ecr describe-images`, which needs `ecr:DescribeImages`.
set -euo pipefail

KUSTOMIZATION=""
TAG_OVERRIDE=""
while [ $# -gt 0 ]; do
  case "$1" in
    --kustomization) KUSTOMIZATION="$2"; shift 2 ;;
    --tag)           TAG_OVERRIDE="$2"; shift 2 ;;
    *) echo "::error::unknown argument: $1" >&2; exit 2 ;;
  esac
done

if [ -z "$KUSTOMIZATION" ] || [ ! -f "$KUSTOMIZATION" ]; then
  echo "::error::--kustomization must point to an existing file (got: '${KUSTOMIZATION}')" >&2
  exit 2
fi
if [ -n "$TAG_OVERRIDE" ] && [[ ! "$TAG_OVERRIDE" =~ ^[a-f0-9]{40}$ ]]; then
  echo "::error::--tag must be a 40-char lowercase git SHA (got: '${TAG_OVERRIDE}')" >&2
  exit 2
fi

# Pull (newName, newTag) pairs out of the images: block. Entries are emitted by
# kustomize as `- name:` / `newName:` / `newTag:`; we pair each newName with the
# newTag that follows it and dedupe (several bases can share one image).
PAIRS_FILE="$(mktemp)"
trap 'rm -f "$PAIRS_FILE"' EXIT
awk '
    /^images:/            { in_images = 1; next }
    in_images && /^[^[:space:]-]/ { in_images = 0 }   # a new top-level key ends the block
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
if [ "$PAIR_COUNT" -eq 0 ]; then
  echo "::error::no images: entries found in ${KUSTOMIZATION}" >&2
  exit 2
fi

echo "Verifying ${PAIR_COUNT} image(s) referenced by ${KUSTOMIZATION}${TAG_OVERRIDE:+ (resolved to ${TAG_OVERRIDE})}"

MISSING=()
CHECKED=0
while IFS=$'\t' read -r newName newTag; do
  [ -z "$newName" ] && continue

  # Resolve to the deploy SHA if requested, preserving any cronjob name prefix.
  if [ -n "$TAG_OVERRIDE" ]; then
    newTag="$(printf '%s' "$newTag" | sed -E "s/[a-f0-9]{40}/${TAG_OVERRIDE}/")"
  fi

  # newName = <account>.dkr.ecr.<region>.amazonaws.com/<repo>
  host="${newName%%/*}"
  repo="${newName#*/}"
  region="$(printf '%s' "$host" | sed -E 's/^[0-9]+\.dkr\.ecr\.([a-z0-9-]+)\.amazonaws\.com$/\1/')"
  if [ "$region" = "$host" ]; then
    echo "::error::could not parse ECR region from image name: ${newName}" >&2
    exit 2
  fi

  if aws ecr describe-images \
        --region "$region" \
        --repository-name "$repo" \
        --image-ids "imageTag=${newTag}" \
        >/dev/null 2>&1; then
    echo "  ok   ${repo}:${newTag}"
  else
    echo "  MISS ${repo}:${newTag}"
    MISSING+=("${repo}:${newTag}")
  fi
  CHECKED=$((CHECKED + 1))
done < "$PAIRS_FILE"

if [ "${#MISSING[@]}" -gt 0 ]; then
  echo "::error::${#MISSING[@]}/${CHECKED} image(s) referenced by ${KUSTOMIZATION} are missing from ECR:" >&2
  for m in "${MISSING[@]}"; do echo "::error::  ${m}" >&2; done
  echo "::error::Build+push these images before the manifest can reference them (for a brand-new service, land the image in a separate PR first; see CONTRIBUTING.md section 14, 'Pull request workflow')." >&2
  exit 1
fi

echo "All ${CHECKED} referenced image(s) exist in ECR."
