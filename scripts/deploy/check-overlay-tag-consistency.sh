#!/usr/bin/env bash
#
# check-overlay-tag-consistency.sh — fail if an ArgoCD-synced overlay's image
# tags do not all name the same commit.
#
# k8s/AGENTS.md: "Never hand-edit image tags in
# k8s/overlays/{staging,prod}/kustomization.yaml — CI owns them." The deploy bot
# stamps every entry in one file to a single deploy SHA, so "all tags share one
# SHA" is that rule expressed as an invariant. A tag written by hand shows up as
# the odd one out.
#
# Why the existing ECR gate cannot cover this (ORB-313, recurrence in #721):
# verify-ecr-images.sh runs inside update-staging / update-prod, but those jobs
# first sed every `newTag` to the deploy SHA and only then verify. A hand-written
# tag is rewritten before it is ever checked, so the gate passes. Meanwhile
# ArgoCD syncs the *merge commit* — with the hand-written tag still in it —
# minutes before the bot stamps the real one. In #721 that tag was
# `reference-capital-indexer-e77e8fc0...`, a branch SHA whose image was never
# built: the new Deployment sat in ImagePullBackOff, the staging health gate went
# Degraded, and the prod promotion was skipped.
#
# Deliberately stateless — it inspects the tree as it stands, not a diff against
# a base ref. A diff-based check fires on every PR whose branch is behind main,
# because the bot's bumps on main read as reverted tags in the branch.
#
# KNOWN GAP: this catches a tag naming the *wrong commit*, not every unbuildable
# tag. A new service added with a tag matching the current majority SHA passes
# here, yet its image does not exist either — nothing built that service at that
# older commit. Closing that needs verify-ecr-images.sh run at PR time, which
# needs an ECR-read role for both accounts (provisioned from the infrastructure
# repo). Until then the majority-SHA case is caught only after merge.
#
# A brand-new service therefore cannot introduce its own tag here. Land the image
# first (see CONTRIBUTING.md section 14, "Pull request workflow"), then let the
# deploy bot stamp the overlay.
#
# Usage:
#   check-overlay-tag-consistency.sh [<kustomization> ...]
#
# Defaults to the two ArgoCD-synced overlays. The bootstrap-*/ overlays are
# deliberately hand-pinned (manual-apply Jobs, never synced) and are out of
# scope; verify-ecr-images.sh covers those.
set -euo pipefail

if [ $# -gt 0 ]; then
  FILES=("$@")
else
  FILES=(
    k8s/overlays/staging/kustomization.yaml
    k8s/overlays/prod/kustomization.yaml
  )
fi

# Emit every newTag value in the file, one per line, quoted or not. Matching
# only quoted values would silently skip an unquoted one and check a subset of
# the file while reporting success.
extract_tags() {
  sed -nE 's/^[[:space:]]*newTag:[[:space:]]*"?([^"[:space:]]+)"?[[:space:]]*$/\1/p' "$1"
}

FAILED=0

for f in "${FILES[@]}"; do
  if [ ! -f "$f" ]; then
    echo "::error::not a file: ${f}" >&2
    FAILED=1
    continue
  fi

  TAG_COUNT=0
  MALFORMED=""
  SHA_LIST=""
  # `while read` rather than mapfile: matches verify-ecr-images.sh and keeps the
  # script working on the bash 3.2 that ships with macOS.
  while IFS= read -r tag; do
    [ -z "$tag" ] && continue
    TAG_COUNT=$((TAG_COUNT + 1))
    # Cronjob entries carry a "<service>-" prefix; regular services are the bare
    # SHA. Either way the commit is the trailing 40 hex chars.
    sha="$(printf '%s' "$tag" | sed -nE 's/.*([a-f0-9]{40})$/\1/p')"
    if [ -n "$sha" ]; then
      SHA_LIST="${SHA_LIST}${sha} ${tag}"$'\n'
    else
      MALFORMED="${MALFORMED}    malformed (no commit SHA): ${tag}"$'\n'
    fi
  done < <(extract_tags "$f")

  if [ "$TAG_COUNT" -eq 0 ]; then
    echo "::error::no newTag entries found in ${f}" >&2
    FAILED=1
    continue
  fi

  UNIQUE_COUNT="$(printf '%s' "$SHA_LIST" | awk 'NF {print $1}' | sort -u | wc -l | tr -d ' ')"

  if [ -z "$MALFORMED" ] && [ "$UNIQUE_COUNT" -eq 1 ]; then
    only="$(printf '%s' "$SHA_LIST" | awk 'NF {print $1}' | sort -u)"
    echo "  ok   ${f} — ${TAG_COUNT} tag(s), all at ${only:0:12}"
    continue
  fi

  FAILED=1
  echo "  BAD  ${f}"
  [ -n "$MALFORMED" ] && printf '%s' "$MALFORMED"

  if [ "$UNIQUE_COUNT" -gt 1 ]; then
    # Name the minority tags explicitly: the majority is almost always the bot's
    # real deploy SHA, so the outliers are what a reviewer needs to see.
    majority="$(printf '%s' "$SHA_LIST" | awk 'NF {print $1}' | sort | uniq -c | sort -rn | awk 'NR==1 {print $2}')"
    majority_n="$(printf '%s' "$SHA_LIST" | awk -v m="$majority" 'NF && $1 == m' | wc -l | tr -d ' ')"
    echo "    majority commit: ${majority} (${majority_n} tag(s))"
    printf '%s' "$SHA_LIST" | awk -v m="$majority" 'NF && $1 != m {print "    outlier: " $2}'
  fi
done

if [ "$FAILED" -ne 0 ]; then
  cat >&2 <<'MSG'
::error::An ArgoCD-synced overlay has image tags that do not all name the same
::error::commit, so at least one was written by hand. CI owns these tags
::error::(k8s/AGENTS.md). ArgoCD syncs the merge commit before the deploy bot
::error::stamps them, so a tag naming an image that does not exist yet causes
::error::ImagePullBackOff, fails the staging health gate, and silently skips the
::error::prod promotion (ORB-313).
::error::For a brand-new service, land its image in a separate PR first, then add
::error::the overlay entry without a tag edit. See CONTRIBUTING.md section 14,
::error::"Pull request workflow".
MSG
  exit 1
fi

echo "All ${#FILES[@]} guarded overlay(s) have internally consistent image tags."
