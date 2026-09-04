#!/usr/bin/env bash
#
# check-image-digest-injection.sh — assert every stamped workload selects the
# digest key for its image alias from the deploy-generated ConfigMap.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
ROSTER_RENDERER="${REPO_ROOT}/scripts/deploy/render-overlay-images.sh"
STAGING_OVERLAY="${REPO_ROOT}/k8s/overlays/staging/kustomization.yaml"
CONFIG_MAP="stl-verify-image-digests"

die() { echo "::error::$*" >&2; exit 1; }

ALIASES="$("$ROSTER_RENDERER" --list aliases)"

has_alias() { printf '%s\n' "$ALIASES" | grep -qx "$1"; }

check_workload() {
  local file="$1" expected_alias="$2" alias config_name key optional
  if grep -q 'STL_DEV_IDENTITY' "$file"; then
    die "${file}: STL_DEV_IDENTITY is a local-only escape and must not be inherited by staging or prod"
  fi
  alias="$(awk '/^[[:space:]]*image:/{print $2; exit}' "$file")"
  [ "$alias" = "$expected_alias" ] || die "${file}: image alias is '${alias:-missing}', expected '${expected_alias}'"
  has_alias "$alias" || die "${file}: image alias '${alias}' is not in k8s/image-roster.txt"

  config_name="$(awk '
    /^[[:space:]]*-[[:space:]]*name:[[:space:]]*IMAGE_DIGEST[[:space:]]*$/ { in_digest = 1; next }
    in_digest && /^[[:space:]]*name:[[:space:]]*/ { sub(/^[^:]*:[[:space:]]*/, ""); print; exit }
  ' "$file")"
  key="$(awk '
    /^[[:space:]]*-[[:space:]]*name:[[:space:]]*IMAGE_DIGEST[[:space:]]*$/ { in_digest = 1; next }
    in_digest && /^[[:space:]]*key:[[:space:]]*/ { sub(/^[^:]*:[[:space:]]*/, ""); print; exit }
  ' "$file")"
  optional="$(awk '
    /^[[:space:]]*-[[:space:]]*name:[[:space:]]*IMAGE_DIGEST[[:space:]]*$/ { in_digest = 1; next }
    in_digest && /^[[:space:]]*optional:[[:space:]]*/ { sub(/^[^:]*:[[:space:]]*/, ""); print; exit }
  ' "$file")"

  [ "$config_name" = "$CONFIG_MAP" ] || die "${file}: IMAGE_DIGEST must reference ConfigMap ${CONFIG_MAP}"
  [ "$key" = "$alias" ] || die "${file}: IMAGE_DIGEST must select key ${alias}, got '${key:-missing}'"
  [ "$optional" = "true" ] || die "${file}: IMAGE_DIGEST reference must stay optional for local kind"
}

while IFS= read -r base; do
  deployment="${REPO_ROOT}/k8s/base/${base}/deployment.yaml"
  [ -f "$deployment" ] || continue
  image_alias="$(awk '/^[[:space:]]*image:/{print $2; exit}' "$deployment")"
  check_workload "$deployment" "$image_alias"
done < <(awk '
  /^resources:/ { in_resources = 1; next }
  in_resources && /^[^[:space:]#]/ { in_resources = 0 }
  in_resources && /^[[:space:]]*-[[:space:]]+\.\.\/\.\.\/base\// {
    sub(/^[[:space:]]*-[[:space:]]+\.\.\/\.\.\/base\//, "")
    print
  }
' "$STAGING_OVERLAY")

check_workload "${REPO_ROOT}/k8s/base/transform-bootstrap/job.yaml" transform-bootstrap

if grep -q '^[[:space:]]*-[[:space:]]*name:[[:space:]]*IMAGE_DIGEST[[:space:]]*$' "${REPO_ROOT}/k8s/base/migrate/job.yaml"; then
  die "k8s/base/migrate/job.yaml: migrate is a PreSync hook and must not consume IMAGE_DIGEST"
fi

for env in staging prod; do
  if grep -R -q 'STL_DEV_IDENTITY' "${REPO_ROOT}/k8s/overlays/${env}"; then
    die "k8s/overlays/${env}: STL_DEV_IDENTITY must be absent"
  fi
done

echo "  ok   IMAGE_DIGEST references cover stamped workloads"
