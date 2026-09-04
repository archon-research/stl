#!/usr/bin/env bash
#
# verify-rendered-image-digests.sh — prove the staging render connects every
# identity-consuming workload to the ECR-verified generated digest ConfigMap.
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
OVERLAY="${REPO_ROOT}/k8s/overlays/staging/kustomization.yaml"
BOOTSTRAP="${REPO_ROOT}/k8s/overlays/staging/bootstrap/kustomization.yaml"
CONFIG_MAP="stl-verify-image-digests"
KUSTOMIZE_BIN="${KUSTOMIZE_BIN:-}"

die() { echo "::error::$*" >&2; exit 1; }

usage() {
  cat <<'EOF'
Usage: verify-rendered-image-digests.sh [--overlay <staging-kustomization>] [--bootstrap <bootstrap-kustomization>]

KUSTOMIZE_BIN may name a kustomize binary. By default the script uses
`kustomize build`, then falls back to `kubectl kustomize`.
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    --overlay) OVERLAY="${2:-}"; [ -n "$OVERLAY" ] || die "--overlay needs a path"; shift 2 ;;
    --bootstrap) BOOTSTRAP="${2:-}"; [ -n "$BOOTSTRAP" ] || die "--bootstrap needs a path"; shift 2 ;;
    -h|--help) usage; exit 0 ;;
    *) usage >&2; die "unknown argument: $1" ;;
  esac
done

[ -f "$OVERLAY" ] || die "not a file: ${OVERLAY}"
[ -f "$BOOTSTRAP" ] || die "not a file: ${BOOTSTRAP}"

TMPDIR_OWN="$(mktemp -d)"
trap 'rm -rf "$TMPDIR_OWN"' EXIT
tmpfile() { mktemp "${TMPDIR_OWN}/XXXXXX"; }

kustomize_build() {
  local target="$1"
  if [ -f "$target" ]; then target="$(dirname "$target")"; fi
  if [ -n "$KUSTOMIZE_BIN" ]; then
    "$KUSTOMIZE_BIN" build "$target"
  elif command -v kustomize >/dev/null 2>&1; then
    kustomize build "$target"
  elif command -v kubectl >/dev/null 2>&1; then
    kubectl kustomize "$target"
  else
    die "kustomize or kubectl is required to verify rendered manifests"
  fi
}

# The static check is deliberately separate from the rendered proof below: it
# catches a missing base reference even before the first deploy bot stamp adds a
# ConfigMap to a new environment.
"${REPO_ROOT}/scripts/deploy/check-image-digest-injection.sh"
"${REPO_ROOT}/scripts/deploy/render-image-digests.sh" --verify "$OVERLAY"

STANDARD_RENDER="$(tmpfile)"
BOOTSTRAP_RENDER="$(tmpfile)"
kustomize_build "$OVERLAY" > "$STANDARD_RENDER"
kustomize_build "$BOOTSTRAP" > "$BOOTSTRAP_RENDER"

RECORDS="$(tmpfile)"
awk -v config_map="$CONFIG_MAP" '
  function scalar(line) {
    sub(/^[^:]*:[[:space:]]*/, "", line)
    sub(/[[:space:]]+#.*$/, "", line)
    gsub(/^"|"$/, "", line)
    return line
  }
  function indent(line, leading) {
    leading = line
    sub(/[^ ].*$/, "", leading)
    return length(leading)
  }
  function flush(    i) {
    if (kind == "ConfigMap" && resource == config_map) {
      for (i in data) print "map\t" i "\t" data[i]
    }
    if (kind == "Deployment" || (kind == "Job" && resource == "transform-bootstrap")) {
      for (i = 1; i <= refs; i++) {
        print "ref\t" kind "\t" resource "\t" ref_config[i] "\t" ref_key[i] "\t" ref_optional[i]
      }
    }
    for (i in data) delete data[i]
    for (i in ref_config) delete ref_config[i]
    for (i in ref_key) delete ref_key[i]
    for (i in ref_optional) delete ref_optional[i]
    kind = ""; resource = ""; in_metadata = 0; in_data = 0
    in_ref = 0; ref_indent = 0; refs = 0
  }
  /^---$/ { flush(); next }
  /^kind:[[:space:]]*/ { kind = scalar($0); next }
  /^metadata:[[:space:]]*$/ { in_metadata = 1; next }
  in_metadata && /^  name:[[:space:]]*/ { resource = scalar($0); in_metadata = 0; next }
  # Kustomize writes ConfigMap data before its kind, so collect data for
  # each document first and select the generated ConfigMap in flush().
  /^data:[[:space:]]*$/ { in_data = 1; next }
  in_data {
    if ($0 ~ /^[^[:space:]]/) in_data = 0
    else if ($0 ~ /^  [a-z0-9]([a-z0-9-]*[a-z0-9])?:[[:space:]]*/) {
      key = $0
      sub(/^  /, "", key)
      sub(/:.*/, "", key)
      data[key] = scalar($0)
      next
    }
  }
  in_ref {
    if ($0 !~ /^[[:space:]]*$/ && indent($0) <= ref_indent) {
      in_ref = 0
    } else {
      if ($0 ~ /^[[:space:]]*name:[[:space:]]*/) ref_config[refs] = scalar($0)
      if ($0 ~ /^[[:space:]]*key:[[:space:]]*/) ref_key[refs] = scalar($0)
      if ($0 ~ /^[[:space:]]*optional:[[:space:]]*/) ref_optional[refs] = scalar($0)
    }
  }
  /^[[:space:]]*-[[:space:]]*name:[[:space:]]*IMAGE_DIGEST[[:space:]]*$/ {
    refs++
    ref_indent = indent($0)
    ref_config[refs] = ""; ref_key[refs] = ""; ref_optional[refs] = ""
    in_ref = 1
  }
  END { flush() }
' "$STANDARD_RENDER" "$BOOTSTRAP_RENDER" > "$RECORDS"

EXPECTED="$(tmpfile)"
while IFS= read -r base; do
  deployment="${REPO_ROOT}/k8s/base/${base}/deployment.yaml"
  [ -f "$deployment" ] || continue
  name="$(awk '
    /^metadata:[[:space:]]*$/ { in_metadata = 1; next }
    in_metadata && /^  name:[[:space:]]*/ { sub(/^[^:]*:[[:space:]]*/, ""); print; exit }
  ' "$deployment")"
  alias="$(awk '/^[[:space:]]*image:/{print $2; exit}' "$deployment")"
  [ -n "$name" ] && [ -n "$alias" ] || die "${deployment}: could not determine deployment name and image alias"
  printf 'Deployment\t%s\t%s\n' "$name" "$alias" >> "$EXPECTED"
done < <(awk '
  /^resources:/ { in_resources = 1; next }
  in_resources && /^[^[:space:]#]/ { in_resources = 0 }
  in_resources && /^[[:space:]]*-[[:space:]]+\.\.\/\.\.\/base\// {
    sub(/^[[:space:]]*-[[:space:]]+\.\.\/\.\.\/base\//, "")
    print
  }
' "$OVERLAY")
printf 'Job\ttransform-bootstrap\ttransform-bootstrap\n' >> "$EXPECTED"
LC_ALL=C sort -o "$EXPECTED" "$EXPECTED"

if [ -n "$(uniq -d "$EXPECTED")" ]; then
  die "staging render expectation has duplicate workload names"
fi

while IFS="$(printf '\t')" read -r kind name alias; do
  refs="$(awk -F '\t' -v kind="$kind" -v name="$name" '$1 == "ref" && $2 == kind && $3 == name { print }' "$RECORDS")"
  ref_count="$(printf '%s\n' "$refs" | awk 'NF { count++ } END { print count + 0 }')"
  [ "$ref_count" = "1" ] || die "rendered ${kind}/${name}: expected exactly one IMAGE_DIGEST reference, found ${ref_count}"

  IFS="$(printf '\t')" read -r _ rendered_kind rendered_name config_name key optional <<EOF
$refs
EOF
  [ "$config_name" = "$CONFIG_MAP" ] || die "rendered ${kind}/${name}: IMAGE_DIGEST references ${config_name:-no ConfigMap}, expected ${CONFIG_MAP}"
  [ "$key" = "$alias" ] || die "rendered ${kind}/${name}: IMAGE_DIGEST selects ${key:-no key}, expected ${alias}"
  [ "$optional" = "true" ] || die "rendered ${kind}/${name}: IMAGE_DIGEST reference must remain optional for local kind"

  digest="$(awk -F '\t' -v alias="$alias" '$1 == "map" && $2 == alias { print $3 }' "$RECORDS")"
  [[ "$digest" =~ ^sha256:[a-f0-9]{64}$ ]] || die "rendered ${kind}/${name}: ConfigMap key ${alias} has no valid digest"
done < "$EXPECTED"

echo "  ok   rendered staging workloads select ECR-verified IMAGE_DIGEST values"
