#!/usr/bin/env bash
#
# render-image-digests.sh — resolve stamped ECR images and render their digests
# into the stable ConfigMap consumed by deployed containers.
set -euo pipefail

die() { echo "::error::$*" >&2; exit 1; }

MODE=""
TARGET=""

usage() {
  cat <<'EOF'
Usage:
  render-image-digests.sh --write <kustomization>
  render-image-digests.sh --check <kustomization>
  render-image-digests.sh --verify <kustomization>
  render-image-digests.sh --strip <kustomization>
EOF
}

while [ $# -gt 0 ]; do
  case "$1" in
    --write|--check|--verify|--strip)
      MODE="${1#--}"
      TARGET="${2:-}"
      [ -n "$TARGET" ] || die "$1 needs a kustomization path"
      shift 2
      ;;
    -h|--help) usage; exit 0 ;;
    *) usage >&2; die "unknown argument: $1" ;;
  esac
done

[ -n "$MODE" ] || { usage >&2; die "one of --write, --check, --strip is required"; }
[ -f "$TARGET" ] || die "not a file: ${TARGET}"

TMPDIR_OWN="$(mktemp -d)"
trap 'rm -rf "$TMPDIR_OWN"' EXIT
tmpfile() { mktemp "${TMPDIR_OWN}/XXXXXX"; }

IMAGE_ENTRIES="$(tmpfile)"

# alias<TAB>newName<TAB>newTag. The images block is produced by
# render-overlay-images.sh, but this remains strict so a malformed stamped
# overlay cannot yield a partial identity map.
parse_image_entries() {
  awk -v target="$TARGET" '
    function value(s) { sub(/^[^:]*:[[:space:]]*/, "", s); sub(/[[:space:]]+#.*$/, "", s); gsub(/^"|"$/, "", s); return s }
    function bad(message) { printf "::error::%s: %s\n", target, message > "/dev/stderr"; ok = 0 }
    function flush() {
      if (alias == "") return
      if (name == "" || tag == "") bad("images entry \"" alias "\" needs newName and newTag")
      else print alias "\t" name "\t" tag
      alias = ""; name = ""; tag = ""
    }
    BEGIN { in_images = 0; ok = 1 }
    /^images:/ { in_images = 1; next }
    in_images && /^[^[:space:]#]/ { flush(); in_images = 0 }
    !in_images { next }
    /^[[:space:]]*#/ || /^[[:space:]]*$/ { next }
    /^[[:space:]]*-[[:space:]]*name:/ { flush(); alias = value($0); next }
    /^[[:space:]]*newName:/ { name = value($0); next }
    /^[[:space:]]*newTag:/ { tag = value($0); next }
    { bad("unexpected line in images block: " $0) }
    END { if (in_images) flush(); if (!ok) exit 1 }
  ' "$TARGET" | LC_ALL=C sort
}

parse_image_entries > "$IMAGE_ENTRIES" || die "could not parse images block"
[ -s "$IMAGE_ENTRIES" ] || die "${TARGET}: no images entries found"
DUPLICATE_ALIASES="$(cut -f1 "$IMAGE_ENTRIES" | LC_ALL=C sort | uniq -d)"
[ -z "$DUPLICATE_ALIASES" ] || die "${TARGET}: duplicate image aliases:\n${DUPLICATE_ALIASES}"

PRE="$(tmpfile)"
REGION="$(tmpfile)"
POST="$(tmpfile)"

split_target() {
  awk -v pre="$PRE" -v region="$REGION" -v post="$POST" '
    BEGIN { state = 0; blanks = "" }
    {
      if (state == 0) {
        if ($0 ~ /^configMapGenerator:/) { state = 1; print > region; next }
        print > pre; next
      }
      if (state == 1) {
        if ($0 ~ /^[[:space:]]*$/) { blanks = blanks $0 "\n"; next }
        if ($0 ~ /^[[:space:]]/) { printf "%s", blanks > region; blanks = ""; print > region; next }
        state = 2; printf "%s", blanks > post; blanks = ""
      }
      print > post
    }
    END { if (state == 1) printf "%s", blanks > post }
  ' "$TARGET"

  if [ "$(grep -Ec '^configMapGenerator:' "$TARGET")" -gt 1 ]; then
    die "${TARGET}: more than one configMapGenerator block"
  fi
}

split_target

config_map_entries() {
  awk -v target="$TARGET" '
    function bad(message) { printf "::error::%s: %s\n", target, message > "/dev/stderr"; ok = 0 }
    BEGIN { in_generator = 0; in_literals = 0; found = 0; ok = 1 }
    /^configMapGenerator:/ { in_generator = 1; next }
    in_generator && /^[^[:space:]#]/ { in_generator = 0 }
    !in_generator { next }
    /^[[:space:]]*-[[:space:]]*name:[[:space:]]*stl-verify-image-digests[[:space:]]*$/ { found = 1; next }
    found && /^[[:space:]]*literals:[[:space:]]*$/ { in_literals = 1; next }
    found && in_literals && /^[[:space:]]*-[[:space:]]*/ {
      value = $0
      sub(/^[[:space:]]*-[[:space:]]*/, "", value)
      pairs = split(value, pair, "=")
      if (pairs != 2 || pair[1] == "" || pair[2] == "") bad("invalid image digest literal: " $0)
      else print pair[1] "\t" pair[2]
      next
    }
    END {
      if (!found) bad("missing stl-verify-image-digests ConfigMap generator")
      if (!in_literals) bad("missing literals for stl-verify-image-digests")
      if (!ok) exit 1
    }
  ' "$TARGET" | LC_ALL=C sort
}

render_block() {
  local aliases pairs pair_file resolved_file mapping_file
  aliases="$(tmpfile)"
  pairs="$(tmpfile)"
  pair_file="$(tmpfile)"
  resolved_file="$(tmpfile)"
  mapping_file="$(tmpfile)"

  cut -f1 "$IMAGE_ENTRIES" > "$aliases"
  cut -f2-3 "$IMAGE_ENTRIES" | LC_ALL=C sort -u > "$pairs"

  while IFS="$(printf '\t')" read -r image_name image_tag; do
    [ -n "$image_name" ] || continue
    host="${image_name%%/*}"
    repository="${image_name#*/}"
    account="${host%%.*}"
    region="$(printf '%s' "$host" | sed -E 's/^[0-9]+\.dkr\.ecr\.([a-z0-9-]+)\.amazonaws\.com$/\1/')"
    if [ "$region" = "$host" ] || [[ ! "$account" =~ ^[0-9]{12}$ ]]; then
      die "could not parse ECR account and region from image name: ${image_name}"
    fi

    digest="$(aws ecr batch-get-image \
      --region "$region" \
      --registry-id "$account" \
      --repository-name "$repository" \
      --image-ids "imageTag=${image_tag}" \
      --query 'images[0].imageId.imageDigest' \
      --output text 2>/dev/null || true)"
    if [[ ! "$digest" =~ ^sha256:[a-f0-9]{64}$ ]]; then
      die "could not resolve a manifest digest for ${repository}:${image_tag} (got '${digest:-none}')"
    fi
    printf '%s\t%s\t%s\n' "$image_name" "$image_tag" "$digest" >> "$resolved_file"
  done < "$pairs"

  while IFS="$(printf '\t')" read -r alias image_name image_tag; do
    digest="$(awk -F '\t' -v name="$image_name" -v tag="$image_tag" '$1 == name && $2 == tag { print $3 }' "$resolved_file")"
    [ -n "$digest" ] || die "no resolved digest for ${alias} (${image_name}:${image_tag})"
    printf '%s\t%s\n' "$alias" "$digest" >> "$mapping_file"
  done < "$IMAGE_ENTRIES"

  if [ "$(sort -u "$aliases" | wc -l | tr -d ' ')" != "$(wc -l < "$mapping_file" | tr -d ' ')" ]; then
    die "could not resolve every image alias"
  fi

  cat <<'HDR'
configMapGenerator:
  # GENERATED from the stamped images block by scripts/deploy/render-image-digests.sh.
  # Do not edit: the deploy bot rewrites this whole block on every deploy.
  - name: stl-verify-image-digests
    options:
      disableNameSuffixHash: true
    literals:
HDR
  LC_ALL=C sort "$mapping_file" | awk -F '\t' '{ printf "      - %s=%s\n", $1, $2 }'
}

case "$MODE" in
  strip)
    cat "$PRE" "$POST"
    ;;
  write)
    OUT="$(tmpfile)"
    {
      cat "$PRE"
      if [ -s "$PRE" ] && [ -n "$(tail -n 1 "$PRE")" ]; then echo; fi
      render_block
      cat "$POST"
    } > "$OUT"
    cp "$OUT" "$TARGET"
    echo "Wrote image digest ConfigMap to ${TARGET}"
    ;;
  check)
    ACTUAL="$(tmpfile)"
    config_map_entries > "$ACTUAL" || die "${TARGET}: malformed image digest ConfigMap"
    EXPECTED="$(tmpfile)"
    cut -f1 "$IMAGE_ENTRIES" | awk '{ print $1 "\t" }' > "$EXPECTED"
    if [ "$(cut -f1 "$ACTUAL")" != "$(cut -f1 "$EXPECTED")" ]; then
      die "${TARGET}: image digest ConfigMap keys do not match the images block aliases"
    fi
    if ! awk -F '\t' '$2 !~ /^sha256:[a-f0-9]{64}$/ { exit 1 }' "$ACTUAL"; then
      die "${TARGET}: image digest ConfigMap contains an invalid digest"
    fi
    echo "  ok   ${TARGET} — image digest ConfigMap covers every images block alias"
    ;;
  verify)
    [ -s "$REGION" ] || die "${TARGET}: no image digest ConfigMap block found"
    EXPECTED_BLOCK="$(tmpfile)"
    render_block > "$EXPECTED_BLOCK"
    if ! diff -u "$REGION" "$EXPECTED_BLOCK"; then
      die "${TARGET}: image digest ConfigMap does not match ECR-resolved image digests"
    fi
    echo "  ok   ${TARGET} — image digest ConfigMap matches ECR"
    ;;
esac
