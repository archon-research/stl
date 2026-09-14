#!/usr/bin/env bash
#
# overlay-tag-parser.sh — the one parser of a YAML scalar line (`name:`,
# `newName:` or `newTag:`) from the `images:` block of
# k8s/overlays/{staging,prod}/kustomization.yaml. Sourced by
# render-overlay-images.sh, check-overlay-tag-consistency.sh and
# compare-image-layers.sh so the three scripts share one implementation
# instead of maintaining their own (VEC-754).
#
# Order is load-bearing: strip the key, THEN a trailing whitespace+comment,
# THEN one layer of surrounding double quotes. Stripping quotes before the
# comment leaves a value's closing quote — and the comment after it —
# untouched whenever the closing quote is not at end-of-line.
#
# Deliberately bash 3.2 + BSD awk compatible, like the scripts that source it.

# Embedded as text and concatenated onto each caller's own awk program
# (rather than loaded via `awk -f`), so every caller stays a single
# self-contained `awk '...'` invocation.
OVERLAY_SCALAR_AWK_FN="$(cat <<'AWK'
function overlay_scalar_value(s) {
  sub(/^[^:]*:[[:space:]]*/, "", s)
  sub(/[[:space:]]+#.*$/, "", s)
  gsub(/^"|"$/, "", s)
  return s
}
AWK
)"
