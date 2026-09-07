#!/usr/bin/env bash
#
# render-overlay-images.sh — generate, write, or verify the `images:` block of an
# ArgoCD-synced overlay from the deploy roster (k8s/image-roster.txt).
#
# Why (ORB-362): every deploy(staging)/deploy(prod) commit used to rewrite every
# newTag line in k8s/overlays/{staging,prod}/kustomization.yaml, so any PR that
# added an image entry conflicted with the bot's bumps until it merged (stl #640:
# four rebase rounds in one afternoon). The block interleaved hand-maintained
# inventory (which images exist, which repo each pulls from) with machine-churned
# state (the tags). Now the roster holds the inventory, this script renders the
# whole block from roster + env + release SHA, and the bot rewrites it wholesale.
# A new-service PR touches base manifests, `resources:` and one roster line —
# never the tag list. The placeholder-tag hack is gone with it, and the overlay
# cannot drift from the promotion list because deploy.yaml reads the same roster.
#
# The block is rendered as text rather than driven through `kustomize edit set
# image`: kustomize edit cannot remove an entry, does not order or quote
# deterministically, and so could not back a byte-exact contract. A text render
# is a pure function of (roster, env, sha), which is what GUARD 4 in deploy.yaml
# and the deploy-prod revision contract compare against.
#
# Usage:
#   render-overlay-images.sh --env staging|prod --tag <40-hex> --print
#   render-overlay-images.sh --env staging|prod --tag <40-hex> --write <kustomization>
#   render-overlay-images.sh --env staging|prod [--tag <40-hex>] --check <kustomization> [--allow-missing]
#   render-overlay-images.sh --strip <kustomization>
#   render-overlay-images.sh --list services|cronjobs|aliases
#   [--roster <path>]   default: <repo root>/k8s/image-roster.txt
#
#   --print   emit the rendered block.
#   --write   replace the file's images block in place; everything else is kept.
#   --check   byte-exact: the file's block must equal the render. Without --tag,
#             the tag is the one SHA every newTag in the file already names (fails
#             if they disagree). This is the deploy-bot / deploy-prod contract.
#   --allow-missing  (with --check) semantic instead: every entry in the file must
#             be one the roster renders (same name, repo, tag), while roster
#             entries the file lacks are reported as pending, not failed; order and
#             comments are ignored. This is the PR-time check — authors never touch
#             the block, so a roster addition legitimately precedes its entry. The
#             one hand edit it expects: removing or re-homing an image means
#             deleting its stale entry from the block in the same PR as the roster
#             change (the bot cannot run before merge).
#   --strip   print the file without its images block (the deploy-prod contract
#             diffs this against the parent revision).
#   --list    print the roster's names of one kind, one per line (deploy.yaml's
#             promotion loops), or every alias (for humans).
#
# Region: the block runs from the top-level `images:` line through every following
# line that is indented or blank; trailing blank lines are left in place. The file
# must contain exactly one `images:` key and no image-entry keys (newName, newTag,
# digest) outside that region — otherwise a comment at column 0 could split the
# block and leave entries no check sees and `--write` would duplicate. Anything
# else in the file (resources, patches, column-0 comments) is never touched.
#
# Deliberately bash 3.2 + BSD awk compatible, like the other scripts here, so it
# behaves the same on a developer macOS and on ubuntu-latest.
set -euo pipefail

die() { echo "::error::$*" >&2; exit 1; }

REPO_ROOT="$(cd "$(dirname "$0")/../.." && pwd)"
ROSTER="${REPO_ROOT}/k8s/image-roster.txt"
ENV_NAME=""
TAG=""
MODE=""
TARGET=""
LIST_KIND=""
ALLOW_MISSING=0

usage() { sed -n '/^# Usage:/,/^# Region:/p' "$0" | sed '$d' | sed 's/^# \{0,1\}//'; }

# $1 = flag, $2 = its value (may be unset): print the value or die.
val() { [ -n "${2:-}" ] || die "$1 needs a value"; printf '%s' "$2"; }

while [ $# -gt 0 ]; do
  case "$1" in
    --env)           ENV_NAME="$(val "$1" "${2:-}")"; shift 2 ;;
    --tag)           TAG="$(val "$1" "${2:-}")"; shift 2 ;;
    --roster)        ROSTER="$(val "$1" "${2:-}")"; shift 2 ;;
    --print)         MODE="print"; shift ;;
    --write)         MODE="write"; TARGET="$(val "$1" "${2:-}")"; shift 2 ;;
    --check)         MODE="check"; TARGET="$(val "$1" "${2:-}")"; shift 2 ;;
    --allow-missing) ALLOW_MISSING=1; shift ;;
    --strip)         MODE="strip"; TARGET="$(val "$1" "${2:-}")"; shift 2 ;;
    --list)          MODE="list"; LIST_KIND="$(val "$1" "${2:-}")"; shift 2 ;;
    -h|--help)       usage; exit 0 ;;
    *) usage >&2; die "unknown argument: $1" ;;
  esac
done
[ -n "$MODE" ] || { usage >&2; die "one of --print, --write, --check, --strip, --list is required"; }
[ -f "$ROSTER" ] || die "roster not found: ${ROSTER}"

require_env() { registry_prefix "$ENV_NAME" >/dev/null; }
require_tag() {
  [ -n "$TAG" ] || die "$1 needs --tag"
  [[ "$TAG" =~ ^[a-f0-9]{40}$ ]] || die "--tag must be a 40-char lowercase git SHA (got '${TAG}')"
}

registry_prefix() {
  case "$1" in
    staging) echo "579039992622.dkr.ecr.eu-west-1.amazonaws.com/stl-sentinelstaging-" ;;
    prod)    echo "030797368798.dkr.ecr.eu-west-1.amazonaws.com/stl-sentinelprod-" ;;
    *) die "--env must be staging or prod (got '${1:-}')" ;;
  esac
}

# One line per (kind, name, alias); alias is "-" for a roster entry with none.
# Validates the roster and fails closed on anything the renderer would not
# understand, so a typo can never produce a half-rendered block. Callers buffer
# the output (parsed="$(parse_roster)") so a failure prints nothing partial.
parse_roster() {
  awk -v roster="$ROSTER" '
    function bad(msg) { printf "::error::%s:%d: %s\n", roster, NR, msg > "/dev/stderr"; ok = 0 }
    BEGIN { ok = 1 }
    {
      sub(/#.*/, "")
      if ($0 ~ /^[[:space:]]*$/) next
      kind = $1; name = $2
      if (kind != "service" && kind != "cronjob") { bad("kind must be service or cronjob, got \"" kind "\""); next }
      if (name !~ /^[a-z0-9]([a-z0-9-]*[a-z0-9])?$/) { bad("name \"" name "\" must be lowercase [a-z0-9-]"); next }
      if (name in seen_name) { bad("name \"" name "\" already listed on line " seen_name[name]); next }
      seen_name[name] = NR
      if (NF < 3) { bad(kind " " name " has no alias column (use - for none)"); next }
      for (i = 3; i <= NF; i++) {
        a = $i
        if (a == "-") {
          if (NF != 3) { bad("\"-\" must be the only alias"); break }
          print kind, name, "-"; break
        }
        if (a !~ /^[a-z0-9]([a-z0-9-]*[a-z0-9])?$/) { bad("alias \"" a "\" must be lowercase [a-z0-9-]"); continue }
        if (a in seen_alias) { bad("alias \"" a "\" already listed on line " seen_alias[a]); continue }
        seen_alias[a] = NR
        print kind, name, a
      }
    }
    END { if (!ok) exit 1 }
  ' "$ROSTER" || die "invalid roster: ${ROSTER}"
}

# alias<TAB>newName<TAB>newTag, sorted by alias so the output never depends on
# roster line order.
render_entries() {
  local prefix parsed
  prefix="$(registry_prefix "$ENV_NAME")"
  parsed="$(parse_roster)"
  printf '%s\n' "$parsed" | awk -v p="$prefix" -v tag="$TAG" '
    $3 == "-" { next }
    {
      if ($1 == "service") { nn = p $2; nt = tag } else { nn = p "cronjob"; nt = $2 "-" tag }
      printf "%s\t%s\t%s\n", $3, nn, nt
    }' | LC_ALL=C sort
}

# $1 = file of render_entries output -> the block text.
block_from_entries() {
  cat <<'HDR'
images:
  # GENERATED from k8s/image-roster.txt by scripts/deploy/render-overlay-images.sh.
  # Do not edit: the deploy bot rewrites this whole block on every deploy
  # (ORB-362). Add or move an image in the roster instead.
HDR
  awk -F'\t' '{ printf "  - name: %s\n    newName: %s\n    newTag: \"%s\"\n", $1, $2, $3 }' "$1"
}

# All temp files live in one private directory so cleanup is a single rm -rf and
# nothing leaks regardless of which die() path exits first.
TMPDIR_OWN="$(mktemp -d)"
trap 'rm -rf "$TMPDIR_OWN"' EXIT
tmpfile() { mktemp "${TMPDIR_OWN}/XXXXXX"; }

# Split TARGET into $PRE (before the block), $REGION (the block) and $POST, then
# reject any layout the region logic could misread: a second top-level
# `images:`, or image-entry keys outside the region (a column-0 comment inside
# the block would otherwise end the region early and strand the entries after it).
split_target() {
  [ -f "$TARGET" ] || die "not a file: ${TARGET}"
  PRE="$(tmpfile)"; REGION="$(tmpfile)"; POST="$(tmpfile)"
  awk -v pre="$PRE" -v region="$REGION" -v post="$POST" '
    BEGIN { state = 0; blanks = "" }
    {
      if (state == 0) {
        if ($0 ~ /^images:/) { state = 1; print > region; next }
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
  local stray
  stray="$(cat "$PRE" "$POST" | grep -nE '^images:|^[[:space:]]*(newName|newTag|digest):' || true)"
  if [ -n "$stray" ]; then
    echo "::error::${TARGET}: image entries or a second images: key found outside the images block (a column-0 comment inside the block splits it; the deploy bot would duplicate what follows):" >&2
    printf '%s\n' "$stray" | sed 's/^/::error::  /' >&2
    exit 1
  fi
}

# The single SHA every newTag in REGION names; fails if there is none or several.
derived_tag() {
  local shas
  shas="$(sed -nE 's/^[[:space:]]*newTag:[[:space:]]*"?([a-z0-9]+-)*([a-f0-9]{40})"?[[:space:]]*(#.*)?$/\2/p' "$REGION" | LC_ALL=C sort -u)"
  if [ -z "$shas" ]; then
    die "${TARGET}: no newTag names a 40-hex SHA, cannot derive the tag (pass --tag)"
  fi
  if [ "$(printf '%s\n' "$shas" | wc -l | tr -d ' ')" != "1" ]; then
    die "${TARGET}: newTag entries name several SHAs, cannot derive the tag (run check-overlay-tag-consistency.sh):
$shas"
  fi
  printf '%s' "$shas"
}

# alias<TAB>newName<TAB>newTag for every entry in REGION (quotes and trailing
# comments dropped), sorted like render_entries. Strict: an entry must be exactly
# name + newName + newTag; anything else (digest, a missing key, an unknown key)
# fails closed, because kustomize would happily render it and no check would see.
region_entries() {
  awk -v target="$TARGET" '
    function val(s) { sub(/^[^:]*:[[:space:]]*/, "", s); sub(/[[:space:]]+#.*$/, "", s); gsub(/^"|"$/, "", s); return s }
    function bad(msg) { printf "::error::%s: %s\n", target, msg > "/dev/stderr"; ok = 0 }
    function flush() {
      if (name == "") {
        # Trailing keys after the last complete entry would otherwise vanish.
        if (nn != "" || nt != "") bad("newName/newTag lines with no owning \"- name:\" entry")
        nn = ""; nt = ""
        return
      }
      if (nn == "" || nt == "") bad("images entry \"" name "\" must have both newName and newTag")
      else printf "%s\t%s\t%s\n", name, nn, nt
      name = ""; nn = ""; nt = ""
    }
    BEGIN { ok = 1; name = ""; nn = ""; nt = "" }
    /^images:/                          { next }
    /^[[:space:]]*$/                    { next }
    /^[[:space:]]*#/                    { next }
    /^[[:space:]]*-[[:space:]]*name:/   { flush(); name = val($0); if (name == "") bad("images entry with an empty name"); next }
    /^[[:space:]]*newName:/             { nn = val($0); next }
    /^[[:space:]]*newTag:/              {
      # Canonical form only: deploy-prod'\''s BAD_TAGS guard requires the quoted
      # tag on every prod deploy, so an unquoted one (kustomize edit output)
      # must fail here at PR time, not there.
      if ($0 !~ /^[[:space:]]*newTag:[[:space:]]*"[^"]*"[[:space:]]*(#.*)?$/) bad("newTag must be double-quoted (the renderer'\''s canonical form): " $0)
      nt = val($0); next
    }
    { bad("unexpected line in the images block (only name/newName/newTag are allowed): " $0) }
    END { flush(); if (!ok) exit 1 }
  ' "$REGION" | LC_ALL=C sort
}

case "$MODE" in
  list)
    # Parse first, print second: an invalid roster must produce no output at all,
    # not a partial list (deploy.yaml feeds this straight into the promotion loop).
    PARSED="$(parse_roster)"
    case "$LIST_KIND" in
      services) printf '%s\n' "$PARSED" | awk '$1 == "service" && !seen[$2]++ { print $2 }' ;;
      cronjobs) printf '%s\n' "$PARSED" | awk '$1 == "cronjob" && !seen[$2]++ { print $2 }' ;;
      aliases)  printf '%s\n' "$PARSED" | awk '$3 != "-" { print $3 }' | LC_ALL=C sort ;;
      *) die "--list takes services, cronjobs or aliases (got '${LIST_KIND}')" ;;
    esac
    ;;

  strip)
    split_target
    cat "$PRE" "$POST"
    ;;

  print)
    require_env; require_tag "--print"
    ENTRIES="$(tmpfile)"; render_entries > "$ENTRIES"
    block_from_entries "$ENTRIES"
    ;;

  write)
    require_env; require_tag "--write"
    split_target
    ENTRIES="$(tmpfile)"; render_entries > "$ENTRIES"
    OUT="$(tmpfile)"
    {
      cat "$PRE"
      # No block yet: keep one blank line between the existing content and ours.
      if [ ! -s "$REGION" ] && [ -s "$PRE" ] && [ -n "$(tail -n 1 "$PRE")" ]; then echo; fi
      block_from_entries "$ENTRIES"
      cat "$POST"
    } > "$OUT"
    cp "$OUT" "$TARGET"   # cp, not mv: keeps the target's mode
    echo "Wrote ${ENV_NAME} images block ($(wc -l < "$ENTRIES" | tr -d ' ') entries at ${TAG}) to ${TARGET}"
    ;;

  check)
    require_env
    split_target
    [ -s "$REGION" ] || die "${TARGET}: no images: block found"
    if [ -n "$TAG" ]; then require_tag "--check"; else TAG="$(derived_tag)"; fi
    WANT_ENTRIES="$(tmpfile)"; render_entries > "$WANT_ENTRIES"
    if [ "$ALLOW_MISSING" = "0" ]; then
      WANT_BLOCK="$(tmpfile)"; block_from_entries "$WANT_ENTRIES" > "$WANT_BLOCK"
      if ! diff -u "$REGION" "$WANT_BLOCK"; then
        die "${TARGET}: images block is not what ${ROSTER} renders for ${ENV_NAME} at ${TAG} (see diff above)"
      fi
      echo "  ok   ${TARGET} — images block is exactly the roster's render at ${TAG:0:12}"
      exit 0
    fi
    FILE_ENTRIES="$(tmpfile)"
    region_entries > "$FILE_ENTRIES" || die "${TARGET}: malformed images block (see above)"
    [ -s "$FILE_ENTRIES" ] || die "${TARGET}: images block has no entries"
    OFFENDERS="$(LC_ALL=C comm -23 "$FILE_ENTRIES" "$WANT_ENTRIES")"
    PENDING="$(LC_ALL=C comm -13 "$FILE_ENTRIES" "$WANT_ENTRIES")"
    if [ -n "$OFFENDERS" ]; then
      echo "::error::${TARGET}: images entries the roster (${ROSTER}) does not render for ${ENV_NAME} at ${TAG}:" >&2
      # Tell the author which of the two legitimate causes applies: an alias the
      # roster no longer has (delete the entry in this PR) vs an alias it has but
      # with a different repo or tag (hand edit or roster move — never edit the
      # tag; fix the roster and delete the stale entry).
      printf '%s\n' "$OFFENDERS" | while IFS="$(printf '\t')" read -r a nn nt; do
        if cut -f1 "$WANT_ENTRIES" | grep -qx "$a"; then
          echo "::error::  ${a}: repo/tag differ from the roster's render (newName: ${nn}, newTag: ${nt}) — hand-edited or the image was re-homed; fix the roster and delete this stale entry" >&2
        else
          echo "::error::  ${a}: alias is not in the roster — a hand-added entry (never add one; add a roster line) or an image that was removed (delete this entry in the same PR)" >&2
        fi
      done
      echo "::error::The deploy bot rewrites this block from the roster on every deploy (ORB-362)." >&2
      exit 1
    fi
    if [ -n "$PENDING" ]; then
      echo "  ok   ${TARGET} — $(wc -l < "$FILE_ENTRIES" | tr -d ' ') entries match the roster at ${TAG:0:12}; pending (written by the next deploy):"
      printf '%s\n' "$PENDING" | awk -F'\t' '{ printf "         %s\n", $1 }'
    else
      echo "  ok   ${TARGET} — $(wc -l < "$FILE_ENTRIES" | tr -d ' ') entries match the roster at ${TAG:0:12}"
    fi
    ;;
esac
