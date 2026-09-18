#!/usr/bin/env bash

# $(CRONJOBS) is auto-discovered from cmd/cronjobs/*/main.go, and two pattern
# rules carry every member to a registry: docker-build-cronjob-% locally, and
# _docker-release-cronjob-%-internal into stl-<env>-cronjob under the tag
# <name>-<sha>. A second target naming the same source pushes that tag twice in
# one docker-release-all run. ECR rejects the second push now that the SHA tags
# are immutable (ARCT-420), and the release dies mid-sequence, taking every
# image queued behind it. VEC-491 shipped that duplicate for block-meta-loader,
# where it survived for as long as the later push could overwrite the earlier one.

set -euo pipefail

cd "$(dirname "$0")/.."

# || true: grep exits 1 when nothing matches, and set -e would abort here before
# the guard below can separate "no cronjob images at all" from a real result.
matches="$(grep -n 'CMD_PATH=cmd/cronjobs/' Makefile || true)"

if [[ -z "$matches" ]]; then
  echo "ERROR: no CMD_PATH=cmd/cronjobs/ line in the Makefile -- check the grep pattern" >&2
  exit 1
fi

# Both pattern rules spell the source cmd/cronjobs/$*. Asserting the count keeps
# this check from passing vacuously if either rule is renamed or reflowed.
generic="$(printf '%s\n' "$matches" | grep -cF 'CMD_PATH=cmd/cronjobs/$*' || true)"
if [[ "$generic" -ne 2 ]]; then
  echo "ERROR: expected 2 cronjob pattern-rule lines (build + release), found $generic:" >&2
  printf '%s\n' "$matches" >&2
  exit 1
fi

offenders="$(printf '%s\n' "$matches" | grep -vF 'CMD_PATH=cmd/cronjobs/$*' || true)"

if [[ -n "$offenders" ]]; then
  echo "ERROR: these Makefile lines name a cmd/cronjobs source outside the pattern rules:" >&2
  printf '%s\n' "$offenders" >&2
  echo "docker-build-cronjob-<name> and docker-release-cronjob-<name> already cover every" >&2
  echo "cmd/cronjobs package. Delete the bespoke target and let \$(CRONJOBS) carry the image." >&2
  exit 1
fi

echo "Every cmd/cronjobs image is built and released by the pattern rules alone."
