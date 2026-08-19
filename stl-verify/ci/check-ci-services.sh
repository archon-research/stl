#!/usr/bin/env bash

# The integration job runs its services from the workflow, while a local run starts
# them from the test code. Nothing forces the two to agree, so a tag bump or a new
# LocalStack service in one place would silently test a different setup in the other.

set -euo pipefail

cd "$(dirname "$0")/.."

images_file=internal/testutil/images.go
workflow=../.github/workflows/go-ci.yml
job=integration-tests

# The integration job's lines only: reading the whole workflow would fail the moment
# an unrelated job gains a container of its own.
job_block="$(awk -v job="  $job:" '
  index($0, job) == 1 { injob = 1; next }
  injob && /^  [^ ]/  { exit }
  injob               { print }
' "$workflow")"
if [[ -z "$job_block" ]]; then
  echo "ERROR: no $job job found in $workflow -- check the job name" >&2
  exit 1
fi

value_of() {
  printf '%s\n' "$job_block" | sed -n "s/^[[:space:]]*$1:[[:space:]]*\([^[:space:]#]*\).*/\1/p"
}

# Every image the job runs must be one the test helpers would start themselves.
# Subset, not equality: a constant used only by a benchmark has no service block.
declared_images="$(grep -oE 'Image[A-Za-z0-9_]+[[:space:]]*=[[:space:]]*"[^"]+"' "$images_file" \
  | grep -oE '"[^"]+"' | tr -d '"' | sort -u)"
if [[ -z "$declared_images" ]]; then
  echo "ERROR: no image constants found in $images_file -- check the grep pattern" >&2
  exit 1
fi

configured_images="$(value_of image | sort -u)"
if [[ -z "$configured_images" ]]; then
  echo "ERROR: no service images found in the $job job -- check the sed pattern" >&2
  exit 1
fi

unknown_images="$(comm -23 <(printf '%s\n' "$configured_images") <(printf '%s\n' "$declared_images"))"
if [[ -n "$unknown_images" ]]; then
  echo "ERROR: the $job job runs images that $images_file does not declare:" >&2
  printf '  %s\n' $unknown_images >&2
  echo "Tests started by hand would run a different version. Bump both." >&2
  exit 1
fi

# The shared LocalStack must enable the union of what every package asks for; a
# package whose service is missing fails with an opaque AWS error instead. Packages
# name their services in the Shared declaration they hand to testutil.RunShared;
# the direct helper call is still valid, so both spellings count. Neither grep may
# sink the script under pipefail before the emptiness check below reports it.
requested_services="$({
    grep -rhoE 'LocalStackServices:[[:space:]]*"[^"]*"' --include='*_test.go' . || true
    grep -rhoE 'StartLocalStackForMain\("[^"]*"' --include='*_test.go' . || true
  } | sed -E 's/^[^"]*"//; s/"$//' | tr ',' '\n' | sed '/^$/d' | sort -u)"
if [[ -z "$requested_services" ]]; then
  echo "ERROR: no LocalStack service requests found -- check the grep patterns" >&2
  exit 1
fi

enabled_services="$(value_of SERVICES | tr ',' '\n' | sed '/^$/d' | sort -u)"
if [[ -z "$enabled_services" ]]; then
  echo "ERROR: no LocalStack SERVICES found in the $job job -- check the sed pattern" >&2
  exit 1
fi

missing_services="$(comm -23 <(printf '%s\n' "$requested_services") <(printf '%s\n' "$enabled_services"))"
if [[ -n "$missing_services" ]]; then
  echo "ERROR: tests ask LocalStack for services the $job job does not enable:" >&2
  printf '  %s\n' $missing_services >&2
  echo "Add them to the localstack service's SERVICES in $workflow." >&2
  exit 1
fi

echo "CI services match the test helpers."
