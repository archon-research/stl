#!/usr/bin/env bash

# A template database is named after a digest of the migration set, so a server named
# by STL_TEST_POSTGRES_DSN that outlives one tree keeps the previous tree's template
# too, each one a full migrated database.
#
# Collecting them from inside the suite is what this script exists to avoid: a process
# caches its template name and clones from it for the rest of its run, so a sibling
# process on another migration set would drop the schema out from under it. Run this
# when nothing is testing against that server.

set -euo pipefail

dsn="${STL_TEST_POSTGRES_DSN:-}"
if [[ -z "$dsn" ]]; then
  echo "ERROR: STL_TEST_POSTGRES_DSN is unset, so there is no shared server to clean." >&2
  echo "Local runs without it start a container per package and throw it away." >&2
  exit 1
fi

templates="$(psql "$dsn" -qtAXc \
  "SELECT datname FROM pg_database WHERE datname LIKE 'stl\_tmpl\_%' ORDER BY datname")"
if [[ -z "$templates" ]]; then
  echo "No test templates on that server."
  exit 0
fi

for template in $templates; do
  # The flag first: Postgres refuses to drop a database while it is marked template.
  psql "$dsn" -qtAXc \
    "UPDATE pg_database SET datistemplate = false WHERE datname = '$template'" >/dev/null
  psql "$dsn" -qtAXc "DROP DATABASE IF EXISTS $template" >/dev/null
  echo "dropped $template"
done
