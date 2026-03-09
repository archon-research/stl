#!/usr/bin/env bash
# stress-test/run.sh — run a k6 stress test scenario against the local kind cluster.
# Run from the stl-verify/ directory:
#
# Usage:
#   ./stress-test/run.sh [options]
#
# Options:
#   -s, --scenario       Script to run: sustained (default), burst
#   -i, --interval       Block interval in ms (default: 12000 ~Ethereum)
#                        Common values: 250 (Base), 2000 (BNB), 12000 (Ethereum)
#   -d, --duration       k6 test duration (default: 2m)
#   -a, --admin-url      Mock server admin URL (default: http://localhost:8547)
#       --reorg          Inject reorgs during the test
#       --reorg-depth    Reorg depth (default: 5, max: 64)
#       --reorg-interval Seconds between reorgs (default: 30)
#       --no-clean       Skip truncating block_states before the run
#   -h, --help           Show this help
#
# Environment:
#   CHAIN_ID             Chain ID for verification queries (default: 1)

set -euo pipefail

if [[ ! -d "stress-test/k6" ]]; then
  echo "error: run this script from the stl-verify/ directory" >&2
  exit 1
fi

SCENARIO="sustained"
INTERVAL_MS="12000"
DURATION="2m"
ADMIN_URL="http://localhost:8547"
REORG=""
REORG_DEPTH="5"
REORG_INTERVAL_S="30"
CHAIN_ID="${CHAIN_ID:-1}"
CLEAN=true

usage() {
  grep '^#' "$0" | sed 's/^# \{0,1\}//'
  exit 0
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -s|--scenario)        SCENARIO="$2";        shift 2 ;;
    -i|--interval)        INTERVAL_MS="$2";     shift 2 ;;
    -d|--duration)        DURATION="$2";        shift 2 ;;
    -a|--admin-url)       ADMIN_URL="$2";       shift 2 ;;
    --reorg)              REORG="1";            shift   ;;
    --reorg-depth)        REORG_DEPTH="$2";     shift 2 ;;
    --reorg-interval)     REORG_INTERVAL_S="$2"; shift 2 ;;
    --no-clean)           CLEAN=false;          shift   ;;
    -h|--help)            usage ;;
    *) echo "Unknown option: $1" >&2; usage ;;
  esac
done

SCRIPT="stress-test/k6/watcher-${SCENARIO}.js"
if [[ ! -f "$SCRIPT" ]]; then
  echo "error: scenario script not found: $SCRIPT" >&2
  echo "Available scenarios: sustained, burst" >&2
  exit 1
fi

echo "==> Scenario:  ${SCENARIO}"
echo "==> Interval:  ${INTERVAL_MS}ms"
echo "==> Duration:  ${DURATION}"
echo "==> Admin URL: ${ADMIN_URL}"
if [[ -n "$REORG" ]]; then
  echo "==> Reorgs:    every ${REORG_INTERVAL_S}s at depth ${REORG_DEPTH}"
fi

if $CLEAN; then
  echo "==> Truncating block_states..."
  kubectl exec -n stl timescaledb-0 -- psql -U postgres -d stl_verify \
    -c "TRUNCATE block_states CASCADE;" -q
  echo "==> Flushing Redis..."
  kubectl exec -n stl deployment/redis -- redis-cli FLUSHDB
  echo "==> Purging SQS queues..."
  # purge_sqs_queue suppresses only the 60 s cooldown error (PurgeQueueInProgress);
  # all other failures (LocalStack unavailable, bad URL, auth error) are fatal.
  purge_sqs_queue() {
    local queue_name="$1" queue_url="$2"
    local out
    out=$(AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
      aws --endpoint-url http://localhost:4566 --region us-east-1 \
      sqs purge-queue --queue-url "$queue_url" 2>&1) && return 0
    if echo "$out" | grep -q "PurgeQueueInProgress"; then
      echo "  warning: ${queue_name} purge skipped (within 60 s cooldown)"
      return 0
    fi
    echo "  error: ${queue_name} purge failed: ${out}" >&2
    return 1
  }
  purge_sqs_queue "transformer" \
    "http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/stl-ethereum-transformer.fifo"
  purge_sqs_queue "backup" \
    "http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/stl-ethereum-backup.fifo"
fi

echo "==> Running k6..."
ADMIN_URL="$ADMIN_URL" \
INTERVAL_MS="$INTERVAL_MS" \
DURATION="$DURATION" \
REORG="$REORG" \
REORG_DEPTH="$REORG_DEPTH" \
REORG_INTERVAL_S="$REORG_INTERVAL_S" \
  k6 run "$SCRIPT"

echo "==> Verifying chain correctness..."
# Run checks.sql with -t (tuples-only) and -A (unaligned) so each query prints a bare number.
# xargs strips whitespace; read assigns the three values in order.
results=$(kubectl exec -i -n stl timescaledb-0 -- psql -U postgres -d stl_verify \
  -v chain_id="$CHAIN_ID" -tA \
  < stress-test/verify/checks.sql)
read -r gaps unexpected_orphans broken_parent_links <<< "$(echo "$results" | xargs)"
echo "  gaps=${gaps}  unexpected_orphans=${unexpected_orphans}  broken_parent_links=${broken_parent_links}"
if [[ "$gaps" -ne 0 || "$unexpected_orphans" -ne 0 || "$broken_parent_links" -ne 0 ]]; then
  echo "error: chain invariants violated — see counts above" >&2
  exit 1
fi
echo "==> Chain OK"
