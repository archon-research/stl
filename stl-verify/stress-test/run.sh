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
  AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
    aws --endpoint-url http://localhost:4566 --region us-east-1 \
    sqs purge-queue --queue-url http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/stl-ethereum-transformer.fifo
  AWS_ACCESS_KEY_ID=test AWS_SECRET_ACCESS_KEY=test \
    aws --endpoint-url http://localhost:4566 --region us-east-1 \
    sqs purge-queue --queue-url http://sqs.us-east-1.localhost.localstack.cloud:4566/000000000000/stl-ethereum-backup.fifo
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
kubectl exec -n stl timescaledb-0 -- psql -U postgres -d stl_verify \
  -c "SELECT count(*) AS gaps FROM generate_series((SELECT min(number) FROM block_states WHERE chain_id = 1 AND NOT is_orphaned),(SELECT max(number) FROM block_states WHERE chain_id = 1 AND NOT is_orphaned)) AS s(n) LEFT JOIN block_states b ON b.number = s.n AND b.chain_id = 1 AND NOT b.is_orphaned WHERE b.number IS NULL;" \
  -c "SELECT count(*) AS unexpected_orphans FROM block_states WHERE chain_id = 1 AND is_orphaned AND number > (SELECT max(number) - 64 FROM block_states WHERE chain_id = 1);" \
  -c "SELECT count(*) AS broken_parent_links FROM block_states b JOIN block_states p ON p.number = b.number - 1 AND p.chain_id = b.chain_id AND NOT p.is_orphaned WHERE NOT b.is_orphaned AND b.chain_id = 1 AND b.parent_hash != p.hash;"
