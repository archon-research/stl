#!/bin/bash
# Query Amp server

AMP_URL="${AMP_URL:-http://localhost:1603}"

if [ -z "$1" ]; then
    echo "Usage: $0 <sql_query>"
    echo ""
    echo "Examples:"
    echo "  $0 'SELECT 1'"
    echo "  $0 'SELECT * FROM \"my_namespace/eth_rpc\".logs LIMIT 10'"
    exit 1
fi

curl -X POST "$AMP_URL" --data "$1"
