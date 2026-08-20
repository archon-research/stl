#!/bin/bash
# LocalStack initialization script for STL-Verify — single source of truth.
#
# `make kind-infra` generates the `localstack-init` ConfigMap from this file and
# LocalStack mounts it at /etc/localstack/init/ready.d/init-aws.sh, so the kind
# cluster runs exactly this script. It can also be run by hand against any
# LocalStack endpoint (set AWS_ENDPOINT_URL).
#
# Creates SNS FIFO topics and SQS FIFO queues that mirror the production AWS setup.
# Architecture: SNS FIFO → Multiple SQS FIFO queues (fan-out pattern)

set -euo pipefail

echo "=== Initializing LocalStack AWS resources ==="

# Region used for all resources
REGION="us-east-1"
ACCOUNT_ID="000000000000"
ENDPOINT="${AWS_ENDPOINT_URL:-http://localhost:4566}"

# Use aws CLI with LocalStack endpoint
AWS="aws --endpoint-url=$ENDPOINT"

# Set dummy credentials for LocalStack
export AWS_ACCESS_KEY_ID="${AWS_ACCESS_KEY_ID:-test}"
export AWS_SECRET_ACCESS_KEY="${AWS_SECRET_ACCESS_KEY:-test}"

# Deployment environment, injected from stl-config. Raw-block bucket names are
# derived from it as stl-sentinel{DEPLOY_ENV}-{chain}-raw, the exact format
# chainutil.ValidateS3BucketForChain enforces at worker startup — so the buckets
# created here and the S3_BUCKET values the workers get cannot drift apart.
DEPLOY_ENV="${DEPLOY_ENV:-development}"

# Helper: create SNS topic, the two always-on SQS queues (transformer, backup),
# their subscriptions, and the raw-block S3 bucket for a chain. Every chain gets
# its own bucket: workers validate that S3_BUCKET matches their CHAIN_ID, so a
# non-Ethereum worker cannot share the Ethereum bucket.
# Worker-specific queues (oracle-price, sparklend, etc.) are created separately below.
create_chain_resources() {
  local CHAIN_NAME=$1

  echo "Creating ${CHAIN_NAME} SNS FIFO topic..."
  $AWS sns create-topic \
    --name "stl-${CHAIN_NAME}-blocks.fifo" \
    --attributes FifoTopic=true,ContentBasedDeduplication=true \
    --region $REGION

  echo "Creating ${CHAIN_NAME} SQS FIFO queues..."

  # Transformer queue + DLQ
  $AWS sqs create-queue \
    --queue-name "stl-${CHAIN_NAME}-transformer-dlq.fifo" \
    --attributes FifoQueue=true,ContentBasedDeduplication=true \
    --region $REGION

  $AWS sqs create-queue \
    --queue-name "stl-${CHAIN_NAME}-transformer.fifo" \
    --attributes FifoQueue=true,ContentBasedDeduplication=true \
    --region $REGION

  # Backup queue + DLQ
  $AWS sqs create-queue \
    --queue-name "stl-${CHAIN_NAME}-backup-dlq.fifo" \
    --attributes FifoQueue=true,ContentBasedDeduplication=true \
    --region $REGION

  $AWS sqs create-queue \
    --queue-name "stl-${CHAIN_NAME}-backup.fifo" \
    --attributes FifoQueue=true,ContentBasedDeduplication=true \
    --region $REGION

  echo "Subscribing ${CHAIN_NAME} queues to topic..."
  $AWS sns subscribe \
    --topic-arn "arn:aws:sns:${REGION}:${ACCOUNT_ID}:stl-${CHAIN_NAME}-blocks.fifo" \
    --protocol sqs \
    --notification-endpoint "arn:aws:sqs:${REGION}:${ACCOUNT_ID}:stl-${CHAIN_NAME}-transformer.fifo" \
    --attributes RawMessageDelivery=true \
    --region $REGION

  $AWS sns subscribe \
    --topic-arn "arn:aws:sns:${REGION}:${ACCOUNT_ID}:stl-${CHAIN_NAME}-blocks.fifo" \
    --protocol sqs \
    --notification-endpoint "arn:aws:sqs:${REGION}:${ACCOUNT_ID}:stl-${CHAIN_NAME}-backup.fifo" \
    --attributes RawMessageDelivery=true \
    --region $REGION

  echo "Creating ${CHAIN_NAME} raw-block S3 bucket..."
  $AWS s3 mb "s3://stl-sentinel${DEPLOY_ENV}-${CHAIN_NAME}-raw" --region $REGION
}

# Helper: create a consumer queue (with DLQ) subscribed to a chain's SNS topic.
# Local convention is stl-<chain>-<queue>.fifo — no sentinel<env> segment,
# unlike the EKS queues.
create_consumer_queue() {
  local CHAIN_NAME=$1
  local QUEUE_NAME=$2

  echo "Creating ${CHAIN_NAME} ${QUEUE_NAME} queues..."
  $AWS sqs create-queue \
    --queue-name "stl-${CHAIN_NAME}-${QUEUE_NAME}-dlq.fifo" \
    --attributes FifoQueue=true,ContentBasedDeduplication=true \
    --region $REGION

  $AWS sqs create-queue \
    --queue-name "stl-${CHAIN_NAME}-${QUEUE_NAME}.fifo" \
    --attributes FifoQueue=true,ContentBasedDeduplication=true \
    --region $REGION

  $AWS sns subscribe \
    --topic-arn "arn:aws:sns:${REGION}:${ACCOUNT_ID}:stl-${CHAIN_NAME}-blocks.fifo" \
    --protocol sqs \
    --notification-endpoint "arn:aws:sqs:${REGION}:${ACCOUNT_ID}:stl-${CHAIN_NAME}-${QUEUE_NAME}.fifo" \
    --attributes RawMessageDelivery=true \
    --region $REGION
}

# Create resources for each supported chain
create_chain_resources "ethereum"
create_chain_resources "avalanche"
# L2 chains
create_chain_resources "base"
create_chain_resources "optimism"
create_chain_resources "unichain"
create_chain_resources "arbitrum"

# Ethereum-only consumers. curve-indexing / uniswap-v3-indexing / uniswap-v4-indexing
# feed the shared dex-indexer image — one Deployment per DEX, DEX env selects the factory.
for queue in morpho-indexing prime-debt curve-indexing uniswap-v3-indexing uniswap-v4-indexing; do
  create_consumer_queue "ethereum" "$queue"
done

# Multi-chain consumers (Ethereum + Avalanche)
for queue in oracle-price sparklend-position allocation-tracker; do
  for chain in ethereum avalanche; do
    create_consumer_queue "$chain" "$queue"
  done
done

# PSM3 indexer runs on the four L2 chains
for chain in base optimism unichain arbitrum; do
  create_consumer_queue "$chain" "psm3-indexer"
done

# Per-chain raw-block buckets are created by create_chain_resources above.
echo "Creating stress-test S3 bucket..."
$AWS s3 mb s3://stress-test-data --region $REGION

echo "=== LocalStack initialization complete ==="
echo ""
echo "SNS Topics:"
$AWS sns list-topics --region $REGION --query 'Topics[].TopicArn' --output table
echo ""
echo "SQS Queues:"
$AWS sqs list-queues --region $REGION --query 'QueueUrls' --output table
echo ""
echo "S3 Buckets:"
$AWS s3 ls
