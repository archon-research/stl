#!/bin/bash
# LocalStack initialization script for STL-Verify
# This script can be run manually to set up LocalStack resources.
#
# Creates SNS FIFO topic and SQS FIFO queues that mirror the production AWS setup.
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

# Helper: create SNS topic, SQS queues, and subscriptions for a chain
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
}

# Create resources for each supported chain
create_chain_resources "ethereum"
create_chain_resources "avalanche"

# Oracle price worker is Ethereum-only
echo "Creating Ethereum oracle price queues..."
$AWS sqs create-queue \
  --queue-name stl-ethereum-oracle-price-dlq.fifo \
  --attributes FifoQueue=true,ContentBasedDeduplication=true \
  --region $REGION

$AWS sqs create-queue \
  --queue-name stl-ethereum-oracle-price.fifo \
  --attributes FifoQueue=true,ContentBasedDeduplication=true \
  --region $REGION

$AWS sns subscribe \
  --topic-arn "arn:aws:sns:${REGION}:${ACCOUNT_ID}:stl-ethereum-blocks.fifo" \
  --protocol sqs \
  --notification-endpoint "arn:aws:sqs:${REGION}:${ACCOUNT_ID}:stl-ethereum-oracle-price.fifo" \
  --attributes RawMessageDelivery=true \
  --region $REGION

echo "=== LocalStack initialization complete ==="
echo ""
echo "SNS Topics:"
$AWS sns list-topics --region $REGION --query 'Topics[].TopicArn' --output table
echo ""
echo "SQS Queues:"
$AWS sqs list-queues --region $REGION --query 'QueueUrls' --output table
