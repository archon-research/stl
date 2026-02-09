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

# Create SNS FIFO topic (single topic for all event types)
echo "Creating SNS FIFO topic..."
$AWS sns create-topic \
  --name stl-ethereum-blocks.fifo \
  --attributes FifoTopic=true,ContentBasedDeduplication=true \
  --region $REGION

# Create SQS FIFO queues for consumers (fan-out pattern)
echo "Creating SQS FIFO queues..."

# Transformer queue + DLQ
$AWS sqs create-queue \
  --queue-name stl-ethereum-transformer-dlq.fifo \
  --attributes FifoQueue=true,ContentBasedDeduplication=true \
  --region $REGION

$AWS sqs create-queue \
  --queue-name stl-ethereum-transformer.fifo \
  --attributes FifoQueue=true,ContentBasedDeduplication=true \
  --region $REGION

# Backup queue + DLQ
$AWS sqs create-queue \
  --queue-name stl-ethereum-backup-dlq.fifo \
  --attributes FifoQueue=true,ContentBasedDeduplication=true \
  --region $REGION

$AWS sqs create-queue \
  --queue-name stl-ethereum-backup.fifo \
  --attributes FifoQueue=true,ContentBasedDeduplication=true \
  --region $REGION

# Oracle price worker queue + DLQ
$AWS sqs create-queue \
  --queue-name stl-ethereum-oracle-price-dlq.fifo \
  --attributes FifoQueue=true,ContentBasedDeduplication=true \
  --region $REGION

$AWS sqs create-queue \
  --queue-name stl-ethereum-oracle-price.fifo \
  --attributes FifoQueue=true,ContentBasedDeduplication=true \
  --region $REGION

# Subscribe SQS FIFO queues to SNS FIFO topic
echo "Subscribing queues to topic..."
$AWS sns subscribe \
  --topic-arn "arn:aws:sns:${REGION}:${ACCOUNT_ID}:stl-ethereum-blocks.fifo" \
  --protocol sqs \
  --notification-endpoint "arn:aws:sqs:${REGION}:${ACCOUNT_ID}:stl-ethereum-transformer.fifo" \
  --attributes RawMessageDelivery=true \
  --region $REGION

$AWS sns subscribe \
  --topic-arn "arn:aws:sns:${REGION}:${ACCOUNT_ID}:stl-ethereum-blocks.fifo" \
  --protocol sqs \
  --notification-endpoint "arn:aws:sqs:${REGION}:${ACCOUNT_ID}:stl-ethereum-backup.fifo" \
  --attributes RawMessageDelivery=true \
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
