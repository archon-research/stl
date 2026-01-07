#!/bin/bash
# LocalStack initialization script for STL-Verify
# This script runs automatically when LocalStack starts.
#
# Creates SNS topics and SQS queues that mirror the production AWS setup.
# Your production SNS adapter can connect to http://localhost:4566 unchanged.

set -euo pipefail

echo "=== Initializing LocalStack AWS resources ==="

# Region used for all resources
REGION="us-east-1"
ACCOUNT_ID="000000000000"

# Create SNS topics for each event type
echo "Creating SNS topics..."
awslocal sns create-topic --name stl-block-events --region $REGION
awslocal sns create-topic --name stl-receipts-events --region $REGION
awslocal sns create-topic --name stl-traces-events --region $REGION
awslocal sns create-topic --name stl-blobs-events --region $REGION

# Create SQS queues for consumers
echo "Creating SQS queues..."
awslocal sqs create-queue --queue-name stl-block-events-queue --region $REGION
awslocal sqs create-queue --queue-name stl-receipts-events-queue --region $REGION
awslocal sqs create-queue --queue-name stl-traces-events-queue --region $REGION
awslocal sqs create-queue --queue-name stl-blobs-events-queue --region $REGION

# Subscribe SQS queues to SNS topics
echo "Subscribing queues to topics..."
awslocal sns subscribe \
  --topic-arn "arn:aws:sns:${REGION}:${ACCOUNT_ID}:stl-block-events" \
  --protocol sqs \
  --notification-endpoint "arn:aws:sqs:${REGION}:${ACCOUNT_ID}:stl-block-events-queue" \
  --region $REGION

awslocal sns subscribe \
  --topic-arn "arn:aws:sns:${REGION}:${ACCOUNT_ID}:stl-receipts-events" \
  --protocol sqs \
  --notification-endpoint "arn:aws:sqs:${REGION}:${ACCOUNT_ID}:stl-receipts-events-queue" \
  --region $REGION

awslocal sns subscribe \
  --topic-arn "arn:aws:sns:${REGION}:${ACCOUNT_ID}:stl-traces-events" \
  --protocol sqs \
  --notification-endpoint "arn:aws:sqs:${REGION}:${ACCOUNT_ID}:stl-traces-events-queue" \
  --region $REGION

awslocal sns subscribe \
  --topic-arn "arn:aws:sns:${REGION}:${ACCOUNT_ID}:stl-blobs-events" \
  --protocol sqs \
  --notification-endpoint "arn:aws:sqs:${REGION}:${ACCOUNT_ID}:stl-blobs-events-queue" \
  --region $REGION

echo "=== LocalStack initialization complete ==="
echo ""
echo "SNS Topics:"
awslocal sns list-topics --region $REGION --query 'Topics[].TopicArn' --output table
echo ""
echo "SQS Queues:"
awslocal sqs list-queues --region $REGION --query 'QueueUrls' --output table
