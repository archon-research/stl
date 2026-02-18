# =============================================================================
# SNS/SQS Messaging Infrastructure - Ethereum
# =============================================================================
# Fan-out pattern: SNS FIFO topic → Multiple SQS FIFO queues
# Each consumer gets its own queue for independent processing
#
# Architecture:
#   SNS FIFO (ethereum-blocks.fifo)
#     ├── SQS FIFO (ethereum-transformer.fifo) → Transform/Load processing
#     │     └── DLQ (ethereum-transformer-dlq.fifo)
#     ├── SQS FIFO (ethereum-backup.fifo) → Backup capability
#     │     └── DLQ (ethereum-backup-dlq.fifo)
#     └── SQS FIFO (ethereum-oracle-price.fifo) → Oracle price tracking
#           └── DLQ (ethereum-oracle-price-dlq.fifo)

# -----------------------------------------------------------------------------
# SNS FIFO Topic - Ethereum Blocks
# -----------------------------------------------------------------------------
# Central topic for Ethereum block events. Publishers send here,
# subscribers receive via their dedicated SQS queues.

resource "aws_sns_topic" "ethereum_blocks" {
  name                        = "${local.prefix}-ethereum-blocks.fifo"
  fifo_topic                  = true
  content_based_deduplication = true

  tags = {
    Name       = "${local.prefix}-ethereum-blocks"
    Blockchain = "ethereum"
    Service    = "messaging"
  }
}

# -----------------------------------------------------------------------------
# SQS FIFO Queue - Transformer
# -----------------------------------------------------------------------------
# Receives block events for transform/load processing

resource "aws_sqs_queue" "ethereum_transformer_dlq" {
  name                        = "${local.prefix}-ethereum-transformer-dlq.fifo"
  fifo_queue                  = true
  content_based_deduplication = true

  # DLQ retention: 14 days for debugging failed messages
  message_retention_seconds = 1209600

  tags = {
    Name       = "${local.prefix}-ethereum-transformer-dlq"
    Blockchain = "ethereum"
    Service    = "messaging"
    Type       = "dlq"
  }
}

# Allow redrive from transformer DLQ back to source queue
resource "aws_sqs_queue_redrive_allow_policy" "ethereum_transformer_dlq" {
  queue_url = aws_sqs_queue.ethereum_transformer_dlq.id

  redrive_allow_policy = jsonencode({
    redrivePermission = "byQueue"
    sourceQueueArns   = [aws_sqs_queue.ethereum_transformer.arn]
  })
}

resource "aws_sqs_queue" "ethereum_transformer" {
  name                        = "${local.prefix}-ethereum-transformer.fifo"
  fifo_queue                  = true
  content_based_deduplication = true

  # Visibility timeout: 5 minutes for processing
  visibility_timeout_seconds = 300

  # Message retention: 14 days (max allowed by SQS)
  message_retention_seconds = 1209600

  # Dead letter queue configuration
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.ethereum_transformer_dlq.arn
    maxReceiveCount     = 3
  })

  tags = {
    Name       = "${local.prefix}-ethereum-transformer"
    Blockchain = "ethereum"
    Service    = "messaging"
    Consumer   = "transformer"
  }
}

# Allow SNS to send messages to transformer queue
data "aws_iam_policy_document" "ethereum_transformer_queue" {
  statement {
    sid    = "AllowSNSPublish"
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["sns.amazonaws.com"]
    }

    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.ethereum_transformer.arn]

    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values   = [aws_sns_topic.ethereum_blocks.arn]
    }
  }
}

resource "aws_sqs_queue_policy" "ethereum_transformer" {
  queue_url = aws_sqs_queue.ethereum_transformer.id
  policy    = data.aws_iam_policy_document.ethereum_transformer_queue.json
}

# SNS subscription with raw message delivery
resource "aws_sns_topic_subscription" "ethereum_transformer" {
  topic_arn            = aws_sns_topic.ethereum_blocks.arn
  protocol             = "sqs"
  endpoint             = aws_sqs_queue.ethereum_transformer.arn
  raw_message_delivery = true
}

# -----------------------------------------------------------------------------
# SQS FIFO Queue - Backup
# -----------------------------------------------------------------------------
# Receives block events for backup/replay capability

resource "aws_sqs_queue" "ethereum_backup_dlq" {
  name                        = "${local.prefix}-ethereum-backup-dlq.fifo"
  fifo_queue                  = true
  content_based_deduplication = true

  # DLQ retention: 14 days for debugging failed messages
  message_retention_seconds = 1209600

  tags = {
    Name       = "${local.prefix}-ethereum-backup-dlq"
    Blockchain = "ethereum"
    Service    = "messaging"
    Type       = "dlq"
  }
}

# Allow redrive from backup DLQ back to source queue
resource "aws_sqs_queue_redrive_allow_policy" "ethereum_backup_dlq" {
  queue_url = aws_sqs_queue.ethereum_backup_dlq.id

  redrive_allow_policy = jsonencode({
    redrivePermission = "byQueue"
    sourceQueueArns   = [aws_sqs_queue.ethereum_backup.arn]
  })
}

resource "aws_sqs_queue" "ethereum_backup" {
  name                        = "${local.prefix}-ethereum-backup.fifo"
  fifo_queue                  = true
  content_based_deduplication = true

  # Visibility timeout: 5 minutes for processing
  visibility_timeout_seconds = 300

  # Message retention: 14 days (longer for backup purposes)
  message_retention_seconds = 1209600

  # Dead letter queue configuration
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.ethereum_backup_dlq.arn
    maxReceiveCount     = 3
  })

  tags = {
    Name       = "${local.prefix}-ethereum-backup"
    Blockchain = "ethereum"
    Service    = "messaging"
    Consumer   = "backup"
  }
}

# Allow SNS to send messages to backup queue
data "aws_iam_policy_document" "ethereum_backup_queue" {
  statement {
    sid    = "AllowSNSPublish"
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["sns.amazonaws.com"]
    }

    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.ethereum_backup.arn]

    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values   = [aws_sns_topic.ethereum_blocks.arn]
    }
  }
}

resource "aws_sqs_queue_policy" "ethereum_backup" {
  queue_url = aws_sqs_queue.ethereum_backup.id
  policy    = data.aws_iam_policy_document.ethereum_backup_queue.json
}

# SNS subscription with raw message delivery
resource "aws_sns_topic_subscription" "ethereum_backup" {
  topic_arn            = aws_sns_topic.ethereum_blocks.arn
  protocol             = "sqs"
  endpoint             = aws_sqs_queue.ethereum_backup.arn
  raw_message_delivery = true
}

# -----------------------------------------------------------------------------
# SQS FIFO Queue - Oracle Price Worker
# -----------------------------------------------------------------------------
# Receives block events for oracle price tracking

resource "aws_sqs_queue" "ethereum_oracle_price_dlq" {
  name                        = "${local.prefix}-ethereum-oracle-price-dlq.fifo"
  fifo_queue                  = true
  content_based_deduplication = true

  # DLQ retention: 14 days for debugging failed messages
  message_retention_seconds = 1209600

  tags = {
    Name       = "${local.prefix}-ethereum-oracle-price-dlq"
    Blockchain = "ethereum"
    Service    = "messaging"
    Type       = "dlq"
  }
}

# Allow redrive from oracle price DLQ back to source queue
resource "aws_sqs_queue_redrive_allow_policy" "ethereum_oracle_price_dlq" {
  queue_url = aws_sqs_queue.ethereum_oracle_price_dlq.id

  redrive_allow_policy = jsonencode({
    redrivePermission = "byQueue"
    sourceQueueArns   = [aws_sqs_queue.ethereum_oracle_price.arn]
  })
}

resource "aws_sqs_queue" "ethereum_oracle_price" {
  name                        = "${local.prefix}-ethereum-oracle-price.fifo"
  fifo_queue                  = true
  content_based_deduplication = true

  # Visibility timeout: 5 minutes for processing
  visibility_timeout_seconds = 300

  # Message retention: 14 days (max allowed by SQS)
  message_retention_seconds = 1209600

  # Dead letter queue configuration
  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.ethereum_oracle_price_dlq.arn
    maxReceiveCount     = 3
  })

  tags = {
    Name       = "${local.prefix}-ethereum-oracle-price"
    Blockchain = "ethereum"
    Service    = "messaging"
    Consumer   = "oracle-price"
  }
}

# Allow SNS to send messages to oracle price queue
data "aws_iam_policy_document" "ethereum_oracle_price_queue" {
  statement {
    sid    = "AllowSNSPublish"
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["sns.amazonaws.com"]
    }

    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.ethereum_oracle_price.arn]

    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values   = [aws_sns_topic.ethereum_blocks.arn]
    }
  }
}

resource "aws_sqs_queue_policy" "ethereum_oracle_price" {
  queue_url = aws_sqs_queue.ethereum_oracle_price.id
  policy    = data.aws_iam_policy_document.ethereum_oracle_price_queue.json
}

# SNS subscription with raw message delivery
resource "aws_sns_topic_subscription" "ethereum_oracle_price" {
  topic_arn            = aws_sns_topic.ethereum_blocks.arn
  protocol             = "sqs"
  endpoint             = aws_sqs_queue.ethereum_oracle_price.arn
  raw_message_delivery = true
}

# -----------------------------------------------------------------------------
# IAM Policy for Publishing to SNS
# -----------------------------------------------------------------------------
# Allows Watcher to publish block events
# Note: Attached to watcher role in iam_ecs_task.tf

data "aws_iam_policy_document" "ethereum_sns_publish" {
  statement {
    sid    = "PublishToEthereumBlocks"
    effect = "Allow"
    actions = [
      "sns:Publish",
    ]
    resources = [aws_sns_topic.ethereum_blocks.arn]
  }
}

resource "aws_iam_policy" "ethereum_sns_publish" {
  name        = "${local.prefix}-ethereum-sns-publish"
  description = "Allows publishing to Ethereum blocks SNS FIFO topic"
  policy      = data.aws_iam_policy_document.ethereum_sns_publish.json
}

# -----------------------------------------------------------------------------
# IAM Policy for Consuming from SQS
# -----------------------------------------------------------------------------
# Allows Workers to consume messages
# Note: Attached to worker role in iam_ecs_task.tf

data "aws_iam_policy_document" "ethereum_sqs_consume" {
  statement {
    sid    = "ConsumeFromEthereumQueues"
    effect = "Allow"
    actions = [
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes",
      "sqs:GetQueueUrl",
      "sqs:ChangeMessageVisibility",
    ]
    resources = [
      aws_sqs_queue.ethereum_transformer.arn,
      aws_sqs_queue.ethereum_backup.arn,
      aws_sqs_queue.ethereum_oracle_price.arn,
    ]
  }

  statement {
    sid    = "AccessDLQs"
    effect = "Allow"
    actions = [
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes",
      "sqs:GetQueueUrl",
      "sqs:SendMessage", # For redrive
    ]
    resources = [
      aws_sqs_queue.ethereum_transformer_dlq.arn,
      aws_sqs_queue.ethereum_backup_dlq.arn,
      aws_sqs_queue.ethereum_oracle_price_dlq.arn,
    ]
  }
}

resource "aws_iam_policy" "ethereum_sqs_consume" {
  name        = "${local.prefix}-ethereum-sqs-consume"
  description = "Allows consuming from Ethereum SQS FIFO queues"
  policy      = data.aws_iam_policy_document.ethereum_sqs_consume.json
}

# -----------------------------------------------------------------------------
# Outputs
# -----------------------------------------------------------------------------

output "ethereum_sns_topic_arn" {
  description = "ARN of the Ethereum blocks SNS FIFO topic"
  value       = aws_sns_topic.ethereum_blocks.arn
}

output "ethereum_transformer_queue_url" {
  description = "URL of the Ethereum transformer SQS FIFO queue"
  value       = aws_sqs_queue.ethereum_transformer.url
}

output "ethereum_transformer_queue_arn" {
  description = "ARN of the Ethereum transformer SQS FIFO queue"
  value       = aws_sqs_queue.ethereum_transformer.arn
}

output "ethereum_backup_queue_url" {
  description = "URL of the Ethereum backup SQS FIFO queue"
  value       = aws_sqs_queue.ethereum_backup.url
}

output "ethereum_backup_queue_arn" {
  description = "ARN of the Ethereum backup SQS FIFO queue"
  value       = aws_sqs_queue.ethereum_backup.arn
}

output "ethereum_transformer_dlq_url" {
  description = "URL of the Ethereum transformer dead letter queue"
  value       = aws_sqs_queue.ethereum_transformer_dlq.url
}

output "ethereum_backup_dlq_url" {
  description = "URL of the Ethereum backup dead letter queue"
  value       = aws_sqs_queue.ethereum_backup_dlq.url
}

output "ethereum_oracle_price_queue_url" {
  description = "URL of the Ethereum oracle price SQS FIFO queue"
  value       = aws_sqs_queue.ethereum_oracle_price.url
}

output "ethereum_oracle_price_queue_arn" {
  description = "ARN of the Ethereum oracle price SQS FIFO queue"
  value       = aws_sqs_queue.ethereum_oracle_price.arn
}

output "ethereum_oracle_price_dlq_url" {
  description = "URL of the Ethereum oracle price dead letter queue"
  value       = aws_sqs_queue.ethereum_oracle_price_dlq.url
}
