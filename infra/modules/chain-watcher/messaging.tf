# =============================================================================
# SNS/SQS Messaging Infrastructure
# =============================================================================
# Fan-out pattern: SNS FIFO topic → Multiple SQS FIFO queues (dynamic)

# -----------------------------------------------------------------------------
# SNS FIFO Topic
# -----------------------------------------------------------------------------

resource "aws_sns_topic" "blocks" {
  name                        = "${local.name_prefix}-blocks.fifo"
  fifo_topic                  = true
  content_based_deduplication = true

  tags = {
    Name       = "${local.name_prefix}-blocks"
    Blockchain = var.chain_name
    Service    = "messaging"
  }
}

# -----------------------------------------------------------------------------
# SQS FIFO Queues (dynamic per consumer)
# -----------------------------------------------------------------------------

# Dead Letter Queues
resource "aws_sqs_queue" "consumer_dlq" {
  for_each = var.sqs_consumers

  name                        = "${local.name_prefix}-${replace(each.key, "_", "-")}-dlq.fifo"
  fifo_queue                  = true
  content_based_deduplication = true
  message_retention_seconds   = 1209600 # 14 days

  tags = {
    Name       = "${local.name_prefix}-${replace(each.key, "_", "-")}-dlq"
    Blockchain = var.chain_name
    Service    = "messaging"
    Type       = "dlq"
  }
}

# Redrive allow policies
resource "aws_sqs_queue_redrive_allow_policy" "consumer_dlq" {
  for_each = var.sqs_consumers

  queue_url = aws_sqs_queue.consumer_dlq[each.key].id

  redrive_allow_policy = jsonencode({
    redrivePermission = "byQueue"
    sourceQueueArns   = [aws_sqs_queue.consumer[each.key].arn]
  })
}

# Main consumer queues
resource "aws_sqs_queue" "consumer" {
  for_each = var.sqs_consumers

  name                        = "${local.name_prefix}-${replace(each.key, "_", "-")}.fifo"
  fifo_queue                  = true
  content_based_deduplication = true
  visibility_timeout_seconds  = each.value.visibility_timeout_seconds
  message_retention_seconds   = each.value.message_retention_seconds

  redrive_policy = jsonencode({
    deadLetterTargetArn = aws_sqs_queue.consumer_dlq[each.key].arn
    maxReceiveCount     = each.value.max_receive_count
  })

  tags = {
    Name       = "${local.name_prefix}-${replace(each.key, "_", "-")}"
    Blockchain = var.chain_name
    Service    = "messaging"
    Consumer   = replace(each.key, "_", "-")
  }
}

# Allow SNS to send messages to each queue
data "aws_iam_policy_document" "consumer_queue" {
  for_each = var.sqs_consumers

  statement {
    sid    = "AllowSNSPublish"
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["sns.amazonaws.com"]
    }

    actions   = ["sqs:SendMessage"]
    resources = [aws_sqs_queue.consumer[each.key].arn]

    condition {
      test     = "ArnEquals"
      variable = "aws:SourceArn"
      values   = [aws_sns_topic.blocks.arn]
    }
  }
}

resource "aws_sqs_queue_policy" "consumer" {
  for_each = var.sqs_consumers

  queue_url = aws_sqs_queue.consumer[each.key].id
  policy    = data.aws_iam_policy_document.consumer_queue[each.key].json
}

# SNS subscriptions with raw message delivery
resource "aws_sns_topic_subscription" "consumer" {
  for_each = var.sqs_consumers

  topic_arn            = aws_sns_topic.blocks.arn
  protocol             = "sqs"
  endpoint             = aws_sqs_queue.consumer[each.key].arn
  raw_message_delivery = true
}

# -----------------------------------------------------------------------------
# IAM Policy for Publishing to SNS
# -----------------------------------------------------------------------------

data "aws_iam_policy_document" "sns_publish" {
  statement {
    sid    = "PublishTo${title(var.chain_name)}Blocks"
    effect = "Allow"
    actions = [
      "sns:Publish",
    ]
    resources = [aws_sns_topic.blocks.arn]
  }
}

resource "aws_iam_policy" "sns_publish" {
  name        = "${local.name_prefix}-sns-publish"
  description = "Allows publishing to ${title(var.chain_name)} blocks SNS FIFO topic"
  policy      = data.aws_iam_policy_document.sns_publish.json
}

# -----------------------------------------------------------------------------
# IAM Policy for Consuming from SQS
# -----------------------------------------------------------------------------

data "aws_iam_policy_document" "sqs_consume" {
  statement {
    sid    = "ConsumeFrom${title(var.chain_name)}Queues"
    effect = "Allow"
    actions = [
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes",
      "sqs:GetQueueUrl",
      "sqs:ChangeMessageVisibility",
    ]
    resources = [for q in aws_sqs_queue.consumer : q.arn]
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
    resources = [for q in aws_sqs_queue.consumer_dlq : q.arn]
  }
}

resource "aws_iam_policy" "sqs_consume" {
  name        = "${local.name_prefix}-sqs-consume"
  description = "Allows consuming from ${title(var.chain_name)} SQS FIFO queues"
  policy      = data.aws_iam_policy_document.sqs_consume.json
}
