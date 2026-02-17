# =============================================================================
# Messaging Monitoring - SNS/SQS Dashboard and Alarms
# =============================================================================

# -----------------------------------------------------------------------------
# SNS Topic for Messaging Alarms
# -----------------------------------------------------------------------------

resource "aws_sns_topic" "messaging_alarms" {
  name = "${local.name_prefix}-messaging-alarms"

  tags = {
    Name    = "${local.name_prefix}-messaging-alarms"
    Service = "messaging"
  }
}

# -----------------------------------------------------------------------------
# CloudWatch Alarms - DLQ Messages (Critical)
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_metric_alarm" "dlq_messages" {
  for_each = var.sqs_consumers

  alarm_name          = "${local.name_prefix}-${replace(each.key, "_", "-")}-dlq-messages"
  alarm_description   = "Messages detected in ${replace(each.key, "_", " ")} DLQ - requires investigation"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 60
  statistic           = "Sum"
  threshold           = 0
  treat_missing_data  = "notBreaching"

  dimensions = {
    QueueName = aws_sqs_queue.consumer_dlq[each.key].name
  }

  alarm_actions = [aws_sns_topic.messaging_alarms.arn]
  ok_actions    = [aws_sns_topic.messaging_alarms.arn]

  tags = {
    Name       = "${local.name_prefix}-${replace(each.key, "_", "-")}-dlq-messages"
    Blockchain = var.chain_name
    Service    = "messaging"
    Severity   = "critical"
  }
}

# -----------------------------------------------------------------------------
# CloudWatch Alarms - Queue Backlog (Warning)
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_metric_alarm" "queue_backlog" {
  for_each = var.sqs_consumers

  alarm_name          = "${local.name_prefix}-${replace(each.key, "_", "-")}-backlog"
  alarm_description   = "${title(replace(each.key, "_", " "))} queue backlog exceeds threshold - consumers may be slow"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 300
  statistic           = "Average"
  threshold           = 100
  treat_missing_data  = "notBreaching"

  dimensions = {
    QueueName = aws_sqs_queue.consumer[each.key].name
  }

  alarm_actions = [aws_sns_topic.messaging_alarms.arn]
  ok_actions    = [aws_sns_topic.messaging_alarms.arn]

  tags = {
    Name       = "${local.name_prefix}-${replace(each.key, "_", "-")}-backlog"
    Blockchain = var.chain_name
    Service    = "messaging"
    Severity   = "warning"
  }
}

# -----------------------------------------------------------------------------
# CloudWatch Alarms - Message Age (Warning)
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_metric_alarm" "message_age" {
  for_each = var.sqs_consumers

  alarm_name          = "${local.name_prefix}-${replace(each.key, "_", "-")}-message-age"
  alarm_description   = "Oldest message in ${replace(each.key, "_", " ")} queue exceeds 5 minutes"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "ApproximateAgeOfOldestMessage"
  namespace           = "AWS/SQS"
  period              = 300
  statistic           = "Maximum"
  threshold           = 300 # 5 minutes
  treat_missing_data  = "notBreaching"

  dimensions = {
    QueueName = aws_sqs_queue.consumer[each.key].name
  }

  alarm_actions = [aws_sns_topic.messaging_alarms.arn]
  ok_actions    = [aws_sns_topic.messaging_alarms.arn]

  tags = {
    Name       = "${local.name_prefix}-${replace(each.key, "_", "-")}-message-age"
    Blockchain = var.chain_name
    Service    = "messaging"
    Severity   = "warning"
  }
}

# -----------------------------------------------------------------------------
# CloudWatch Alarms - SNS Publish Failures (Critical)
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_metric_alarm" "sns_publish_failures" {
  alarm_name          = "${local.name_prefix}-sns-publish-failures"
  alarm_description   = "SNS topic experiencing delivery failures"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "NumberOfNotificationsFailed"
  namespace           = "AWS/SNS"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  treat_missing_data  = "notBreaching"

  dimensions = {
    TopicName = aws_sns_topic.blocks.name
  }

  alarm_actions = [aws_sns_topic.messaging_alarms.arn]
  ok_actions    = [aws_sns_topic.messaging_alarms.arn]

  tags = {
    Name       = "${local.name_prefix}-sns-publish-failures"
    Blockchain = var.chain_name
    Service    = "messaging"
    Severity   = "critical"
  }
}

# -----------------------------------------------------------------------------
# CloudWatch Dashboard
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_dashboard" "messaging" {
  dashboard_name = "${local.name_prefix}-messaging"

  dashboard_body = jsonencode({
    widgets = concat(
      # Header
      [{
        type   = "text"
        x      = 0
        y      = 0
        width  = 24
        height = 1
        properties = {
          markdown = "# ${title(var.chain_name)} Block Messaging - SNS/SQS Fan-out"
        }
      }],
      # SNS Topic Overview
      [
        {
          type   = "metric"
          x      = 0
          y      = 1
          width  = 8
          height = 6
          properties = {
            title  = "SNS - Messages Published"
            region = var.aws_region
            stat   = "Sum"
            period = 60
            metrics = [
              ["AWS/SNS", "NumberOfMessagesPublished", "TopicName", aws_sns_topic.blocks.name]
            ]
          }
        },
        {
          type   = "metric"
          x      = 8
          y      = 1
          width  = 8
          height = 6
          properties = {
            title  = "SNS - Notifications Delivered"
            region = var.aws_region
            stat   = "Sum"
            period = 60
            metrics = [
              ["AWS/SNS", "NumberOfNotificationsDelivered", "TopicName", aws_sns_topic.blocks.name]
            ]
          }
        },
        {
          type   = "metric"
          x      = 16
          y      = 1
          width  = 8
          height = 6
          properties = {
            title  = "SNS - Notification Failures"
            region = var.aws_region
            stat   = "Sum"
            period = 60
            metrics = [
              ["AWS/SNS", "NumberOfNotificationsFailed", "TopicName", aws_sns_topic.blocks.name]
            ]
            annotations = {
              horizontal = [{ value = 1, color = "#d62728", label = "Failure Threshold" }]
            }
          }
        }
      ],
      # Per-consumer queue widgets
      flatten([for idx, name in sort(keys(var.sqs_consumers)) : [
        {
          type   = "text"
          x      = 0
          y      = 7 + (idx * 8)
          width  = 24
          height = 1
          properties = {
            markdown = "## ${title(replace(name, "_", " "))} Queue"
          }
        },
        {
          type   = "metric"
          x      = 0
          y      = 8 + (idx * 8)
          width  = 6
          height = 6
          properties = {
            title  = "Messages Visible (Backlog)"
            region = var.aws_region
            stat   = "Average"
            period = 60
            metrics = [
              ["AWS/SQS", "ApproximateNumberOfMessagesVisible", "QueueName", aws_sqs_queue.consumer[name].name]
            ]
            annotations = {
              horizontal = [{ value = 100, color = "#ff7f0e", label = "Warning Threshold" }]
            }
          }
        },
        {
          type   = "metric"
          x      = 6
          y      = 8 + (idx * 8)
          width  = 6
          height = 6
          properties = {
            title  = "Messages In Flight"
            region = var.aws_region
            stat   = "Average"
            period = 60
            metrics = [
              ["AWS/SQS", "ApproximateNumberOfMessagesNotVisible", "QueueName", aws_sqs_queue.consumer[name].name]
            ]
          }
        },
        {
          type   = "metric"
          x      = 12
          y      = 8 + (idx * 8)
          width  = 6
          height = 6
          properties = {
            title  = "Age of Oldest Message (seconds)"
            region = var.aws_region
            stat   = "Maximum"
            period = 60
            metrics = [
              ["AWS/SQS", "ApproximateAgeOfOldestMessage", "QueueName", aws_sqs_queue.consumer[name].name]
            ]
          }
        },
        {
          type   = "metric"
          x      = 18
          y      = 8 + (idx * 8)
          width  = 6
          height = 6
          properties = {
            title  = "Throughput (Received vs Deleted)"
            region = var.aws_region
            stat   = "Sum"
            period = 60
            metrics = [
              ["AWS/SQS", "NumberOfMessagesReceived", "QueueName", aws_sqs_queue.consumer[name].name],
              [".", "NumberOfMessagesDeleted", ".", "."]
            ]
          }
        }
      ]]),
      # DLQ section
      [{
        type   = "text"
        x      = 0
        y      = 7 + (length(var.sqs_consumers) * 8)
        width  = 24
        height = 1
        properties = {
          markdown = "## Dead Letter Queues (DLQ)"
        }
      }],
      [for idx, name in sort(keys(var.sqs_consumers)) : {
        type   = "metric"
        x      = idx * 8
        y      = 8 + (length(var.sqs_consumers) * 8)
        width  = 8
        height = 6
        properties = {
          title  = "${title(replace(name, "_", " "))} DLQ - Messages"
          region = var.aws_region
          stat   = "Sum"
          period = 60
          metrics = [
            ["AWS/SQS", "ApproximateNumberOfMessagesVisible", "QueueName", aws_sqs_queue.consumer_dlq[name].name]
          ]
          annotations = {
            horizontal = [{ value = 1, color = "#d62728", label = "Alert: Messages in DLQ" }]
          }
        }
      }],
      # Alarm status
      [{
        type   = "text"
        x      = 0
        y      = 14 + (length(var.sqs_consumers) * 8)
        width  = 24
        height = 1
        properties = {
          markdown = "## Alarm Status"
        }
      }],
      [{
        type   = "alarm"
        x      = 0
        y      = 15 + (length(var.sqs_consumers) * 8)
        width  = 24
        height = 3
        properties = {
          title = "Messaging Alarms"
          alarms = concat(
            [for name in sort(keys(var.sqs_consumers)) : aws_cloudwatch_metric_alarm.dlq_messages[name].arn],
            [for name in sort(keys(var.sqs_consumers)) : aws_cloudwatch_metric_alarm.queue_backlog[name].arn],
            [for name in sort(keys(var.sqs_consumers)) : aws_cloudwatch_metric_alarm.message_age[name].arn],
            [aws_cloudwatch_metric_alarm.sns_publish_failures.arn]
          )
        }
      }]
    )
  })
}
