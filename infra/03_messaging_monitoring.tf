# =============================================================================
# Messaging Monitoring - SNS/SQS Dashboard and Alarms
# =============================================================================
# CloudWatch dashboard and alarms for Ethereum block messaging infrastructure

# -----------------------------------------------------------------------------
# SNS Topic for Messaging Alarms
# -----------------------------------------------------------------------------

resource "aws_sns_topic" "messaging_alarms" {
  name = "${local.prefix}-messaging-alarms"

  tags = {
    Name    = "${local.prefix}-messaging-alarms"
    Service = "messaging"
  }
}

# -----------------------------------------------------------------------------
# CloudWatch Alarms
# -----------------------------------------------------------------------------

# DLQ Alarms - Critical: Any message in DLQ needs attention
resource "aws_cloudwatch_metric_alarm" "transformer_dlq_messages" {
  alarm_name          = "${local.prefix}-ethereum-transformer-dlq-messages"
  alarm_description   = "Messages detected in transformer DLQ - requires investigation"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 60
  statistic           = "Sum"
  threshold           = 0
  treat_missing_data  = "notBreaching"

  dimensions = {
    QueueName = aws_sqs_queue.ethereum_transformer_dlq.name
  }

  alarm_actions = [aws_sns_topic.messaging_alarms.arn]
  ok_actions    = [aws_sns_topic.messaging_alarms.arn]

  tags = {
    Name       = "${local.prefix}-ethereum-transformer-dlq-messages"
    Blockchain = "ethereum"
    Service    = "messaging"
    Severity   = "critical"
  }
}

resource "aws_cloudwatch_metric_alarm" "backup_dlq_messages" {
  alarm_name          = "${local.prefix}-ethereum-backup-dlq-messages"
  alarm_description   = "Messages detected in backup DLQ - requires investigation"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 60
  statistic           = "Sum"
  threshold           = 0
  treat_missing_data  = "notBreaching"

  dimensions = {
    QueueName = aws_sqs_queue.ethereum_backup_dlq.name
  }

  alarm_actions = [aws_sns_topic.messaging_alarms.arn]
  ok_actions    = [aws_sns_topic.messaging_alarms.arn]

  tags = {
    Name       = "${local.prefix}-ethereum-backup-dlq-messages"
    Blockchain = "ethereum"
    Service    = "messaging"
    Severity   = "critical"
  }
}

resource "aws_cloudwatch_metric_alarm" "oracle_price_dlq_messages" {
  alarm_name          = "${local.prefix}-ethereum-oracle-price-dlq-messages"
  alarm_description   = "Messages detected in oracle price DLQ - requires investigation"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 60
  statistic           = "Sum"
  threshold           = 0
  treat_missing_data  = "notBreaching"

  dimensions = {
    QueueName = aws_sqs_queue.ethereum_oracle_price_dlq.name
  }

  alarm_actions = [aws_sns_topic.messaging_alarms.arn]
  ok_actions    = [aws_sns_topic.messaging_alarms.arn]

  tags = {
    Name       = "${local.prefix}-ethereum-oracle-price-dlq-messages"
    Blockchain = "ethereum"
    Service    = "messaging"
    Severity   = "critical"
  }
}

# Queue Backlog Alarms - Warning: Messages piling up
resource "aws_cloudwatch_metric_alarm" "transformer_queue_backlog" {
  alarm_name          = "${local.prefix}-ethereum-transformer-backlog"
  alarm_description   = "Transformer queue backlog exceeds threshold - consumers may be slow"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 300
  statistic           = "Average"
  threshold           = 100
  treat_missing_data  = "notBreaching"

  dimensions = {
    QueueName = aws_sqs_queue.ethereum_transformer.name
  }

  alarm_actions = [aws_sns_topic.messaging_alarms.arn]
  ok_actions    = [aws_sns_topic.messaging_alarms.arn]

  tags = {
    Name       = "${local.prefix}-ethereum-transformer-backlog"
    Blockchain = "ethereum"
    Service    = "messaging"
    Severity   = "warning"
  }
}

resource "aws_cloudwatch_metric_alarm" "backup_queue_backlog" {
  alarm_name          = "${local.prefix}-ethereum-backup-backlog"
  alarm_description   = "Backup queue backlog exceeds threshold - consumers may be slow"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 300
  statistic           = "Average"
  threshold           = 100
  treat_missing_data  = "notBreaching"

  dimensions = {
    QueueName = aws_sqs_queue.ethereum_backup.name
  }

  alarm_actions = [aws_sns_topic.messaging_alarms.arn]
  ok_actions    = [aws_sns_topic.messaging_alarms.arn]

  tags = {
    Name       = "${local.prefix}-ethereum-backup-backlog"
    Blockchain = "ethereum"
    Service    = "messaging"
    Severity   = "warning"
  }
}

resource "aws_cloudwatch_metric_alarm" "oracle_price_queue_backlog" {
  alarm_name          = "${local.prefix}-ethereum-oracle-price-backlog"
  alarm_description   = "Oracle price queue backlog exceeds threshold - consumers may be slow"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  metric_name         = "ApproximateNumberOfMessagesVisible"
  namespace           = "AWS/SQS"
  period              = 300
  statistic           = "Average"
  threshold           = 100
  treat_missing_data  = "notBreaching"

  dimensions = {
    QueueName = aws_sqs_queue.ethereum_oracle_price.name
  }

  alarm_actions = [aws_sns_topic.messaging_alarms.arn]
  ok_actions    = [aws_sns_topic.messaging_alarms.arn]

  tags = {
    Name       = "${local.prefix}-ethereum-oracle-price-backlog"
    Blockchain = "ethereum"
    Service    = "messaging"
    Severity   = "warning"
  }
}

# Message Age Alarms - Warning: Old messages indicate processing issues
resource "aws_cloudwatch_metric_alarm" "transformer_message_age" {
  alarm_name          = "${local.prefix}-ethereum-transformer-message-age"
  alarm_description   = "Oldest message in transformer queue exceeds 5 minutes"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "ApproximateAgeOfOldestMessage"
  namespace           = "AWS/SQS"
  period              = 300
  statistic           = "Maximum"
  threshold           = 300 # 5 minutes in seconds
  treat_missing_data  = "notBreaching"

  dimensions = {
    QueueName = aws_sqs_queue.ethereum_transformer.name
  }

  alarm_actions = [aws_sns_topic.messaging_alarms.arn]
  ok_actions    = [aws_sns_topic.messaging_alarms.arn]

  tags = {
    Name       = "${local.prefix}-ethereum-transformer-message-age"
    Blockchain = "ethereum"
    Service    = "messaging"
    Severity   = "warning"
  }
}

resource "aws_cloudwatch_metric_alarm" "backup_message_age" {
  alarm_name          = "${local.prefix}-ethereum-backup-message-age"
  alarm_description   = "Oldest message in backup queue exceeds 5 minutes"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "ApproximateAgeOfOldestMessage"
  namespace           = "AWS/SQS"
  period              = 300
  statistic           = "Maximum"
  threshold           = 300 # 5 minutes in seconds
  treat_missing_data  = "notBreaching"

  dimensions = {
    QueueName = aws_sqs_queue.ethereum_backup.name
  }

  alarm_actions = [aws_sns_topic.messaging_alarms.arn]
  ok_actions    = [aws_sns_topic.messaging_alarms.arn]

  tags = {
    Name       = "${local.prefix}-ethereum-backup-message-age"
    Blockchain = "ethereum"
    Service    = "messaging"
    Severity   = "warning"
  }
}

resource "aws_cloudwatch_metric_alarm" "oracle_price_message_age" {
  alarm_name          = "${local.prefix}-ethereum-oracle-price-message-age"
  alarm_description   = "Oldest message in oracle price queue exceeds 5 minutes"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "ApproximateAgeOfOldestMessage"
  namespace           = "AWS/SQS"
  period              = 300
  statistic           = "Maximum"
  threshold           = 300 # 5 minutes in seconds
  treat_missing_data  = "notBreaching"

  dimensions = {
    QueueName = aws_sqs_queue.ethereum_oracle_price.name
  }

  alarm_actions = [aws_sns_topic.messaging_alarms.arn]
  ok_actions    = [aws_sns_topic.messaging_alarms.arn]

  tags = {
    Name       = "${local.prefix}-ethereum-oracle-price-message-age"
    Blockchain = "ethereum"
    Service    = "messaging"
    Severity   = "warning"
  }
}

# SNS Publish Failures - Critical: Messages not being delivered
resource "aws_cloudwatch_metric_alarm" "sns_publish_failures" {
  alarm_name          = "${local.prefix}-ethereum-sns-publish-failures"
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
    TopicName = aws_sns_topic.ethereum_blocks.name
  }

  alarm_actions = [aws_sns_topic.messaging_alarms.arn]
  ok_actions    = [aws_sns_topic.messaging_alarms.arn]

  tags = {
    Name       = "${local.prefix}-ethereum-sns-publish-failures"
    Blockchain = "ethereum"
    Service    = "messaging"
    Severity   = "critical"
  }
}

# -----------------------------------------------------------------------------
# CloudWatch Dashboard
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_dashboard" "messaging" {
  dashboard_name = "${local.prefix}-ethereum-messaging"

  dashboard_body = jsonencode({
    widgets = [
      # Row 1: SNS Topic Overview
      {
        type   = "text"
        x      = 0
        y      = 0
        width  = 24
        height = 1
        properties = {
          markdown = "# Ethereum Block Messaging - SNS/SQS Fan-out"
        }
      },
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
            ["AWS/SNS", "NumberOfMessagesPublished", "TopicName", aws_sns_topic.ethereum_blocks.name]
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
            ["AWS/SNS", "NumberOfNotificationsDelivered", "TopicName", aws_sns_topic.ethereum_blocks.name]
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
            ["AWS/SNS", "NumberOfNotificationsFailed", "TopicName", aws_sns_topic.ethereum_blocks.name]
          ]
          annotations = {
            horizontal = [
              {
                value = 1
                color = "#d62728"
                label = "Failure Threshold"
              }
            ]
          }
        }
      },

      # Row 2: Transformer Queue
      {
        type   = "text"
        x      = 0
        y      = 7
        width  = 24
        height = 1
        properties = {
          markdown = "## Transformer Queue"
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 8
        width  = 6
        height = 6
        properties = {
          title  = "Messages Visible (Backlog)"
          region = var.aws_region
          stat   = "Average"
          period = 60
          metrics = [
            ["AWS/SQS", "ApproximateNumberOfMessagesVisible", "QueueName", aws_sqs_queue.ethereum_transformer.name]
          ]
          annotations = {
            horizontal = [
              {
                value = 100
                color = "#ff7f0e"
                label = "Warning Threshold"
              }
            ]
          }
        }
      },
      {
        type   = "metric"
        x      = 6
        y      = 8
        width  = 6
        height = 6
        properties = {
          title  = "Messages In Flight"
          region = var.aws_region
          stat   = "Average"
          period = 60
          metrics = [
            ["AWS/SQS", "ApproximateNumberOfMessagesNotVisible", "QueueName", aws_sqs_queue.ethereum_transformer.name]
          ]
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 8
        width  = 6
        height = 6
        properties = {
          title  = "Age of Oldest Message (seconds)"
          region = var.aws_region
          stat   = "Maximum"
          period = 60
          metrics = [
            ["AWS/SQS", "ApproximateAgeOfOldestMessage", "QueueName", aws_sqs_queue.ethereum_transformer.name]
          ]
          annotations = {
            horizontal = [
              {
                value = 300
                color = "#ff7f0e"
                label = "5 min Warning"
              }
            ]
          }
        }
      },
      {
        type   = "metric"
        x      = 18
        y      = 8
        width  = 6
        height = 6
        properties = {
          title  = "Throughput (Received vs Deleted)"
          region = var.aws_region
          stat   = "Sum"
          period = 60
          metrics = [
            ["AWS/SQS", "NumberOfMessagesReceived", "QueueName", aws_sqs_queue.ethereum_transformer.name],
            [".", "NumberOfMessagesDeleted", ".", "."]
          ]
        }
      },

      # Row 3: Backup Queue
      {
        type   = "text"
        x      = 0
        y      = 14
        width  = 24
        height = 1
        properties = {
          markdown = "## Backup Queue"
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 15
        width  = 6
        height = 6
        properties = {
          title  = "Messages Visible (Backlog)"
          region = var.aws_region
          stat   = "Average"
          period = 60
          metrics = [
            ["AWS/SQS", "ApproximateNumberOfMessagesVisible", "QueueName", aws_sqs_queue.ethereum_backup.name]
          ]
          annotations = {
            horizontal = [
              {
                value = 100
                color = "#ff7f0e"
                label = "Warning Threshold"
              }
            ]
          }
        }
      },
      {
        type   = "metric"
        x      = 6
        y      = 15
        width  = 6
        height = 6
        properties = {
          title  = "Messages In Flight"
          region = var.aws_region
          stat   = "Average"
          period = 60
          metrics = [
            ["AWS/SQS", "ApproximateNumberOfMessagesNotVisible", "QueueName", aws_sqs_queue.ethereum_backup.name]
          ]
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 15
        width  = 6
        height = 6
        properties = {
          title  = "Age of Oldest Message (seconds)"
          region = var.aws_region
          stat   = "Maximum"
          period = 60
          metrics = [
            ["AWS/SQS", "ApproximateAgeOfOldestMessage", "QueueName", aws_sqs_queue.ethereum_backup.name]
          ]
        }
      },
      {
        type   = "metric"
        x      = 18
        y      = 15
        width  = 6
        height = 6
        properties = {
          title  = "Throughput (Received vs Deleted)"
          region = var.aws_region
          stat   = "Sum"
          period = 60
          metrics = [
            ["AWS/SQS", "NumberOfMessagesReceived", "QueueName", aws_sqs_queue.ethereum_backup.name],
            [".", "NumberOfMessagesDeleted", ".", "."]
          ]
        }
      },

      # Row 4: Oracle Price Queue
      {
        type   = "text"
        x      = 0
        y      = 21
        width  = 24
        height = 1
        properties = {
          markdown = "## Oracle Price Queue"
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 22
        width  = 6
        height = 6
        properties = {
          title  = "Messages Visible (Backlog)"
          region = var.aws_region
          stat   = "Average"
          period = 60
          metrics = [
            ["AWS/SQS", "ApproximateNumberOfMessagesVisible", "QueueName", aws_sqs_queue.ethereum_oracle_price.name]
          ]
          annotations = {
            horizontal = [
              {
                value = 100
                color = "#ff7f0e"
                label = "Warning Threshold"
              }
            ]
          }
        }
      },
      {
        type   = "metric"
        x      = 6
        y      = 22
        width  = 6
        height = 6
        properties = {
          title  = "Messages In Flight"
          region = var.aws_region
          stat   = "Average"
          period = 60
          metrics = [
            ["AWS/SQS", "ApproximateNumberOfMessagesNotVisible", "QueueName", aws_sqs_queue.ethereum_oracle_price.name]
          ]
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 22
        width  = 6
        height = 6
        properties = {
          title  = "Age of Oldest Message (seconds)"
          region = var.aws_region
          stat   = "Maximum"
          period = 60
          metrics = [
            ["AWS/SQS", "ApproximateAgeOfOldestMessage", "QueueName", aws_sqs_queue.ethereum_oracle_price.name]
          ]
          annotations = {
            horizontal = [
              {
                value = 300
                color = "#ff7f0e"
                label = "5 min Warning"
              }
            ]
          }
        }
      },
      {
        type   = "metric"
        x      = 18
        y      = 22
        width  = 6
        height = 6
        properties = {
          title  = "Throughput (Received vs Deleted)"
          region = var.aws_region
          stat   = "Sum"
          period = 60
          metrics = [
            ["AWS/SQS", "NumberOfMessagesReceived", "QueueName", aws_sqs_queue.ethereum_oracle_price.name],
            [".", "NumberOfMessagesDeleted", ".", "."]
          ]
        }
      },

      # Row 5: Dead Letter Queues (Critical)
      {
        type   = "text"
        x      = 0
        y      = 28
        width  = 24
        height = 1
        properties = {
          markdown = "## Dead Letter Queues (DLQ)"
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 29
        width  = 8
        height = 6
        properties = {
          title  = "Transformer DLQ - Messages"
          region = var.aws_region
          stat   = "Sum"
          period = 60
          metrics = [
            ["AWS/SQS", "ApproximateNumberOfMessagesVisible", "QueueName", aws_sqs_queue.ethereum_transformer_dlq.name]
          ]
          annotations = {
            horizontal = [
              {
                value = 1
                color = "#d62728"
                label = "Alert: Messages in DLQ"
              }
            ]
          }
        }
      },
      {
        type   = "metric"
        x      = 8
        y      = 29
        width  = 8
        height = 6
        properties = {
          title  = "Backup DLQ - Messages"
          region = var.aws_region
          stat   = "Sum"
          period = 60
          metrics = [
            ["AWS/SQS", "ApproximateNumberOfMessagesVisible", "QueueName", aws_sqs_queue.ethereum_backup_dlq.name]
          ]
          annotations = {
            horizontal = [
              {
                value = 1
                color = "#d62728"
                label = "Alert: Messages in DLQ"
              }
            ]
          }
        }
      },
      {
        type   = "metric"
        x      = 16
        y      = 29
        width  = 8
        height = 6
        properties = {
          title  = "Oracle Price DLQ - Messages"
          region = var.aws_region
          stat   = "Sum"
          period = 60
          metrics = [
            ["AWS/SQS", "ApproximateNumberOfMessagesVisible", "QueueName", aws_sqs_queue.ethereum_oracle_price_dlq.name]
          ]
          annotations = {
            horizontal = [
              {
                value = 1
                color = "#d62728"
                label = "Alert: Messages in DLQ"
              }
            ]
          }
        }
      },

      # Row 6: Alarm Status
      {
        type   = "text"
        x      = 0
        y      = 35
        width  = 24
        height = 1
        properties = {
          markdown = "## Alarm Status"
        }
      },
      {
        type   = "alarm"
        x      = 0
        y      = 36
        width  = 24
        height = 3
        properties = {
          title = "Messaging Alarms"
          alarms = [
            aws_cloudwatch_metric_alarm.transformer_dlq_messages.arn,
            aws_cloudwatch_metric_alarm.backup_dlq_messages.arn,
            aws_cloudwatch_metric_alarm.oracle_price_dlq_messages.arn,
            aws_cloudwatch_metric_alarm.transformer_queue_backlog.arn,
            aws_cloudwatch_metric_alarm.backup_queue_backlog.arn,
            aws_cloudwatch_metric_alarm.oracle_price_queue_backlog.arn,
            aws_cloudwatch_metric_alarm.transformer_message_age.arn,
            aws_cloudwatch_metric_alarm.backup_message_age.arn,
            aws_cloudwatch_metric_alarm.oracle_price_message_age.arn,
            aws_cloudwatch_metric_alarm.sns_publish_failures.arn
          ]
        }
      }
    ]
  })
}

# -----------------------------------------------------------------------------
# Outputs
# -----------------------------------------------------------------------------

output "messaging_alarms_topic_arn" {
  description = "ARN of the messaging alarms SNS topic"
  value       = aws_sns_topic.messaging_alarms.arn
}

output "messaging_dashboard_url" {
  description = "URL to the messaging CloudWatch dashboard"
  value       = "https://${var.aws_region}.console.aws.amazon.com/cloudwatch/home?region=${var.aws_region}#dashboards:name=${aws_cloudwatch_dashboard.messaging.dashboard_name}"
}
