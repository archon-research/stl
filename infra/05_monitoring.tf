# =============================================================================
# Networking Monitoring
# =============================================================================
# VPC Flow Logs, CloudWatch Alarms, and Dashboard for networking resources

# -----------------------------------------------------------------------------
# VPC Flow Logs to S3
# -----------------------------------------------------------------------------
# Cost-effective storage with Athena query capability

resource "aws_s3_bucket" "flow_logs" {
  bucket = "${local.prefix_lowercase}-vpc-flow-logs${local.resource_suffix}"
  
  # Only allow force_destroy for dev - protect staging/prod logs
  force_destroy = var.environment == "sentineldev"

  tags = {
    Name = "${local.prefix}-vpc-flow-logs"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "flow_logs" {
  bucket = aws_s3_bucket.flow_logs.id

  rule {
    id     = "expire-old-logs"
    status = "Enabled"

    expiration {
      days = 30
    }

    transition {
      days          = 7
      storage_class = "INTELLIGENT_TIERING"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "flow_logs" {
  bucket = aws_s3_bucket.flow_logs.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Bucket policy required for VPC Flow Logs to write to S3
# Note: aws_caller_identity data source is defined in 00_validations.tf

data "aws_iam_policy_document" "flow_logs_bucket" {
  statement {
    sid    = "AWSLogDeliveryWrite"
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["delivery.logs.amazonaws.com"]
    }

    actions   = ["s3:PutObject"]
    resources = ["${aws_s3_bucket.flow_logs.arn}/AWSLogs/${data.aws_caller_identity.current.account_id}/*"]

    condition {
      test     = "StringEquals"
      variable = "s3:x-amz-acl"
      values   = ["bucket-owner-full-control"]
    }

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }

    condition {
      test     = "ArnLike"
      variable = "aws:SourceArn"
      values   = ["arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:*"]
    }
  }

  statement {
    sid    = "AWSLogDeliveryAclCheck"
    effect = "Allow"

    principals {
      type        = "Service"
      identifiers = ["delivery.logs.amazonaws.com"]
    }

    actions   = ["s3:GetBucketAcl"]
    resources = [aws_s3_bucket.flow_logs.arn]

    condition {
      test     = "StringEquals"
      variable = "aws:SourceAccount"
      values   = [data.aws_caller_identity.current.account_id]
    }

    condition {
      test     = "ArnLike"
      variable = "aws:SourceArn"
      values   = ["arn:aws:logs:${var.aws_region}:${data.aws_caller_identity.current.account_id}:*"]
    }
  }
}

resource "aws_s3_bucket_policy" "flow_logs" {
  bucket = aws_s3_bucket.flow_logs.id
  policy = data.aws_iam_policy_document.flow_logs_bucket.json

  depends_on = [aws_s3_bucket_public_access_block.flow_logs]
}

resource "aws_flow_log" "main" {
  vpc_id               = aws_vpc.main.id
  log_destination      = aws_s3_bucket.flow_logs.arn
  log_destination_type = "s3"
  traffic_type         = "ALL"

  destination_options {
    file_format        = "parquet"
    per_hour_partition = true
  }

  tags = {
    Name = "${local.prefix}-vpc-flow-log"
  }
}

# -----------------------------------------------------------------------------
# SNS Topic for Alarms
# -----------------------------------------------------------------------------

resource "aws_sns_topic" "networking_alarms" {
  name = "${local.prefix}-networking-alarms"

  tags = {
    Name = "${local.prefix}-networking-alarms"
  }
}

# -----------------------------------------------------------------------------
# NAT Gateway Alarms
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_metric_alarm" "nat_packets_dropped" {
  alarm_name          = "${local.prefix}-nat-packets-dropped"
  alarm_description   = "NAT Gateway is dropping packets - potential overload"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "PacketsDropCount"
  namespace           = "AWS/NATGateway"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  treat_missing_data  = "notBreaching"

  dimensions = {
    NatGatewayId = aws_nat_gateway.main.id
  }

  alarm_actions = [aws_sns_topic.networking_alarms.arn]
  ok_actions    = [aws_sns_topic.networking_alarms.arn]

  tags = {
    Name = "${local.prefix}-nat-packets-dropped"
  }
}

resource "aws_cloudwatch_metric_alarm" "nat_port_allocation_error" {
  alarm_name          = "${local.prefix}-nat-port-exhaustion"
  alarm_description   = "NAT Gateway port exhaustion - increase NAT capacity"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 1
  metric_name         = "ErrorPortAllocation"
  namespace           = "AWS/NATGateway"
  period              = 300
  statistic           = "Sum"
  threshold           = 0
  treat_missing_data  = "notBreaching"

  dimensions = {
    NatGatewayId = aws_nat_gateway.main.id
  }

  alarm_actions = [aws_sns_topic.networking_alarms.arn]
  ok_actions    = [aws_sns_topic.networking_alarms.arn]

  tags = {
    Name = "${local.prefix}-nat-port-exhaustion"
  }
}

resource "aws_cloudwatch_metric_alarm" "nat_bandwidth_high" {
  alarm_name          = "${local.prefix}-nat-bandwidth-high"
  alarm_description   = "NAT Gateway bandwidth exceeding 80% of limit (45Gbps)"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  metric_name         = "BytesOutToDestination"
  namespace           = "AWS/NATGateway"
  period              = 300
  statistic           = "Sum"
  # 45 Gbps * 80% * 300 seconds / 8 bits = ~1.35 TB per 5 min period
  threshold          = 1350000000000
  treat_missing_data = "notBreaching"

  dimensions = {
    NatGatewayId = aws_nat_gateway.main.id
  }

  alarm_actions = [aws_sns_topic.networking_alarms.arn]

  tags = {
    Name = "${local.prefix}-nat-bandwidth-high"
  }
}

# -----------------------------------------------------------------------------
# CloudWatch Dashboard
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_dashboard" "networking" {
  dashboard_name = "${local.prefix}-networking"

  dashboard_body = jsonencode({
    widgets = [
      # Row 1: NAT Gateway Overview
      {
        type   = "text"
        x      = 0
        y      = 0
        width  = 24
        height = 1
        properties = {
          markdown = "# NAT Gateway Metrics"
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 1
        width  = 8
        height = 6
        properties = {
          title  = "NAT Active Connections"
          region = var.aws_region
          metrics = [
            ["AWS/NATGateway", "ActiveConnectionCount", "NatGatewayId", aws_nat_gateway.main.id]
          ]
          stat   = "Maximum"
          period = 60
        }
      },
      {
        type   = "metric"
        x      = 8
        y      = 1
        width  = 8
        height = 6
        properties = {
          title  = "NAT Data Transfer"
          region = var.aws_region
          metrics = [
            ["AWS/NATGateway", "BytesOutToDestination", "NatGatewayId", aws_nat_gateway.main.id, { label = "Bytes Out" }],
            [".", "BytesInFromDestination", ".", ".", { label = "Bytes In" }]
          ]
          stat   = "Sum"
          period = 300
        }
      },
      {
        type   = "metric"
        x      = 16
        y      = 1
        width  = 8
        height = 6
        properties = {
          title  = "NAT Errors (Packets Dropped / Port Exhaustion)"
          region = var.aws_region
          metrics = [
            ["AWS/NATGateway", "PacketsDropCount", "NatGatewayId", aws_nat_gateway.main.id, { label = "Packets Dropped", color = "#d62728" }],
            [".", "ErrorPortAllocation", ".", ".", { label = "Port Allocation Errors", color = "#ff7f0e" }]
          ]
          stat   = "Sum"
          period = 300
        }
      },

      # Row 2: ALB placeholder (metrics available after ALB is created)
      {
        type   = "text"
        x      = 0
        y      = 7
        width  = 24
        height = 1
        properties = {
          markdown = "# Application Load Balancer Metrics (Available after ALB deployment)"
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 8
        width  = 6
        height = 6
        properties = {
          title  = "ALB Request Count"
          region = var.aws_region
          metrics = [
            ["AWS/ApplicationELB", "RequestCount", "LoadBalancer", "${local.prefix}-alb", { stat = "Sum" }]
          ]
          period = 60
          view   = "timeSeries"
        }
      },
      {
        type   = "metric"
        x      = 6
        y      = 8
        width  = 6
        height = 6
        properties = {
          title  = "ALB Response Time"
          region = var.aws_region
          metrics = [
            ["AWS/ApplicationELB", "TargetResponseTime", "LoadBalancer", "${local.prefix}-alb", { stat = "p50", label = "p50" }],
            ["...", { stat = "p95", label = "p95" }],
            ["...", { stat = "p99", label = "p99" }]
          ]
          period = 60
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 8
        width  = 6
        height = 6
        properties = {
          title  = "ALB HTTP Errors"
          region = var.aws_region
          metrics = [
            ["AWS/ApplicationELB", "HTTPCode_ELB_4XX_Count", "LoadBalancer", "${local.prefix}-alb", { label = "4XX", color = "#ff7f0e" }],
            [".", "HTTPCode_ELB_5XX_Count", ".", ".", { label = "5XX", color = "#d62728" }]
          ]
          stat   = "Sum"
          period = 60
        }
      },
      {
        type   = "metric"
        x      = 18
        y      = 8
        width  = 6
        height = 6
        properties = {
          title  = "ALB Healthy/Unhealthy Hosts"
          region = var.aws_region
          metrics = [
            ["AWS/ApplicationELB", "HealthyHostCount", "LoadBalancer", "${local.prefix}-alb", { label = "Healthy", color = "#2ca02c" }],
            [".", "UnHealthyHostCount", ".", ".", { label = "Unhealthy", color = "#d62728" }]
          ]
          stat   = "Average"
          period = 60
        }
      },

      # Row 3: VPC Flow Logs Summary
      {
        type   = "text"
        x      = 0
        y      = 14
        width  = 24
        height = 1
        properties = {
          markdown = "# VPC Network Traffic"
        }
      },
      {
        type   = "metric"
        x      = 0
        y      = 15
        width  = 12
        height = 6
        properties = {
          title  = "NAT Connection Attempts"
          region = var.aws_region
          metrics = [
            ["AWS/NATGateway", "ConnectionAttemptCount", "NatGatewayId", aws_nat_gateway.main.id]
          ]
          stat   = "Sum"
          period = 300
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 15
        width  = 12
        height = 6
        properties = {
          title  = "NAT Idle Timeout Connections"
          region = var.aws_region
          metrics = [
            ["AWS/NATGateway", "IdleTimeoutCount", "NatGatewayId", aws_nat_gateway.main.id]
          ]
          stat   = "Sum"
          period = 300
        }
      }
    ]
  })
}

# -----------------------------------------------------------------------------
# Outputs
# -----------------------------------------------------------------------------

output "flow_logs_bucket" {
  description = "S3 bucket for VPC Flow Logs"
  value       = aws_s3_bucket.flow_logs.id
}

output "networking_alarms_topic_arn" {
  description = "SNS topic ARN for networking alarms"
  value       = aws_sns_topic.networking_alarms.arn
}

output "networking_dashboard_url" {
  description = "CloudWatch dashboard URL"
  value       = "https://${var.aws_region}.console.aws.amazon.com/cloudwatch/home?region=${var.aws_region}#dashboards:name=${aws_cloudwatch_dashboard.networking.dashboard_name}"
}
