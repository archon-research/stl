# =============================================================================
# Redis Monitoring & Alarms
# =============================================================================
# CloudWatch alarms and dashboard for ElastiCache Redis observability

# -----------------------------------------------------------------------------
# SNS Topic for Redis Alarms
# -----------------------------------------------------------------------------

resource "aws_sns_topic" "redis_alarms" {
  name = "${local.prefix}-redis-alarms"

  tags = {
    Name    = "${local.prefix}-redis-alarms"
    Service = "redis"
  }
}

# -----------------------------------------------------------------------------
# CPU Utilization Alarms
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_metric_alarm" "redis_cpu_high" {
  alarm_name          = "${local.prefix}-redis-cpu-high"
  alarm_description   = "Redis CPU utilization exceeds 75%"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  metric_name         = "CPUUtilization"
  namespace           = "AWS/ElastiCache"
  period              = 300
  statistic           = "Average"
  threshold           = 75
  treat_missing_data  = "breaching"

  dimensions = {
    CacheClusterId = "${aws_elasticache_replication_group.redis.id}-001"
  }

  alarm_actions = [aws_sns_topic.redis_alarms.arn]
  ok_actions    = [aws_sns_topic.redis_alarms.arn]

  tags = {
    Name    = "${local.prefix}-redis-cpu-high"
    Service = "redis"
  }
}

resource "aws_cloudwatch_metric_alarm" "redis_cpu_critical" {
  alarm_name          = "${local.prefix}-redis-cpu-critical"
  alarm_description   = "Redis CPU utilization exceeds 90% - CRITICAL"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "CPUUtilization"
  namespace           = "AWS/ElastiCache"
  period              = 60
  statistic           = "Average"
  threshold           = 90
  treat_missing_data  = "breaching"

  dimensions = {
    CacheClusterId = "${aws_elasticache_replication_group.redis.id}-001"
  }

  alarm_actions = [aws_sns_topic.redis_alarms.arn]
  ok_actions    = [aws_sns_topic.redis_alarms.arn]

  tags = {
    Name    = "${local.prefix}-redis-cpu-critical"
    Service = "redis"
  }
}

# -----------------------------------------------------------------------------
# Memory Utilization Alarms
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_metric_alarm" "redis_memory_high" {
  alarm_name          = "${local.prefix}-redis-memory-high"
  alarm_description   = "Redis memory usage exceeds 75%"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  metric_name         = "DatabaseMemoryUsagePercentage"
  namespace           = "AWS/ElastiCache"
  period              = 300
  statistic           = "Average"
  threshold           = 75
  treat_missing_data  = "breaching"

  dimensions = {
    CacheClusterId = "${aws_elasticache_replication_group.redis.id}-001"
  }

  alarm_actions = [aws_sns_topic.redis_alarms.arn]
  ok_actions    = [aws_sns_topic.redis_alarms.arn]

  tags = {
    Name    = "${local.prefix}-redis-memory-high"
    Service = "redis"
  }
}

resource "aws_cloudwatch_metric_alarm" "redis_memory_critical" {
  alarm_name          = "${local.prefix}-redis-memory-critical"
  alarm_description   = "Redis memory usage exceeds 90% - evictions likely"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "DatabaseMemoryUsagePercentage"
  namespace           = "AWS/ElastiCache"
  period              = 60
  statistic           = "Average"
  threshold           = 90
  treat_missing_data  = "breaching"

  dimensions = {
    CacheClusterId = "${aws_elasticache_replication_group.redis.id}-001"
  }

  alarm_actions = [aws_sns_topic.redis_alarms.arn]
  ok_actions    = [aws_sns_topic.redis_alarms.arn]

  tags = {
    Name    = "${local.prefix}-redis-memory-critical"
    Service = "redis"
  }
}

# -----------------------------------------------------------------------------
# Evictions Alarm
# -----------------------------------------------------------------------------
# Evictions indicate memory pressure - keys are being removed before expiry

resource "aws_cloudwatch_metric_alarm" "redis_evictions" {
  alarm_name          = "${local.prefix}-redis-evictions"
  alarm_description   = "Redis is evicting keys - memory pressure detected"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "Evictions"
  namespace           = "AWS/ElastiCache"
  period              = 300
  statistic           = "Sum"
  threshold           = 100
  treat_missing_data  = "notBreaching"

  dimensions = {
    CacheClusterId = "${aws_elasticache_replication_group.redis.id}-001"
  }

  alarm_actions = [aws_sns_topic.redis_alarms.arn]
  ok_actions    = [aws_sns_topic.redis_alarms.arn]

  tags = {
    Name    = "${local.prefix}-redis-evictions"
    Service = "redis"
  }
}

# -----------------------------------------------------------------------------
# Cache Hit Rate Alarm
# -----------------------------------------------------------------------------
# Low hit rate may indicate cache sizing issues or ineffective caching strategy

resource "aws_cloudwatch_metric_alarm" "redis_cache_hit_rate_low" {
  alarm_name          = "${local.prefix}-redis-cache-hit-rate-low"
  alarm_description   = "Redis cache hit rate below 80%"
  comparison_operator = "LessThanThreshold"
  evaluation_periods  = 6
  threshold           = 80
  treat_missing_data  = "notBreaching"

  metric_query {
    id          = "hit_rate"
    expression  = "(hits / (hits + misses)) * 100"
    label       = "Cache Hit Rate"
    return_data = true
  }

  metric_query {
    id = "hits"
    metric {
      metric_name = "CacheHits"
      namespace   = "AWS/ElastiCache"
      period      = 300
      stat        = "Sum"
      dimensions = {
        CacheClusterId = "${aws_elasticache_replication_group.redis.id}-001"
      }
    }
  }

  metric_query {
    id = "misses"
    metric {
      metric_name = "CacheMisses"
      namespace   = "AWS/ElastiCache"
      period      = 300
      stat        = "Sum"
      dimensions = {
        CacheClusterId = "${aws_elasticache_replication_group.redis.id}-001"
      }
    }
  }

  alarm_actions = [aws_sns_topic.redis_alarms.arn]
  ok_actions    = [aws_sns_topic.redis_alarms.arn]

  tags = {
    Name    = "${local.prefix}-redis-cache-hit-rate-low"
    Service = "redis"
  }
}

# -----------------------------------------------------------------------------
# Connection Alarms
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_metric_alarm" "redis_connections_high" {
  alarm_name          = "${local.prefix}-redis-connections-high"
  alarm_description   = "Redis connection count is high"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  metric_name         = "CurrConnections"
  namespace           = "AWS/ElastiCache"
  period              = 300
  statistic           = "Average"
  threshold           = 1000
  treat_missing_data  = "notBreaching"

  dimensions = {
    CacheClusterId = "${aws_elasticache_replication_group.redis.id}-001"
  }

  alarm_actions = [aws_sns_topic.redis_alarms.arn]
  ok_actions    = [aws_sns_topic.redis_alarms.arn]

  tags = {
    Name    = "${local.prefix}-redis-connections-high"
    Service = "redis"
  }
}

# -----------------------------------------------------------------------------
# Replication Lag Alarm (for multi-node clusters)
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_metric_alarm" "redis_replication_lag" {
  count = var.redis_num_cache_clusters > 1 ? 1 : 0

  alarm_name          = "${local.prefix}-redis-replication-lag"
  alarm_description   = "Redis replication lag exceeds 1 second"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  metric_name         = "ReplicationLag"
  namespace           = "AWS/ElastiCache"
  period              = 60
  statistic           = "Average"
  threshold           = 1
  treat_missing_data  = "notBreaching"

  dimensions = {
    CacheClusterId = "${aws_elasticache_replication_group.redis.id}-002"
  }

  alarm_actions = [aws_sns_topic.redis_alarms.arn]
  ok_actions    = [aws_sns_topic.redis_alarms.arn]

  tags = {
    Name    = "${local.prefix}-redis-replication-lag"
    Service = "redis"
  }
}

# -----------------------------------------------------------------------------
# CloudWatch Dashboard
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_dashboard" "redis" {
  dashboard_name = "${local.prefix}-redis"

  dashboard_body = jsonencode({
    widgets = [
      # Row 1: Overview metrics
      {
        type   = "metric"
        x      = 0
        y      = 0
        width  = 8
        height = 6
        properties = {
          title  = "CPU Utilization"
          region = var.aws_region
          metrics = [
            ["AWS/ElastiCache", "CPUUtilization", "CacheClusterId", "${aws_elasticache_replication_group.redis.id}-001", { label = "Primary" }]
          ]
          yAxis = { left = { min = 0, max = 100 } }
          annotations = {
            horizontal = [
              { value = 75, label = "Warning", color = "#ff7f0e" },
              { value = 90, label = "Critical", color = "#d62728" }
            ]
          }
        }
      },
      {
        type   = "metric"
        x      = 8
        y      = 0
        width  = 8
        height = 6
        properties = {
          title  = "Memory Usage %"
          region = var.aws_region
          metrics = [
            ["AWS/ElastiCache", "DatabaseMemoryUsagePercentage", "CacheClusterId", "${aws_elasticache_replication_group.redis.id}-001", { label = "Primary" }]
          ]
          yAxis = { left = { min = 0, max = 100 } }
          annotations = {
            horizontal = [
              { value = 75, label = "Warning", color = "#ff7f0e" },
              { value = 90, label = "Critical", color = "#d62728" }
            ]
          }
        }
      },
      {
        type   = "metric"
        x      = 16
        y      = 0
        width  = 8
        height = 6
        properties = {
          title  = "Current Connections"
          region = var.aws_region
          metrics = [
            ["AWS/ElastiCache", "CurrConnections", "CacheClusterId", "${aws_elasticache_replication_group.redis.id}-001", { label = "Connections" }]
          ]
        }
      },
      # Row 2: Cache performance
      {
        type   = "metric"
        x      = 0
        y      = 6
        width  = 8
        height = 6
        properties = {
          title  = "Cache Hits vs Misses"
          region = var.aws_region
          metrics = [
            ["AWS/ElastiCache", "CacheHits", "CacheClusterId", "${aws_elasticache_replication_group.redis.id}-001", { label = "Hits", color = "#2ca02c" }],
            [".", "CacheMisses", ".", ".", { label = "Misses", color = "#d62728" }]
          ]
          stat = "Sum"
        }
      },
      {
        type   = "metric"
        x      = 8
        y      = 6
        width  = 8
        height = 6
        properties = {
          title  = "Evictions"
          region = var.aws_region
          metrics = [
            ["AWS/ElastiCache", "Evictions", "CacheClusterId", "${aws_elasticache_replication_group.redis.id}-001", { label = "Evictions", color = "#d62728" }]
          ]
          stat = "Sum"
        }
      },
      {
        type   = "metric"
        x      = 16
        y      = 6
        width  = 8
        height = 6
        properties = {
          title  = "Commands Processed"
          region = var.aws_region
          metrics = [
            ["AWS/ElastiCache", "GetTypeCmds", "CacheClusterId", "${aws_elasticache_replication_group.redis.id}-001", { label = "GET commands" }],
            [".", "SetTypeCmds", ".", ".", { label = "SET commands" }]
          ]
          stat = "Sum"
        }
      },
      # Row 3: Network and latency
      {
        type   = "metric"
        x      = 0
        y      = 12
        width  = 8
        height = 6
        properties = {
          title  = "Network Throughput"
          region = var.aws_region
          metrics = [
            ["AWS/ElastiCache", "NetworkBytesIn", "CacheClusterId", "${aws_elasticache_replication_group.redis.id}-001", { label = "Bytes In" }],
            [".", "NetworkBytesOut", ".", ".", { label = "Bytes Out" }]
          ]
          stat = "Sum"
        }
      },
      {
        type   = "metric"
        x      = 8
        y      = 12
        width  = 8
        height = 6
        properties = {
          title  = "String-Based Command Latency (μs)"
          region = var.aws_region
          metrics = [
            ["AWS/ElastiCache", "StringBasedCmdsLatency", "CacheClusterId", "${aws_elasticache_replication_group.redis.id}-001", { label = "Latency" }]
          ]
        }
      },
      {
        type   = "metric"
        x      = 16
        y      = 12
        width  = 8
        height = 6
        properties = {
          title  = "New Connections/sec"
          region = var.aws_region
          metrics = [
            ["AWS/ElastiCache", "NewConnections", "CacheClusterId", "${aws_elasticache_replication_group.redis.id}-001", { label = "New Connections" }]
          ]
          stat = "Sum"
        }
      },
      # Row 4: Items and reclaimed
      {
        type   = "metric"
        x      = 0
        y      = 18
        width  = 12
        height = 6
        properties = {
          title  = "Cached Items"
          region = var.aws_region
          metrics = [
            ["AWS/ElastiCache", "CurrItems", "CacheClusterId", "${aws_elasticache_replication_group.redis.id}-001", { label = "Current Items" }]
          ]
        }
      },
      {
        type   = "metric"
        x      = 12
        y      = 18
        width  = 12
        height = 6
        properties = {
          title  = "Reclaimed (Expired Keys)"
          region = var.aws_region
          metrics = [
            ["AWS/ElastiCache", "Reclaimed", "CacheClusterId", "${aws_elasticache_replication_group.redis.id}-001", { label = "Reclaimed" }]
          ]
          stat = "Sum"
        }
      }
    ]
  })
}

# -----------------------------------------------------------------------------
# Outputs
# -----------------------------------------------------------------------------

output "redis_alarms_topic_arn" {
  description = "SNS topic ARN for Redis alarms"
  value       = aws_sns_topic.redis_alarms.arn
}

output "redis_dashboard_url" {
  description = "CloudWatch dashboard URL for Redis monitoring"
  value       = "https://${var.aws_region}.console.aws.amazon.com/cloudwatch/home?region=${var.aws_region}#dashboards:name=${local.prefix}-redis"
}
