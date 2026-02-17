# =============================================================================
# ElastiCache Redis Configuration
# =============================================================================

# -----------------------------------------------------------------------------
# Subnet Group
# -----------------------------------------------------------------------------

resource "aws_elasticache_subnet_group" "redis" {
  name       = "${local.name_prefix}-redis"
  subnet_ids = [var.isolated_subnet_id]

  tags = {
    Name       = "${local.name_prefix}-redis-subnet-group"
    Blockchain = var.chain_name
  }
}

# -----------------------------------------------------------------------------
# Parameter Group
# -----------------------------------------------------------------------------

resource "aws_elasticache_parameter_group" "redis" {
  name   = "${local.name_prefix}-redis-params"
  family = "valkey8"

  parameter {
    name  = "maxmemory-policy"
    value = "allkeys-lru"
  }

  parameter {
    name  = "maxmemory-samples"
    value = "10"
  }

  parameter {
    name  = "tcp-keepalive"
    value = "300"
  }

  parameter {
    name  = "timeout"
    value = "300"
  }

  parameter {
    name  = "notify-keyspace-events"
    value = "Ex"
  }

  tags = {
    Name       = "${local.name_prefix}-redis-params"
    Blockchain = var.chain_name
  }
}

# -----------------------------------------------------------------------------
# ElastiCache Redis Cluster
# -----------------------------------------------------------------------------

resource "aws_elasticache_replication_group" "redis" {
  replication_group_id = "${var.prefix}-${substr(var.chain_name, 0, 3)}-redis"
  description          = "${title(var.chain_name)} block cache for ${var.environment}"

  engine               = "valkey"
  engine_version       = var.redis_engine_version
  node_type            = var.redis_node_type
  parameter_group_name = aws_elasticache_parameter_group.redis.name

  num_cache_clusters = var.redis_num_cache_clusters
  port               = 6379

  subnet_group_name  = aws_elasticache_subnet_group.redis.name
  security_group_ids = [var.redis_sg_id]

  at_rest_encryption_enabled = false
  transit_encryption_enabled = var.redis_transit_encryption
  auth_token                 = var.redis_transit_encryption ? random_password.redis_auth[0].result : null

  automatic_failover_enabled = var.redis_num_cache_clusters > 1
  multi_az_enabled           = var.redis_num_cache_clusters > 1

  maintenance_window       = "sun:05:00-sun:06:00"
  snapshot_window          = var.redis_snapshot_retention > 0 ? "04:00-05:00" : null
  snapshot_retention_limit = var.redis_snapshot_retention

  auto_minor_version_upgrade = true
  apply_immediately          = var.environment != "sentinelprod"

  tags = {
    Name        = "${local.name_prefix}-redis"
    Environment = var.environment
    Blockchain  = var.chain_name
  }

  timeouts {
    create = "25m"
    delete = "25m"
    update = "80m"
  }

  lifecycle {
    ignore_changes = [engine_version]
  }
}

# -----------------------------------------------------------------------------
# Auth Token
# -----------------------------------------------------------------------------

resource "random_password" "redis_auth" {
  count   = var.redis_transit_encryption ? 1 : 0
  length  = 32
  special = false
}

# -----------------------------------------------------------------------------
# Secrets Manager
# -----------------------------------------------------------------------------

resource "aws_secretsmanager_secret" "redis" {
  name        = "${local.name_prefix}-redis"
  description = "${title(var.chain_name)} Redis connection details for ${var.environment}"

  recovery_window_in_days = var.environment == "sentineldev" ? 0 : 7

  tags = {
    Name       = "${local.name_prefix}-redis"
    Service    = "redis"
    Blockchain = var.chain_name
  }
}

resource "aws_secretsmanager_secret_version" "redis" {
  secret_id = aws_secretsmanager_secret.redis.id
  secret_string = jsonencode({
    host               = aws_elasticache_replication_group.redis.primary_endpoint_address
    port               = 6379
    auth_token         = var.redis_transit_encryption ? random_password.redis_auth[0].result : null
    transit_encryption = var.redis_transit_encryption
    reader_endpoint    = var.redis_num_cache_clusters > 1 ? aws_elasticache_replication_group.redis.reader_endpoint_address : null
  })

  lifecycle {
    ignore_changes = [secret_string]
  }
}

# -----------------------------------------------------------------------------
# IAM Policy for Reading Redis Secret
# -----------------------------------------------------------------------------

data "aws_iam_policy_document" "redis_secret_read" {
  statement {
    sid    = "Read${title(var.chain_name)}RedisSecret"
    effect = "Allow"

    actions = [
      "secretsmanager:GetSecretValue",
    ]

    resources = [
      aws_secretsmanager_secret.redis.arn,
    ]
  }
}

resource "aws_iam_policy" "redis_secret_read" {
  name        = "${local.name_prefix}-redis-secret-read"
  description = "Allows reading ${title(var.chain_name)} Redis credentials from Secrets Manager"
  policy      = data.aws_iam_policy_document.redis_secret_read.json
}

# =============================================================================
# Monitoring & Alarms
# =============================================================================

locals {
  redis_member_clusters     = sort(tolist(aws_elasticache_replication_group.redis.member_clusters))
  redis_primary_cluster_id  = local.redis_member_clusters[0]
  redis_replica_cluster_ids = slice(local.redis_member_clusters, 1, length(local.redis_member_clusters))
}

resource "aws_sns_topic" "redis_alarms" {
  name = "${local.name_prefix}-redis-alarms"

  tags = {
    Name    = "${local.name_prefix}-redis-alarms"
    Service = "redis"
  }
}

resource "aws_cloudwatch_metric_alarm" "redis_cpu_high" {
  alarm_name          = "${local.name_prefix}-redis-cpu-high"
  alarm_description   = "${title(var.chain_name)} Redis CPU utilization exceeds 75%"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  metric_name         = "CPUUtilization"
  namespace           = "AWS/ElastiCache"
  period              = 300
  statistic           = "Average"
  threshold           = 75
  treat_missing_data  = "breaching"

  dimensions = {
    CacheClusterId = local.redis_primary_cluster_id
  }

  alarm_actions = [aws_sns_topic.redis_alarms.arn]
  ok_actions    = [aws_sns_topic.redis_alarms.arn]

  tags = {
    Name    = "${local.name_prefix}-redis-cpu-high"
    Service = "redis"
  }
}

resource "aws_cloudwatch_metric_alarm" "redis_cpu_critical" {
  alarm_name          = "${local.name_prefix}-redis-cpu-critical"
  alarm_description   = "${title(var.chain_name)} Redis CPU utilization exceeds 90% - CRITICAL"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "CPUUtilization"
  namespace           = "AWS/ElastiCache"
  period              = 60
  statistic           = "Average"
  threshold           = 90
  treat_missing_data  = "breaching"

  dimensions = {
    CacheClusterId = local.redis_primary_cluster_id
  }

  alarm_actions = [aws_sns_topic.redis_alarms.arn]
  ok_actions    = [aws_sns_topic.redis_alarms.arn]

  tags = {
    Name    = "${local.name_prefix}-redis-cpu-critical"
    Service = "redis"
  }
}

resource "aws_cloudwatch_metric_alarm" "redis_memory_high" {
  alarm_name          = "${local.name_prefix}-redis-memory-high"
  alarm_description   = "${title(var.chain_name)} Redis memory usage exceeds 75%"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  metric_name         = "DatabaseMemoryUsagePercentage"
  namespace           = "AWS/ElastiCache"
  period              = 300
  statistic           = "Average"
  threshold           = 75
  treat_missing_data  = "breaching"

  dimensions = {
    CacheClusterId = local.redis_primary_cluster_id
  }

  alarm_actions = [aws_sns_topic.redis_alarms.arn]
  ok_actions    = [aws_sns_topic.redis_alarms.arn]

  tags = {
    Name    = "${local.name_prefix}-redis-memory-high"
    Service = "redis"
  }
}

resource "aws_cloudwatch_metric_alarm" "redis_memory_critical" {
  alarm_name          = "${local.name_prefix}-redis-memory-critical"
  alarm_description   = "${title(var.chain_name)} Redis memory usage exceeds 90% - evictions likely"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "DatabaseMemoryUsagePercentage"
  namespace           = "AWS/ElastiCache"
  period              = 60
  statistic           = "Average"
  threshold           = 90
  treat_missing_data  = "breaching"

  dimensions = {
    CacheClusterId = local.redis_primary_cluster_id
  }

  alarm_actions = [aws_sns_topic.redis_alarms.arn]
  ok_actions    = [aws_sns_topic.redis_alarms.arn]

  tags = {
    Name    = "${local.name_prefix}-redis-memory-critical"
    Service = "redis"
  }
}

resource "aws_cloudwatch_metric_alarm" "redis_evictions" {
  alarm_name          = "${local.name_prefix}-redis-evictions"
  alarm_description   = "${title(var.chain_name)} Redis is evicting keys - memory pressure detected"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "Evictions"
  namespace           = "AWS/ElastiCache"
  period              = 300
  statistic           = "Sum"
  threshold           = 100
  treat_missing_data  = "notBreaching"

  dimensions = {
    CacheClusterId = local.redis_primary_cluster_id
  }

  alarm_actions = [aws_sns_topic.redis_alarms.arn]
  ok_actions    = [aws_sns_topic.redis_alarms.arn]

  tags = {
    Name    = "${local.name_prefix}-redis-evictions"
    Service = "redis"
  }
}

resource "aws_cloudwatch_metric_alarm" "redis_cache_hit_rate_low" {
  alarm_name          = "${local.name_prefix}-redis-cache-hit-rate-low"
  alarm_description   = "${title(var.chain_name)} Redis cache hit rate below 80%"
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
        CacheClusterId = local.redis_primary_cluster_id
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
        CacheClusterId = local.redis_primary_cluster_id
      }
    }
  }

  alarm_actions = [aws_sns_topic.redis_alarms.arn]
  ok_actions    = [aws_sns_topic.redis_alarms.arn]

  tags = {
    Name    = "${local.name_prefix}-redis-cache-hit-rate-low"
    Service = "redis"
  }
}

resource "aws_cloudwatch_metric_alarm" "redis_connections_high" {
  alarm_name          = "${local.name_prefix}-redis-connections-high"
  alarm_description   = "${title(var.chain_name)} Redis connection count is high"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  metric_name         = "CurrConnections"
  namespace           = "AWS/ElastiCache"
  period              = 300
  statistic           = "Average"
  threshold           = 1000
  treat_missing_data  = "notBreaching"

  dimensions = {
    CacheClusterId = local.redis_primary_cluster_id
  }

  alarm_actions = [aws_sns_topic.redis_alarms.arn]
  ok_actions    = [aws_sns_topic.redis_alarms.arn]

  tags = {
    Name    = "${local.name_prefix}-redis-connections-high"
    Service = "redis"
  }
}

resource "aws_cloudwatch_metric_alarm" "redis_replication_lag" {
  count = var.redis_num_cache_clusters > 1 ? 1 : 0

  alarm_name          = "${local.name_prefix}-redis-replication-lag"
  alarm_description   = "${title(var.chain_name)} Redis replication lag exceeds 1 second"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  metric_name         = "ReplicationLag"
  namespace           = "AWS/ElastiCache"
  period              = 60
  statistic           = "Average"
  threshold           = 1
  treat_missing_data  = "notBreaching"

  dimensions = {
    ReplicationGroupId = aws_elasticache_replication_group.redis.id
  }

  alarm_actions = [aws_sns_topic.redis_alarms.arn]
  ok_actions    = [aws_sns_topic.redis_alarms.arn]

  tags = {
    Name    = "${local.name_prefix}-redis-replication-lag"
    Service = "redis"
  }
}

# -----------------------------------------------------------------------------
# CloudWatch Dashboard
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_dashboard" "redis" {
  dashboard_name = "${local.name_prefix}-redis"

  dashboard_body = jsonencode({
    widgets = [
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
            ["AWS/ElastiCache", "CPUUtilization", "CacheClusterId", local.redis_primary_cluster_id, { label = "Primary" }]
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
            ["AWS/ElastiCache", "DatabaseMemoryUsagePercentage", "CacheClusterId", local.redis_primary_cluster_id, { label = "Primary" }]
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
            ["AWS/ElastiCache", "CurrConnections", "CacheClusterId", local.redis_primary_cluster_id, { label = "Connections" }]
          ]
        }
      },
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
            ["AWS/ElastiCache", "CacheHits", "CacheClusterId", local.redis_primary_cluster_id, { label = "Hits", color = "#2ca02c" }],
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
            ["AWS/ElastiCache", "Evictions", "CacheClusterId", local.redis_primary_cluster_id, { label = "Evictions", color = "#d62728" }]
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
            ["AWS/ElastiCache", "GetTypeCmds", "CacheClusterId", local.redis_primary_cluster_id, { label = "GET commands" }],
            [".", "SetTypeCmds", ".", ".", { label = "SET commands" }]
          ]
          stat = "Sum"
        }
      },
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
            ["AWS/ElastiCache", "NetworkBytesIn", "CacheClusterId", local.redis_primary_cluster_id, { label = "Bytes In" }],
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
            ["AWS/ElastiCache", "StringBasedCmdsLatency", "CacheClusterId", local.redis_primary_cluster_id, { label = "Latency" }]
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
            ["AWS/ElastiCache", "NewConnections", "CacheClusterId", local.redis_primary_cluster_id, { label = "New Connections" }]
          ]
          stat = "Sum"
        }
      },
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
            ["AWS/ElastiCache", "CurrItems", "CacheClusterId", local.redis_primary_cluster_id, { label = "Current Items" }]
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
            ["AWS/ElastiCache", "Reclaimed", "CacheClusterId", local.redis_primary_cluster_id, { label = "Reclaimed" }]
          ]
          stat = "Sum"
        }
      }
    ]
  })
}
