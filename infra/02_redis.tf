# =============================================================================
# ElastiCache Redis Configuration - Ethereum
# =============================================================================
# Redis cache for Ethereum blockchain:
# - Block cache (watcher/worker coordination)
# Note: Each blockchain gets its own cache cluster

# -----------------------------------------------------------------------------
# Subnet Group
# -----------------------------------------------------------------------------
# Redis runs in the isolated subnet (no internet access)

resource "aws_elasticache_subnet_group" "ethereum_redis" {
  name       = "${local.prefix}-ethereum-redis"
  subnet_ids = [aws_subnet.isolated.id]

  tags = {
    Name       = "${local.prefix}-ethereum-redis-subnet-group"
    Blockchain = "ethereum"
  }
}

# -----------------------------------------------------------------------------
# Parameter Group
# -----------------------------------------------------------------------------
# Custom parameters for our use case

resource "aws_elasticache_parameter_group" "ethereum_redis" {
  name   = "${local.prefix}-ethereum-redis-params"
  family = "valkey8"

  # Eviction policy: evict least recently used keys when memory is full
  # allkeys-lru is safer than volatile-lru for caches with ~4 hour TTLs
  parameter {
    name  = "maxmemory-policy"
    value = "allkeys-lru"
  }

  # Increase eviction accuracy (default 5, higher = more accurate under load)
  parameter {
    name  = "maxmemory-samples"
    value = "10"
  }

  # TCP keepalive for connection health (seconds)
  parameter {
    name  = "tcp-keepalive"
    value = "300"
  }

  # Close idle connections after 5 minutes (0 = never)
  parameter {
    name  = "timeout"
    value = "300"
  }

  # Enable keyspace notifications for expiry events (useful for distributed locks)
  parameter {
    name  = "notify-keyspace-events"
    value = "Ex"
  }

  tags = {
    Name       = "${local.prefix}-ethereum-redis-params"
    Blockchain = "ethereum"
  }
}

# -----------------------------------------------------------------------------
# ElastiCache Redis Cluster - Ethereum
# -----------------------------------------------------------------------------

resource "aws_elasticache_replication_group" "ethereum_redis" {
  replication_group_id = "${local.prefix}-eth-redis"
  description          = "Ethereum block cache for ${var.environment}"

  # Engine configuration (Valkey - AWS's Redis-compatible fork)
  engine               = "valkey"
  engine_version       = var.redis_engine_version
  node_type            = var.redis_node_type
  parameter_group_name = aws_elasticache_parameter_group.ethereum_redis.name

  # Cluster configuration
  num_cache_clusters = var.redis_num_cache_clusters
  port               = 6379

  # Network configuration
  subnet_group_name  = aws_elasticache_subnet_group.ethereum_redis.name
  security_group_ids = [aws_security_group.redis.id]

  # Encryption
  at_rest_encryption_enabled = false
  transit_encryption_enabled = var.redis_transit_encryption
  auth_token                 = var.redis_transit_encryption ? random_password.ethereum_redis_auth[0].result : null

  # High availability (only for multi-node)
  automatic_failover_enabled = var.redis_num_cache_clusters > 1
  multi_az_enabled           = var.redis_num_cache_clusters > 1

  # Maintenance
  maintenance_window       = "sun:05:00-sun:06:00"
  snapshot_window          = var.redis_snapshot_retention > 0 ? "04:00-05:00" : null
  snapshot_retention_limit = var.redis_snapshot_retention

  # Auto minor version upgrades
  auto_minor_version_upgrade = true

  # Apply changes immediately in staging, during maintenance window in prod
  apply_immediately = var.environment != "sentinelprod"

  tags = {
    Name        = "${local.prefix}-ethereum-redis"
    Environment = var.environment
    Blockchain  = "ethereum"
  }

  timeouts {
    create = "25m"
    delete = "25m"
    update = "80m"
  }

  lifecycle {
    ignore_changes = [
      # Don't replace cluster if engine version is upgraded outside Terraform
      engine_version,
    ]
  }
}

# -----------------------------------------------------------------------------
# Auth Token (password) for transit encryption
# -----------------------------------------------------------------------------

resource "random_password" "ethereum_redis_auth" {
  count   = var.redis_transit_encryption ? 1 : 0
  length  = 32
  special = false # ElastiCache auth token doesn't support all special chars
}

# -----------------------------------------------------------------------------
# Store Ethereum Redis credentials in Secrets Manager
# -----------------------------------------------------------------------------

resource "aws_secretsmanager_secret" "ethereum_redis" {
  name        = "${local.prefix}-ethereum-redis"
  description = "Ethereum Redis connection details for ${var.environment}"

  recovery_window_in_days = var.environment == "sentineldev" ? 0 : 7

  tags = {
    Name       = "${local.prefix}-ethereum-redis"
    Service    = "redis"
    Blockchain = "ethereum"
  }
}

resource "aws_secretsmanager_secret_version" "ethereum_redis" {
  secret_id = aws_secretsmanager_secret.ethereum_redis.id
  secret_string = jsonencode({
    host               = aws_elasticache_replication_group.ethereum_redis.primary_endpoint_address
    port               = 6379
    auth_token         = var.redis_transit_encryption ? random_password.ethereum_redis_auth[0].result : null
    transit_encryption = var.redis_transit_encryption
    reader_endpoint    = var.redis_num_cache_clusters > 1 ? aws_elasticache_replication_group.ethereum_redis.reader_endpoint_address : null
  })

  lifecycle {
    # Ignore changes to prevent Terraform from overwriting credentials if:
    # 1. Auth token is rotated manually outside of Terraform
    # 2. Endpoint addresses change during maintenance/failover
    # To force an update, use: tofu taint aws_secretsmanager_secret_version.ethereum_redis
    ignore_changes = [secret_string]
  }
}

# -----------------------------------------------------------------------------
# IAM Policy for reading Redis secret
# -----------------------------------------------------------------------------

data "aws_iam_policy_document" "ethereum_redis_secret_read" {
  statement {
    sid    = "ReadEthereumRedisSecret"
    effect = "Allow"

    actions = [
      "secretsmanager:GetSecretValue",
    ]

    resources = [
      aws_secretsmanager_secret.ethereum_redis.arn,
    ]
  }
}

resource "aws_iam_policy" "ethereum_redis_secret_read" {
  name        = "${local.prefix}-ethereum-redis-secret-read"
  description = "Allows reading Ethereum Redis credentials from Secrets Manager"
  policy      = data.aws_iam_policy_document.ethereum_redis_secret_read.json
}

resource "aws_iam_role_policy_attachment" "ethereum_redis_secret_access" {
  role       = aws_iam_role.ethereum_raw_data_access.name
  policy_arn = aws_iam_policy.ethereum_redis_secret_read.arn
}

# =============================================================================
# Section 2: Monitoring & Alarms
# =============================================================================
# CloudWatch alarms and dashboard for Ethereum ElastiCache Redis observability

# Local Variables for Monitoring
# Use member_clusters attribute for robustness - this dynamically resolves
# cluster IDs after creation, surviving failovers and ID changes
locals {
  # Convert set to sorted list for consistent indexing
  ethereum_redis_member_clusters = sort(tolist(aws_elasticache_replication_group.ethereum_redis.member_clusters))
  # Primary cluster is always the first member (alphabetically sorted, -001 comes first)
  ethereum_redis_primary_cluster_id = local.ethereum_redis_member_clusters[0]
  # All replica cluster IDs (everything except the primary) for replication lag monitoring
  ethereum_redis_replica_cluster_ids = slice(local.ethereum_redis_member_clusters, 1, length(local.ethereum_redis_member_clusters))
}

# SNS Topic for Redis Alarms
resource "aws_sns_topic" "ethereum_redis_alarms" {
  name = "${local.prefix}-ethereum-redis-alarms"

  tags = {
    Name    = "${local.prefix}-ethereum-redis-alarms"
    Service = "redis"
  }
}

# CPU Utilization - High
resource "aws_cloudwatch_metric_alarm" "ethereum_redis_cpu_high" {
  alarm_name          = "${local.prefix}-ethereum-redis-cpu-high"
  alarm_description   = "Ethereum Redis CPU utilization exceeds 75%"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  metric_name         = "CPUUtilization"
  namespace           = "AWS/ElastiCache"
  period              = 300
  statistic           = "Average"
  threshold           = 75
  treat_missing_data  = "breaching"

  dimensions = {
    CacheClusterId = local.ethereum_redis_primary_cluster_id
  }

  alarm_actions = [aws_sns_topic.ethereum_redis_alarms.arn]
  ok_actions    = [aws_sns_topic.ethereum_redis_alarms.arn]

  tags = {
    Name    = "${local.prefix}-ethereum-redis-cpu-high"
    Service = "redis"
  }
}

# CPU Utilization - Critical
resource "aws_cloudwatch_metric_alarm" "ethereum_redis_cpu_critical" {
  alarm_name          = "${local.prefix}-ethereum-redis-cpu-critical"
  alarm_description   = "Ethereum Redis CPU utilization exceeds 90% - CRITICAL"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "CPUUtilization"
  namespace           = "AWS/ElastiCache"
  period              = 60
  statistic           = "Average"
  threshold           = 90
  treat_missing_data  = "breaching"

  dimensions = {
    CacheClusterId = local.ethereum_redis_primary_cluster_id
  }

  alarm_actions = [aws_sns_topic.ethereum_redis_alarms.arn]
  ok_actions    = [aws_sns_topic.ethereum_redis_alarms.arn]

  tags = {
    Name    = "${local.prefix}-ethereum-redis-cpu-critical"
    Service = "redis"
  }
}

# Memory Utilization - High
resource "aws_cloudwatch_metric_alarm" "ethereum_redis_memory_high" {
  alarm_name          = "${local.prefix}-ethereum-redis-memory-high"
  alarm_description   = "Ethereum Redis memory usage exceeds 75%"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  metric_name         = "DatabaseMemoryUsagePercentage"
  namespace           = "AWS/ElastiCache"
  period              = 300
  statistic           = "Average"
  threshold           = 75
  treat_missing_data  = "breaching"

  dimensions = {
    CacheClusterId = local.ethereum_redis_primary_cluster_id
  }

  alarm_actions = [aws_sns_topic.ethereum_redis_alarms.arn]
  ok_actions    = [aws_sns_topic.ethereum_redis_alarms.arn]

  tags = {
    Name    = "${local.prefix}-ethereum-redis-memory-high"
    Service = "redis"
  }
}

# Memory Utilization - Critical
resource "aws_cloudwatch_metric_alarm" "ethereum_redis_memory_critical" {
  alarm_name          = "${local.prefix}-ethereum-redis-memory-critical"
  alarm_description   = "Ethereum Redis memory usage exceeds 90% - evictions likely"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "DatabaseMemoryUsagePercentage"
  namespace           = "AWS/ElastiCache"
  period              = 60
  statistic           = "Average"
  threshold           = 90
  treat_missing_data  = "breaching"

  dimensions = {
    CacheClusterId = local.ethereum_redis_primary_cluster_id
  }

  alarm_actions = [aws_sns_topic.ethereum_redis_alarms.arn]
  ok_actions    = [aws_sns_topic.ethereum_redis_alarms.arn]

  tags = {
    Name    = "${local.prefix}-ethereum-redis-memory-critical"
    Service = "redis"
  }
}

# Evictions - Indicates memory pressure
resource "aws_cloudwatch_metric_alarm" "ethereum_redis_evictions" {
  alarm_name          = "${local.prefix}-ethereum-redis-evictions"
  alarm_description   = "Ethereum Redis is evicting keys - memory pressure detected"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 2
  metric_name         = "Evictions"
  namespace           = "AWS/ElastiCache"
  period              = 300
  statistic           = "Sum"
  threshold           = 100
  treat_missing_data  = "notBreaching"

  dimensions = {
    CacheClusterId = local.ethereum_redis_primary_cluster_id
  }

  alarm_actions = [aws_sns_topic.ethereum_redis_alarms.arn]
  ok_actions    = [aws_sns_topic.ethereum_redis_alarms.arn]

  tags = {
    Name    = "${local.prefix}-ethereum-redis-evictions"
    Service = "redis"
  }
}

# Cache Hit Rate - Low hit rate may indicate sizing issues
resource "aws_cloudwatch_metric_alarm" "ethereum_redis_cache_hit_rate_low" {
  alarm_name          = "${local.prefix}-ethereum-redis-cache-hit-rate-low"
  alarm_description   = "Ethereum Redis cache hit rate below 80%"
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
        CacheClusterId = local.ethereum_redis_primary_cluster_id
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
        CacheClusterId = local.ethereum_redis_primary_cluster_id
      }
    }
  }

  alarm_actions = [aws_sns_topic.ethereum_redis_alarms.arn]
  ok_actions    = [aws_sns_topic.ethereum_redis_alarms.arn]

  tags = {
    Name    = "${local.prefix}-ethereum-redis-cache-hit-rate-low"
    Service = "redis"
  }
}

# Connection Count - High connections
resource "aws_cloudwatch_metric_alarm" "ethereum_redis_connections_high" {
  alarm_name          = "${local.prefix}-ethereum-redis-connections-high"
  alarm_description   = "Ethereum Redis connection count is high"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  metric_name         = "CurrConnections"
  namespace           = "AWS/ElastiCache"
  period              = 300
  statistic           = "Average"
  threshold           = 1000
  treat_missing_data  = "notBreaching"

  dimensions = {
    CacheClusterId = local.ethereum_redis_primary_cluster_id
  }

  alarm_actions = [aws_sns_topic.ethereum_redis_alarms.arn]
  ok_actions    = [aws_sns_topic.ethereum_redis_alarms.arn]

  tags = {
    Name    = "${local.prefix}-ethereum-redis-connections-high"
    Service = "redis"
  }
}

# Replication Lag - For multi-node clusters only
resource "aws_cloudwatch_metric_alarm" "ethereum_redis_replication_lag" {
  count = var.redis_num_cache_clusters > 1 ? 1 : 0

  alarm_name          = "${local.prefix}-ethereum-redis-replication-lag"
  alarm_description   = "Ethereum Redis replication lag exceeds 1 second"
  comparison_operator = "GreaterThanThreshold"
  evaluation_periods  = 3
  metric_name         = "ReplicationLag"
  namespace           = "AWS/ElastiCache"
  period              = 60
  statistic           = "Average"
  threshold           = 1
  treat_missing_data  = "notBreaching"

  dimensions = {
    ReplicationGroupId = aws_elasticache_replication_group.ethereum_redis.id
  }

  alarm_actions = [aws_sns_topic.ethereum_redis_alarms.arn]
  ok_actions    = [aws_sns_topic.ethereum_redis_alarms.arn]

  tags = {
    Name    = "${local.prefix}-ethereum-redis-replication-lag"
    Service = "redis"
  }
}

# CloudWatch Dashboard
resource "aws_cloudwatch_dashboard" "redis" {
  dashboard_name = "${local.prefix}-ethereum-redis"

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
            ["AWS/ElastiCache", "CPUUtilization", "CacheClusterId", local.ethereum_redis_primary_cluster_id, { label = "Primary" }]
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
            ["AWS/ElastiCache", "DatabaseMemoryUsagePercentage", "CacheClusterId", local.ethereum_redis_primary_cluster_id, { label = "Primary" }]
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
            ["AWS/ElastiCache", "CurrConnections", "CacheClusterId", local.ethereum_redis_primary_cluster_id, { label = "Connections" }]
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
            ["AWS/ElastiCache", "CacheHits", "CacheClusterId", local.ethereum_redis_primary_cluster_id, { label = "Hits", color = "#2ca02c" }],
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
            ["AWS/ElastiCache", "Evictions", "CacheClusterId", local.ethereum_redis_primary_cluster_id, { label = "Evictions", color = "#d62728" }]
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
            ["AWS/ElastiCache", "GetTypeCmds", "CacheClusterId", local.ethereum_redis_primary_cluster_id, { label = "GET commands" }],
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
            ["AWS/ElastiCache", "NetworkBytesIn", "CacheClusterId", local.ethereum_redis_primary_cluster_id, { label = "Bytes In" }],
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
            ["AWS/ElastiCache", "StringBasedCmdsLatency", "CacheClusterId", local.ethereum_redis_primary_cluster_id, { label = "Latency" }]
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
            ["AWS/ElastiCache", "NewConnections", "CacheClusterId", local.ethereum_redis_primary_cluster_id, { label = "New Connections" }]
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
            ["AWS/ElastiCache", "CurrItems", "CacheClusterId", local.ethereum_redis_primary_cluster_id, { label = "Current Items" }]
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
            ["AWS/ElastiCache", "Reclaimed", "CacheClusterId", local.ethereum_redis_primary_cluster_id, { label = "Reclaimed" }]
          ]
          stat = "Sum"
        }
      }
    ]
  })
}

# =============================================================================
# Outputs
# =============================================================================

output "ethereum_redis_endpoint" {
  description = "Ethereum Redis primary endpoint"
  value       = aws_elasticache_replication_group.ethereum_redis.primary_endpoint_address
  sensitive   = true
}

output "ethereum_redis_port" {
  description = "Ethereum Redis port"
  value       = 6379
}

output "ethereum_redis_secret_arn" {
  description = "ARN of the Ethereum Redis credentials secret"
  value       = aws_secretsmanager_secret.ethereum_redis.arn
}

output "ethereum_redis_alarms_topic_arn" {
  description = "SNS topic ARN for Redis alarms"
  value       = aws_sns_topic.ethereum_redis_alarms.arn
}

output "redis_dashboard_url" {
  description = "CloudWatch dashboard URL for Redis monitoring"
  value       = "https://${var.aws_region}.console.aws.amazon.com/cloudwatch/home?region=${var.aws_region}#dashboards:name=${local.prefix}-ethereum-redis"
}
