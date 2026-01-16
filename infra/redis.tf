# =============================================================================
# ElastiCache Redis Configuration
# =============================================================================
# Redis is used for:
# - Block cache (watcher/worker coordination)

# -----------------------------------------------------------------------------
# Subnet Group
# -----------------------------------------------------------------------------
# Redis runs in the isolated subnet (no internet access)

resource "aws_elasticache_subnet_group" "redis" {
  name       = "${local.prefix}-redis"
  subnet_ids = [aws_subnet.isolated.id]

  tags = {
    Name = "${local.prefix}-redis-subnet-group"
  }
}

# -----------------------------------------------------------------------------
# Parameter Group
# -----------------------------------------------------------------------------
# Custom parameters for our use case

resource "aws_elasticache_parameter_group" "redis" {
  name   = "${local.prefix}-redis-params"
  family = "redis7"

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

  # Async deletion to prevent blocking on large key evictions
  parameter {
    name  = "lazyfree-lazy-eviction"
    value = "yes"
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
    Name = "${local.prefix}-redis-params"
  }
}

# -----------------------------------------------------------------------------
# ElastiCache Redis Cluster
# -----------------------------------------------------------------------------

resource "aws_elasticache_replication_group" "redis" {
  replication_group_id = "${local.prefix}-redis"
  description          = "Redis cache for ${var.environment}"

  # Engine configuration
  engine               = "redis"
  engine_version       = var.redis_engine_version
  node_type            = var.redis_node_type
  parameter_group_name = aws_elasticache_parameter_group.redis.name

  # Cluster configuration
  num_cache_clusters = var.redis_num_cache_clusters
  port               = 6379

  # Network configuration
  subnet_group_name  = aws_elasticache_subnet_group.redis.name
  security_group_ids = [aws_security_group.redis.id]

  # Encryption
  at_rest_encryption_enabled = false
  transit_encryption_enabled = var.redis_transit_encryption
  auth_token                 = var.redis_transit_encryption ? random_password.redis_auth[0].result : null

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
    Name        = "${local.prefix}-redis"
    Environment = var.environment
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

resource "random_password" "redis_auth" {
  count   = var.redis_transit_encryption ? 1 : 0
  length  = 32
  special = false # ElastiCache auth token doesn't support all special chars
}

# -----------------------------------------------------------------------------
# Store Redis credentials in Secrets Manager
# -----------------------------------------------------------------------------

resource "aws_secretsmanager_secret" "redis" {
  name        = "${local.prefix}-redis"
  description = "Redis connection details for ${var.environment}"

  recovery_window_in_days = 7

  tags = {
    Name    = "${local.prefix}-redis"
    Service = "redis"
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
# IAM Policy for reading Redis secret
# -----------------------------------------------------------------------------

data "aws_iam_policy_document" "redis_secret_read" {
  statement {
    sid    = "ReadRedisSecret"
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
  name        = "${local.prefix}-redis-secret-read"
  description = "Allows reading Redis credentials from Secrets Manager"
  policy      = data.aws_iam_policy_document.redis_secret_read.json
}

resource "aws_iam_role_policy_attachment" "redis_secret_access" {
  role       = aws_iam_role.ethereum_raw_data_access.name
  policy_arn = aws_iam_policy.redis_secret_read.arn
}

# -----------------------------------------------------------------------------
# Outputs
# -----------------------------------------------------------------------------

output "redis_endpoint" {
  description = "Redis primary endpoint"
  value       = aws_elasticache_replication_group.redis.primary_endpoint_address
  sensitive   = true
}

output "redis_port" {
  description = "Redis port"
  value       = 6379
}

output "redis_secret_arn" {
  description = "ARN of the Redis credentials secret"
  value       = aws_secretsmanager_secret.redis.arn
}
