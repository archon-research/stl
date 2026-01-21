# =============================================================================
# ECS Watcher Service
# =============================================================================
# Runs the blockchain watcher on Fargate

# -----------------------------------------------------------------------------
# Watcher Configuration Secret
# -----------------------------------------------------------------------------
# Stores sensitive configuration like API keys

resource "aws_secretsmanager_secret" "watcher_config" {
  name        = "${local.prefix}-watcher-config"
  description = "Watcher service configuration for ${var.environment}"

  recovery_window_in_days = 7

  tags = {
    Name    = "${local.prefix}-watcher-config"
    Service = "watcher"
  }
}

# Initial placeholder - update via AWS CLI after deployment
resource "aws_secretsmanager_secret_version" "watcher_config" {
  secret_id = aws_secretsmanager_secret.watcher_config.id
  secret_string = jsonencode({
    alchemy_api_key = var.alchemy_api_key
  })

  lifecycle {
    ignore_changes = [secret_string]
  }
}

# -----------------------------------------------------------------------------
# ECS Task Definition - Watcher
# -----------------------------------------------------------------------------

resource "aws_ecs_task_definition" "watcher" {
  family                   = "${local.prefix}-watcher"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.watcher_cpu
  memory                   = var.watcher_memory
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn
  task_role_arn            = aws_iam_role.ethereum_watcher.arn

  # Use ARM64 for Graviton (cost-effective)
  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "ARM64"
  }

  container_definitions = jsonencode([
    {
      name      = "watcher"
      image     = "${aws_ecr_repository.watcher.repository_url}:${var.watcher_image_tag}"
      essential = true

      # Command to run - disable blobs for mainnet (not supported by Alchemy)
      command = ["--disable-blobs"]

      # Environment variables (non-sensitive)
      environment = [
        {
          name  = "CHAIN_ID"
          value = tostring(var.chain_id)
        },
        {
          name  = "ALCHEMY_HTTP_URL"
          value = var.alchemy_http_url
        },
        {
          name  = "ALCHEMY_WS_URL"
          value = var.alchemy_ws_url
        },
        {
          name  = "AWS_REGION"
          value = var.aws_region
        },
        {
          name  = "AWS_SNS_TOPIC_ARN"
          value = aws_sns_topic.ethereum_blocks.arn
        },
        {
          name  = "AWS_SNS_ENDPOINT"
          value = "https://sns.${var.aws_region}.amazonaws.com"
        },
        {
          name  = "REDIS_ADDR"
          value = "${aws_elasticache_replication_group.ethereum_redis.primary_endpoint_address}:6379"
        },
        {
          name  = "ENVIRONMENT"
          value = var.environment
        },
        {
          name  = "ENABLE_BACKFILL"
          value = "false"
        },
      ]

      # Secrets from AWS Secrets Manager
      secrets = [
        {
          name      = "ALCHEMY_API_KEY"
          valueFrom = "${aws_secretsmanager_secret.watcher_config.arn}:alchemy_api_key::"
        },
        {
          name      = "DATABASE_URL"
          valueFrom = "${aws_secretsmanager_secret.tigerdata_db.arn}:pooler_url::"
        },
      ]

      # Logging configuration
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.watcher.name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "ecs"
        }
      }

      # Health check - the watcher is long-running, basic health check
      healthCheck = {
        command     = ["CMD-SHELL", "pgrep watcher || exit 1"]
        interval    = 30
        timeout     = 5
        retries     = 3
        startPeriod = 60
      }
    }
  ])

  tags = {
    Name    = "${local.prefix}-watcher"
    Service = "watcher"
  }
}

# -----------------------------------------------------------------------------
# ECS Service - Watcher
# -----------------------------------------------------------------------------

resource "aws_ecs_service" "watcher" {
  name            = "${local.prefix}-watcher"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.watcher.arn
  desired_count   = var.watcher_desired_count
  launch_type     = "FARGATE"

  # Deployment configuration
  deployment_minimum_healthy_percent = 0   # Allow stopping old task first
  deployment_maximum_percent         = 100 # Only one task at a time (singleton)

  # Network configuration
  network_configuration {
    subnets          = [aws_subnet.private.id]
    security_groups  = [aws_security_group.watcher.id]
    assign_public_ip = false
  }

  # Prevent Terraform from reverting to old task definition during updates
  lifecycle {
    ignore_changes = [task_definition]
  }

  tags = {
    Name    = "${local.prefix}-watcher"
    Service = "watcher"
  }

  depends_on = [
    aws_iam_role_policy_attachment.ecs_task_execution_policy,
    aws_iam_role_policy_attachment.ecs_secrets_access,
  ]
}
