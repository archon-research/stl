# =============================================================================
# ECS SparkLend Position Tracker Service
# =============================================================================
# Consumes block events from SQS, tracks SparkLend positions on-chain,
# and stores position changes in PostgreSQL.

# -----------------------------------------------------------------------------
# ECR Repository - SparkLend Position Tracker
# -----------------------------------------------------------------------------

resource "aws_ecr_repository" "sparklend_position_tracker" {
  name                 = "${local.prefix}-sparklend-position-tracker"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = {
    Name    = "${local.prefix}-sparklend-position-tracker"
    Service = "sparklend-position-tracker"
  }
}

# Lifecycle policy to clean up old images
resource "aws_ecr_lifecycle_policy" "sparklend_position_tracker" {
  repository = aws_ecr_repository.sparklend_position_tracker.name

  policy = jsonencode({
    rules = [
      {
        rulePriority = 1
        description  = "Keep last 10 images"
        selection = {
          tagStatus   = "any"
          countType   = "imageCountMoreThan"
          countNumber = 10
        }
        action = {
          type = "expire"
        }
      }
    ]
  })
}

# -----------------------------------------------------------------------------
# CloudWatch Log Group - SparkLend Position Tracker
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "sparklend_position_tracker" {
  name              = "/ecs/${local.prefix}-sparklend-position-tracker"
  retention_in_days = 30

  tags = {
    Name    = "${local.prefix}-sparklend-position-tracker-logs"
    Service = "sparklend-position-tracker"
  }
}

# -----------------------------------------------------------------------------
# IAM Role - SparkLend Position Tracker (Task Role)
# -----------------------------------------------------------------------------
# Minimal permissions for sparklend position tracker:
# - SQS: Consume from sparklend position queue only
# - CloudWatch: ADOT metrics
# - Secrets Manager: TigerData app credentials (via shared policy)

data "aws_iam_policy_document" "sparklend_position_tracker_assume_role" {
  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "sparklend_position_tracker" {
  name               = "${local.prefix}-sparklend-position-tracker"
  assume_role_policy = data.aws_iam_policy_document.sparklend_position_tracker_assume_role.json

  tags = {
    Name    = "${local.prefix}-sparklend-position-tracker"
    Service = "sparklend-position-tracker"
  }
}

# SQS consume policy - sparklend position queue only
data "aws_iam_policy_document" "sparklend_position_tracker_sqs" {
  statement {
    sid    = "ConsumeFromSparkLendPositionQueue"
    effect = "Allow"
    actions = [
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes",
      "sqs:ChangeMessageVisibility",
    ]
    resources = [aws_sqs_queue.ethereum_sparklend_position.arn]
  }
}

resource "aws_iam_policy" "sparklend_position_tracker_sqs" {
  name        = "${local.prefix}-sparklend-position-tracker-sqs"
  description = "Allows sparklend position tracker to consume from sparklend position SQS queue"
  policy      = data.aws_iam_policy_document.sparklend_position_tracker_sqs.json
}

resource "aws_iam_role_policy_attachment" "sparklend_position_tracker_sqs" {
  role       = aws_iam_role.sparklend_position_tracker.name
  policy_arn = aws_iam_policy.sparklend_position_tracker_sqs.arn
}

# SparkLend position tracker gets CloudWatch write access for ADOT metrics
resource "aws_iam_role_policy_attachment" "sparklend_position_tracker_cloudwatch" {
  role       = aws_iam_role.sparklend_position_tracker.name
  policy_arn = "arn:aws:iam::aws:policy/CloudWatchAgentServerPolicy"
}

# TigerData app secret read access (stl_read_write user)
resource "aws_iam_role_policy_attachment" "sparklend_position_tracker_tigerdata" {
  role       = aws_iam_role.sparklend_position_tracker.name
  policy_arn = aws_iam_policy.tigerdata_app_secret_read.arn
}

# -----------------------------------------------------------------------------
# ECS Task Definition - SparkLend Position Tracker
# -----------------------------------------------------------------------------

resource "aws_ecs_task_definition" "sparklend_position_tracker" {
  family                   = "${local.prefix}-sparklend-position-tracker"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.sparklend_position_tracker_cpu
  memory                   = var.sparklend_position_tracker_memory
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn
  task_role_arn            = aws_iam_role.sparklend_position_tracker.arn

  # Use ARM64 for Graviton (cost-effective)
  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "ARM64"
  }

  container_definitions = jsonencode([
    {
      name      = "sparklend-position-tracker"
      image     = "${aws_ecr_repository.sparklend_position_tracker.repository_url}:${var.sparklend_position_tracker_image_tag}"
      essential = true

      # Environment variables
      environment = [
        {
          name  = "AWS_SQS_QUEUE_URL"
          value = aws_sqs_queue.ethereum_sparklend_position.url
        },
        {
          name  = "AWS_REGION"
          value = var.aws_region
        },
        {
          name  = "ALCHEMY_HTTP_URL"
          value = var.alchemy_http_url
        },
        {
          name  = "REDIS_ADDR"
          value = "${aws_elasticache_replication_group.ethereum_redis.primary_endpoint_address}:6379"
        },
        {
          name  = "CHAIN_ID"
          value = "1"
        },
        {
          name  = "OTEL_EXPORTER_OTLP_ENDPOINT"
          value = "http://localhost:4317"
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
          valueFrom = "${aws_secretsmanager_secret.tigerdata_app.arn}:pooler_url::"
        },
      ]

      # Logging configuration
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.sparklend_position_tracker.name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "ecs"
        }
      }

      # Health check - the worker is long-running, basic health check
      healthCheck = {
        command     = ["CMD-SHELL", "pgrep sparklend_positio || exit 1"]
        interval    = 30
        timeout     = 5
        retries     = 3
        startPeriod = 60
      }
    },
    # ADOT Collector sidecar for CloudWatch metrics
    {
      name      = "adot-collector"
      image     = "public.ecr.aws/aws-observability/aws-otel-collector:v0.40.0"
      essential = false # Don't kill the task if collector fails

      # Use custom config that binds to localhost only
      command = ["--config=env:AOT_CONFIG_CONTENT"]

      environment = [
        {
          name = "AOT_CONFIG_CONTENT"
          value = yamlencode({
            extensions = {
              health_check = {
                endpoint = "localhost:13133"
              }
            }
            receivers = {
              otlp = {
                protocols = {
                  grpc = {
                    endpoint = "localhost:4317"
                  }
                  http = {
                    endpoint = "localhost:4318"
                  }
                }
              }
            }
            processors = {
              batch = {}
              resourcedetection = {
                detectors = ["env", "ecs"]
                timeout   = "5s"
                override  = false
              }
            }
            exporters = {
              awsemf = {
                namespace               = "STL/SparkLendPositionTracker"
                region                  = var.aws_region
                log_group_name          = "/ecs/${local.prefix}-sparklend-position-tracker/metrics"
                dimension_rollup_option = "ZeroAndSingleDimensionRollup"
              }
            }
            service = {
              extensions = ["health_check"]
              pipelines = {
                metrics = {
                  receivers  = ["otlp"]
                  processors = ["resourcedetection", "batch"]
                  exporters  = ["awsemf"]
                }
              }
            }
          })
        }
      ]

      # Resource limits for sidecar
      cpu    = 64
      memory = 128

      # Logging configuration
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.sparklend_position_tracker.name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "adot"
        }
      }

      # Health check for ADOT
      healthCheck = {
        command     = ["CMD", "/healthcheck"]
        interval    = 30
        timeout     = 5
        retries     = 3
        startPeriod = 10
      }
    }
  ])

  tags = {
    Name    = "${local.prefix}-sparklend-position-tracker"
    Service = "sparklend-position-tracker"
  }
}

# -----------------------------------------------------------------------------
# ECS Service - SparkLend Position Tracker
# -----------------------------------------------------------------------------

resource "aws_ecs_service" "sparklend_position_tracker" {
  name            = "${local.prefix}-sparklend-position-tracker"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.sparklend_position_tracker.arn
  desired_count   = var.sparklend_position_tracker_desired_count
  launch_type     = "FARGATE"

  # Deployment configuration
  deployment_minimum_healthy_percent = 0   # Allow stopping old task first
  deployment_maximum_percent         = 100 # Only one task at a time

  # Network configuration
  network_configuration {
    subnets          = [aws_subnet.private.id]
    security_groups  = [aws_security_group.worker.id]
    assign_public_ip = false
  }

  tags = {
    Name    = "${local.prefix}-sparklend-position-tracker"
    Service = "sparklend-position-tracker"
  }

  depends_on = [
    aws_iam_role_policy_attachment.ecs_task_execution_policy,
    aws_iam_role_policy_attachment.ecs_secrets_access,
  ]
}
