# =============================================================================
# ECS Oracle Price Worker Service
# =============================================================================
# Consumes block events from SQS, fetches on-chain oracle prices via
# multicall, and stores price changes in PostgreSQL.

# -----------------------------------------------------------------------------
# ECR Repository - Oracle Price Worker
# -----------------------------------------------------------------------------

resource "aws_ecr_repository" "oracle_price_worker" {
  name                 = "${local.prefix}-oracle-price-worker"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = {
    Name    = "${local.prefix}-oracle-price-worker"
    Service = "oracle-price-worker"
  }
}

# Lifecycle policy to clean up old images
resource "aws_ecr_lifecycle_policy" "oracle_price_worker" {
  repository = aws_ecr_repository.oracle_price_worker.name

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
# CloudWatch Log Group - Oracle Price Worker
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "oracle_price_worker" {
  name              = "/ecs/${local.prefix}-oracle-price-worker"
  retention_in_days = 30

  tags = {
    Name    = "${local.prefix}-oracle-price-worker-logs"
    Service = "oracle-price-worker"
  }
}

# -----------------------------------------------------------------------------
# IAM Role - Oracle Price Worker (Task Role)
# -----------------------------------------------------------------------------
# Minimal permissions for oracle price worker:
# - SQS: Consume from oracle price queue only
# - CloudWatch: ADOT metrics
# - Secrets Manager: TigerData app credentials (via shared policy)

data "aws_iam_policy_document" "oracle_price_worker_assume_role" {
  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "oracle_price_worker" {
  name               = "${local.prefix}-oracle-price-worker"
  assume_role_policy = data.aws_iam_policy_document.oracle_price_worker_assume_role.json

  tags = {
    Name    = "${local.prefix}-oracle-price-worker"
    Service = "oracle-price-worker"
  }
}

# SQS consume policy - oracle price queue only
data "aws_iam_policy_document" "oracle_price_worker_sqs" {
  statement {
    sid    = "ConsumeFromOraclePriceQueue"
    effect = "Allow"
    actions = [
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes",
      "sqs:ChangeMessageVisibility",
    ]
    resources = [aws_sqs_queue.ethereum_oracle_price.arn]
  }
}

resource "aws_iam_policy" "oracle_price_worker_sqs" {
  name        = "${local.prefix}-oracle-price-worker-sqs"
  description = "Allows oracle price worker to consume from oracle price SQS queue"
  policy      = data.aws_iam_policy_document.oracle_price_worker_sqs.json
}

resource "aws_iam_role_policy_attachment" "oracle_price_worker_sqs" {
  role       = aws_iam_role.oracle_price_worker.name
  policy_arn = aws_iam_policy.oracle_price_worker_sqs.arn
}

# Oracle price worker gets CloudWatch write access for ADOT metrics
resource "aws_iam_role_policy_attachment" "oracle_price_worker_cloudwatch" {
  role       = aws_iam_role.oracle_price_worker.name
  policy_arn = "arn:aws:iam::aws:policy/CloudWatchAgentServerPolicy"
}

# TigerData app secret read access (stl_read_write user)
resource "aws_iam_role_policy_attachment" "oracle_price_worker_tigerdata" {
  role       = aws_iam_role.oracle_price_worker.name
  policy_arn = aws_iam_policy.tigerdata_app_secret_read.arn
}

# -----------------------------------------------------------------------------
# ECS Task Definition - Oracle Price Worker
# -----------------------------------------------------------------------------

resource "aws_ecs_task_definition" "oracle_price_worker" {
  family                   = "${local.prefix}-oracle-price-worker"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.oracle_price_worker_cpu
  memory                   = var.oracle_price_worker_memory
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn
  task_role_arn            = aws_iam_role.oracle_price_worker.arn

  # Use ARM64 for Graviton (cost-effective)
  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "ARM64"
  }

  container_definitions = jsonencode([
    {
      name      = "oracle-price-worker"
      image     = "${aws_ecr_repository.oracle_price_worker.repository_url}:${var.oracle_price_worker_image_tag}"
      essential = true

      # Environment variables
      environment = [
        {
          name  = "AWS_SQS_QUEUE_URL"
          value = aws_sqs_queue.ethereum_oracle_price.url
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
          "awslogs-group"         = aws_cloudwatch_log_group.oracle_price_worker.name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "ecs"
        }
      }

      # Health check - the worker is long-running, basic health check
      healthCheck = {
        command     = ["CMD-SHELL", "pgrep oracle_price_worker || exit 1"]
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
                namespace               = "STL/OraclePrice"
                region                  = var.aws_region
                log_group_name          = "/ecs/${local.prefix}-oracle-price-worker/metrics"
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
          "awslogs-group"         = aws_cloudwatch_log_group.oracle_price_worker.name
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
    Name    = "${local.prefix}-oracle-price-worker"
    Service = "oracle-price-worker"
  }
}

# -----------------------------------------------------------------------------
# ECS Service - Oracle Price Worker
# -----------------------------------------------------------------------------

resource "aws_ecs_service" "oracle_price_worker" {
  name            = "${local.prefix}-oracle-price-worker"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.oracle_price_worker.arn
  desired_count   = var.oracle_price_worker_desired_count
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
    Name    = "${local.prefix}-oracle-price-worker"
    Service = "oracle-price-worker"
  }

  depends_on = [
    aws_iam_role_policy_attachment.ecs_task_execution_policy,
    aws_iam_role_policy_attachment.ecs_secrets_access,
  ]
}
