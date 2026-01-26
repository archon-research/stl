# =============================================================================
# ECS Raw Data Backup Worker Service
# =============================================================================
# Consumes block events from SQS, fetches data from Redis cache,
# and stores it to S3 for long-term backup.

# -----------------------------------------------------------------------------
# ECR Repository - Backup Worker
# -----------------------------------------------------------------------------

resource "aws_ecr_repository" "backup_worker" {
  name                 = "${local.prefix}-backup-worker"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = {
    Name    = "${local.prefix}-backup-worker"
    Service = "backup-worker"
  }
}

# Lifecycle policy to clean up old images
resource "aws_ecr_lifecycle_policy" "backup_worker" {
  repository = aws_ecr_repository.backup_worker.name

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
# CloudWatch Log Group - Backup Worker
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "backup_worker" {
  name              = "/ecs/${local.prefix}-backup-worker"
  retention_in_days = 30

  tags = {
    Name    = "${local.prefix}-backup-worker-logs"
    Service = "backup-worker"
  }
}

# -----------------------------------------------------------------------------
# IAM Role - Backup Worker (Task Role)
# -----------------------------------------------------------------------------
# Minimal permissions for backup worker:
# - SQS: Consume from backup queue only
# - S3: Write to raw data bucket
# - Redis: Access via network (no IAM needed)

data "aws_iam_policy_document" "backup_worker_assume_role" {
  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "backup_worker" {
  name               = "${local.prefix}-backup-worker"
  assume_role_policy = data.aws_iam_policy_document.backup_worker_assume_role.json

  tags = {
    Name    = "${local.prefix}-backup-worker"
    Service = "backup-worker"
  }
}

# SQS consume policy - backup queue only
data "aws_iam_policy_document" "backup_worker_sqs" {
  statement {
    sid    = "ConsumeFromBackupQueue"
    effect = "Allow"
    actions = [
      "sqs:ReceiveMessage",
      "sqs:DeleteMessage",
      "sqs:GetQueueAttributes",
      "sqs:ChangeMessageVisibility",
    ]
    resources = [aws_sqs_queue.ethereum_backup.arn]
  }
}

resource "aws_iam_policy" "backup_worker_sqs" {
  name        = "${local.prefix}-backup-worker-sqs"
  description = "Allows backup worker to consume from backup SQS queue"
  policy      = data.aws_iam_policy_document.backup_worker_sqs.json
}

resource "aws_iam_role_policy_attachment" "backup_worker_sqs" {
  role       = aws_iam_role.backup_worker.name
  policy_arn = aws_iam_policy.backup_worker_sqs.arn
}

# S3 write policy - raw data bucket only
data "aws_iam_policy_document" "backup_worker_s3" {
  # Check if file exists (for idempotency)
  statement {
    sid    = "GetObjects"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion",
    ]
    resources = ["${aws_s3_bucket.main.arn}/*"]
  }

  # Write backup files
  statement {
    sid    = "WriteObjects"
    effect = "Allow"
    actions = [
      "s3:PutObject",
    ]
    resources = ["${aws_s3_bucket.main.arn}/*"]
  }

  # List bucket (for prefix checks)
  statement {
    sid    = "ListBucket"
    effect = "Allow"
    actions = [
      "s3:ListBucket",
    ]
    resources = [aws_s3_bucket.main.arn]
  }
}

resource "aws_iam_policy" "backup_worker_s3" {
  name        = "${local.prefix}-backup-worker-s3"
  description = "Allows backup worker to write to S3 raw data bucket"
  policy      = data.aws_iam_policy_document.backup_worker_s3.json
}

resource "aws_iam_role_policy_attachment" "backup_worker_s3" {
  role       = aws_iam_role.backup_worker.name
  policy_arn = aws_iam_policy.backup_worker_s3.arn
}

# Backup worker gets CloudWatch write access for ADOT metrics
resource "aws_iam_role_policy_attachment" "backup_worker_cloudwatch" {
  role       = aws_iam_role.backup_worker.name
  policy_arn = "arn:aws:iam::aws:policy/CloudWatchAgentServerPolicy"
}

# -----------------------------------------------------------------------------
# ECS Task Definition - Backup Worker
# -----------------------------------------------------------------------------

resource "aws_ecs_task_definition" "backup_worker" {
  family                   = "${local.prefix}-backup-worker"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.backup_worker_cpu
  memory                   = var.backup_worker_memory
  execution_role_arn       = aws_iam_role.ecs_task_execution.arn
  task_role_arn            = aws_iam_role.backup_worker.arn

  # Use ARM64 for Graviton (cost-effective)
  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "ARM64"
  }

  container_definitions = jsonencode([
    {
      name      = "backup-worker"
      image     = "${aws_ecr_repository.backup_worker.repository_url}:${var.backup_worker_image_tag}"
      essential = true

      # Environment variables
      environment = [
        {
          name  = "CHAIN_ID"
          value = tostring(var.chain_id)
        },
        {
          name  = "SQS_QUEUE_URL"
          value = aws_sqs_queue.ethereum_backup.url
        },
        {
          name  = "S3_BUCKET"
          value = aws_s3_bucket.main.id
        },
        {
          name  = "REDIS_ADDR"
          value = "${aws_elasticache_replication_group.ethereum_redis.primary_endpoint_address}:6379"
        },
        {
          name  = "AWS_REGION"
          value = var.aws_region
        },
        {
          name  = "WORKERS"
          value = tostring(var.backup_worker_workers)
        },
        {
          name  = "OTEL_EXPORTER_OTLP_ENDPOINT"
          value = "http://localhost:4317"
        },
      ]

      # Logging configuration
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.backup_worker.name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "ecs"
        }
      }

      # Health check - the worker is long-running, basic health check
      healthCheck = {
        command     = ["CMD-SHELL", "pgrep raw_data_backup || exit 1"]
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
                namespace               = "STL/Backup"
                region                  = var.aws_region
                log_group_name          = "/ecs/${local.prefix}-backup-worker/metrics"
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
      cpu    = 256
      memory = 512

      # Logging configuration
      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.backup_worker.name
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
    Name    = "${local.prefix}-backup-worker"
    Service = "backup-worker"
  }
}

# -----------------------------------------------------------------------------
# ECS Service - Backup Worker
# -----------------------------------------------------------------------------

resource "aws_ecs_service" "backup_worker" {
  name            = "${local.prefix}-backup-worker"
  cluster         = aws_ecs_cluster.main.id
  task_definition = aws_ecs_task_definition.backup_worker.arn
  desired_count   = var.backup_worker_desired_count
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
    Name    = "${local.prefix}-backup-worker"
    Service = "backup-worker"
  }

  depends_on = [
    aws_iam_role_policy_attachment.ecs_task_execution_policy,
    aws_iam_role_policy_attachment.ecs_secrets_access,
  ]
}

# -----------------------------------------------------------------------------
# Security Group Rule - Backup Worker to Redis
# -----------------------------------------------------------------------------
# The worker security group already has egress to Redis and internet.
# We need to ensure Redis allows ingress from the backup worker.
# This is already covered by redis_from_worker in security_groups.tf
# since we're reusing the worker security group.
