# =============================================================================
# ECS Raw Data Backup Worker Service
# =============================================================================

# -----------------------------------------------------------------------------
# CloudWatch Log Group
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "backup_worker" {
  name              = "/ecs/${local.name_prefix}-backup-worker"
  retention_in_days = 30

  tags = {
    Name    = "${local.name_prefix}-backup-worker-logs"
    Service = "backup-worker"
  }
}

# -----------------------------------------------------------------------------
# IAM Role - Backup Worker (Task Role)
# -----------------------------------------------------------------------------

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
  name               = "${local.name_prefix}-backup-worker"
  assume_role_policy = data.aws_iam_policy_document.backup_worker_assume_role.json

  tags = {
    Name    = "${local.name_prefix}-backup-worker"
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
    resources = [aws_sqs_queue.consumer["backup"].arn]
  }
}

resource "aws_iam_policy" "backup_worker_sqs" {
  name        = "${local.name_prefix}-backup-worker-sqs"
  description = "Allows backup worker to consume from backup SQS queue"
  policy      = data.aws_iam_policy_document.backup_worker_sqs.json
}

resource "aws_iam_role_policy_attachment" "backup_worker_sqs" {
  role       = aws_iam_role.backup_worker.name
  policy_arn = aws_iam_policy.backup_worker_sqs.arn
}

# S3 write policy
data "aws_iam_policy_document" "backup_worker_s3" {
  statement {
    sid    = "GetObjects"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion",
    ]
    resources = ["${aws_s3_bucket.raw_data.arn}/*"]
  }

  statement {
    sid    = "WriteObjects"
    effect = "Allow"
    actions = [
      "s3:PutObject",
    ]
    resources = ["${aws_s3_bucket.raw_data.arn}/*"]
  }

  statement {
    sid    = "ListBucket"
    effect = "Allow"
    actions = [
      "s3:ListBucket",
    ]
    resources = [aws_s3_bucket.raw_data.arn]
  }
}

resource "aws_iam_policy" "backup_worker_s3" {
  name        = "${local.name_prefix}-backup-worker-s3"
  description = "Allows backup worker to write to S3 raw data bucket"
  policy      = data.aws_iam_policy_document.backup_worker_s3.json
}

resource "aws_iam_role_policy_attachment" "backup_worker_s3" {
  role       = aws_iam_role.backup_worker.name
  policy_arn = aws_iam_policy.backup_worker_s3.arn
}

resource "aws_iam_role_policy_attachment" "backup_worker_cloudwatch" {
  role       = aws_iam_role.backup_worker.name
  policy_arn = "arn:aws:iam::aws:policy/CloudWatchAgentServerPolicy"
}

# -----------------------------------------------------------------------------
# ECS Task Definition - Backup Worker
# -----------------------------------------------------------------------------

resource "aws_ecs_task_definition" "backup_worker" {
  family                   = "${local.name_prefix}-backup-worker"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.backup_worker_cpu
  memory                   = var.backup_worker_memory
  execution_role_arn       = var.ecs_task_execution_role_arn
  task_role_arn            = aws_iam_role.backup_worker.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "ARM64"
  }

  container_definitions = jsonencode([
    {
      name      = "backup-worker"
      image     = "${var.backup_worker_ecr_url}:${var.backup_worker_image_tag}"
      essential = true

      environment = [
        {
          name  = "CHAIN_ID"
          value = tostring(var.chain_id)
        },
        {
          name  = "SQS_QUEUE_URL"
          value = aws_sqs_queue.consumer["backup"].url
        },
        {
          name  = "S3_BUCKET"
          value = aws_s3_bucket.raw_data.id
        },
        {
          name  = "REDIS_ADDR"
          value = "${aws_elasticache_replication_group.redis.primary_endpoint_address}:6379"
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

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.backup_worker.name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "ecs"
        }
      }

      healthCheck = {
        command     = ["CMD-SHELL", "pgrep raw_data_backup || exit 1"]
        interval    = 30
        timeout     = 5
        retries     = 3
        startPeriod = 60
      }
    },
    # ADOT Collector sidecar
    {
      name      = "adot-collector"
      image     = "public.ecr.aws/aws-observability/aws-otel-collector:v0.40.0"
      essential = false

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
                  grpc = { endpoint = "localhost:4317" }
                  http = { endpoint = "localhost:4318" }
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
                log_group_name          = "/ecs/${local.name_prefix}-backup-worker/metrics"
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

      cpu    = 256
      memory = 512

      logConfiguration = {
        logDriver = "awslogs"
        options = {
          "awslogs-group"         = aws_cloudwatch_log_group.backup_worker.name
          "awslogs-region"        = var.aws_region
          "awslogs-stream-prefix" = "adot"
        }
      }

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
    Name    = "${local.name_prefix}-backup-worker"
    Service = "backup-worker"
  }
}

# -----------------------------------------------------------------------------
# ECS Service - Backup Worker
# -----------------------------------------------------------------------------

resource "aws_ecs_service" "backup_worker" {
  name            = "${local.name_prefix}-backup-worker"
  cluster         = var.ecs_cluster_id
  task_definition = aws_ecs_task_definition.backup_worker.arn
  desired_count   = var.backup_worker_desired_count
  launch_type     = "FARGATE"

  deployment_minimum_healthy_percent = 0
  deployment_maximum_percent         = 100

  network_configuration {
    subnets          = [var.private_subnet_id]
    security_groups  = [var.worker_sg_id]
    assign_public_ip = false
  }

  tags = {
    Name    = "${local.name_prefix}-backup-worker"
    Service = "backup-worker"
  }
}
