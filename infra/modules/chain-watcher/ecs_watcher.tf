# =============================================================================
# ECS Watcher Service
# =============================================================================

# -----------------------------------------------------------------------------
# CloudWatch Log Group
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "watcher" {
  name              = "/ecs/${local.name_prefix}-watcher"
  retention_in_days = 30

  tags = {
    Name    = "${local.name_prefix}-watcher-logs"
    Service = "watcher"
  }
}

# -----------------------------------------------------------------------------
# ECS Task Definition - Watcher
# -----------------------------------------------------------------------------

resource "aws_ecs_task_definition" "watcher" {
  family                   = "${local.name_prefix}-watcher"
  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = var.watcher_cpu
  memory                   = var.watcher_memory
  execution_role_arn       = var.ecs_task_execution_role_arn
  task_role_arn            = aws_iam_role.watcher.arn

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "ARM64"
  }

  container_definitions = jsonencode([
    merge(
      {
        name      = "watcher"
        image     = "${var.watcher_ecr_url}:${var.watcher_image_tag}"
        essential = true

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
            value = aws_sns_topic.blocks.arn
          },
          {
            name  = "AWS_SNS_ENDPOINT"
            value = "https://sns.${var.aws_region}.amazonaws.com"
          },
          {
            name  = "REDIS_ADDR"
            value = "${aws_elasticache_replication_group.redis.primary_endpoint_address}:6379"
          },
          {
            name  = "ENVIRONMENT"
            value = var.environment
          },
          {
            name  = "ENABLE_BACKFILL"
            value = tostring(var.enable_backfill)
          },
          {
            name  = "OTEL_EXPORTER_OTLP_ENDPOINT"
            value = "http://localhost:4317"
          },
        ]

        secrets = [
          {
            name      = "ALCHEMY_API_KEY"
            valueFrom = "${var.watcher_config_secret_arn}:alchemy_api_key::"
          },
          {
            name      = "DATABASE_URL"
            valueFrom = "${var.tigerdata_app_secret_arn}:pooler_url::"
          },
        ]

        logConfiguration = {
          logDriver = "awslogs"
          options = {
            "awslogs-group"         = aws_cloudwatch_log_group.watcher.name
            "awslogs-region"        = var.aws_region
            "awslogs-stream-prefix" = "ecs"
          }
        }
      },
      length(var.watcher_command) > 0 ? { command = var.watcher_command } : {}
    ),
    # ADOT Collector sidecar for X-Ray tracing
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
              awsxray = {
                region = var.aws_region
              }
              awsemf = {
                namespace               = "STL/Watcher"
                region                  = var.aws_region
                log_group_name          = "/ecs/${local.name_prefix}-watcher/metrics"
                dimension_rollup_option = "ZeroAndSingleDimensionRollup"
              }
            }
            service = {
              extensions = ["health_check"]
              pipelines = {
                traces = {
                  receivers  = ["otlp"]
                  processors = ["resourcedetection", "batch"]
                  exporters  = ["awsxray"]
                }
                metrics = {
                  receivers  = ["otlp"]
                  processors = ["batch"]
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
          "awslogs-group"         = aws_cloudwatch_log_group.watcher.name
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
    Name    = "${local.name_prefix}-watcher"
    Service = "watcher"
  }
}

# -----------------------------------------------------------------------------
# ECS Service - Watcher
# -----------------------------------------------------------------------------

resource "aws_ecs_service" "watcher" {
  name            = "${local.name_prefix}-watcher"
  cluster         = var.ecs_cluster_id
  task_definition = aws_ecs_task_definition.watcher.arn
  desired_count   = var.watcher_desired_count
  launch_type     = "FARGATE"

  # Recreate strategy: stop old before starting new (single writer)
  deployment_minimum_healthy_percent = 0
  deployment_maximum_percent         = 100

  network_configuration {
    subnets          = [var.private_subnet_id]
    security_groups  = [var.watcher_sg_id]
    assign_public_ip = false
  }

  tags = {
    Name    = "${local.name_prefix}-watcher"
    Service = "watcher"
  }
}
