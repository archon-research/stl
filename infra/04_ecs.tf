# =============================================================================
# ECS Cluster and Common Resources
# =============================================================================
# Shared ECS cluster for all services (Watcher, Worker, API)

# -----------------------------------------------------------------------------
# ECS Cluster
# -----------------------------------------------------------------------------

resource "aws_ecs_cluster" "main" {
  name = "${local.prefix}-cluster"

  setting {
    name  = "containerInsights"
    value = "enabled"
  }

  tags = {
    Name = "${local.prefix}-cluster"
  }
}

# Cluster capacity providers - use Fargate
resource "aws_ecs_cluster_capacity_providers" "main" {
  cluster_name = aws_ecs_cluster.main.name

  capacity_providers = ["FARGATE", "FARGATE_SPOT"]

  default_capacity_provider_strategy {
    base              = 1
    weight            = 100
    capacity_provider = "FARGATE"
  }
}

# -----------------------------------------------------------------------------
# ECR Repository - Watcher
# -----------------------------------------------------------------------------

resource "aws_ecr_repository" "watcher" {
  name                 = "${local.prefix}-watcher"
  image_tag_mutability = "MUTABLE"

  image_scanning_configuration {
    scan_on_push = true
  }

  tags = {
    Name    = "${local.prefix}-watcher"
    Service = "watcher"
  }
}

# Lifecycle policy to clean up old images
resource "aws_ecr_lifecycle_policy" "watcher" {
  repository = aws_ecr_repository.watcher.name

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
# CloudWatch Log Group - Watcher
# -----------------------------------------------------------------------------

resource "aws_cloudwatch_log_group" "watcher" {
  name              = "/ecs/${local.prefix}-watcher"
  retention_in_days = 30

  tags = {
    Name    = "${local.prefix}-watcher-logs"
    Service = "watcher"
  }
}

# -----------------------------------------------------------------------------
# ECS Task Execution Role
# -----------------------------------------------------------------------------
# This role is used by ECS to:
# - Pull container images from ECR
# - Write logs to CloudWatch
# - Fetch secrets from Secrets Manager

data "aws_iam_policy_document" "ecs_task_execution_assume_role" {
  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
  }
}

resource "aws_iam_role" "ecs_task_execution" {
  name               = "${local.prefix}-ecs-task-execution"
  assume_role_policy = data.aws_iam_policy_document.ecs_task_execution_assume_role.json

  tags = {
    Name = "${local.prefix}-ecs-task-execution"
  }
}

# Attach AWS managed policy for basic ECS task execution
resource "aws_iam_role_policy_attachment" "ecs_task_execution_policy" {
  role       = aws_iam_role.ecs_task_execution.name
  policy_arn = "arn:aws:iam::aws:policy/service-role/AmazonECSTaskExecutionRolePolicy"
}

# Policy to read secrets for container environment variables
data "aws_iam_policy_document" "ecs_secrets_access" {
  statement {
    sid    = "ReadSecrets"
    effect = "Allow"

    actions = [
      "secretsmanager:GetSecretValue",
    ]

    resources = [
      data.aws_secretsmanager_secret.watcher_config.arn,
      aws_secretsmanager_secret.tigerdata_db.arn,
    ]
  }
}

resource "aws_iam_policy" "ecs_secrets_access" {
  name        = "${local.prefix}-ecs-secrets-access"
  description = "Allows ECS task execution role to read secrets"
  policy      = data.aws_iam_policy_document.ecs_secrets_access.json
}

resource "aws_iam_role_policy_attachment" "ecs_secrets_access" {
  role       = aws_iam_role.ecs_task_execution.name
  policy_arn = aws_iam_policy.ecs_secrets_access.arn
}
