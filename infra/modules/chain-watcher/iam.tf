# =============================================================================
# IAM Roles for ECS Tasks
# =============================================================================

# -----------------------------------------------------------------------------
# Trust Policy (shared)
# -----------------------------------------------------------------------------

data "aws_iam_policy_document" "ecs_task_assume_role" {
  statement {
    effect = "Allow"
    principals {
      type        = "Service"
      identifiers = ["ecs-tasks.amazonaws.com"]
    }
    actions = ["sts:AssumeRole"]
  }
}

# -----------------------------------------------------------------------------
# S3 Access Policy (shared by watcher and worker roles)
# -----------------------------------------------------------------------------

data "aws_iam_policy_document" "s3_access" {
  statement {
    sid    = "ListBucket"
    effect = "Allow"
    actions = [
      "s3:ListBucket",
      "s3:GetBucketLocation"
    ]
    resources = [aws_s3_bucket.raw_data.arn]
  }

  statement {
    sid    = "ReadObjects"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion",
      "s3:GetObjectTagging"
    ]
    resources = ["${aws_s3_bucket.raw_data.arn}/*"]
  }

  statement {
    sid    = "WriteObjects"
    effect = "Allow"
    actions = [
      "s3:PutObject",
      "s3:PutObjectTagging",
      "s3:DeleteObject",
      "s3:DeleteObjectVersion"
    ]
    resources = ["${aws_s3_bucket.raw_data.arn}/*"]
  }
}

resource "aws_iam_policy" "s3_access" {
  name        = "${local.name_prefix}-s3-access"
  description = "Read and write access to ${title(var.chain_name)} raw S3 bucket"
  policy      = data.aws_iam_policy_document.s3_access.json
}

# -----------------------------------------------------------------------------
# Watcher Role
# -----------------------------------------------------------------------------

resource "aws_iam_role" "watcher" {
  name               = "${local.name_prefix}-watcher"
  assume_role_policy = data.aws_iam_policy_document.ecs_task_assume_role.json

  tags = {
    Name       = "${local.name_prefix}-watcher"
    Blockchain = var.chain_name
    Service    = "watcher"
  }
}

resource "aws_iam_role_policy_attachment" "watcher_s3_access" {
  role       = aws_iam_role.watcher.name
  policy_arn = aws_iam_policy.s3_access.arn
}

resource "aws_iam_role_policy_attachment" "watcher_tigerdata_app_secret" {
  role       = aws_iam_role.watcher.name
  policy_arn = var.tigerdata_app_secret_read_policy_arn
}

resource "aws_iam_role_policy_attachment" "watcher_redis_secret" {
  role       = aws_iam_role.watcher.name
  policy_arn = aws_iam_policy.redis_secret_read.arn
}

resource "aws_iam_role_policy_attachment" "watcher_sns_publish" {
  role       = aws_iam_role.watcher.name
  policy_arn = aws_iam_policy.sns_publish.arn
}

resource "aws_iam_role_policy_attachment" "watcher_xray" {
  role       = aws_iam_role.watcher.name
  policy_arn = "arn:aws:iam::aws:policy/AWSXRayDaemonWriteAccess"
}

resource "aws_iam_role_policy_attachment" "watcher_cloudwatch" {
  role       = aws_iam_role.watcher.name
  policy_arn = "arn:aws:iam::aws:policy/CloudWatchAgentServerPolicy"
}

# -----------------------------------------------------------------------------
# Worker Role
# -----------------------------------------------------------------------------

resource "aws_iam_role" "worker" {
  name               = "${local.name_prefix}-worker"
  assume_role_policy = data.aws_iam_policy_document.ecs_task_assume_role.json

  tags = {
    Name       = "${local.name_prefix}-worker"
    Blockchain = var.chain_name
    Service    = "worker"
  }
}

resource "aws_iam_role_policy_attachment" "worker_s3_access" {
  role       = aws_iam_role.worker.name
  policy_arn = aws_iam_policy.s3_access.arn
}

resource "aws_iam_role_policy_attachment" "worker_tigerdata_app_secret" {
  role       = aws_iam_role.worker.name
  policy_arn = var.tigerdata_app_secret_read_policy_arn
}

resource "aws_iam_role_policy_attachment" "worker_redis_secret" {
  role       = aws_iam_role.worker.name
  policy_arn = aws_iam_policy.redis_secret_read.arn
}

resource "aws_iam_role_policy_attachment" "worker_sqs_consume" {
  role       = aws_iam_role.worker.name
  policy_arn = aws_iam_policy.sqs_consume.arn
}
