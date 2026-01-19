# =============================================================================
# IAM Roles for ECS Tasks
# =============================================================================
# Separate roles for Watcher and Worker with distinct permissions:
# - Watcher: Publishes to SNS, reads/writes S3
# - Worker: Consumes from SQS, reads/writes S3

# -----------------------------------------------------------------------------
# Trust Policy (shared by both roles)
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
# S3 Access Policy (shared by both roles)
# -----------------------------------------------------------------------------

data "aws_iam_policy_document" "ethereum_s3_access" {
  # List bucket contents
  statement {
    sid    = "ListBucket"
    effect = "Allow"
    actions = [
      "s3:ListBucket",
      "s3:GetBucketLocation"
    ]
    resources = [aws_s3_bucket.main.arn]
  }

  # Read objects
  statement {
    sid    = "ReadObjects"
    effect = "Allow"
    actions = [
      "s3:GetObject",
      "s3:GetObjectVersion",
      "s3:GetObjectTagging"
    ]
    resources = ["${aws_s3_bucket.main.arn}/*"]
  }

  # Write objects
  statement {
    sid    = "WriteObjects"
    effect = "Allow"
    actions = [
      "s3:PutObject",
      "s3:PutObjectTagging",
      "s3:DeleteObject",
      "s3:DeleteObjectVersion"
    ]
    resources = ["${aws_s3_bucket.main.arn}/*"]
  }
}

resource "aws_iam_policy" "ethereum_s3_access" {
  name        = "${local.prefix}-ethereum-s3-access"
  description = "Read and write access to Ethereum raw S3 bucket"
  policy      = data.aws_iam_policy_document.ethereum_s3_access.json
}

# -----------------------------------------------------------------------------
# Watcher Role
# -----------------------------------------------------------------------------
# Watcher: Listens for blockchain events and publishes to SNS

resource "aws_iam_role" "ethereum_watcher" {
  name               = "${local.prefix}-ethereum-watcher"
  assume_role_policy = data.aws_iam_policy_document.ecs_task_assume_role.json

  tags = {
    Name       = "${local.prefix}-ethereum-watcher"
    Blockchain = "ethereum"
    Service    = "watcher"
  }
}

# Watcher gets S3 access
resource "aws_iam_role_policy_attachment" "watcher_s3_access" {
  role       = aws_iam_role.ethereum_watcher.name
  policy_arn = aws_iam_policy.ethereum_s3_access.arn
}

# Watcher gets TigerData secret access
resource "aws_iam_role_policy_attachment" "watcher_tigerdata_secret" {
  role       = aws_iam_role.ethereum_watcher.name
  policy_arn = aws_iam_policy.tigerdata_secret_read.arn
}

# Watcher gets Redis secret access
resource "aws_iam_role_policy_attachment" "watcher_redis_secret" {
  role       = aws_iam_role.ethereum_watcher.name
  policy_arn = aws_iam_policy.ethereum_redis_secret_read.arn
}

# Watcher gets SNS publish access (defined in messaging.tf)
resource "aws_iam_role_policy_attachment" "watcher_sns_publish" {
  role       = aws_iam_role.ethereum_watcher.name
  policy_arn = aws_iam_policy.ethereum_sns_publish.arn
}

# -----------------------------------------------------------------------------
# Worker Role
# -----------------------------------------------------------------------------
# Worker: Consumes from SQS and processes messages

resource "aws_iam_role" "ethereum_worker" {
  name               = "${local.prefix}-ethereum-worker"
  assume_role_policy = data.aws_iam_policy_document.ecs_task_assume_role.json

  tags = {
    Name       = "${local.prefix}-ethereum-worker"
    Blockchain = "ethereum"
    Service    = "worker"
  }
}

# Worker gets S3 access
resource "aws_iam_role_policy_attachment" "worker_s3_access" {
  role       = aws_iam_role.ethereum_worker.name
  policy_arn = aws_iam_policy.ethereum_s3_access.arn
}

# Worker gets TigerData secret access
resource "aws_iam_role_policy_attachment" "worker_tigerdata_secret" {
  role       = aws_iam_role.ethereum_worker.name
  policy_arn = aws_iam_policy.tigerdata_secret_read.arn
}

# Worker gets Redis secret access
resource "aws_iam_role_policy_attachment" "worker_redis_secret" {
  role       = aws_iam_role.ethereum_worker.name
  policy_arn = aws_iam_policy.ethereum_redis_secret_read.arn
}

# Worker gets SQS consume access (defined in messaging.tf)
resource "aws_iam_role_policy_attachment" "worker_sqs_consume" {
  role       = aws_iam_role.ethereum_worker.name
  policy_arn = aws_iam_policy.ethereum_sqs_consume.arn
}
