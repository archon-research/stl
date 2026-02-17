# =============================================================================
# IAM Roles for ECS Tasks
# =============================================================================
# Chain-specific watcher/worker roles are defined in the chain-watcher module.
# This file contains the shared trust policy and legacy role.

# -----------------------------------------------------------------------------
# Trust Policy (shared by all ECS task roles at root level)
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
# Legacy Role (kept for backward compatibility during migration)
# -----------------------------------------------------------------------------
# TODO: Remove after migrating ECS tasks to new roles

resource "aws_iam_role" "ethereum_raw_data_access" {
  name               = "${local.prefix}-ethereum-raw-data-access"
  assume_role_policy = data.aws_iam_policy_document.ecs_task_assume_role.json

  tags = {
    Name       = "${local.prefix}-ethereum-raw-data-access"
    Blockchain = "ethereum"
    Deprecated = "true"
  }
}


# Attach TigerData secret read policy to allow ECS tasks to fetch DB credentials
resource "aws_iam_role_policy_attachment" "tigerdata_secret_access" {
  role       = aws_iam_role.ethereum_raw_data_access.name
  policy_arn = aws_iam_policy.tigerdata_secret_read.arn
}

# Attach Ethereum Redis secret read policy to legacy role
resource "aws_iam_role_policy_attachment" "legacy_ethereum_redis_secret_access" {
  role       = aws_iam_role.ethereum_raw_data_access.name
  policy_arn = module.ethereum.redis_secret_read_policy_arn
}
