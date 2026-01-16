# =============================================================================
# AWS Secrets Manager - TigerData Credentials
# =============================================================================
# Stores TigerData credentials securely for use by ECS tasks.

# -----------------------------------------------------------------------------
# TigerData API Credentials Secret (for Terraform provider)
# -----------------------------------------------------------------------------

resource "aws_secretsmanager_secret" "tigerdata" {
  name        = "${local.prefix}-tigerdata"
  description = "TigerData (TimescaleDB) API credentials for ${var.environment}"

  # Allow recovery for 7 days (minimum) - set to 0 to force delete immediately
  recovery_window_in_days = 7

  tags = {
    Name    = "${local.prefix}-tigerdata"
    Service = "tigerdata"
  }
}

# Initial placeholder value - will be replaced via CLI
resource "aws_secretsmanager_secret_version" "tigerdata" {
  secret_id = aws_secretsmanager_secret.tigerdata.id
  secret_string = jsonencode({
    project_id = var.tigerdata_project_id
    access_key = var.tigerdata_access_key
    secret_key = var.tigerdata_secret_key
  })

  lifecycle {
    # Don't update if the secret was changed outside of Terraform
    # This prevents overwriting manually-set credentials
    ignore_changes = [secret_string]
  }
}

# -----------------------------------------------------------------------------
# TigerData Database Credentials Secret (for ECS tasks)
# -----------------------------------------------------------------------------

resource "aws_secretsmanager_secret" "tigerdata_db" {
  name        = "${local.prefix}-tigerdata-db"
  description = "TigerData database connection credentials for ${var.environment}"

  recovery_window_in_days = 7

  tags = {
    Name    = "${local.prefix}-tigerdata-db"
    Service = "tigerdata"
  }
}

resource "aws_secretsmanager_secret_version" "tigerdata_db" {
  secret_id = aws_secretsmanager_secret.tigerdata_db.id
  secret_string = jsonencode({
    username        = timescale_service.main.username
    password        = timescale_service.main.password
    hostname        = timescale_service.main.hostname
    port            = timescale_service.main.port
    pooler_hostname = timescale_service.main.pooler_hostname
    # Connection string for convenience
    connection_url = "postgres://${timescale_service.main.username}:${timescale_service.main.password}@${timescale_service.main.hostname}:${timescale_service.main.port}/tsdb?sslmode=require"
    pooler_url     = "postgres://${timescale_service.main.username}:${timescale_service.main.password}@${timescale_service.main.pooler_hostname}:${timescale_service.main.port}/tsdb?sslmode=require"
  })

  lifecycle {
    # Don't update if credentials were rotated outside of Terraform
    ignore_changes = [secret_string]
  }
}

# -----------------------------------------------------------------------------
# IAM Policy for ECS tasks to read the secrets
# -----------------------------------------------------------------------------
# Note: ECS tasks read credentials directly from Secrets Manager at runtime.
# The Timescale provider uses TF_VAR environment variables instead of this
# data source (provider configuration happens before data sources are read).
# -----------------------------------------------------------------------------

data "aws_iam_policy_document" "tigerdata_secret_read" {
  statement {
    sid    = "ReadTigerDataSecrets"
    effect = "Allow"

    actions = [
      "secretsmanager:GetSecretValue",
    ]

    resources = [
      aws_secretsmanager_secret.tigerdata.arn,
      aws_secretsmanager_secret.tigerdata_db.arn,
    ]
  }
}

resource "aws_iam_policy" "tigerdata_secret_read" {
  name        = "${local.prefix}-tigerdata-secret-read"
  description = "Allows reading TigerData credentials from Secrets Manager"
  policy      = data.aws_iam_policy_document.tigerdata_secret_read.json
}

# -----------------------------------------------------------------------------
# Outputs
# -----------------------------------------------------------------------------

output "tigerdata_secret_arn" {
  description = "ARN of the TigerData API credentials secret"
  value       = aws_secretsmanager_secret.tigerdata.arn
}

output "tigerdata_db_secret_arn" {
  description = "ARN of the TigerData database credentials secret (for ECS task definitions)"
  value       = aws_secretsmanager_secret.tigerdata_db.arn
}

output "tigerdata_secret_policy_arn" {
  description = "ARN of the IAM policy for reading TigerData secrets"
  value       = aws_iam_policy.tigerdata_secret_read.arn
}
