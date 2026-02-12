# =============================================================================
# TigerData Application User Credentials
# =============================================================================
# Generates secure passwords and stores them in AWS Secrets Manager.
# Database users/roles are created by migration, passwords set via Makefile.
#
# See docs/tigerdata-operations.md for setup workflow.

# -----------------------------------------------------------------------------
# Generate Secure Passwords
# -----------------------------------------------------------------------------

resource "random_password" "stl_read_write" {
  length           = 32
  special          = true
  override_special = "!#$%&*()-_=+[]{}<>:?"
}

resource "random_password" "stl_read_only" {
  length           = 32
  special          = true
  override_special = "!#$%&*()-_=+[]{}<>:?"
}

# -----------------------------------------------------------------------------
# Secrets Manager - Application User Credentials
# -----------------------------------------------------------------------------

resource "aws_secretsmanager_secret" "tigerdata_app" {
  name        = "${local.prefix}-tigerdata-app"
  description = "TigerData read/write application user credentials for ${var.environment}"

  recovery_window_in_days = 7

  tags = {
    Name    = "${local.prefix}-tigerdata-app"
    Service = "tigerdata"
    Access  = "readwrite"
  }
}

resource "aws_secretsmanager_secret_version" "tigerdata_app" {
  secret_id = aws_secretsmanager_secret.tigerdata_app.id
  secret_string = jsonencode({
    username        = "stl_read_write"
    password        = random_password.stl_read_write.result
    hostname        = timescale_service.main.hostname
    port            = timescale_service.main.port
    pooler_hostname = timescale_service.main.pooler_hostname
    database        = "tsdb"
    # Connection strings for convenience
    connection_url = "postgres://stl_read_write:${urlencode(random_password.stl_read_write.result)}@${timescale_service.main.hostname}:${timescale_service.main.port}/tsdb?sslmode=require"
    pooler_url     = "postgres://stl_read_write:${urlencode(random_password.stl_read_write.result)}@${timescale_service.main.pooler_hostname}:${timescale_service.main.port}/tsdb?sslmode=require"
  })

  lifecycle {
    # Hostname/port may show as changed during resize but typically stay the same.
    # To force an update, use: tofu taint aws_secretsmanager_secret_version.tigerdata_app
    ignore_changes = [secret_string]
  }
}

resource "aws_secretsmanager_secret" "tigerdata_readonly" {
  name        = "${local.prefix}-tigerdata-readonly"
  description = "TigerData read-only application user credentials for ${var.environment}"

  recovery_window_in_days = 7

  tags = {
    Name    = "${local.prefix}-tigerdata-readonly"
    Service = "tigerdata"
    Access  = "readonly"
  }
}

resource "aws_secretsmanager_secret_version" "tigerdata_readonly" {
  secret_id = aws_secretsmanager_secret.tigerdata_readonly.id
  secret_string = jsonencode({
    username        = "stl_read_only"
    password        = random_password.stl_read_only.result
    hostname        = timescale_service.main.hostname
    port            = timescale_service.main.port
    pooler_hostname = timescale_service.main.pooler_hostname
    database        = "tsdb"
    # Connection strings for convenience (read-only, use replica if available)
    connection_url         = "postgres://stl_read_only:${urlencode(random_password.stl_read_only.result)}@${timescale_service.main.hostname}:${timescale_service.main.port}/tsdb?sslmode=require"
    pooler_url             = "postgres://stl_read_only:${urlencode(random_password.stl_read_only.result)}@${timescale_service.main.pooler_hostname}:${timescale_service.main.port}/tsdb?sslmode=require"
    replica_connection_url = try(timescale_service.main.replica_hostname, null) != null ? "postgres://stl_read_only:${urlencode(random_password.stl_read_only.result)}@${timescale_service.main.replica_hostname}:${timescale_service.main.port}/tsdb?sslmode=require" : ""
  })

  lifecycle {
    # Hostname/port may show as changed during resize but typically stay the same.
    # To force an update, use: tofu taint aws_secretsmanager_secret_version.tigerdata_readonly
    ignore_changes = [secret_string]
  }
}

# -----------------------------------------------------------------------------
# IAM Policy for Reading App User Secrets
# -----------------------------------------------------------------------------

data "aws_iam_policy_document" "tigerdata_app_secret_read" {
  statement {
    sid    = "ReadTigerDataAppSecrets"
    effect = "Allow"

    actions = [
      "secretsmanager:GetSecretValue",
    ]

    resources = [
      aws_secretsmanager_secret.tigerdata_app.arn,
      aws_secretsmanager_secret.tigerdata_readonly.arn,
    ]
  }
}

resource "aws_iam_policy" "tigerdata_app_secret_read" {
  name        = "${local.prefix}-tigerdata-app-secret-read"
  description = "Allows reading TigerData application user credentials from Secrets Manager"
  policy      = data.aws_iam_policy_document.tigerdata_app_secret_read.json
}

# -----------------------------------------------------------------------------
# Outputs
# -----------------------------------------------------------------------------

output "tigerdata_app_secret_arn" {
  description = "ARN of the TigerData read/write application user secret"
  value       = aws_secretsmanager_secret.tigerdata_app.arn
}

output "tigerdata_readonly_secret_arn" {
  description = "ARN of the TigerData read-only application user secret"
  value       = aws_secretsmanager_secret.tigerdata_readonly.arn
}

output "tigerdata_app_secret_policy_arn" {
  description = "ARN of the IAM policy for reading TigerData app user secrets"
  value       = aws_iam_policy.tigerdata_app_secret_read.arn
}
