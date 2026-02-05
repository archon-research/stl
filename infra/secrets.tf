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

# =============================================================================
# AWS Secrets Manager - CoinGecko API Key
# =============================================================================
# Stores CoinGecko Pro API key for price fetching service.

resource "aws_secretsmanager_secret" "coingecko" {
  name        = "coingecko_api_key"
  description = "CoinGecko Pro API key for price fetching"

  recovery_window_in_days = 7

  tags = {
    Name    = "coingecko_api_key"
    Service = "price-fetcher"
  }

  lifecycle {
    # Secret already exists - prevent recreation on name change
    prevent_destroy = true
  }
}

resource "aws_secretsmanager_secret_version" "coingecko" {
  secret_id     = aws_secretsmanager_secret.coingecko.id
  secret_string = var.coingecko_api_key

  lifecycle {
    # Don't update if the secret was changed outside of Terraform
    ignore_changes = [secret_string]
  }
}

# -----------------------------------------------------------------------------
# IAM Policy for reading CoinGecko secret
# -----------------------------------------------------------------------------

data "aws_iam_policy_document" "coingecko_secret_read" {
  statement {
    sid    = "ReadCoinGeckoSecret"
    effect = "Allow"

    actions = [
      "secretsmanager:GetSecretValue",
    ]

    resources = [
      aws_secretsmanager_secret.coingecko.arn,
    ]
  }
}

resource "aws_iam_policy" "coingecko_secret_read" {
  name        = "${local.prefix}-coingecko-secret-read"
  description = "Allows reading CoinGecko API key from Secrets Manager"
  policy      = data.aws_iam_policy_document.coingecko_secret_read.json
}

output "coingecko_secret_arn" {
  description = "ARN of the CoinGecko API key secret"
  value       = aws_secretsmanager_secret.coingecko.arn
}

output "coingecko_secret_policy_arn" {
  description = "ARN of the IAM policy for reading CoinGecko secret"
  value       = aws_iam_policy.coingecko_secret_read.arn
}

# =============================================================================
# AWS Secrets Manager - Etherscan API Key
# =============================================================================
# Stores Etherscan API key for blockchain data verification.

resource "aws_secretsmanager_secret" "etherscan" {
  name        = "etherscan_api_key"
  description = "Etherscan API key for blockchain data verification"

  recovery_window_in_days = 7

  tags = {
    Name    = "etherscan_api_key"
    Service = "blockchain-verification"
  }
}

resource "aws_secretsmanager_secret_version" "etherscan" {
  secret_id     = aws_secretsmanager_secret.etherscan.id
  secret_string = var.etherscan_api_key

  lifecycle {
    # Don't update if the secret was changed outside of Terraform
    ignore_changes = [secret_string]
  }
}

# -----------------------------------------------------------------------------
# IAM Policy for reading Etherscan secret
# -----------------------------------------------------------------------------

data "aws_iam_policy_document" "etherscan_secret_read" {
  statement {
    sid    = "ReadEtherscanSecret"
    effect = "Allow"

    actions = [
      "secretsmanager:GetSecretValue",
    ]

    resources = [
      aws_secretsmanager_secret.etherscan.arn,
    ]
  }
}

resource "aws_iam_policy" "etherscan_secret_read" {
  name        = "${local.prefix}-etherscan-secret-read"
  description = "Allows reading Etherscan API key from Secrets Manager"
  policy      = data.aws_iam_policy_document.etherscan_secret_read.json
}

output "etherscan_secret_arn" {
  description = "ARN of the Etherscan API key secret"
  value       = aws_secretsmanager_secret.etherscan.arn
}

output "etherscan_secret_policy_arn" {
  description = "ARN of the IAM policy for reading Etherscan secret"
  value       = aws_iam_policy.etherscan_secret_read.arn
}
