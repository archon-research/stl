# =============================================================================
# Initial Secrets for Main Infrastructure
# =============================================================================
# Bootstrap creates the initial secrets that main infrastructure will use.
# 
# Required credentials (validated at bootstrap time):
#   - TIGERDATA_PROJECT_ID, TIGERDATA_ACCESS_KEY, TIGERDATA_SECRET_KEY
#   - ALCHEMY_API_KEY
#
# Optional credentials (use "placeholder" if not provided):
#   - COINGECKO_API_KEY, ETHERSCAN_API_KEY
#   - TAILSCALE_AUTH_KEY (only for staging/prod with bastion)

# TigerData API credentials secret
resource "aws_secretsmanager_secret" "tigerdata" {
  name                    = "${local.prefix}-tigerdata"
  description             = "TigerData (TimescaleDB) API credentials for ${var.environment}"
  recovery_window_in_days = var.environment == "sentineldev" ? 0 : 7

  tags = {
    Name        = "${local.prefix}-tigerdata"
    Environment = var.environment
    ManagedBy   = "bootstrap"
  }
}

resource "aws_secretsmanager_secret_version" "tigerdata" {
  secret_id = aws_secretsmanager_secret.tigerdata.id
  secret_string = jsonencode({
    project_id = var.tigerdata_project_id
    access_key = var.tigerdata_access_key
    secret_key = var.tigerdata_secret_key
  })

  lifecycle {
    ignore_changes = [secret_string]
  }
}

# TODO: Rename watcher_config to blockchain_api_keys or external_api_keys
# This secret contains API keys for multiple external services (Alchemy, CoinGecko, Etherscan),
# not just watcher configuration. The name is historical and should be refactored.

# Consolidated external API keys secret (Alchemy, CoinGecko, Etherscan)
resource "aws_secretsmanager_secret" "watcher_config" {
  name                    = "${local.prefix}-watcher-config"
  description             = "External blockchain API keys (Alchemy, CoinGecko, Etherscan) for ${var.environment}"
  recovery_window_in_days = var.environment == "sentineldev" ? 0 : 7

  tags = {
    Name        = "${local.prefix}-watcher-config"
    Environment = var.environment
    ManagedBy   = "bootstrap"
  }
}

resource "aws_secretsmanager_secret_version" "watcher_config" {
  secret_id = aws_secretsmanager_secret.watcher_config.id
  secret_string = jsonencode({
    alchemy_api_key   = var.alchemy_api_key
    coingecko_api_key = var.coingecko_api_key != "" ? var.coingecko_api_key : "placeholder"
    etherscan_api_key = var.etherscan_api_key != "" ? var.etherscan_api_key : "placeholder"
  })

  lifecycle {
    ignore_changes = [secret_string]
  }
}

# Tailscale auth key secret (optional, only for staging/prod with bastion + VPN)
resource "aws_secretsmanager_secret" "tailscale_auth_key" {
  count                   = var.tailscale_auth_key != "" ? 1 : 0
  name                    = "${local.prefix}-tailscale-auth-key"
  description             = "Tailscale auth key for ${var.environment}"
  recovery_window_in_days = var.environment == "sentineldev" ? 0 : 7

  tags = {
    Name        = "${local.prefix}-tailscale-auth-key"
    Environment = var.environment
    ManagedBy   = "bootstrap"
  }
}

resource "aws_secretsmanager_secret_version" "tailscale_auth_key" {
  count         = var.tailscale_auth_key != "" ? 1 : 0
  secret_id     = aws_secretsmanager_secret.tailscale_auth_key[0].id
  secret_string = var.tailscale_auth_key
}

# =============================================================================
# Outputs
# =============================================================================

output "tigerdata_secret_arn" {
  description = "ARN of TigerData credentials secret"
  value       = aws_secretsmanager_secret.tigerdata.arn
}

output "watcher_config_secret_arn" {
  description = "ARN of Watcher config secret"
  value       = aws_secretsmanager_secret.watcher_config.arn
}

output "tailscale_auth_key_secret_name" {
  description = "Name of Tailscale auth key secret (if created)"
  value       = var.tailscale_auth_key != "" ? aws_secretsmanager_secret.tailscale_auth_key[0].name : ""
  sensitive   = true
}
