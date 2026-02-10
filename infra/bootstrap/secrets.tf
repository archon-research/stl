# =============================================================================
# Initial Secrets for Main Infrastructure
# =============================================================================
# Bootstrap creates the initial secrets that main infrastructure will use.
# These are populated from variables passed during tf-bootstrap.

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
  count         = var.tigerdata_project_id != "" ? 1 : 0
  secret_id     = aws_secretsmanager_secret.tigerdata.id
  secret_string = jsonencode({
    project_id = var.tigerdata_project_id
    access_key = var.tigerdata_access_key
    secret_key = var.tigerdata_secret_key
  })
}

# Watcher config secret (Alchemy API key)
resource "aws_secretsmanager_secret" "watcher_config" {
  name                    = "${local.prefix}-watcher-config"
  description             = "Watcher service configuration for ${var.environment}"
  recovery_window_in_days = var.environment == "sentineldev" ? 0 : 7

  tags = {
    Name        = "${local.prefix}-watcher-config"
    Environment = var.environment
    ManagedBy   = "bootstrap"
  }
}

resource "aws_secretsmanager_secret_version" "watcher_config" {
  count         = var.alchemy_api_key != "" ? 1 : 0
  secret_id     = aws_secretsmanager_secret.watcher_config.id
  secret_string = jsonencode({
    alchemy_api_key = var.alchemy_api_key
  })
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
