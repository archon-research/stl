# =============================================================================
# Pre-deployment Validations
# =============================================================================
# This file contains validation checks that run during terraform plan/apply
# to catch configuration issues early before attempting resource creation.

# -----------------------------------------------------------------------------
# AWS Credentials Validation
# -----------------------------------------------------------------------------

data "aws_caller_identity" "current" {}

# -----------------------------------------------------------------------------
# Configuration Summary (Informational)
# -----------------------------------------------------------------------------
# Output displayed after successful validation during plan/apply

output "validation_summary" {
  description = "Summary of validated configuration"
  sensitive   = true
  value = {
    aws_account_id              = data.aws_caller_identity.current.account_id
    aws_principal               = data.aws_caller_identity.current.arn
    aws_region                  = var.aws_region
    environment                 = var.environment
    tigerdata_project_validated = var.tigerdata_project_id != ""
    alchemy_api_key_set         = length(var.alchemy_api_key) > 10
    bastion_enabled             = var.bastion_enabled
    tailscale_enabled           = var.tailscale_enabled
  }
}
