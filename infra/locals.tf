# =============================================================================
# Shared Local Values
# =============================================================================
# Common naming conventions and prefixes used across all resources

locals {
  # Standard naming prefix: ${project}-${environment}
  prefix           = "${var.project_name}-${var.environment}"
  prefix_lowercase = lower(local.prefix)
  
  # Resource name suffix (dev only, includes hyphen if present)
  resource_suffix = var.resource_suffix != "" ? "-${var.resource_suffix}" : ""
}
