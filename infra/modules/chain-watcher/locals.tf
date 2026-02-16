# =============================================================================
# Chain Watcher Module - Locals
# =============================================================================

locals {
  # Naming convention: {prefix}-{chain_name}-{resource}
  name_prefix      = "${var.prefix}-${var.chain_name}"
  prefix_lowercase = lower(var.prefix)

  # Resource name suffix (dev only, includes hyphen if present)
  resource_suffix = var.resource_suffix != "" ? "-${var.resource_suffix}" : ""

  # S3 bucket suffix: staging keeps existing suffix, new envs use resource_suffix or none
  bucket_suffix = var.environment == "sentinelstaging" ? "-89d540d0" : local.resource_suffix
}
