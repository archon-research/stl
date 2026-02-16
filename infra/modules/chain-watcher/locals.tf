# =============================================================================
# Chain Watcher Module - Locals
# =============================================================================

locals {
  # Naming convention: {prefix}-{chain_name}-{resource}
  name_prefix      = "${var.prefix}-${var.chain_name}"
  prefix_lowercase = lower(var.prefix)

  # Resource name suffix (dev only, includes hyphen if present)
  resource_suffix = var.resource_suffix != "" ? "-${var.resource_suffix}" : ""
}
