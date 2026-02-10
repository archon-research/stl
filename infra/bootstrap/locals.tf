locals {
  prefix            = "${var.project_name}-${var.environment}"
  prefix_lowercase  = lower(local.prefix)
  state_bucket_name = var.environment == "sentineldev" ? "${local.prefix_lowercase}-terraform-state-${random_id.bucket_suffix.hex}" : "${local.prefix_lowercase}-terraform-state"
  locks_table_name  = var.environment == "sentineldev" ? "${local.prefix_lowercase}-terraform-locks-${random_id.bucket_suffix.hex}" : "${local.prefix_lowercase}-terraform-locks"
}
