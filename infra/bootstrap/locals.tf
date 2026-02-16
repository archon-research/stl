locals {
  prefix           = "${var.project_name}-${var.environment}"
  prefix_lowercase = lower(local.prefix)

  # State bucket name with conditional suffix
  # - Dev: random suffix (changes per bootstrap run)
  # - Staging: fixed suffix to match existing bucket (89d540d0)
  # - Prod: fixed suffix (TBD)
  state_bucket_name = (
    var.environment == "sentineldev" ? "${local.prefix_lowercase}-terraform-state-${random_id.bucket_suffix[0].hex}" :
    var.environment == "sentinelstaging" ? "${local.prefix_lowercase}-terraform-state-89d540d0" :
    "${local.prefix_lowercase}-terraform-state"
  )

  # Locks table name (no suffix needed - not globally unique)
  locks_table_name = "${local.prefix_lowercase}-terraform-locks"
}
