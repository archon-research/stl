# Environment-specific configuration
aws_region  = "eu-west-1"
environment = "sentinelstaging"

# -----------------------------------------------------------------------------
# TigerData (TimescaleDB) Configuration
# -----------------------------------------------------------------------------
# Note: Credentials should be passed via environment variables or secrets:
#   export TF_VAR_tigerdata_access_key="..."
#   export TF_VAR_tigerdata_secret_key="..."

tigerdata_project_id = "p71n930y81" # Get from TigerData console

# Staging: smallest instance
tigerdata_milli_cpu   = 500 # 0.5 CPU
tigerdata_memory_gb   = 2   # 2 GB RAM
tigerdata_ha_replicas = 0   # No HA for staging
