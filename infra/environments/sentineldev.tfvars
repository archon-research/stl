# Environment-specific configuration
aws_region  = "eu-west-1"
environment = "sentineldev"

# -----------------------------------------------------------------------------
# TigerData (TimescaleDB) Configuration
# -----------------------------------------------------------------------------
# Note: Credentials should be passed via environment variables or secrets:
#   export TF_VAR_tigerdata_access_key="..."
#   export TF_VAR_tigerdata_secret_key="..."

tigerdata_project_id = "p71n930y81" # Get from TigerData console

# Dev: smallest instance
tigerdata_milli_cpu   = 500 # 0.5 CPU
tigerdata_memory_gb   = 2   # 2 GB RAM
tigerdata_ha_replicas = 0   # No HA for dev

# -----------------------------------------------------------------------------
# ElastiCache Redis Configuration
# -----------------------------------------------------------------------------

# Dev: burstable tier for cost savings
redis_node_type          = "cache.t4g.small" # 1.55 GB, ~$30/month
redis_engine_version     = "8.0"             # Valkey 8.0 (AWS Redis-compatible fork)
redis_num_cache_clusters = 1                  # Single node, no HA
redis_transit_encryption = false              # No TLS for simplicity
redis_snapshot_retention = 0                  # No backups

# -----------------------------------------------------------------------------
# ECS Watcher Configuration
# -----------------------------------------------------------------------------

# Dev: minimal sizing for cost savings
watcher_cpu           = 512      # 0.5 vCPU
watcher_memory        = 1024     # 1 GB
watcher_desired_count = 1        # Singleton
watcher_image_tag     = "latest" # Override in CI/CD

# Ethereum mainnet
chain_id         = 1
alchemy_http_url = "https://eth-mainnet.g.alchemy.com/v2"
alchemy_ws_url   = "wss://eth-mainnet.g.alchemy.com/v2"
# alchemy_api_key - set via TF_VAR_alchemy_api_key environment variable

# -----------------------------------------------------------------------------
# ECS Backup Worker Configuration
# -----------------------------------------------------------------------------

# Dev: minimal resources for backup worker
backup_worker_cpu           = 256      # 0.25 vCPU
backup_worker_memory        = 512      # 0.5 GB
backup_worker_desired_count = 1        # Single instance
backup_worker_image_tag     = "latest" # Override in CI/CD
backup_worker_workers       = 2        # Concurrent workers per task

# Bastion Host Configuration
# Dev: bastion ENABLED for TigerData tunnel access
bastion_enabled = true

# Tailscale VPN Configuration  
# Only activated if bastion_enabled=true
tailscale_enabled = true
# tailscale_auth_key_secret_name is auto-set to "stl-sentineldev-tailscale-auth-key"
# (created during bootstrap if TAILSCALE_AUTH_KEY is provided)
