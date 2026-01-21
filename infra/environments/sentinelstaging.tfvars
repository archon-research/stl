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

# -----------------------------------------------------------------------------
# ElastiCache Redis Configuration
# -----------------------------------------------------------------------------

# Staging: minimal single-node setup
redis_node_type          = "cache.t4g.micro" # ~$12/month
redis_engine_version     = "8.0"             # Valkey 8.0 (AWS Redis-compatible fork)
redis_num_cache_clusters = 1                 # Single node, no HA
redis_transit_encryption = false             # No TLS for simplicity
redis_snapshot_retention = 0                 # No backups

# -----------------------------------------------------------------------------
# ECS Watcher Configuration
# -----------------------------------------------------------------------------

# Staging: 4 vCPU with 8GB memory (Fargate Graviton)
watcher_cpu           = 4096     # 4 vCPU
watcher_memory        = 8192     # 8 GB
watcher_desired_count = 1        # Singleton
watcher_image_tag     = "latest" # Override in CI/CD

# Ethereum mainnet
chain_id         = 1
alchemy_http_url = "https://eth-mainnet.g.alchemy.com/v2"
alchemy_ws_url   = "wss://eth-mainnet.g.alchemy.com/v2"
# alchemy_api_key - set via TF_VAR_alchemy_api_key environment variable
