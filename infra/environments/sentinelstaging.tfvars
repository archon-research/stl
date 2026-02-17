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

# Staging: scaled up for oracle backfill (was 500/2)
tigerdata_milli_cpu   = 2000 # 2 CPU
tigerdata_memory_gb   = 8    # 8 GB RAM
tigerdata_ha_replicas = 0    # No HA for staging

# -----------------------------------------------------------------------------
# ElastiCache Redis Configuration
# -----------------------------------------------------------------------------

# Staging: memory-optimized for block cache (~6GB steady state)
redis_node_type          = "cache.r7g.large" # 13.07 GB, ~$150/month
redis_engine_version     = "8.0"              # Valkey 8.0 (AWS Redis-compatible fork)
redis_num_cache_clusters = 1                  # Single node, no HA
redis_transit_encryption = false              # No TLS for simplicity
redis_snapshot_retention = 0                  # No backups

# -----------------------------------------------------------------------------
# ECS Watcher Configuration
# -----------------------------------------------------------------------------

# Staging: 4 vCPU with 8GB memory (Fargate Graviton)
watcher_cpu           = 4096
watcher_memory        = 8192
watcher_desired_count = 1        # Singleton
watcher_image_tag     = "latest" # Override in CI/CD

# Ethereum mainnet
alchemy_http_url = "https://eth-mainnet.g.alchemy.com/v2"
alchemy_ws_url   = "wss://eth-mainnet.g.alchemy.com/v2"
# alchemy_api_key - set via TF_VAR_alchemy_api_key environment variable

# -----------------------------------------------------------------------------
# ECS Backup Worker Configuration
# -----------------------------------------------------------------------------

# Staging: minimal resources for backup worker
backup_worker_cpu           = 1024
backup_worker_memory        = 2048
backup_worker_desired_count = 1        # Single instance
backup_worker_image_tag     = "latest" # Override in CI/CD
backup_worker_workers       = 2        # Concurrent workers per task

# -----------------------------------------------------------------------------
# ECS Oracle Price Worker Configuration
# -----------------------------------------------------------------------------

oracle_price_worker_cpu           = 1024
oracle_price_worker_memory        = 2048
oracle_price_worker_desired_count = 1        # Single instance
oracle_price_worker_image_tag     = "latest" # Override in CI/CD

# -----------------------------------------------------------------------------
# Avalanche C-Chain Configuration
# -----------------------------------------------------------------------------

# RPC endpoints
avalanche_alchemy_http_url = "https://avax-mainnet.g.alchemy.com/v2"
avalanche_alchemy_ws_url   = "wss://avax-mainnet.g.alchemy.com/v2"

# Watcher sizing (Fargate Graviton)
avalanche_watcher_cpu           = 4096
avalanche_watcher_memory        = 8192
avalanche_watcher_desired_count = 1        # Singleton
avalanche_watcher_image_tag     = "latest" # Override in CI/CD

# Backup worker sizing
avalanche_backup_worker_cpu           = 1024
avalanche_backup_worker_memory        = 2048
avalanche_backup_worker_desired_count = 1        # Single instance
avalanche_backup_worker_image_tag     = "latest" # Override in CI/CD
avalanche_backup_worker_workers       = 2        # Concurrent workers per task

# Redis
avalanche_redis_node_type          = "cache.r7g.large" # 13.07 GB
avalanche_redis_engine_version     = "8.0"              # Valkey 8.0
avalanche_redis_num_cache_clusters = 1                  # Single node, no HA
avalanche_redis_transit_encryption = false              # No TLS for simplicity
avalanche_redis_snapshot_retention = 0                  # No backups

# -----------------------------------------------------------------------------
# Bastion Host Configuration
# -----------------------------------------------------------------------------

# Enable bastion for local access to TigerData and Redis via Tailscale
bastion_enabled       = true
bastion_instance_type = "t4g.nano" # Smallest ARM instance (~$3/month)

# Tailscale auth key stored in Secrets Manager (create manually first)
# Tailscale auth key stored in Secrets Manager (auto-created during bootstrap)
# Set via: make tf-bootstrap ENV=sentinelstaging TAILSCALE_AUTH_KEY="tskey_..."

# Enable Tailscale for staging only (NOT for production)
tailscale_enabled = true
