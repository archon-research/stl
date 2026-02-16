# Environment-specific configuration
aws_region  = "eu-west-1"
environment = "sentinelprod"

# -----------------------------------------------------------------------------
# TigerData (TimescaleDB) Configuration
# -----------------------------------------------------------------------------
# Note: Credentials should be passed via environment variables or secrets:
#   export TF_VAR_tigerdata_access_key="..."
#   export TF_VAR_tigerdata_secret_key="..."

tigerdata_project_id = "REPLACE_ME" # Get from TigerData console

# Production: Sized for workload with HA
tigerdata_milli_cpu   = 2000 # 2 CPU
tigerdata_memory_gb   = 8    # 8 GB RAM
tigerdata_ha_replicas = 1    # 1 HA replica for failover

# -----------------------------------------------------------------------------
# ElastiCache Redis Configuration
# -----------------------------------------------------------------------------

# Production: Multi-node with read replicas for many readers/writers
# Cache designed for ~4 hour TTLs
redis_node_type          = "cache.r7g.large" # 13GB memory, memory-optimized
redis_engine_version     = "8.0"             # Valkey 8.0 (AWS Redis-compatible fork)
redis_num_cache_clusters = 3                 # 1 primary + 2 read replicas
redis_transit_encryption = true              # TLS enabled
redis_snapshot_retention = 7                 # 7 days of backups

# -----------------------------------------------------------------------------
# ECS Watcher Configuration
# -----------------------------------------------------------------------------

# Production: Higher resources for block watching
watcher_cpu           = 2048     # 2 vCPU
watcher_memory        = 4096     # 4 GB
watcher_desired_count = 1        # Singleton
watcher_image_tag     = "latest" # Override in CI/CD

# Ethereum mainnet
alchemy_http_url = "https://eth-mainnet.g.alchemy.com/v2"
alchemy_ws_url   = "wss://eth-mainnet.g.alchemy.com/v2"
# alchemy_api_key - set via TF_VAR_alchemy_api_key environment variable

# -----------------------------------------------------------------------------
# ECS Backup Worker Configuration
# -----------------------------------------------------------------------------

# Production: More resources for reliable backup processing
backup_worker_cpu           = 512      # 0.5 vCPU
backup_worker_memory        = 1024     # 1 GB
backup_worker_desired_count = 1        # Single instance (SQS handles backpressure)
backup_worker_image_tag     = "latest" # Override in CI/CD
backup_worker_workers       = 4        # More concurrent workers for throughput

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
avalanche_watcher_cpu           = 2048     # 2 vCPU
avalanche_watcher_memory        = 4096     # 4 GB
avalanche_watcher_desired_count = 1        # Singleton
avalanche_watcher_image_tag     = "latest" # Override in CI/CD

# Backup worker sizing
avalanche_backup_worker_cpu           = 512
avalanche_backup_worker_memory        = 1024
avalanche_backup_worker_desired_count = 1        # Single instance
avalanche_backup_worker_image_tag     = "latest" # Override in CI/CD
avalanche_backup_worker_workers       = 4        # More concurrent workers for throughput

# Redis (production: HA with TLS)
avalanche_redis_node_type          = "cache.r7g.large" # 13 GB memory
avalanche_redis_engine_version     = "8.0"             # Valkey 8.0
avalanche_redis_num_cache_clusters = 3                  # 1 primary + 2 read replicas
avalanche_redis_transit_encryption = true              # TLS enabled
avalanche_redis_snapshot_retention = 7                 # 7 days of backups
