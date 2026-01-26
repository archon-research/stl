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
watcher_cpu           = 4096     # 4 vCPU
watcher_memory        = 8192     # 8 GB
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

# Production: More resources for reliable backup processing
backup_worker_cpu           = 512      # 0.5 vCPU
backup_worker_memory        = 1024     # 1 GB
backup_worker_desired_count = 1        # Single instance (SQS handles backpressure)
backup_worker_image_tag     = "latest" # Override in CI/CD
backup_worker_workers       = 4        # More concurrent workers for throughput
