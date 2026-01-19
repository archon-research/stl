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
