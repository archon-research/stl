variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "eu-west-1"
}

variable "project_name" {
  description = "Name of the project, used as prefix for all resources"
  type        = string
  default     = "stl"
}

variable "environment" {
  description = "Environment name (e.g. sentinelstaging, sentinelprod)"
  type        = string
  # No default - must be specified explicitly
}

variable "resource_suffix" {
  description = "Random suffix for resource naming (from bootstrap, dev only)"
  type        = string
  default     = ""
}

# -----------------------------------------------------------------------------
# TigerData (TimescaleDB) Configuration
# -----------------------------------------------------------------------------
# TigerData (TimescaleDB) Configuration
# -----------------------------------------------------------------------------
# Credentials are stored in AWS Secrets Manager (see secrets.tf)
# These variables are only used on initial secret creation.
# After that, credentials are read from Secrets Manager and these are ignored.

variable "tigerdata_project_id" {
  description = "TigerData project ID from console. Set via TF_VAR_tigerdata_project_id env var."
  type        = string
  # No default - must be provided via environment variable or -var flag
}

variable "tigerdata_access_key" {
  description = "TigerData API access key. Set via TF_VAR_tigerdata_access_key env var."
  type        = string
  sensitive   = true
  # No default - must be provided via environment variable or -var flag
}

variable "tigerdata_secret_key" {
  description = "TigerData API secret key. Set via TF_VAR_tigerdata_secret_key env var."
  type        = string
  sensitive   = true
  # No default - must be provided via environment variable or -var flag
}

variable "tigerdata_milli_cpu" {
  description = "TigerData CPU in millicores (500, 1000, 2000, 4000, 8000, 16000, 32000)"
  type        = number
  default     = 500 # 0.5 CPU - smallest for staging
}

variable "tigerdata_memory_gb" {
  description = "TigerData memory in GB (2, 4, 8, 16, 32, 64, 128)"
  type        = number
  default     = 2 # Smallest for staging
}

variable "tigerdata_ha_replicas" {
  description = "Number of HA replicas (0 for staging, 1 for prod)"
  type        = number
  default     = 0
}

# -----------------------------------------------------------------------------
# ElastiCache Redis Configuration
# -----------------------------------------------------------------------------

variable "redis_node_type" {
  description = "ElastiCache node type. Use cache.t4g.micro for staging, cache.r7g.large+ for prod"
  type        = string
}

variable "redis_engine_version" {
  description = "Redis engine version"
  type        = string
}

variable "redis_num_cache_clusters" {
  description = "Number of cache clusters (nodes). 1 for staging, 2+ for prod (enables HA)"
  type        = number
}

variable "redis_transit_encryption" {
  description = "Enable TLS for Redis connections. Recommended for prod"
  type        = bool
}

variable "redis_snapshot_retention" {
  description = "Number of days to retain snapshots. 0 disables snapshots"
  type        = number
}

# -----------------------------------------------------------------------------
# ECS Watcher Configuration
# -----------------------------------------------------------------------------

variable "watcher_cpu" {
  description = "CPU units for Watcher task (256, 512, 1024, 2048, 4096)"
  type        = number
  default     = 256
}

variable "watcher_memory" {
  description = "Memory for Watcher task in MB (512, 1024, 2048, etc.)"
  type        = number
  default     = 512
}

variable "watcher_desired_count" {
  description = "Number of Watcher tasks to run (should be 1 - singleton)"
  type        = number
  default     = 1
}

variable "watcher_image_tag" {
  description = "Docker image tag for Watcher"
  type        = string
  default     = "latest"
}

variable "chain_id" {
  description = "Blockchain chain ID (1 = Ethereum mainnet)"
  type        = number
  default     = 1
}

variable "alchemy_api_key" {
  description = "Alchemy API key. Set via TF_VAR_alchemy_api_key env var."
  type        = string
  sensitive   = true
  default     = "placeholder" # Will be updated in Secrets Manager
}

variable "alchemy_http_url" {
  description = "Alchemy HTTP RPC base URL"
  type        = string
  default     = "https://eth-mainnet.g.alchemy.com/v2"
}

variable "alchemy_ws_url" {
  description = "Alchemy WebSocket RPC base URL"
  type        = string
  default     = "wss://eth-mainnet.g.alchemy.com/v2"
}

# -----------------------------------------------------------------------------
# ECS Backup Worker Configuration
# -----------------------------------------------------------------------------

variable "backup_worker_cpu" {
  description = "CPU units for Backup Worker task (256, 512, 1024, 2048, 4096)"
  type        = number
  default     = 256
}

variable "backup_worker_memory" {
  description = "Memory for Backup Worker task in MB (512, 1024, 2048, etc.)"
  type        = number
  default     = 512
}

variable "backup_worker_desired_count" {
  description = "Number of Backup Worker tasks to run"
  type        = number
  default     = 1
}

variable "backup_worker_image_tag" {
  description = "Docker image tag for Backup Worker"
  type        = string
  default     = "latest"
}

variable "backup_worker_workers" {
  description = "Number of concurrent workers within each Backup Worker task"
  type        = number
  default     = 2
}
