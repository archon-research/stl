variable "aws_region" {
  description = "AWS region for resources"
  type        = string
}

variable "project_name" {
  description = "Name of the project, used as prefix for all resources"
  type        = string
  default     = "stl"
}

variable "environment" {
  description = "Environment name (e.g. sentinelstaging, sentinelprod)"
  type        = string

  validation {
    condition     = contains(["sentineldev", "sentinelstaging", "sentinelprod"], var.environment)
    error_message = "Environment must be one of: sentineldev, sentinelstaging, sentinelprod."
  }
}

variable "resource_suffix" {
  description = "Random suffix for resource naming (from bootstrap, dev only)"
  type        = string
  default     = ""
}

# -----------------------------------------------------------------------------
# TigerData (TimescaleDB) Configuration
# -----------------------------------------------------------------------------
# Credentials are stored in AWS Secrets Manager (see secrets.tf)
# These variables are only used on initial secret creation.
# After that, credentials are read from Secrets Manager and these are ignored.

variable "tigerdata_project_id" {
  description = "TigerData project ID from console. Set via TF_VAR_tigerdata_project_id env var."
  type        = string

  validation {
    condition     = can(regex("^[a-z0-9-]+$", var.tigerdata_project_id))
    error_message = "TigerData project ID must be set and contain only lowercase letters, numbers, and hyphens."
  }
}

variable "tigerdata_access_key" {
  description = "TigerData API access key. Set via TF_VAR_tigerdata_access_key env var."
  type        = string
  sensitive   = true

  validation {
    condition     = length(var.tigerdata_access_key) > 0
    error_message = "TigerData access key must be set. Add to .env file or set TF_VAR_tigerdata_access_key."
  }
}

variable "tigerdata_secret_key" {
  description = "TigerData API secret key. Set via TF_VAR_tigerdata_secret_key env var."
  type        = string
  sensitive   = true

  validation {
    condition     = length(var.tigerdata_secret_key) > 0
    error_message = "TigerData secret key must be set. Add to .env file or set TF_VAR_tigerdata_secret_key."
  }
}

variable "tigerdata_milli_cpu" {
  description = "TigerData CPU in millicores (500, 1000, 2000, 4000, 8000, 16000, 32000)"
  type        = number
}

variable "tigerdata_memory_gb" {
  description = "TigerData memory in GB (2, 4, 8, 16, 32, 64, 128)"
  type        = number
}

variable "tigerdata_ha_replicas" {
  description = "Number of HA replicas (0 for staging, 1 for prod)"
  type        = number
}

# -----------------------------------------------------------------------------
# Ethereum ElastiCache Redis Configuration
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
# Ethereum ECS Watcher Configuration
# -----------------------------------------------------------------------------

variable "watcher_cpu" {
  description = "CPU units for Watcher task (256, 512, 1024, 2048, 4096)"
  type        = number
}

variable "watcher_memory" {
  description = "Memory for Watcher task in MB (512, 1024, 2048, etc.)"
  type        = number
}

variable "watcher_desired_count" {
  description = "Number of Watcher tasks to run (should be 1 - singleton)"
  type        = number
}

variable "watcher_image_tag" {
  description = "Docker image tag for Watcher"
  type        = string
}

variable "alchemy_api_key" {
  description = "Alchemy API key. Set via TF_VAR_alchemy_api_key env var."
  type        = string
  sensitive   = true

  validation {
    condition     = length(var.alchemy_api_key) > 10
    error_message = "Alchemy API key must be set (get from https://dashboard.alchemy.com/apps). Add to .env file or set TF_VAR_alchemy_api_key."
  }
}

variable "coingecko_api_key" {
  description = "CoinGecko Pro API key. Set via TF_VAR_coingecko_api_key env var."
  type        = string
  sensitive   = true
}

variable "etherscan_api_key" {
  description = "Etherscan API key. Set via TF_VAR_etherscan_api_key env var."
  type        = string
  sensitive   = true
}

variable "alchemy_http_url" {
  description = "Alchemy HTTP RPC base URL"
  type        = string
}

variable "alchemy_ws_url" {
  description = "Alchemy WebSocket RPC base URL"
  type        = string
}

# -----------------------------------------------------------------------------
# Ethereum ECS Backup Worker Configuration
# -----------------------------------------------------------------------------

variable "backup_worker_cpu" {
  description = "CPU units for Backup Worker task (256, 512, 1024, 2048, 4096)"
  type        = number
}

variable "backup_worker_memory" {
  description = "Memory for Backup Worker task in MB (512, 1024, 2048, etc.)"
  type        = number
}

variable "backup_worker_desired_count" {
  description = "Number of Backup Worker tasks to run"
  type        = number
}

variable "backup_worker_image_tag" {
  description = "Docker image tag for Backup Worker"
  type        = string
}

variable "backup_worker_workers" {
  description = "Number of concurrent workers within each Backup Worker task"
  type        = number
}

# -----------------------------------------------------------------------------
# ECS Oracle Price Worker Configuration
# -----------------------------------------------------------------------------

variable "oracle_price_worker_cpu" {
  description = "CPU units for Oracle Price Worker task (256, 512, 1024, 2048, 4096)"
  type        = number
}

variable "oracle_price_worker_memory" {
  description = "Memory for Oracle Price Worker task in MB (512, 1024, 2048, etc.)"
  type        = number
}

variable "oracle_price_worker_desired_count" {
  description = "Number of Oracle Price Worker tasks to run"
  type        = number
}

variable "oracle_price_worker_image_tag" {
  description = "Docker image tag for Oracle Price Worker"
  type        = string
}

# -----------------------------------------------------------------------------
# Avalanche C-Chain Configuration
# -----------------------------------------------------------------------------

variable "avalanche_alchemy_http_url" {
  description = "Alchemy HTTP RPC base URL for Avalanche C-Chain"
  type        = string
}

variable "avalanche_alchemy_ws_url" {
  description = "Alchemy WebSocket RPC base URL for Avalanche C-Chain"
  type        = string
}

variable "avalanche_watcher_cpu" {
  description = "CPU units for Avalanche Watcher task"
  type        = number
}

variable "avalanche_watcher_memory" {
  description = "Memory for Avalanche Watcher task in MB"
  type        = number
}

variable "avalanche_watcher_desired_count" {
  description = "Number of Avalanche Watcher tasks"
  type        = number
}

variable "avalanche_watcher_image_tag" {
  description = "Docker image tag for Avalanche Watcher"
  type        = string
}

variable "avalanche_backup_worker_cpu" {
  description = "CPU units for Avalanche Backup Worker task"
  type        = number
}

variable "avalanche_backup_worker_memory" {
  description = "Memory for Avalanche Backup Worker task in MB"
  type        = number
}

variable "avalanche_backup_worker_desired_count" {
  description = "Number of Avalanche Backup Worker tasks"
  type        = number
}

variable "avalanche_backup_worker_image_tag" {
  description = "Docker image tag for Avalanche Backup Worker"
  type        = string
}

variable "avalanche_backup_worker_workers" {
  description = "Number of concurrent workers per Avalanche Backup Worker task"
  type        = number
}

variable "avalanche_redis_node_type" {
  description = "ElastiCache node type for Avalanche"
  type        = string
}

variable "avalanche_redis_engine_version" {
  description = "Redis engine version for Avalanche"
  type        = string
}

variable "avalanche_redis_num_cache_clusters" {
  description = "Number of cache clusters for Avalanche"
  type        = number
}

variable "avalanche_redis_transit_encryption" {
  description = "Enable TLS for Avalanche Redis"
  type        = bool
}

variable "avalanche_redis_snapshot_retention" {
  description = "Snapshot retention days for Avalanche Redis"
  type        = number
}

# -----------------------------------------------------------------------------
# Bastion Host Configuration
# -----------------------------------------------------------------------------

variable "bastion_enabled" {
  description = "Whether to create the bastion host"
  type        = bool
  default     = false
}

variable "bastion_instance_type" {
  description = "EC2 instance type for bastion (t4g.nano is sufficient)"
  type        = string
  default     = "t4g.nano"
}

variable "tailscale_auth_key_secret_name" {
  description = "Name of the Secrets Manager secret containing the Tailscale auth key"
  type        = string
  default     = ""
}

variable "tailscale_enabled" {
  description = "Install and configure Tailscale on the bastion host (should only be enabled for sentinelstaging)"
  type        = bool
  default     = false
}

# -----------------------------------------------------------------------------
# ECS SparkLend Position Tracker Configuration
# -----------------------------------------------------------------------------

variable "sparklend_position_tracker_cpu" {
  description = "CPU units for SparkLend Position Tracker task (256, 512, 1024, 2048, 4096)"
  type        = number
  default     = 256
}

variable "sparklend_position_tracker_memory" {
  description = "Memory for SparkLend Position Tracker task in MB (512, 1024, 2048, etc.)"
  type        = number
  default     = 512
}

variable "sparklend_position_tracker_desired_count" {
  description = "Number of SparkLend Position Tracker tasks to run"
  type        = number
  default     = 1
}

variable "sparklend_position_tracker_image_tag" {
  description = "Docker image tag for SparkLend Position Tracker"
  type        = string
  default     = "latest"
}
