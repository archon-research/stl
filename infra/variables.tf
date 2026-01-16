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
