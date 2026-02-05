variable "aws_region" {
  description = "AWS region for resources"
  type        = string
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "project_name" {
  description = "Project name"
  type        = string
}

variable "prefix" {
  description = "Naming prefix for resources"
  type        = string
}

# Redis Configuration
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

# TigerData Configuration
variable "tigerdata_project_id" {
  description = "TigerData project ID"
  type        = string
}

variable "tigerdata_access_key" {
  description = "TigerData access key"
  type        = string
  sensitive   = true
}

variable "tigerdata_secret_key" {
  description = "TigerData secret key"
  type        = string
  sensitive   = true
}

variable "tigerdata_milli_cpu" {
  description = "TigerData CPU in millicores"
  type        = number
}

variable "tigerdata_memory_gb" {
  description = "TigerData memory in GB"
  type        = number
}

variable "tigerdata_ha_replicas" {
  description = "Number of HA replicas"
  type        = number
}
