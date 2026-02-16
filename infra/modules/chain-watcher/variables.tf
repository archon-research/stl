# =============================================================================
# Chain Watcher Module - Variables
# =============================================================================

# -----------------------------------------------------------------------------
# Chain Identity
# -----------------------------------------------------------------------------

variable "chain_name" {
  description = "Short chain name (e.g. 'ethereum', 'avalanche')"
  type        = string
}

variable "chain_id" {
  description = "Blockchain chain ID (e.g. 1, 43114)"
  type        = number
}

# -----------------------------------------------------------------------------
# Shared Resource References
# -----------------------------------------------------------------------------

variable "prefix" {
  description = "Naming prefix: {project}-{environment}"
  type        = string
}

variable "environment" {
  description = "Environment name (sentineldev, sentinelstaging, sentinelprod)"
  type        = string
}

variable "aws_region" {
  description = "AWS region"
  type        = string
}

variable "resource_suffix" {
  description = "Dev-only random suffix for unique naming"
  type        = string
  default     = ""
}

variable "ecs_cluster_id" {
  description = "ECS cluster ID"
  type        = string
}

variable "private_subnet_id" {
  description = "Private subnet ID for ECS tasks"
  type        = string
}

variable "isolated_subnet_id" {
  description = "Isolated subnet ID for Redis"
  type        = string
}

variable "watcher_sg_id" {
  description = "Security group ID for watcher tasks"
  type        = string
}

variable "worker_sg_id" {
  description = "Security group ID for worker tasks"
  type        = string
}

variable "redis_sg_id" {
  description = "Security group ID for Redis"
  type        = string
}

variable "ecs_task_execution_role_arn" {
  description = "ARN of the ECS task execution role"
  type        = string
}

variable "watcher_config_secret_arn" {
  description = "ARN of the watcher config secret (Alchemy API key)"
  type        = string
}

variable "tigerdata_app_secret_arn" {
  description = "ARN of the TigerData app (read_write) credentials secret"
  type        = string
}

variable "tigerdata_app_secret_read_policy_arn" {
  description = "ARN of the IAM policy for reading TigerData app secret"
  type        = string
}

variable "watcher_ecr_url" {
  description = "Watcher ECR repository URL"
  type        = string
}

variable "backup_worker_ecr_url" {
  description = "Backup worker ECR repository URL"
  type        = string
}

# -----------------------------------------------------------------------------
# Chain-Specific Config
# -----------------------------------------------------------------------------

variable "alchemy_http_url" {
  description = "Alchemy HTTP RPC base URL"
  type        = string
}

variable "alchemy_ws_url" {
  description = "Alchemy WebSocket RPC base URL"
  type        = string
}

variable "watcher_command" {
  description = "CLI args for watcher container (e.g. ['--enable-traces=false'])"
  type        = list(string)
  default     = []
}

variable "enable_backfill" {
  description = "Enable backfill in the watcher"
  type        = bool
  default     = true
}

# -----------------------------------------------------------------------------
# Watcher Sizing
# -----------------------------------------------------------------------------

variable "watcher_cpu" {
  description = "CPU units for watcher task"
  type        = number
  default     = 256
}

variable "watcher_memory" {
  description = "Memory for watcher task in MB"
  type        = number
  default     = 512
}

variable "watcher_desired_count" {
  description = "Number of watcher tasks (should be 1)"
  type        = number
  default     = 1
}

variable "watcher_image_tag" {
  description = "Docker image tag for watcher"
  type        = string
  default     = "latest"
}

# -----------------------------------------------------------------------------
# Backup Worker Sizing
# -----------------------------------------------------------------------------

variable "backup_worker_cpu" {
  description = "CPU units for backup worker task"
  type        = number
  default     = 256
}

variable "backup_worker_memory" {
  description = "Memory for backup worker task in MB"
  type        = number
  default     = 512
}

variable "backup_worker_desired_count" {
  description = "Number of backup worker tasks"
  type        = number
  default     = 1
}

variable "backup_worker_image_tag" {
  description = "Docker image tag for backup worker"
  type        = string
  default     = "latest"
}

variable "backup_worker_workers" {
  description = "Number of concurrent workers per backup worker task"
  type        = number
  default     = 2
}

# -----------------------------------------------------------------------------
# Redis Config
# -----------------------------------------------------------------------------

variable "redis_node_type" {
  description = "ElastiCache node type"
  type        = string
}

variable "redis_engine_version" {
  description = "Redis/Valkey engine version"
  type        = string
}

variable "redis_num_cache_clusters" {
  description = "Number of cache clusters (1 for no HA, 2+ for HA)"
  type        = number
}

variable "redis_transit_encryption" {
  description = "Enable TLS for Redis connections"
  type        = bool
}

variable "redis_snapshot_retention" {
  description = "Number of days to retain snapshots (0 disables)"
  type        = number
}

# -----------------------------------------------------------------------------
# SQS Consumers (dynamic)
# -----------------------------------------------------------------------------

variable "sqs_consumers" {
  description = "Map of consumer name to SQS queue configuration"
  type = map(object({
    visibility_timeout_seconds = optional(number, 300)
    message_retention_seconds  = optional(number, 1209600)
    max_receive_count          = optional(number, 3)
  }))
}
