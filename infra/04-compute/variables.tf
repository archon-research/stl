variable "aws_region" {
  description = "AWS region for resources"
  type        = string
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "prefix" {
  description = "Naming prefix for resources"
  type        = string
}

# Watcher Configuration
variable "watcher_cpu" {
  description = "CPU units for Watcher task"
  type        = number
}

variable "watcher_memory" {
  description = "Memory for Watcher task in MB"
  type        = number
}

variable "watcher_desired_count" {
  description = "Number of Watcher tasks to run"
  type        = number
}

variable "watcher_image_tag" {
  description = "Docker image tag for Watcher"
  type        = string
}

variable "chain_id" {
  description = "Blockchain chain ID"
  type        = number
}

variable "alchemy_api_key" {
  description = "Alchemy API key"
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

# Backup Worker Configuration
variable "backup_worker_cpu" {
  description = "CPU units for Backup Worker task"
  type        = number
}

variable "backup_worker_memory" {
  description = "Memory for Backup Worker task in MB"
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
