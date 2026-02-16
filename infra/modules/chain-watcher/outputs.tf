# =============================================================================
# Chain Watcher Module - Outputs
# =============================================================================

# SNS
output "sns_topic_arn" {
  description = "ARN of the blocks SNS FIFO topic"
  value       = aws_sns_topic.blocks.arn
}

# SQS
output "sqs_queue_urls" {
  description = "Map of consumer name to queue URL"
  value       = { for k, q in aws_sqs_queue.consumer : k => q.url }
}

output "sqs_queue_arns" {
  description = "Map of consumer name to queue ARN"
  value       = { for k, q in aws_sqs_queue.consumer : k => q.arn }
}

output "sqs_dlq_urls" {
  description = "Map of consumer name to DLQ URL"
  value       = { for k, q in aws_sqs_queue.consumer_dlq : k => q.url }
}

output "sqs_dlq_arns" {
  description = "Map of consumer name to DLQ ARN"
  value       = { for k, q in aws_sqs_queue.consumer_dlq : k => q.arn }
}

# Redis
output "redis_endpoint" {
  description = "Redis primary endpoint"
  value       = aws_elasticache_replication_group.redis.primary_endpoint_address
  sensitive   = true
}

output "redis_port" {
  description = "Redis port"
  value       = 6379
}

output "redis_secret_arn" {
  description = "ARN of the Redis credentials secret"
  value       = aws_secretsmanager_secret.redis.arn
}

output "redis_secret_read_policy_arn" {
  description = "ARN of the IAM policy for reading Redis credentials"
  value       = aws_iam_policy.redis_secret_read.arn
}

# S3
output "s3_bucket_arn" {
  description = "ARN of the raw data S3 bucket"
  value       = aws_s3_bucket.raw_data.arn
}

output "s3_bucket_id" {
  description = "ID/name of the raw data S3 bucket"
  value       = aws_s3_bucket.raw_data.id
}

# IAM
output "watcher_role_arn" {
  description = "ARN of the watcher task role"
  value       = aws_iam_role.watcher.arn
}

output "watcher_role_name" {
  description = "Name of the watcher task role"
  value       = aws_iam_role.watcher.name
}

output "worker_role_arn" {
  description = "ARN of the worker task role"
  value       = aws_iam_role.worker.arn
}

output "worker_role_name" {
  description = "Name of the worker task role"
  value       = aws_iam_role.worker.name
}

output "backup_worker_role_arn" {
  description = "ARN of the backup worker task role"
  value       = aws_iam_role.backup_worker.arn
}

output "backup_worker_role_name" {
  description = "Name of the backup worker task role"
  value       = aws_iam_role.backup_worker.name
}

output "sns_publish_policy_arn" {
  description = "ARN of the SNS publish policy"
  value       = aws_iam_policy.sns_publish.arn
}

output "sqs_consume_policy_arn" {
  description = "ARN of the SQS consume policy"
  value       = aws_iam_policy.sqs_consume.arn
}

# Monitoring
output "redis_alarms_topic_arn" {
  description = "ARN of the Redis alarms SNS topic"
  value       = aws_sns_topic.redis_alarms.arn
}

output "messaging_alarms_topic_arn" {
  description = "ARN of the messaging alarms SNS topic"
  value       = aws_sns_topic.messaging_alarms.arn
}

# ECS
output "watcher_service_name" {
  description = "Name of the watcher ECS service"
  value       = aws_ecs_service.watcher.name
}

output "backup_worker_service_name" {
  description = "Name of the backup worker ECS service"
  value       = aws_ecs_service.backup_worker.name
}

output "watcher_task_definition_arn" {
  description = "ARN of the watcher task definition"
  value       = aws_ecs_task_definition.watcher.arn
}

output "backup_worker_task_definition_arn" {
  description = "ARN of the backup worker task definition"
  value       = aws_ecs_task_definition.backup_worker.arn
}
