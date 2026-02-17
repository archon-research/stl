# =============================================================================
# Moved Blocks - Ethereum Migration into chain-watcher Module
# =============================================================================
# These blocks map existing Ethereum resources to their new module addresses.
# After `tofu plan` shows 0 changes, the migration is verified.

# -----------------------------------------------------------------------------
# Redis (02_redis.tf → module.ethereum redis.tf)
# -----------------------------------------------------------------------------

moved {
  from = aws_elasticache_replication_group.ethereum_redis
  to   = module.ethereum.aws_elasticache_replication_group.redis
}

moved {
  from = aws_elasticache_subnet_group.ethereum_redis
  to   = module.ethereum.aws_elasticache_subnet_group.redis
}

moved {
  from = aws_elasticache_parameter_group.ethereum_redis
  to   = module.ethereum.aws_elasticache_parameter_group.redis
}

moved {
  from = random_password.ethereum_redis_auth
  to   = module.ethereum.random_password.redis_auth
}

moved {
  from = aws_secretsmanager_secret.ethereum_redis
  to   = module.ethereum.aws_secretsmanager_secret.redis
}

moved {
  from = aws_secretsmanager_secret_version.ethereum_redis
  to   = module.ethereum.aws_secretsmanager_secret_version.redis
}

moved {
  from = aws_iam_policy.ethereum_redis_secret_read
  to   = module.ethereum.aws_iam_policy.redis_secret_read
}

# Redis monitoring
moved {
  from = aws_sns_topic.ethereum_redis_alarms
  to   = module.ethereum.aws_sns_topic.redis_alarms
}

moved {
  from = aws_cloudwatch_metric_alarm.ethereum_redis_cpu_high
  to   = module.ethereum.aws_cloudwatch_metric_alarm.redis_cpu_high
}

moved {
  from = aws_cloudwatch_metric_alarm.ethereum_redis_cpu_critical
  to   = module.ethereum.aws_cloudwatch_metric_alarm.redis_cpu_critical
}

moved {
  from = aws_cloudwatch_metric_alarm.ethereum_redis_memory_high
  to   = module.ethereum.aws_cloudwatch_metric_alarm.redis_memory_high
}

moved {
  from = aws_cloudwatch_metric_alarm.ethereum_redis_memory_critical
  to   = module.ethereum.aws_cloudwatch_metric_alarm.redis_memory_critical
}

moved {
  from = aws_cloudwatch_metric_alarm.ethereum_redis_evictions
  to   = module.ethereum.aws_cloudwatch_metric_alarm.redis_evictions
}

moved {
  from = aws_cloudwatch_metric_alarm.ethereum_redis_cache_hit_rate_low
  to   = module.ethereum.aws_cloudwatch_metric_alarm.redis_cache_hit_rate_low
}

moved {
  from = aws_cloudwatch_metric_alarm.ethereum_redis_connections_high
  to   = module.ethereum.aws_cloudwatch_metric_alarm.redis_connections_high
}

moved {
  from = aws_cloudwatch_metric_alarm.ethereum_redis_replication_lag
  to   = module.ethereum.aws_cloudwatch_metric_alarm.redis_replication_lag
}

moved {
  from = aws_cloudwatch_dashboard.redis
  to   = module.ethereum.aws_cloudwatch_dashboard.redis
}

# -----------------------------------------------------------------------------
# S3 (02_s3.tf → module.ethereum s3.tf)
# -----------------------------------------------------------------------------

moved {
  from = aws_s3_bucket.main
  to   = module.ethereum.aws_s3_bucket.raw_data
}

moved {
  from = aws_s3_bucket_versioning.main
  to   = module.ethereum.aws_s3_bucket_versioning.raw_data
}

moved {
  from = aws_s3_bucket_public_access_block.main
  to   = module.ethereum.aws_s3_bucket_public_access_block.raw_data
}

moved {
  from = aws_s3_bucket_ownership_controls.main
  to   = module.ethereum.aws_s3_bucket_ownership_controls.raw_data
}

moved {
  from = aws_s3_bucket_policy.enforce_ssl
  to   = module.ethereum.aws_s3_bucket_policy.enforce_ssl
}

moved {
  from = aws_s3_bucket_lifecycle_configuration.main
  to   = module.ethereum.aws_s3_bucket_lifecycle_configuration.raw_data
}

# Access logs bucket stays at root (shared across chains), just renamed
moved {
  from = aws_s3_bucket.logs
  to   = aws_s3_bucket.access_logs
}

moved {
  from = aws_s3_bucket_public_access_block.logs
  to   = aws_s3_bucket_public_access_block.access_logs
}

moved {
  from = aws_s3_bucket_ownership_controls.logs
  to   = aws_s3_bucket_ownership_controls.access_logs
}

moved {
  from = aws_s3_bucket_lifecycle_configuration.logs
  to   = aws_s3_bucket_lifecycle_configuration.access_logs
}

moved {
  from = aws_s3_bucket_logging.main
  to   = module.ethereum.aws_s3_bucket_logging.raw_data
}

# -----------------------------------------------------------------------------
# Messaging (03_messaging.tf → module.ethereum messaging.tf)
# -----------------------------------------------------------------------------

moved {
  from = aws_sns_topic.ethereum_blocks
  to   = module.ethereum.aws_sns_topic.blocks
}

# SQS queues (individual → for_each)
moved {
  from = aws_sqs_queue.ethereum_transformer
  to   = module.ethereum.aws_sqs_queue.consumer["transformer"]
}

moved {
  from = aws_sqs_queue.ethereum_transformer_dlq
  to   = module.ethereum.aws_sqs_queue.consumer_dlq["transformer"]
}

moved {
  from = aws_sqs_queue.ethereum_backup
  to   = module.ethereum.aws_sqs_queue.consumer["backup"]
}

moved {
  from = aws_sqs_queue.ethereum_backup_dlq
  to   = module.ethereum.aws_sqs_queue.consumer_dlq["backup"]
}

moved {
  from = aws_sqs_queue.ethereum_oracle_price
  to   = module.ethereum.aws_sqs_queue.consumer["oracle_price"]
}

moved {
  from = aws_sqs_queue.ethereum_oracle_price_dlq
  to   = module.ethereum.aws_sqs_queue.consumer_dlq["oracle_price"]
}

# Redrive allow policies
moved {
  from = aws_sqs_queue_redrive_allow_policy.ethereum_transformer_dlq
  to   = module.ethereum.aws_sqs_queue_redrive_allow_policy.consumer_dlq["transformer"]
}

moved {
  from = aws_sqs_queue_redrive_allow_policy.ethereum_backup_dlq
  to   = module.ethereum.aws_sqs_queue_redrive_allow_policy.consumer_dlq["backup"]
}

moved {
  from = aws_sqs_queue_redrive_allow_policy.ethereum_oracle_price_dlq
  to   = module.ethereum.aws_sqs_queue_redrive_allow_policy.consumer_dlq["oracle_price"]
}

# Queue policies
moved {
  from = aws_sqs_queue_policy.ethereum_transformer
  to   = module.ethereum.aws_sqs_queue_policy.consumer["transformer"]
}

moved {
  from = aws_sqs_queue_policy.ethereum_backup
  to   = module.ethereum.aws_sqs_queue_policy.consumer["backup"]
}

moved {
  from = aws_sqs_queue_policy.ethereum_oracle_price
  to   = module.ethereum.aws_sqs_queue_policy.consumer["oracle_price"]
}

# SNS subscriptions
moved {
  from = aws_sns_topic_subscription.ethereum_transformer
  to   = module.ethereum.aws_sns_topic_subscription.consumer["transformer"]
}

moved {
  from = aws_sns_topic_subscription.ethereum_backup
  to   = module.ethereum.aws_sns_topic_subscription.consumer["backup"]
}

moved {
  from = aws_sns_topic_subscription.ethereum_oracle_price
  to   = module.ethereum.aws_sns_topic_subscription.consumer["oracle_price"]
}

# IAM policies
moved {
  from = aws_iam_policy.ethereum_sns_publish
  to   = module.ethereum.aws_iam_policy.sns_publish
}

moved {
  from = aws_iam_policy.ethereum_sqs_consume
  to   = module.ethereum.aws_iam_policy.sqs_consume
}

# -----------------------------------------------------------------------------
# Messaging Monitoring (03_messaging_monitoring.tf → module.ethereum)
# -----------------------------------------------------------------------------

moved {
  from = aws_sns_topic.messaging_alarms
  to   = module.ethereum.aws_sns_topic.messaging_alarms
}

# DLQ alarms
moved {
  from = aws_cloudwatch_metric_alarm.transformer_dlq_messages
  to   = module.ethereum.aws_cloudwatch_metric_alarm.dlq_messages["transformer"]
}

moved {
  from = aws_cloudwatch_metric_alarm.backup_dlq_messages
  to   = module.ethereum.aws_cloudwatch_metric_alarm.dlq_messages["backup"]
}

moved {
  from = aws_cloudwatch_metric_alarm.oracle_price_dlq_messages
  to   = module.ethereum.aws_cloudwatch_metric_alarm.dlq_messages["oracle_price"]
}

# Queue backlog alarms
moved {
  from = aws_cloudwatch_metric_alarm.transformer_queue_backlog
  to   = module.ethereum.aws_cloudwatch_metric_alarm.queue_backlog["transformer"]
}

moved {
  from = aws_cloudwatch_metric_alarm.backup_queue_backlog
  to   = module.ethereum.aws_cloudwatch_metric_alarm.queue_backlog["backup"]
}

moved {
  from = aws_cloudwatch_metric_alarm.oracle_price_queue_backlog
  to   = module.ethereum.aws_cloudwatch_metric_alarm.queue_backlog["oracle_price"]
}

# Message age alarms
moved {
  from = aws_cloudwatch_metric_alarm.transformer_message_age
  to   = module.ethereum.aws_cloudwatch_metric_alarm.message_age["transformer"]
}

moved {
  from = aws_cloudwatch_metric_alarm.backup_message_age
  to   = module.ethereum.aws_cloudwatch_metric_alarm.message_age["backup"]
}

moved {
  from = aws_cloudwatch_metric_alarm.oracle_price_message_age
  to   = module.ethereum.aws_cloudwatch_metric_alarm.message_age["oracle_price"]
}

# SNS publish failures
moved {
  from = aws_cloudwatch_metric_alarm.sns_publish_failures
  to   = module.ethereum.aws_cloudwatch_metric_alarm.sns_publish_failures
}

# Dashboard
moved {
  from = aws_cloudwatch_dashboard.messaging
  to   = module.ethereum.aws_cloudwatch_dashboard.messaging
}

# -----------------------------------------------------------------------------
# ECS Watcher (04_ecs.tf + 04_ecs_watcher.tf → module.ethereum)
# -----------------------------------------------------------------------------

moved {
  from = aws_cloudwatch_log_group.watcher
  to   = module.ethereum.aws_cloudwatch_log_group.watcher
}

moved {
  from = aws_ecs_task_definition.watcher
  to   = module.ethereum.aws_ecs_task_definition.watcher
}

moved {
  from = aws_ecs_service.watcher
  to   = module.ethereum.aws_ecs_service.watcher
}

# -----------------------------------------------------------------------------
# ECS Backup Worker (04_ecs_backup_worker.tf → module.ethereum)
# -----------------------------------------------------------------------------

moved {
  from = aws_cloudwatch_log_group.backup_worker
  to   = module.ethereum.aws_cloudwatch_log_group.backup_worker
}

moved {
  from = aws_ecs_task_definition.backup_worker
  to   = module.ethereum.aws_ecs_task_definition.backup_worker
}

moved {
  from = aws_ecs_service.backup_worker
  to   = module.ethereum.aws_ecs_service.backup_worker
}

moved {
  from = aws_iam_role.backup_worker
  to   = module.ethereum.aws_iam_role.backup_worker
}

moved {
  from = aws_iam_policy.backup_worker_sqs
  to   = module.ethereum.aws_iam_policy.backup_worker_sqs
}

moved {
  from = aws_iam_role_policy_attachment.backup_worker_sqs
  to   = module.ethereum.aws_iam_role_policy_attachment.backup_worker_sqs
}

moved {
  from = aws_iam_policy.backup_worker_s3
  to   = module.ethereum.aws_iam_policy.backup_worker_s3
}

moved {
  from = aws_iam_role_policy_attachment.backup_worker_s3
  to   = module.ethereum.aws_iam_role_policy_attachment.backup_worker_s3
}

moved {
  from = aws_iam_role_policy_attachment.backup_worker_cloudwatch
  to   = module.ethereum.aws_iam_role_policy_attachment.backup_worker_cloudwatch
}

# -----------------------------------------------------------------------------
# IAM Roles (05_iam_ecs_task.tf → module.ethereum iam.tf)
# -----------------------------------------------------------------------------

moved {
  from = aws_iam_role.ethereum_watcher
  to   = module.ethereum.aws_iam_role.watcher
}

moved {
  from = aws_iam_role.ethereum_worker
  to   = module.ethereum.aws_iam_role.worker
}

moved {
  from = aws_iam_policy.ethereum_s3_access
  to   = module.ethereum.aws_iam_policy.s3_access
}

moved {
  from = aws_iam_role_policy_attachment.watcher_s3_access
  to   = module.ethereum.aws_iam_role_policy_attachment.watcher_s3_access
}

moved {
  from = aws_iam_role_policy_attachment.watcher_tigerdata_secret
  to   = module.ethereum.aws_iam_role_policy_attachment.watcher_tigerdata_app_secret
}

moved {
  from = aws_iam_role_policy_attachment.watcher_redis_secret
  to   = module.ethereum.aws_iam_role_policy_attachment.watcher_redis_secret
}

moved {
  from = aws_iam_role_policy_attachment.watcher_sns_publish
  to   = module.ethereum.aws_iam_role_policy_attachment.watcher_sns_publish
}

moved {
  from = aws_iam_role_policy_attachment.watcher_xray
  to   = module.ethereum.aws_iam_role_policy_attachment.watcher_xray
}

moved {
  from = aws_iam_role_policy_attachment.watcher_cloudwatch
  to   = module.ethereum.aws_iam_role_policy_attachment.watcher_cloudwatch
}

moved {
  from = aws_iam_role_policy_attachment.worker_s3_access
  to   = module.ethereum.aws_iam_role_policy_attachment.worker_s3_access
}

moved {
  from = aws_iam_role_policy_attachment.worker_tigerdata_secret
  to   = module.ethereum.aws_iam_role_policy_attachment.worker_tigerdata_app_secret
}

moved {
  from = aws_iam_role_policy_attachment.worker_redis_secret
  to   = module.ethereum.aws_iam_role_policy_attachment.worker_redis_secret
}

moved {
  from = aws_iam_role_policy_attachment.worker_sqs_consume
  to   = module.ethereum.aws_iam_role_policy_attachment.worker_sqs_consume
}

# Redis secret access for legacy role (stays at root)
moved {
  from = aws_iam_role_policy_attachment.ethereum_redis_secret_access
  to   = aws_iam_role_policy_attachment.legacy_ethereum_redis_secret_access
}
