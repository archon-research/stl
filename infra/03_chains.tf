# =============================================================================
# Chain Watcher Module Calls
# =============================================================================
# Each chain gets its own watcher, backup worker, Redis, S3, and messaging.

# -----------------------------------------------------------------------------
# Ethereum Mainnet
# -----------------------------------------------------------------------------

module "ethereum" {
  source     = "./modules/chain-watcher"
  chain_name = "ethereum"
  chain_id   = 1

  # Shared references
  prefix                               = local.prefix
  environment                          = var.environment
  aws_region                           = var.aws_region
  resource_suffix                      = var.resource_suffix
  ecs_cluster_id                       = aws_ecs_cluster.main.id
  private_subnet_id                    = aws_subnet.private.id
  isolated_subnet_id                   = aws_subnet.isolated.id
  watcher_sg_id                        = aws_security_group.watcher.id
  worker_sg_id                         = aws_security_group.worker.id
  redis_sg_id                          = aws_security_group.redis.id
  ecs_task_execution_role_arn          = aws_iam_role.ecs_task_execution.arn
  watcher_config_secret_arn            = data.aws_secretsmanager_secret.watcher_config.arn
  tigerdata_app_secret_arn             = aws_secretsmanager_secret.tigerdata_app.arn
  tigerdata_app_secret_read_policy_arn = aws_iam_policy.tigerdata_app_secret_read.arn
  watcher_ecr_url                      = aws_ecr_repository.watcher.repository_url
  backup_worker_ecr_url                = aws_ecr_repository.backup_worker.repository_url
  access_logs_bucket_id                = aws_s3_bucket.access_logs.id
  bucket_suffix                        = var.environment == "sentinelstaging" ? "-89d540d0" : ""

  # Chain config
  alchemy_http_url = var.alchemy_http_url
  alchemy_ws_url   = var.alchemy_ws_url
  watcher_command  = []

  # SQS consumers
  sqs_consumers = {
    transformer  = {}
    backup       = {}
    oracle_price = {}
  }

  # Watcher sizing
  watcher_cpu           = var.watcher_cpu
  watcher_memory        = var.watcher_memory
  watcher_desired_count = var.watcher_desired_count
  watcher_image_tag     = var.watcher_image_tag

  # Backup worker sizing
  backup_worker_cpu           = var.backup_worker_cpu
  backup_worker_memory        = var.backup_worker_memory
  backup_worker_desired_count = var.backup_worker_desired_count
  backup_worker_image_tag     = var.backup_worker_image_tag
  backup_worker_workers       = var.backup_worker_workers

  # Redis
  redis_node_type          = var.redis_node_type
  redis_engine_version     = var.redis_engine_version
  redis_num_cache_clusters = var.redis_num_cache_clusters
  redis_transit_encryption = var.redis_transit_encryption
  redis_snapshot_retention = var.redis_snapshot_retention
}

# -----------------------------------------------------------------------------
# Avalanche C-Chain
# -----------------------------------------------------------------------------

module "avalanche" {
  source     = "./modules/chain-watcher"
  chain_name = "avalanche"
  chain_id   = 43114

  # Shared references
  prefix                               = local.prefix
  environment                          = var.environment
  aws_region                           = var.aws_region
  resource_suffix                      = var.resource_suffix
  ecs_cluster_id                       = aws_ecs_cluster.main.id
  private_subnet_id                    = aws_subnet.private.id
  isolated_subnet_id                   = aws_subnet.isolated.id
  watcher_sg_id                        = aws_security_group.watcher.id
  worker_sg_id                         = aws_security_group.worker.id
  redis_sg_id                          = aws_security_group.redis.id
  ecs_task_execution_role_arn          = aws_iam_role.ecs_task_execution.arn
  watcher_config_secret_arn            = data.aws_secretsmanager_secret.watcher_config.arn
  tigerdata_app_secret_arn             = aws_secretsmanager_secret.tigerdata_app.arn
  tigerdata_app_secret_read_policy_arn = aws_iam_policy.tigerdata_app_secret_read.arn
  watcher_ecr_url                      = aws_ecr_repository.watcher.repository_url
  backup_worker_ecr_url                = aws_ecr_repository.backup_worker.repository_url
  access_logs_bucket_id                = aws_s3_bucket.access_logs.id

  # Chain config
  alchemy_http_url = var.avalanche_alchemy_http_url
  alchemy_ws_url   = var.avalanche_alchemy_ws_url
  watcher_command  = ["--enable-traces=false"]

  # SQS consumers (backup only for Avalanche)
  sqs_consumers = {
    backup = {}
  }

  # Watcher sizing
  watcher_cpu           = var.avalanche_watcher_cpu
  watcher_memory        = var.avalanche_watcher_memory
  watcher_desired_count = var.avalanche_watcher_desired_count
  watcher_image_tag     = var.avalanche_watcher_image_tag

  # Backup worker sizing
  backup_worker_cpu           = var.avalanche_backup_worker_cpu
  backup_worker_memory        = var.avalanche_backup_worker_memory
  backup_worker_desired_count = var.avalanche_backup_worker_desired_count
  backup_worker_image_tag     = var.avalanche_backup_worker_image_tag
  backup_worker_workers       = var.avalanche_backup_worker_workers

  # Redis
  redis_node_type          = var.avalanche_redis_node_type
  redis_engine_version     = var.avalanche_redis_engine_version
  redis_num_cache_clusters = var.avalanche_redis_num_cache_clusters
  redis_transit_encryption = var.avalanche_redis_transit_encryption
  redis_snapshot_retention = var.avalanche_redis_snapshot_retention
}
