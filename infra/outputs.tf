# =============================================================================
# Ethereum Chain Outputs
# =============================================================================

output "ethereum_bucket_name" {
  description = "Name of the Ethereum S3 bucket"
  value       = module.ethereum.s3_bucket_id
}

output "ethereum_bucket_arn" {
  description = "ARN of the Ethereum S3 bucket"
  value       = module.ethereum.s3_bucket_arn
}

# Watcher Role (SNS publish + read-only resources)
output "ethereum_watcher_role_arn" {
  description = "ARN of the Ethereum Watcher ECS task role"
  value       = module.ethereum.watcher_role_arn
}

output "ethereum_watcher_role_name" {
  description = "Name of the Ethereum Watcher ECS task role"
  value       = module.ethereum.watcher_role_name
}

# Worker Role (SQS consume + read-only resources)
output "ethereum_worker_role_arn" {
  description = "ARN of the Ethereum Worker ECS task role"
  value       = module.ethereum.worker_role_arn
}

output "ethereum_worker_role_name" {
  description = "Name of the Ethereum Worker ECS task role"
  value       = module.ethereum.worker_role_name
}

# Legacy role (deprecated - kept for backward compatibility)
output "ethereum_raw_data_role_arn" {
  description = "DEPRECATED: Use ethereum_watcher_role_arn or ethereum_worker_role_arn instead"
  value       = aws_iam_role.ethereum_raw_data_access.arn
}

output "ethereum_raw_data_role_name" {
  description = "DEPRECATED: Use ethereum_watcher_role_name or ethereum_worker_role_name instead"
  value       = aws_iam_role.ethereum_raw_data_access.name
}

# ECS Services
output "ethereum_watcher_service_name" {
  description = "Name of the Ethereum Watcher ECS service"
  value       = module.ethereum.watcher_service_name
}

output "ethereum_watcher_task_definition_arn" {
  description = "ARN of the Ethereum Watcher task definition"
  value       = module.ethereum.watcher_task_definition_arn
}

output "ethereum_backup_worker_role_arn" {
  description = "ARN of the Ethereum Backup Worker ECS task role"
  value       = module.ethereum.backup_worker_role_arn
}

output "ethereum_backup_worker_role_name" {
  description = "Name of the Ethereum Backup Worker ECS task role"
  value       = module.ethereum.backup_worker_role_name
}

output "ethereum_backup_worker_service_name" {
  description = "Name of the Ethereum Backup Worker ECS service"
  value       = module.ethereum.backup_worker_service_name
}

output "ethereum_backup_worker_task_definition_arn" {
  description = "ARN of the Ethereum Backup Worker task definition"
  value       = module.ethereum.backup_worker_task_definition_arn
}

# =============================================================================
# Avalanche Chain Outputs
# =============================================================================

output "avalanche_bucket_name" {
  description = "Name of the Avalanche S3 bucket"
  value       = module.avalanche.s3_bucket_id
}

output "avalanche_bucket_arn" {
  description = "ARN of the Avalanche S3 bucket"
  value       = module.avalanche.s3_bucket_arn
}

output "avalanche_watcher_role_arn" {
  description = "ARN of the Avalanche Watcher ECS task role"
  value       = module.avalanche.watcher_role_arn
}

output "avalanche_watcher_service_name" {
  description = "Name of the Avalanche Watcher ECS service"
  value       = module.avalanche.watcher_service_name
}

output "avalanche_backup_worker_role_arn" {
  description = "ARN of the Avalanche Backup Worker ECS task role"
  value       = module.avalanche.backup_worker_role_arn
}

output "avalanche_backup_worker_service_name" {
  description = "Name of the Avalanche Backup Worker ECS service"
  value       = module.avalanche.backup_worker_service_name
}

# =============================================================================
# VPC Outputs
# =============================================================================

output "vpc_id" {
  description = "ID of the VPC"
  value       = aws_vpc.main.id
}

output "vpc_cidr" {
  description = "CIDR block of the VPC"
  value       = aws_vpc.main.cidr_block
}

output "public_subnet_id" {
  description = "ID of the public subnet"
  value       = aws_subnet.public.id
}

output "private_subnet_id" {
  description = "ID of the private subnet (application tier)"
  value       = aws_subnet.private.id
}

output "isolated_subnet_id" {
  description = "ID of the isolated subnet (data tier)"
  value       = aws_subnet.isolated.id
}

output "nat_gateway_public_ip" {
  description = "Public IP of the NAT Gateway"
  value       = aws_eip.nat.public_ip
}

# =============================================================================
# Security Group Outputs
# =============================================================================

output "alb_security_group_id" {
  description = "ID of the ALB security group"
  value       = aws_security_group.alb.id
}

output "api_security_group_id" {
  description = "ID of the API service security group"
  value       = aws_security_group.api.id
}

output "watcher_security_group_id" {
  description = "ID of the Watcher service security group"
  value       = aws_security_group.watcher.id
}

output "worker_security_group_id" {
  description = "ID of the Worker service security group"
  value       = aws_security_group.worker.id
}

output "redis_security_group_id" {
  description = "ID of the ElastiCache Redis security group"
  value       = aws_security_group.redis.id
}

# =============================================================================
# TigerData (TimescaleDB) Outputs
# =============================================================================

output "tigerdata_hostname" {
  description = "TigerData primary hostname"
  value       = timescale_service.main.hostname
  sensitive   = true
}

output "tigerdata_port" {
  description = "TigerData primary port"
  value       = timescale_service.main.port
}

output "tigerdata_pooler_hostname" {
  description = "TigerData connection pooler hostname"
  value       = timescale_service.main.pooler_hostname
  sensitive   = true
}

output "tigerdata_pooler_port" {
  description = "TigerData connection pooler port"
  value       = timescale_service.main.pooler_port
}

output "tigerdata_replica_hostname" {
  description = "TigerData HA replica hostname (if enabled)"
  value       = timescale_service.main.replica_hostname
  sensitive   = true
}

output "tigerdata_vpc_peering_id" {
  description = "VPC peering connection ID"
  value       = aws_vpc_peering_connection_accepter.tigerdata.id
}

# =============================================================================
# ECS Cluster Outputs
# =============================================================================

output "ecs_cluster_name" {
  description = "Name of the ECS cluster"
  value       = aws_ecs_cluster.main.name
}

output "ecs_cluster_arn" {
  description = "ARN of the ECS cluster"
  value       = aws_ecs_cluster.main.arn
}

# =============================================================================
# ECR Repository Outputs
# =============================================================================

output "watcher_ecr_repository_url" {
  description = "URL of the Watcher ECR repository"
  value       = aws_ecr_repository.watcher.repository_url
}

output "watcher_ecr_repository_arn" {
  description = "ARN of the Watcher ECR repository"
  value       = aws_ecr_repository.watcher.arn
}

output "backup_worker_ecr_repository_url" {
  description = "URL of the Backup Worker ECR repository"
  value       = aws_ecr_repository.backup_worker.repository_url
}

output "backup_worker_ecr_repository_arn" {
  description = "ARN of the Backup Worker ECR repository"
  value       = aws_ecr_repository.backup_worker.arn
}

# =============================================================================
# Oracle Price Worker Outputs
# =============================================================================

output "oracle_price_worker_ecr_repository_url" {
  description = "URL of the Oracle Price Worker ECR repository"
  value       = aws_ecr_repository.oracle_price_worker.repository_url
}

output "oracle_price_worker_ecr_repository_arn" {
  description = "ARN of the Oracle Price Worker ECR repository"
  value       = aws_ecr_repository.oracle_price_worker.arn
}

output "oracle_price_worker_role_arn" {
  description = "ARN of the Oracle Price Worker ECS task role"
  value       = aws_iam_role.oracle_price_worker.arn
}

output "oracle_price_worker_role_name" {
  description = "Name of the Oracle Price Worker ECS task role"
  value       = aws_iam_role.oracle_price_worker.name
}

output "oracle_price_worker_service_name" {
  description = "Name of the Oracle Price Worker ECS service"
  value       = aws_ecs_service.oracle_price_worker.name
}

output "oracle_price_worker_task_definition_arn" {
  description = "ARN of the Oracle Price Worker task definition"
  value       = aws_ecs_task_definition.oracle_price_worker.arn
}
