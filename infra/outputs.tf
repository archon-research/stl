output "bucket_name" {
  description = "Name of the S3 bucket"
  value       = aws_s3_bucket.main.id
}

output "bucket_arn" {
  description = "ARN of the S3 bucket"
  value       = aws_s3_bucket.main.arn
}

output "bucket_region" {
  description = "Region of the S3 bucket"
  value       = aws_s3_bucket.main.region
}

# =============================================================================
# Ethereum Raw Data Access Role
# =============================================================================

output "ethereum_raw_data_role_arn" {
  description = "ARN of the IAM role for accessing the ethereum-raw S3 bucket"
  value       = aws_iam_role.ethereum_raw_data_access.arn
}

output "ethereum_raw_data_role_name" {
  description = "Name of the IAM role for accessing the ethereum-raw S3 bucket"
  value       = aws_iam_role.ethereum_raw_data_access.name
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

output "rds_security_group_id" {
  description = "ID of the RDS security group"
  value       = aws_security_group.rds.id
}

output "redis_security_group_id" {
  description = "ID of the ElastiCache Redis security group"
  value       = aws_security_group.redis.id
}
