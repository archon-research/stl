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
