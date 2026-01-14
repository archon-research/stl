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

output "external_uploads_bucket_name" {
  description = "Name of the external uploads bucket"
  value       = aws_s3_bucket.external_uploads.id
}

output "external_uploads_bucket_arn" {
  description = "ARN of the external uploads bucket"
  value       = aws_s3_bucket.external_uploads.arn
}

# =============================================================================
# External Uploader Credentials
# =============================================================================
# Share these securely with the external party

output "external_uploader_access_key_id" {
  description = "Access Key ID for external uploader (share with external party)"
  value       = aws_iam_access_key.external_uploader.id
}

output "external_uploader_secret_access_key" {
  description = "Secret Access Key for external uploader (share securely with external party)"
  value       = aws_iam_access_key.external_uploader.secret
  sensitive   = true
}
