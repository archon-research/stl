# =============================================================================
# Shared S3 Access Logs Bucket
# =============================================================================
# Single access logs bucket shared by all chain raw data buckets.

locals {
  access_logs_bucket_name = var.environment == "sentinelstaging" ? "${local.prefix_lowercase}-access-logs-89d540d0" : "${local.prefix_lowercase}-access-logs${local.resource_suffix}"
}

resource "aws_s3_bucket" "access_logs" {
  bucket = local.access_logs_bucket_name

  tags = {
    Name    = "${local.prefix}-access-logs"
    Purpose = "s3-access-logging"
  }
}

resource "aws_s3_bucket_public_access_block" "access_logs" {
  bucket = aws_s3_bucket.access_logs.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "access_logs" {
  bucket = aws_s3_bucket.access_logs.id

  rule {
    object_ownership = "BucketOwnerPreferred"
  }
}

resource "aws_s3_bucket_lifecycle_configuration" "access_logs" {
  bucket = aws_s3_bucket.access_logs.id

  rule {
    id     = "expire-old-logs"
    status = "Enabled"

    expiration {
      days = 90
    }
  }
}
