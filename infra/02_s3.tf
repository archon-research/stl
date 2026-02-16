locals {
  # S3-specific locals
  # Staging buckets keep their existing suffix, new envs use resource_suffix (dev) or no suffix (prod)
  bucket_suffix = var.environment == "sentinelstaging" ? "-89d540d0" : local.resource_suffix
  bucket_name   = "${local.prefix_lowercase}-ethereum-raw${local.bucket_suffix}"
}

# S3 Bucket - configured to never be deleted and fully private
resource "aws_s3_bucket" "main" {
  bucket        = local.bucket_name
  force_destroy = var.environment == "sentineldev" ? true : false

  tags = {
    Name = local.bucket_name
  }
}

# Enable versioning for data protection and recovery
resource "aws_s3_bucket_versioning" "main" {
  bucket = aws_s3_bucket.main.id

  versioning_configuration {
    status = "Enabled"
  }
}

# Block ALL public access - defense in depth
resource "aws_s3_bucket_public_access_block" "main" {
  bucket = aws_s3_bucket.main.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Enforce bucket ownership controls - disable ACLs
resource "aws_s3_bucket_ownership_controls" "main" {
  bucket = aws_s3_bucket.main.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

# Policy to enforce SSL/TLS for all requests
data "aws_iam_policy_document" "enforce_ssl" {
  statement {
    sid    = "EnforceSSLOnly"
    effect = "Deny"

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    actions = ["s3:*"]
    resources = [
      aws_s3_bucket.main.arn,
      "${aws_s3_bucket.main.arn}/*"
    ]

    condition {
      test     = "Bool"
      variable = "aws:SecureTransport"
      values   = ["false"]
    }
  }

  statement {
    sid    = "EnforceTLSVersion"
    effect = "Deny"

    principals {
      type        = "*"
      identifiers = ["*"]
    }

    actions = ["s3:*"]
    resources = [
      aws_s3_bucket.main.arn,
      "${aws_s3_bucket.main.arn}/*"
    ]

    condition {
      test     = "NumericLessThan"
      variable = "s3:TlsVersion"
      values   = ["1.2"]
    }
  }
}

resource "aws_s3_bucket_policy" "enforce_ssl" {
  bucket = aws_s3_bucket.main.id
  policy = data.aws_iam_policy_document.enforce_ssl.json

  # Ensure public access block is applied first
  depends_on = [aws_s3_bucket_public_access_block.main]
}

# Lifecycle rule for cost optimization on old versions
resource "aws_s3_bucket_lifecycle_configuration" "main" {
  bucket = aws_s3_bucket.main.id

  rule {
    id     = "cleanup-old-versions"
    status = "Enabled"

    noncurrent_version_transition {
      noncurrent_days = 30
      storage_class   = "STANDARD_IA"
    }

    noncurrent_version_expiration {
      noncurrent_days = 90
    }
  }
}

# =============================================================================
# S3 Access Logging
# =============================================================================

# Dedicated bucket for access logs
resource "aws_s3_bucket" "logs" {
  # Preserve existing staging bucket name to avoid data loss
  bucket = var.environment == "sentinelstaging" ? "${local.prefix_lowercase}-access-logs-89d540d0" : "${local.prefix_lowercase}-access-logs${local.resource_suffix}"

  tags = {
    Name    = "${local.prefix}-access-logs"
    Purpose = "s3-access-logging"
  }
}

# Block public access on logs bucket
resource "aws_s3_bucket_public_access_block" "logs" {
  bucket = aws_s3_bucket.logs.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Ownership controls for logs bucket
resource "aws_s3_bucket_ownership_controls" "logs" {
  bucket = aws_s3_bucket.logs.id

  rule {
    object_ownership = "BucketOwnerPreferred"
  }
}

# Lifecycle rule to expire old logs
resource "aws_s3_bucket_lifecycle_configuration" "logs" {
  bucket = aws_s3_bucket.logs.id

  rule {
    id     = "expire-old-logs"
    status = "Enabled"

    expiration {
      days = 90
    }
  }
}

# Enable access logging on main bucket
resource "aws_s3_bucket_logging" "main" {
  bucket = aws_s3_bucket.main.id

  target_bucket = aws_s3_bucket.logs.id
  target_prefix = "${local.prefix}-ethereum-raw/"
}
