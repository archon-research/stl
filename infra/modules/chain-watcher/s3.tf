# =============================================================================
# S3 Raw Data Storage
# =============================================================================

resource "aws_s3_bucket" "raw_data" {
  bucket        = "${local.prefix_lowercase}-${var.chain_name}-raw${var.bucket_suffix}"
  force_destroy = var.environment == "sentineldev" ? true : false

  tags = {
    Name = "${local.prefix_lowercase}-${var.chain_name}-raw${var.bucket_suffix}"
  }
}

resource "aws_s3_bucket_versioning" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_public_access_block" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

resource "aws_s3_bucket_ownership_controls" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

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
      aws_s3_bucket.raw_data.arn,
      "${aws_s3_bucket.raw_data.arn}/*"
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
      aws_s3_bucket.raw_data.arn,
      "${aws_s3_bucket.raw_data.arn}/*"
    ]

    condition {
      test     = "NumericLessThan"
      variable = "s3:TlsVersion"
      values   = ["1.2"]
    }
  }
}

resource "aws_s3_bucket_policy" "enforce_ssl" {
  bucket = aws_s3_bucket.raw_data.id
  policy = data.aws_iam_policy_document.enforce_ssl.json

  depends_on = [aws_s3_bucket_public_access_block.raw_data]
}

resource "aws_s3_bucket_lifecycle_configuration" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id

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

resource "aws_s3_bucket_logging" "raw_data" {
  bucket = aws_s3_bucket.raw_data.id

  target_bucket = var.access_logs_bucket_id
  target_prefix = "${var.prefix}-${var.chain_name}-raw/"
}
