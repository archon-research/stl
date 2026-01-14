# =============================================================================
# External Upload Bucket
# =============================================================================
# Bucket for external parties to upload data. Provides:
# - Write-only access for external party (cannot read/delete)
# - Full access for bucket owner
# - Same security baseline as main bucket

locals {
  external_bucket_name = "stl-external-uploads-${random_id.bucket_suffix.hex}"
}

# =============================================================================
# IAM User for External Party
# =============================================================================

resource "aws_iam_user" "external_uploader" {
  name = "stl-external-uploader"
  path = "/external/"

  tags = {
    Purpose   = "external-uploads"
  }
}

# IAM policy granting write-only access to the bucket
resource "aws_iam_user_policy" "external_uploader" {
  name = "s3-upload-only"
  user = aws_iam_user.external_uploader.name

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "AllowUpload"
        Effect = "Allow"
        Action = [
          "s3:PutObject",
          "s3:PutObjectTagging"
        ]
        Resource = "${aws_s3_bucket.external_uploads.arn}/*"
      },
      {
        Sid      = "AllowListBucket"
        Effect   = "Allow"
        Action   = "s3:ListBucket"
        Resource = aws_s3_bucket.external_uploads.arn
        Condition = {
          StringLike = {
            "s3:prefix" = ["uploads/*"]
          }
        }
      }
    ]
  })
}

# Access key for the external party - they will use this to authenticate
resource "aws_iam_access_key" "external_uploader" {
  user = aws_iam_user.external_uploader.name
}

# S3 Bucket for external uploads
resource "aws_s3_bucket" "external_uploads" {
  bucket = local.external_bucket_name

  lifecycle {
    prevent_destroy = true
  }

  tags = {
    Name      = local.external_bucket_name
    Purpose   = "external-uploads"
  }
}

# Enable versioning
resource "aws_s3_bucket_versioning" "external_uploads" {
  bucket = aws_s3_bucket.external_uploads.id

  versioning_configuration {
    status = "Enabled"
  }
}

# Block public access
resource "aws_s3_bucket_public_access_block" "external_uploads" {
  bucket = aws_s3_bucket.external_uploads.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# Enforce bucket ownership
resource "aws_s3_bucket_ownership_controls" "external_uploads" {
  bucket = aws_s3_bucket.external_uploads.id

  rule {
    object_ownership = "BucketOwnerEnforced"
  }
}

# Bucket policy: SSL enforcement only (access is via IAM user policy)
resource "aws_s3_bucket_policy" "external_uploads" {
  bucket = aws_s3_bucket.external_uploads.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid       = "EnforceSSLOnly"
        Effect    = "Deny"
        Principal = "*"
        Action    = "s3:*"
        Resource = [
          aws_s3_bucket.external_uploads.arn,
          "${aws_s3_bucket.external_uploads.arn}/*"
        ]
        Condition = {
          Bool = {
            "aws:SecureTransport" = "false"
          }
        }
      },
      {
        Sid       = "EnforceTLSVersion"
        Effect    = "Deny"
        Principal = "*"
        Action    = "s3:*"
        Resource = [
          aws_s3_bucket.external_uploads.arn,
          "${aws_s3_bucket.external_uploads.arn}/*"
        ]
        Condition = {
          NumericLessThan = {
            "s3:TlsVersion" = "1.2"
          }
        }
      }
    ]
  })

  depends_on = [aws_s3_bucket_public_access_block.external_uploads]
}

# Lifecycle rule for uploads
resource "aws_s3_bucket_lifecycle_configuration" "external_uploads" {
  bucket = aws_s3_bucket.external_uploads.id

  rule {
    id     = "cleanup-old-versions"
    status = "Enabled"

    noncurrent_version_transition {
      noncurrent_days = 30
      storage_class   = "STANDARD_IA"
    }

    noncurrent_version_transition {
      noncurrent_days = 90
      storage_class   = "GLACIER"
    }

    noncurrent_version_expiration {
      noncurrent_days = 365
    }
  }
}
