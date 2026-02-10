# S3 bucket for Terraform state
# 
# IMPORTANT: This should only be destroyed in sentineldev.
# For sentinelstaging and sentinelprod, destroy operations should be prevented
# via operational controls (CI/CD, IAM policies, manual review).
# 
# Note: Terraform's prevent_destroy must be a literal boolean and cannot 
# reference variables, so protection must be enforced externally for staging/prod.
resource "aws_s3_bucket" "terraform_state" {
  bucket              = local.state_bucket_name
  force_destroy       = var.environment == "sentineldev" ? true : false  # Only allow force deletion in dev

  lifecycle {
    prevent_destroy = false  # Required false for dev; staging/prod protected via external controls
  }
}

resource "aws_s3_bucket_versioning" "terraform_state" {
  bucket = aws_s3_bucket.terraform_state.id

  versioning_configuration {
    status = "Enabled"
  }
}

resource "aws_s3_bucket_server_side_encryption_configuration" "terraform_state" {
  bucket = aws_s3_bucket.terraform_state.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

resource "aws_s3_bucket_public_access_block" "terraform_state" {
  bucket = aws_s3_bucket.terraform_state.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# DynamoDB table for state locking
# DynamoDB table for state locking
# 
# IMPORTANT: This should only be destroyed in sentineldev.
# For sentinelstaging and sentinelprod, destroy operations should be prevented
# via operational controls (CI/CD, IAM policies, manual review).
resource "aws_dynamodb_table" "terraform_locks" {
  name         = local.locks_table_name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "LockID"

  attribute {
    name = "LockID"
    type = "S"
  }

  lifecycle {
    prevent_destroy = false  # Required false for dev; staging/prod protected via external controls
  }
}

# Random suffix for globally unique S3 bucket name (dev only)
resource "random_id" "bucket_suffix" {
  byte_length = 4
}

# Outputs for reference
output "state_bucket_name" {
  value = aws_s3_bucket.terraform_state.id
}

output "locks_table_name" {
  value = aws_dynamodb_table.terraform_locks.id
}