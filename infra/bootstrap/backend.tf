# S3 bucket for Terraform state
# 
# Protection strategy:
# - force_destroy = true ONLY for sentineldev (allows deletion with objects inside)
# - force_destroy = false for staging/prod (must manually empty bucket first)
# - prevent_destroy is disabled to allow programmatic cleanup (dev) while staging/prod
#   are protected via CI/CD workflow checks and IAM policies
#
# Staging/prod destruction is blocked by:
#   1. GitHub Actions workflow checks (only allows sentineldev)
#   2. Manual confirmation required for local runs
#   3. force_destroy = false means bucket must be manually emptied first
resource "aws_s3_bucket" "terraform_state" {
  bucket        = local.state_bucket_name
  force_destroy = var.environment == "sentineldev"

  lifecycle {
    prevent_destroy = false
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
# 
# Protection strategy:
# - prevent_destroy is disabled to allow programmatic cleanup (dev)
# - Staging/prod are protected via CI/CD workflow checks and IAM policies
resource "aws_dynamodb_table" "terraform_locks" {
  name         = local.locks_table_name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "LockID"

  attribute {
    name = "LockID"
    type = "S"
  }

  lifecycle {
    prevent_destroy = false
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