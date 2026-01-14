# =============================================================================
# Terraform State Backend Resources
# =============================================================================
# These resources store Terraform state remotely with locking.
# After initial apply, uncomment the backend block in main.tf and run:
#   tofu init -migrate-state

locals {
  state_bucket_name = "${local.prefix_lowercase}-terraform-state"
  locks_table_name  = "${local.prefix_lowercase}-terraform-locks"
}

# S3 bucket for state storage
resource "aws_s3_bucket" "terraform_state" {
  bucket = local.state_bucket_name

  lifecycle {
    prevent_destroy = true
  }

  tags = {
    Name    = local.state_bucket_name
    Purpose = "terraform-state"
  }
}

# Enable versioning for state history and recovery
resource "aws_s3_bucket_versioning" "terraform_state" {
  bucket = aws_s3_bucket.terraform_state.id

  versioning_configuration {
    status = "Enabled"
  }
}

# Enable encryption at rest
resource "aws_s3_bucket_server_side_encryption_configuration" "terraform_state" {
  bucket = aws_s3_bucket.terraform_state.id

  rule {
    apply_server_side_encryption_by_default {
      sse_algorithm = "AES256"
    }
  }
}

# Block all public access
resource "aws_s3_bucket_public_access_block" "terraform_state" {
  bucket = aws_s3_bucket.terraform_state.id

  block_public_acls       = true
  block_public_policy     = true
  ignore_public_acls      = true
  restrict_public_buckets = true
}

# DynamoDB table for state locking
resource "aws_dynamodb_table" "terraform_locks" {
  name         = local.locks_table_name
  billing_mode = "PAY_PER_REQUEST"
  hash_key     = "LockID"

  attribute {
    name = "LockID"
    type = "S"
  }

  tags = {
    Name    = local.locks_table_name
    Purpose = "terraform-state-locking"
  }
}
