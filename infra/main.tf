# =============================================================================
# Root Terraform Configuration - STL Infrastructure
# =============================================================================
# This configuration uses a flat file structure with numbered prefixes:
# - 01_*.tf: Networking (VPC, subnets, security groups)
# - 02_*.tf: Data layer (S3, Redis, TigerData)
# - 03_*.tf: Messaging (SNS, SQS)
# - 04_*.tf: Compute (ECS, tasks)
# - 05_*.tf: Operations (IAM, secrets, monitoring, bastion)

terraform {
  required_version = "~> 1.0"

  # Backend configured via -backend-config file per environment
  # Usage: terraform init -backend-config=environments/sentinelstaging.backend.hcl
  backend "s3" {}

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = "~> 6.28"
    }
    random = {
      source  = "hashicorp/random"
      version = "~> 3.8"
    }
    timescale = {
      source  = "timescale/timescale"
      version = "~> 2.7"
    }
  }
}

provider "aws" {
  region = var.aws_region

  default_tags {
    tags = {
      Project     = "stl"
      Environment = var.environment
      ManagedBy   = "opentofu"
    }
  }
}

# TigerData (TimescaleDB) provider
# Uses input variables for provider configuration (available at config time)
provider "timescale" {
  project_id = var.tigerdata_project_id
  access_key = var.tigerdata_access_key
  secret_key = var.tigerdata_secret_key
}

