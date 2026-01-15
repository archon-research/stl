terraform {
  required_version = "~> 1.0"

  # Backend configured via -backend-config file per environment
  # Usage: tofu init -backend-config=environments/sentinelstaging.backend.hcl
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
