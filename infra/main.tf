terraform {
  required_version = "~> 1.0"

  backend "s3" {
    bucket         = "stl-sentinelstaging-terraform-state"
    key            = "infra/terraform.tfstate"
    region         = "eu-west-1"
    encrypt        = true
    dynamodb_table = "stl-sentinelstaging-terraform-locks"
  }

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
