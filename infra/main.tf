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
# Note: Data sources are read AFTER provider configuration, so we can't use
# data.aws_secretsmanager_secret_version here. Use environment variables or
# -var flags to pass credentials:
#   export TF_VAR_tigerdata_project_id="..."
#   export TF_VAR_tigerdata_access_key="..."
#   export TF_VAR_tigerdata_secret_key="..."
provider "timescale" {
  project_id = var.tigerdata_project_id
  access_key = var.tigerdata_access_key
  secret_key = var.tigerdata_secret_key
}
