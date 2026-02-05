# =============================================================================
# Root Terraform Module - STL Infrastructure
# =============================================================================
# This file orchestrates all infrastructure layers organized in numbered directories.
# Each directory represents a logical layer of the infrastructure stack.

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

# Core configuration (variables and locals)
module "core" {
  source      = "./00-core"
  aws_region  = var.aws_region
  environment = var.environment
  prefix      = local.prefix
}

# Networking layer (VPC, security groups)
module "networking" {
  source      = "./01-networking"
  aws_region  = var.aws_region
  environment = var.environment
  prefix      = local.prefix
}

# Data layer (S3, Redis, TimescaleDB)
module "data_layer" {
  source                   = "./02-data-layer"
  aws_region               = var.aws_region
  environment              = var.environment
  project_name             = var.project_name
  prefix                   = local.prefix
  redis_node_type          = var.redis_node_type
  redis_engine_version     = var.redis_engine_version
  redis_num_cache_clusters = var.redis_num_cache_clusters
  redis_transit_encryption = var.redis_transit_encryption
  redis_snapshot_retention = var.redis_snapshot_retention
  tigerdata_project_id     = var.tigerdata_project_id
  tigerdata_access_key     = var.tigerdata_access_key
  tigerdata_secret_key     = var.tigerdata_secret_key
  tigerdata_milli_cpu      = var.tigerdata_milli_cpu
  tigerdata_memory_gb      = var.tigerdata_memory_gb
  tigerdata_ha_replicas    = var.tigerdata_ha_replicas
}

# Messaging layer (SQS, SNS, monitoring)
module "messaging" {
  source      = "./03-messaging"
  aws_region  = var.aws_region
  environment = var.environment
  prefix      = local.prefix
}

# Compute layer (ECS, containers)
module "compute" {
  source                      = "./04-compute"
  aws_region                  = var.aws_region
  environment                 = var.environment
  prefix                      = local.prefix
  watcher_cpu                 = var.watcher_cpu
  watcher_memory              = var.watcher_memory
  watcher_desired_count       = var.watcher_desired_count
  watcher_image_tag           = var.watcher_image_tag
  chain_id                    = var.chain_id
  alchemy_api_key             = var.alchemy_api_key
  alchemy_http_url            = var.alchemy_http_url
  alchemy_ws_url              = var.alchemy_ws_url
  backup_worker_cpu           = var.backup_worker_cpu
  backup_worker_memory        = var.backup_worker_memory
  backup_worker_desired_count = var.backup_worker_desired_count
  backup_worker_image_tag     = var.backup_worker_image_tag
  backup_worker_workers       = var.backup_worker_workers
}

# Operations layer (backend, monitoring, secrets, IAM)
module "operations" {
  source               = "./05-operations"
  aws_region           = var.aws_region
  environment          = var.environment
  prefix               = local.prefix
  tigerdata_project_id = var.tigerdata_project_id
  tigerdata_access_key = var.tigerdata_access_key
  tigerdata_secret_key = var.tigerdata_secret_key
}


