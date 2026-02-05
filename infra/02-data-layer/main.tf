# =============================================================================
# Data Layer Module - Main Entry Point
# =============================================================================
# This module consolidates all data layer resources.
# Source files: s3.tf, redis.tf, tigerdata.tf

# All data layer resources are defined in separate .tf files in this directory
# and are loaded automatically by Terraform when this directory is used as a module.

terraform {
  required_providers {
    timescale = {
      source  = "timescale/timescale"
      version = "~> 2.7"
    }
  }
}
