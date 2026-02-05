variable "aws_region" {
  description = "AWS region for resources"
  type        = string
}

variable "environment" {
  description = "Environment name"
  type        = string
}

variable "prefix" {
  description = "Naming prefix for resources"
  type        = string
}

# TigerData Configuration
variable "tigerdata_project_id" {
  description = "TigerData project ID"
  type        = string
}

variable "tigerdata_access_key" {
  description = "TigerData access key"
  type        = string
  sensitive   = true
}

variable "tigerdata_secret_key" {
  description = "TigerData secret key"
  type        = string
  sensitive   = true
}
