variable "aws_region" {
  description = "AWS region to deploy the Erigon node"
  type        = string
  default     = "eu-west-1"
}

variable "instance_type" {
  description = "EC2 instance type (must be c8gd family for NVMe instance store)"
  type        = string
  default     = "c8gd.48xlarge"
}

variable "main_infra_prefix" {
  description = "Name prefix of the main infrastructure (used for VPC/subnet tag lookups)"
  type        = string
  default     = "stl-sentinelstaging"
}

variable "tigerdata_vpc_cidr" {
  description = "CIDR block of the TigerData VPC (for security group egress documentation)"
  type        = string
  default     = "10.1.0.0/24"
}

variable "s3_bucket_name" {
  description = "S3 bucket name for storing extracted data"
  type        = string
  default     = "stl-sentinelstaging-ethereum-raw-89d540d0"
}

variable "key_name" {
  description = "SSH key pair name for EC2 access (optional, using Tailscale instead)"
  type        = string
  default     = null
}

variable "environment" {
  description = "Environment name"
  type        = string
  default     = "staging"
}

variable "project" {
  description = "Project name for tagging"
  type        = string
  default     = "stl-verify"
}
