variable "aws_region" {
  description = "AWS region to deploy the Erigon node"
  type        = string
  default     = "eu-west-1"
}

variable "instance_type" {
  description = "EC2 instance type (must be c8gd family for NVMe instance store)"
  type        = string
  default     = "c8gd.48xlarge" # 192 vCPUs (Graviton4), 384GB RAM, 6x 1900GB NVMe
}

variable "vpc_cidr" {
  description = "CIDR block for the isolated VPC"
  type        = string
  default     = "10.100.0.0/16"
}

variable "subnet_cidr" {
  description = "CIDR block for the public subnet"
  type        = string
  default     = "10.100.1.0/24"
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