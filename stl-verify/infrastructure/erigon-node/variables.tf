variable "aws_region" {
  description = "AWS region to deploy the Erigon node"
  type        = string
  default     = "eu-west-1"
}

variable "instance_type" {
  description = "EC2 instance type"
  type        = string
  default     = "r7g.4xlarge" # 16 vCPUs, 128GB RAM, ARM (Graviton3)
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

variable "volume_size_gb" {
  description = "EBS volume size in GB for Erigon data"
  type        = number
  default     = 4000 # 4TB
}

variable "volume_iops" {
  description = "EBS volume IOPS (gp3)"
  type        = number
  default     = 16000
}

variable "volume_throughput" {
  description = "EBS volume throughput in MB/s (gp3)"
  type        = number
  default     = 1000
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


