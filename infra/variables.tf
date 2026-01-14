variable "aws_region" {
  description = "AWS region for resources"
  type        = string
  default     = "eu-west-1"
}

variable "project_name" {
  description = "Name of the project, used as prefix for all resources"
  type        = string
  default     = "stl"
}

variable "environment" {
  description = "Environment name (e.g., dev, staging, prod)"
  type        = string
  # No default - must be specified explicitly
}
