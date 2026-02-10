variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "eu-west-1"
}

variable "environment" {
  description = "Environment name (sentineldev, sentinelstaging)"
  type        = string
}

variable "project_name" {
  description = "Project name"
  type        = string
  default     = "stl"
}
# =============================================================================
# Initial Secrets (created during bootstrap)
# =============================================================================
# These are required for the main infrastructure to work.
# Pass via: -var tigerdata_project_id="..." -var tigerdata_access_key="..." etc.

variable "tigerdata_project_id" {
  description = "TigerData project ID (get from TigerData console)"
  type        = string
  sensitive   = true
  default     = ""
}

variable "tigerdata_access_key" {
  description = "TigerData API access key"
  type        = string
  sensitive   = true
  default     = ""
}

variable "tigerdata_secret_key" {
  description = "TigerData API secret key"
  type        = string
  sensitive   = true
  default     = ""
}

variable "alchemy_api_key" {
  description = "Alchemy API key for blockchain data"
  type        = string
  sensitive   = true
  default     = ""
}

variable "tailscale_auth_key" {
  description = "Tailscale auth key (optional, for bastion VPN setup in staging/prod)"
  type        = string
  sensitive   = true
  default     = ""
}