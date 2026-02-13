variable "aws_region" {
  description = "AWS region"
  type        = string
  default     = "eu-west-1"
}

variable "environment" {
  description = "Environment name (sentineldev, sentinelstaging, sentinelprod)"
  type        = string
  
  validation {
    condition     = contains(["sentineldev", "sentinelstaging", "sentinelprod"], var.environment)
    error_message = "Environment must be one of: sentineldev, sentinelstaging, sentinelprod. CRITICAL: Only sentineldev infrastructure should be destroyed."
  }
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

  validation {
    condition     = var.tigerdata_project_id != ""
    error_message = "TIGERDATA_PROJECT_ID is required for bootstrap. Set it in .env and source the file before running tf-bootstrap."
  }
}

variable "tigerdata_access_key" {
  description = "TigerData API access key"
  type        = string
  sensitive   = true
  default     = ""

  validation {
    condition     = var.tigerdata_access_key != ""
    error_message = "TIGERDATA_ACCESS_KEY is required for bootstrap. Set it in .env and source the file before running tf-bootstrap."
  }
}

variable "tigerdata_secret_key" {
  description = "TigerData API secret key"
  type        = string
  sensitive   = true
  default     = ""

  validation {
    condition     = var.tigerdata_secret_key != ""
    error_message = "TIGERDATA_SECRET_KEY is required for bootstrap. Set it in .env and source the file before running tf-bootstrap."
  }
}

variable "alchemy_api_key" {
  description = "Alchemy API key for blockchain data"
  type        = string
  sensitive   = true
  default     = ""

  validation {
    condition     = var.alchemy_api_key != "" && length(var.alchemy_api_key) > 10
    error_message = "ALCHEMY_API_KEY is required for bootstrap (min 10 chars). Get from https://dashboard.alchemy.com/apps and set in .env."
  }
}

variable "coingecko_api_key" {
  description = "CoinGecko Pro API key for price data"
  type        = string
  sensitive   = true
  default     = ""
}

variable "etherscan_api_key" {
  description = "Etherscan API key for contract verification"
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