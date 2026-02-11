# Backend configuration for sentineldev environment
# Usage: tofu init -backend-config=environments/sentineldev.backend.hcl
# Note: bucket and dynamodb_table are auto-populated by bootstrap with random suffix

bucket = "stl-sentineldev-terraform-state-392398e1"
key            = "infra/terraform.tfstate"
region         = "eu-west-1"
encrypt        = true
dynamodb_table = "stl-sentineldev-terraform-locks-392398e1"
