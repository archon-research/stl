# Backend configuration for sentineldev environment
# Usage: tofu init -backend-config=environments/sentineldev.backend.hcl
# Note: bucket and dynamodb_table are auto-populated by bootstrap with random suffix

bucket = "stl-sentineldev-terraform-state-6bc72af4"
key            = "infra/terraform.tfstate"
region         = "eu-west-1"
