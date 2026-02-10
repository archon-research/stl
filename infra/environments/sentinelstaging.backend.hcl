# Backend configuration for sentinelstaging environment
# Usage: tofu init -backend-config=environments/sentinelstaging.backend.hcl

bucket         = "stl-sentinelstaging-terraform-state"
key            = "infra/terraform.tfstate"
region         = "eu-west-1"
encrypt        = true
dynamodb_table = "stl-sentinelstaging-terraform-locks"
