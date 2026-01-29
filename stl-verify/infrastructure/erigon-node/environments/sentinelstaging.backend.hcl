# Backend configuration for erigon-node (sentinelstaging)
# Usage: tofu init -backend-config=environments/sentinelstaging.backend.hcl

bucket         = "stl-sentinelstaging-terraform-state-89d540d0"
key            = "erigon-node/terraform.tfstate"
region         = "eu-west-1"
encrypt        = true
dynamodb_table = "stl-sentinelstaging-terraform-locks"
