bucket         = "stl-sentinelprod-terraform-state"
key            = "infra/terraform.tfstate"
region         = "eu-west-1"
encrypt        = true
dynamodb_table = "stl-sentinelprod-terraform-locks"
