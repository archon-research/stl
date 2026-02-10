# STL Infrastructure Deployment Guide

## Overview

Complete automation for provisioning STL infrastructure across environments (sentineldev, sentinelstaging).

## Prerequisites

- OpenTofu/Terraform 1.0+: `brew install opentofu`
- AWS CLI configured with appropriate credentials
- Secure storage for bootstrap state (encrypted S3, password manager, or secure vault)
- **Important**: Ensure `*.tfstate` files are in `.gitignore` (never commit state files)

## Environments

- **sentineldev**: Cost-optimized development environment
- **sentinelstaging**: Production-like staging environment

## Deployment Workflow

### 1. One-Time Bootstrap (Per Environment)

Creates S3 backend bucket and DynamoDB state lock table.

```bash
cd stl-verify
make tf-bootstrap ENV=sentineldev
```

This will:
- Create S3 bucket: `stl-sentineldev-terraform-state-{suffix}`
- Create DynamoDB table: `stl-sentineldev-terraform-locks`
- Create initial secrets in AWS Secrets Manager
- Generate local `infra/bootstrap/terraform.tfstate`

**⚠️ SECURITY WARNING: Never commit terraform.tfstate to git!**

The bootstrap state file contains sensitive values (secret ARNs, versions, etc.) in plaintext. Either:
1. **Recommended**: Migrate bootstrap state to its own S3 backend (separate from main infra)
2. Store the state file in a secure, encrypted location (e.g., 1Password, AWS Secrets Manager)
3. Ensure the state file is in `.gitignore` and securely backed up elsewhere

### 2. Initialize Remote Backend

Migrates state from local to remote S3 backend.

```bash
make tf-init ENV=sentineldev
```

This will reconfigure Terraform to use the S3 backend.

### 3. Validate Configuration

```bash
make tf-validate ENV=sentineldev
```

### 4. Plan Changes

```bash
make tf-plan ENV=sentineldev
```

Review the plan output to see what resources will be created/modified.

### 5. Apply Changes

```bash
make tf-apply ENV=sentineldev
```

This provisions all infrastructure defined in Terraform.

## Quick Start (Combined Commands)

### Full workflow in one command:
```bash
make tf-check ENV=sentineldev  # bootstrap + init + validate + plan
make tf-apply ENV=sentineldev   # apply all resources
```

## Makefile Targets

| Target | Purpose |
|--------|---------|
| `check-tofu` | Verify OpenTofu is installed |
| `tf-bootstrap ENV=<env>` | Create S3 bucket + DynamoDB table (one-time) |
| `tf-init ENV=<env>` | Initialize Terraform with remote backend |
| `tf-validate ENV=<env>` | Validate Terraform configuration |
| `tf-plan ENV=<env>` | Plan changes (dry-run) |
| `tf-apply ENV=<env>` | Apply changes (provision infrastructure) |
| `tf-check ENV=<env>` | Run: bootstrap → init → validate → plan |

## Environment Variables

Environment-specific configuration is stored in:
- `infra/environments/{ENV}.tfvars` - Infrastructure variables
- `infra/environments/{ENV}.backend.hcl` - Backend configuration

### Secrets Management

- **TigerData credentials**: Loaded from AWS Secrets Manager (`stl-{ENV}-tigerdata`)
- **Stored locally**: `.env` files (git-ignored)

The Makefile automatically fetches credentials from Secrets Manager during plan/apply.

## File Structure

```
infra/
├── main.tf                         # Root config, providers
├── variables.tf                    # All input variables
├── locals.tf                       # Shared values
├── outputs.tf                      # Output values
├── backend.tf                      # S3 bucket + DynamoDB table
├── 01_vpc.tf                       # VPC, subnets
├── 01_security_groups.tf           # Security groups
├── 02_s3.tf                        # S3 buckets
├── 02_redis.tf                     # ElastiCache Redis
├── 02_tigerdata.tf                 # TimescaleDB configs
├── 03_messaging.tf                 # SNS, SQS
├── 03_messaging_monitoring.tf      # Monitoring for messaging
├── 04_ecs.tf                       # ECS cluster
├── 04_ecs_watcher.tf               # Watcher task
├── 04_ecs_backup_worker.tf         # Backup worker task
├── 04_ecs_oracle_price_worker.tf   # Oracle price worker task
├── 05_bastion.tf                   # Bastion host
├── 05_iam_ecs_task.tf              # Task IAM roles
├── 05_monitoring.tf                # Monitoring, alarms
├── 05_redis_monitoring.tf          # Redis monitoring
├── 05_secrets.tf                   # AWS Secrets Manager
├── 05_tigerdata_users.tf           # Database users
└── environments/
    ├── sentineldev.tfvars          # Dev variables
    ├── sentineldev.backend.hcl     # Dev backend config
    ├── sentinelstaging.tfvars      # Staging variables
    └── sentinelstaging.backend.hcl # Staging backend config
```

## Troubleshooting

### "terraform.tfstate not found"
Run `make tf-bootstrap ENV=<env>` first to create the backend.

### "S3 bucket already exists"
This is normal on subsequent runs. The bootstrap is idempotent.

### "Backend initialization required"
Run `make tf-init ENV=<env>` to initialize the remote backend.

### AWS credentials issues
- Verify AWS CLI is configured: `aws sts get-caller-identity`
- Check IAM permissions for creating S3 buckets, DynamoDB tables, etc.

## State Management

- **Bootstrap state**: `infra/bootstrap/terraform.tfstate` (NEVER commit to git - contains sensitive data)
- **Main infra state**: S3 bucket (locking via DynamoDB)
- **State backup**: Automatically kept by S3 versioning

### Securing Bootstrap State

The bootstrap state contains secret ARNs and metadata. Options for secure storage:

1. **S3 Backend (Recommended)**: Configure bootstrap to use its own remote backend
2. **Encrypted Storage**: Store in 1Password, AWS Secrets Manager, or encrypted S3 bucket
3. **Local Only**: Keep locally but ensure it's in `.gitignore` and backed up securely

Never commit `terraform.tfstate` files to version control.

## Destroying Infrastructure

To destroy all resources:
```bash
make tf-destroy ENV=sentineldev
```

**Note**: S3 bucket and DynamoDB table have `prevent_destroy = true` to protect against accidental deletion.

## Multi-Environment Deployment

Deploy to both environments:
```bash
# Bootstrap both environments
make tf-bootstrap ENV=sentineldev
make tf-bootstrap ENV=sentinelstaging

# Securely backup bootstrap state (DO NOT commit to git)
# Option 1: Upload to encrypted S3 bucket
aws s3 cp infra/bootstrap/terraform.tfstate s3://your-secure-bucket/bootstrap-states/sentineldev.tfstate --sse AES256

# Option 2: Store in password manager or secure vault

# Deploy main infrastructure
make tf-init ENV=sentineldev && make tf-apply ENV=sentineldev
make tf-init ENV=sentinelstaging && make tf-apply ENV=sentinelstaging
```
