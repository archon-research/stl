# STL Infrastructure Deployment Workflow

Complete, fully-automated deployment workflow for all environments (sentineldev, sentinelstaging, sentinelprod).

## Prerequisites

1. **AWS Credentials**: Configured via `aws configure` or environment variables
2. **OpenTofu**: Installed (see [OpenTofu Installation](https://opentofu.org/docs/intro/install/))
3. **jq**: For JSON parsing (brew install jq on macOS)

## Step 1: Prepare Environment Variables

```bash
# Copy the example file
cp .env.example .env

# Edit .env with your actual credentials
vi .env

# Source the variables (or add to your shell profile)
source .env
```

### Required Credentials

| Variable | Source | Example |
|----------|--------|---------|
| `TIGERDATA_PROJECT_ID` | TigerData console `/account/settings` | `p71n930y81` |
| `TIGERDATA_ACCESS_KEY` | TigerData console `/account/api-keys` | `tsak_xxx...` |
| `TIGERDATA_SECRET_KEY` | TigerData console `/account/api-keys` | `tssk_xxx...` |
| `ALCHEMY_API_KEY` | Alchemy dashboard `/settings/api-keys` | `alchemy_xxx...` |
| `TAILSCALE_AUTH_KEY` | Tailscale admin console (staging/prod only) | `tskey_xxx...` |

## Step 2: Bootstrap Backend Infrastructure

Bootstrap creates the S3 bucket and DynamoDB table that Terraform state will use.

### Development Environment (sentineldev)

```bash
cd stl-verify

# Bootstrap
make tf-bootstrap ENV=sentineldev \
  TIGERDATA_PROJECT_ID="$TIGERDATA_PROJECT_ID" \
  TIGERDATA_ACCESS_KEY="$TIGERDATA_ACCESS_KEY" \
  TIGERDATA_SECRET_KEY="$TIGERDATA_SECRET_KEY" \
  ALCHEMY_API_KEY="$ALCHEMY_API_KEY"

# What it creates:
# ✓ S3 bucket: stl-sentineldev-terraform-state-{suffix}
# ✓ DynamoDB table: stl-sentineldev-terraform-locks
# ✓ Secret: stl-sentineldev-tigerdata
# ✓ Secret: stl-sentineldev-watcher-config
```

### Staging Environment (sentinelstaging - WITH Tailscale)

```bash
cd stl-verify

# Bootstrap
make tf-bootstrap ENV=sentinelstaging \
  TIGERDATA_PROJECT_ID="$TIGERDATA_PROJECT_ID" \
  TIGERDATA_ACCESS_KEY="$TIGERDATA_ACCESS_KEY" \
  TIGERDATA_SECRET_KEY="$TIGERDATA_SECRET_KEY" \
  ALCHEMY_API_KEY="$ALCHEMY_API_KEY" \
  TAILSCALE_AUTH_KEY="$TAILSCALE_AUTH_KEY"

# What it creates:
# ✓ S3 bucket: stl-sentinelstaging-terraform-state-{suffix}
# ✓ DynamoDB table: stl-sentinelstaging-terraform-locks
# ✓ Secret: stl-sentinelstaging-tigerdata
# ✓ Secret: stl-sentinelstaging-watcher-config
# ✓ Secret: stl-sentinelstaging-tailscale-auth-key
```

### Production Environment (sentinelprod)

```bash
make tf-bootstrap ENV=sentinelprod \
  TIGERDATA_PROJECT_ID="$TIGERDATA_PROJECT_ID" \
  TIGERDATA_ACCESS_KEY="$TIGERDATA_ACCESS_KEY" \
  TIGERDATA_SECRET_KEY="$TIGERDATA_SECRET_KEY" \
  ALCHEMY_API_KEY="$ALCHEMY_API_KEY" \
  TAILSCALE_AUTH_KEY="$TAILSCALE_AUTH_KEY"
```

## Step 3: Deploy Main Infrastructure

After bootstrap completes successfully, deploy the main infrastructure.

```bash
# Initialize main infrastructure (migrates state from local to remote S3)
make tf-init ENV=sentineldev

# Validate configuration
make tf-validate ENV=sentineldev

# Review planned changes
make tf-plan ENV=sentineldev > tfplan.txt
# Review tfplan.txt for what will be created

# Apply infrastructure (LIVE - creates actual AWS resources)
make tf-apply ENV=sentineldev
```

### Full Automated Workflow

```bash
# One-liner to complete bootstrap → init → validate → plan
make tf-check ENV=sentineldev
```

## Step 4: Verify Deployment

```bash
# Check created resources
make tf-state ENV=sentineldev

# View outputs (secrets, endpoints, etc.)
tofu output -json -chdir=infra
```

## Step 5: Setup Bastion / Tailscale

```bash
# After infrastructure is deployed, connect to Tailscale
# (bastion host automatically joins Tailscale network with provided auth key)

# Verify Tailscale status on bastion
make bastion-tailscale-status ENV=sentinelstaging
make bastion-connect ENV=sentinelstaging

# Create SSH tunnel to TigerData via bastion
make tigerdata-tunnel BASTION_IP=<tailscale-ip>
```

## Cleanup / Destroy Infrastructure

⚠️ **WARNING: This will delete all infrastructure and data**

```bash
# Destroy main infrastructure (but keep bootstrap resources)
make tf-destroy ENV=sentineldev

# Destroy bootstrap (state bucket, secrets, etc.)
cd stl-verify && make tf-destroy ENV=sentineldev -- BOOTSTRAP=true
```

## Troubleshooting

### Bootstrap Fails in Phase 1

```
Error: Error putting object in S3 bucket
```

**Solution**: Check AWS credentials and permissions. S3 bucket names must be globally unique.

### Bootstrap Fails in Phase 2 (Secrets)

```
Error: Unable to create service, got error: target VPC must have an active peering connection
```

**Solution**: This is expected on first apply due to VPC peering timing. The time_sleep resource ensures peering is active. Retry `make tf-apply`.

### tf-init Fails

```
Error: No state file was found
```

**Solution**: Ensure bootstrap completed successfully. Check that `infra/bootstrap/terraform.tfstate` exists:

```bash
ls -la infra/bootstrap/terraform.tfstate
```

### Can't Access TigerData from Local Machine

**For Development**: Skip Tailscale (bastion disabled)
- Use TigerData's VPC peering and aws_vpc_peering_connection

**For Staging/Prod**: Use Tailscale
1. Install Tailscale: `brew install tailscale`
2. Connect: `tailscale up`
3. Verify bastion is reachable: `ping <bastion-tailscale-ip>`
4. Create tunnel: `make tigerdata-tunnel BASTION_IP=<ip>`

## Architecture Notes

### Two-Phase Deployment

1. **Bootstrap Phase** (infra/bootstrap/)
   - Creates backend infrastructure (state bucket, locks table)
   - Creates initial secrets (TigerData API, Alchemy key, Tailscale key)
   - Uses **local state** (versioned in git)
   - One-time per environment

2. **Main Infrastructure Phase** (infra/)
   - Depends on bootstrap state
   - Creates application infrastructure (VPC, ECS, databases, etc.)
   - Uses **remote state** (S3 backend, DynamoDB locks)
   - Can be deployed/updated multiple times

### Secrets Hierarchy

| Secret Name | Created By | Used By | Optional |
|-------------|-----------|---------|----------|
| `stl-{env}-tigerdata` | Bootstrap | Terraform provider | No |
| `stl-{env}-watcher-config` | Bootstrap | ECS Watcher task | No |
| `stl-{env}-tigerdata-db` | Main infra | ECS tasks | No (auto-generated) |
| `stl-{env}-tigerdata-app` | Main infra | Application services | No (auto-generated) |
| `stl-{env}-tigerdata-readonly` | Main infra | Application services | No (auto-generated) |
| `stl-{env}-tailscale-auth-key` | Bootstrap | Bastion EC2 | Yes (staging/prod only) |

## Automation Scripts

For fully programmatic deployment (CI/CD):

```bash
#!/bin/bash
ENV=${1:-sentineldev}

cd stl-verify

# Bootstrap
make tf-bootstrap \
  ENV=$ENV \
  TIGERDATA_PROJECT_ID="$TIGERDATA_PROJECT_ID" \
  TIGERDATA_ACCESS_KEY="$TIGERDATA_ACCESS_KEY" \
  TIGERDATA_SECRET_KEY="$TIGERDATA_SECRET_KEY" \
  ALCHEMY_API_KEY="$ALCHEMY_API_KEY" \
  TAILSCALE_AUTH_KEY="$TAILSCALE_AUTH_KEY"

# Main infrastructure
make tf-init ENV=$ENV
make tf-validate ENV=$ENV
make tf-plan ENV=$ENV
make tf-apply ENV=$ENV
```

## Cost Estimates

| Environment | Components | Monthly Cost |
|-------------|-----------|--------------|
| sentineldev | Minimal: t4g.nano bastion, cache.t4g.small Redis, 0.5 CPU TigerData | ~$100-150 |
| sentinelstaging | Full: t4g.nano bastion, cache.r7g.xlarge Redis, 0.5-4 CPU TigerData | ~$400-600 |
| sentinelprod | Production: t3.medium+ bastion, cache.r7g.4xlarge+ Redis, 16+ CPU TigerData | ~$2000+ |

(Excludes ECS Fargate, networking, and data transfer costs)
