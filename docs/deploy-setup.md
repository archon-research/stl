# Deploy Pipeline

## How It Works

When code merges to `main`, CI builds Docker images and pushes them to ECR with the commit SHA as the tag. ArgoCD Image Updater polls ECR every 2 minutes, detects new tags, and commits the updated image references to git. A GitHub Action then creates PRs from those commits.

```
merge to main
  → CI builds images, pushes to staging + prod ECR (tagged with commit SHA)
  → Image Updater detects new SHA tags in ECR
  → Commits tag updates to deploy/staging and deploy/prod branches
  → GitHub Action (deploy-pr.yaml) creates PRs:
      staging → PR auto-merges → ArgoCD auto-syncs
      prod    → PR awaits review → merge → ArgoCD manual sync
```

### Staging

Fully automatic. Image Updater pushes to the `deploy/staging` branch, the deploy-pr workflow creates a PR and auto-merges it via the `stl-deploy-bot` GitHub App. ArgoCD has automated sync enabled for staging, so it picks up the change immediately.

### Production

Gated. Image Updater pushes to the `deploy/prod` branch, the deploy-pr workflow creates a PR that requires review from `@archon-research/vector-engineers` (CODEOWNERS). After merge, an operator manually syncs in ArgoCD.

## Components

### ArgoCD Image Updater

Runs on the prod EKS cluster (`argocd` namespace). Polls staging and prod ECR registries for new image tags matching `^[0-9a-f]{7,40}$` (git SHAs). Uses `newest-build` strategy to pick the latest image.

Configured via annotations on the ArgoCD Application CRs in the infrastructure repo:
- `gitops/apps/archon-staging/vector/stl.yaml`
- `gitops/apps/archon-prod/vector/stl.yaml`

Write-back method is `git` — Image Updater commits `.argocd-source-<appname>.yaml` files to `deploy/staging` and `deploy/prod` branches.

### GitHub App (`stl-deploy-bot`)

A GitHub App installed on this repo with `Contents: Write` and `Pull requests: Write` permissions. Added to the branch protection bypass list for `main` so it can auto-merge staging PRs without review.

Credentials stored as org-level GitHub Actions secrets: `DEPLOY_APP_ID` and `DEPLOY_APP_PRIVATE_KEY`.

### Deploy PR Workflow (`.github/workflows/deploy-pr.yaml`)

Triggered on pushes to `deploy/staging` and `deploy/prod` branches. Generates a short-lived token from the GitHub App, creates a PR to main (or detects an existing one), and enables auto-merge for staging.

### CI Loop Prevention

The CI workflow (`.github/workflows/ci.yml`) has `paths-ignore` for `.argocd-source-*.yaml` files. This prevents the staging auto-merge from re-triggering the full CI → build → deploy pipeline.

## Setup

One-time manual steps required before this pipeline works.

### 1. Create the GitHub App

1. Go to `https://github.com/organizations/archon-research/settings/apps/new`
2. **Name**: `stl-deploy-bot`
3. **Webhook**: Uncheck "Active"
4. **Permissions**: Contents (Read & Write), Pull requests (Read & Write)
5. **Install on**: `archon-research/stl` only
6. Store **App ID** and **Private key** (.pem) as GitHub Actions org secrets: `DEPLOY_APP_ID`, `DEPLOY_APP_PRIVATE_KEY`

### 2. Add App to Branch Protection Bypass

1. `stl` repo → Settings → Branches → `main` protection rule
2. Under "Require a pull request before merging", add `stl-deploy-bot` to "Allow specified actors to bypass required pull requests"

### 3. Upgrade Deploy Key to Read-Write

Image Updater needs write access to push to `deploy/*` branches.

1. Regenerate the deploy key:
   ```bash
   ssh-keygen -t ed25519 -C "argocd-stl-deploy-key" -f /tmp/stl-deploy-key -N ""
   ```
2. Update the deploy key in GitHub (`stl` repo → Settings → Deploy keys) with write access enabled
3. Update `stl-<env>-stl-deploy-key` in AWS Secrets Manager with the new private key
4. Restart Image Updater:
   ```bash
   kubectl rollout restart deployment argocd-image-updater -n argocd
   ```
