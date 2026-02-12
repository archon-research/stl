#!/bin/bash

# Log everything
exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1

echo "=== Starting Backfill Worker setup ==="

# Install SSM agent first (not pre-installed on AL2023)
echo "=== Installing SSM agent ==="
dnf install -y amazon-ssm-agent
systemctl enable amazon-ssm-agent
systemctl start amazon-ssm-agent

# Update system and install dependencies
dnf update -y
dnf install -y tmux htop jq

# Install Tailscale for secure access (no SSH keys needed)
echo "=== Installing Tailscale ==="
curl -fsSL https://tailscale.com/install.sh | sh

echo "=== Updating Tailscale to latest ==="
tailscale update --yes || echo "Tailscale update not needed or failed"

# Fetch Tailscale auth key from Secrets Manager
echo "=== Fetching Tailscale auth key ==="
TAILSCALE_AUTH_KEY=$(aws secretsmanager get-secret-value \
  --secret-id stl-erigon-tailscale-auth-key \
  --query SecretString \
  --output text \
  --region eu-west-1)
if [ $? -ne 0 ]; then
  echo "ERROR: Failed to fetch Tailscale auth key from Secrets Manager"
fi

if [ -n "$TAILSCALE_AUTH_KEY" ]; then
  echo "=== Starting Tailscale ==="
  tailscale up --auth-key="$TAILSCALE_AUTH_KEY" --ssh || echo "WARNING: Tailscale failed to start"
else
  echo "WARNING: Could not fetch Tailscale auth key from Secrets Manager"
fi

echo "=== Setup complete ==="
echo ""
echo "Worker instance is ready."
echo "Deploy the backfill binary and run it against the Erigon RPC endpoint."
echo ""
