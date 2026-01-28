#!/bin/bash

# Log everything
exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1

echo "=== Starting Erigon Archive Node setup ==="

# Install SSM agent first (not pre-installed on AL2023)
echo "=== Installing SSM agent ==="
dnf install -y amazon-ssm-agent
systemctl enable amazon-ssm-agent
systemctl start amazon-ssm-agent

# Update system and install dependencies
dnf update -y
dnf install -y git wget tmux htop nvme-cli

# Install Tailscale for secure access (no SSH keys needed)
echo "=== Installing Tailscale ==="
curl -fsSL https://tailscale.com/install.sh | sh

# Fetch Tailscale auth key from Secrets Manager
echo "=== Fetching Tailscale auth key ==="
TAILSCALE_AUTH_KEY=$(aws secretsmanager get-secret-value \
  --secret-id stl-erigon-tailscale-auth-key \
  --query SecretString \
  --output text \
  --region eu-west-1) || true

if [ -n "$TAILSCALE_AUTH_KEY" ]; then
  echo "=== Starting Tailscale ==="
  # --ssh enables Tailscale SSH for secure access without traditional SSH keys
  tailscale up --auth-key="$TAILSCALE_AUTH_KEY" --ssh || echo "WARNING: Tailscale failed to start"
else
  echo "WARNING: Could not fetch Tailscale auth key from Secrets Manager"
fi

# From here on, fail on errors
set -e

# Wait for EBS volume to attach
echo "Waiting for EBS volume..."
while [ ! -e /dev/nvme1n1 ] && [ ! -e /dev/xvdf ]; do
  sleep 5
done

# Determine the device name (NVMe vs traditional)
if [ -e /dev/nvme1n1 ]; then
  DEVICE="/dev/nvme1n1"
else
  DEVICE="/dev/xvdf"
fi

echo "Found device: $DEVICE"

# Format and mount the data volume (only if not already formatted)
if ! blkid $DEVICE; then
  echo "Formatting $DEVICE..."
  mkfs.xfs $DEVICE
fi

mkdir -p /data
echo "$DEVICE /data xfs defaults,nofail 0 2" >> /etc/fstab
mount /data

# Create erigon user
useradd -r -s /bin/false erigon || true
mkdir -p /data/erigon
chown -R erigon:erigon /data/erigon

# Download Erigon (ARM64)
# Note: Erigon 3.x tarball naming: erigon_v${VERSION}_linux_arm64.tar.gz
# and extracts to erigon_v${VERSION}_linux_arm64/ directory
ERIGON_VERSION="3.3.4"
echo "Downloading Erigon $ERIGON_VERSION (ARM64)..."
cd /tmp
wget "https://github.com/erigontech/erigon/releases/download/v${ERIGON_VERSION}/erigon_v${ERIGON_VERSION}_linux_arm64.tar.gz"
tar -xzf "erigon_v${ERIGON_VERSION}_linux_arm64.tar.gz"
mv "erigon_v${ERIGON_VERSION}_linux_arm64/erigon" /usr/local/bin/
chmod +x /usr/local/bin/erigon

# Verify installation
/usr/local/bin/erigon --version

# Create systemd service for Erigon - FULL ARCHIVE MODE
# Key flags for Erigon 3.x:
#   --prune.mode=archive    : Full archive mode, keep everything
#   --snap.keepblocks       : Keep all block bodies (don't prune after snapshot import)
#   Snapshots are enabled by default in Erigon 3.x
cat > /etc/systemd/system/erigon.service << 'EOF'
[Unit]
Description=Erigon Ethereum Archive Node
After=network.target

[Service]
Type=simple
User=erigon
ExecStart=/usr/local/bin/erigon \
  --datadir=/data/erigon \
  --chain=mainnet \
  --prune.mode=archive \
  --snap.keepblocks \
  --http \
  --http.addr=0.0.0.0 \
  --http.port=8545 \
  --http.api=eth,debug,net,trace,web3,erigon,txpool \
  --http.vhosts=* \
  --http.corsdomain=* \
  --private.api.addr=localhost:9090 \
  --torrent.download.rate=500mb \
  --torrent.upload.rate=50mb \
  --db.size.limit=4TB \
  --batchSize=512M \
  --rpc.batch.limit=1000 \
  --rpc.batch.concurrency=32 \
  --rpc.returndata.limit=1000000 \
  --db.read.concurrency=128 \
  --log.console.verbosity=info
Restart=always
RestartSec=10
LimitNOFILE=1048576
LimitMEMLOCK=infinity

[Install]
WantedBy=multi-user.target
EOF

# Enable and start Erigon
systemctl daemon-reload
systemctl enable erigon

echo "=== Setup complete ==="
echo ""
echo "Erigon is configured in FULL ARCHIVE mode with official snapshots."
echo ""
echo "Snapshots will be downloaded via BitTorrent from the official Erigon repository."
echo "This is much faster than syncing from genesis (~hours instead of days)."
echo ""
echo "Commands:"
echo "  Start:   systemctl start erigon"
echo "  Logs:    journalctl -u erigon -f"
echo "  Status:  curl -s localhost:8545 -X POST -H 'Content-Type: application/json' \\"
echo "           --data '{\"jsonrpc\":\"2.0\",\"method\":\"eth_syncing\",\"params\":[],\"id\":1}'"
echo ""
echo "  Get block: curl -s localhost:8545 -X POST -H 'Content-Type: application/json' \\"
echo "             --data '{\"jsonrpc\":\"2.0\",\"method\":\"eth_getBlockByNumber\",\"params\":[\"0xF42400\",true],\"id\":1}'"
echo ""

# Start Erigon automatically
systemctl start erigon

echo "=== Erigon started! Snapshot download beginning... ==="
