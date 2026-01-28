#!/bin/bash

# Log everything
exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1

echo "=== Starting Erigon Archive Node setup (c8gd.16xlarge with NVMe) ==="

# Install SSM agent first (not pre-installed on AL2023)
echo "=== Installing SSM agent ==="
dnf install -y amazon-ssm-agent
systemctl enable amazon-ssm-agent
systemctl start amazon-ssm-agent

# Update system and install dependencies
dnf update -y
dnf install -y git wget tmux htop nvme-cli mdadm

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

# =============================================================================
# Setup RAID 0 on the two local NVMe instance store disks
# c8gd.16xlarge has 2x 1900GB NVMe disks
# =============================================================================
echo "=== Setting up NVMe RAID 0 ==="

# Wait for NVMe devices to appear
echo "Waiting for NVMe devices..."
sleep 10

# List all NVMe devices
nvme list

# Find the instance store NVMe devices (not the root EBS volume)
# Instance store devices are typically nvme1n1, nvme2n1, etc.
NVME_DEVICES=()
for dev in /dev/nvme*n1; do
  # Skip if it's the root device (has partitions like nvme0n1p1)
  if [ -e "${dev}p1" ]; then
    echo "Skipping root device: $dev"
    continue
  fi
  # Check if it's an instance store (Amazon EC2 NVMe Instance Storage)
  if nvme id-ctrl "$dev" 2>/dev/null | grep -q "Amazon EC2 NVMe Instance Storage"; then
    NVME_DEVICES+=("$dev")
    echo "Found instance store NVMe: $dev"
  fi
done

echo "Instance store devices: ${NVME_DEVICES[*]}"
NUM_DEVICES=${#NVME_DEVICES[@]}

if [ "$NUM_DEVICES" -lt 1 ]; then
  echo "ERROR: No NVMe instance store devices found!"
  exit 1
fi

if [ "$NUM_DEVICES" -eq 1 ]; then
  # Single device, no RAID needed
  DEVICE="${NVME_DEVICES[0]}"
  echo "Single NVMe device: $DEVICE (no RAID needed)"
else
  # Multiple devices - create RAID 0
  echo "Creating RAID 0 with $NUM_DEVICES devices..."
  
  # Stop any existing RAID array on these devices
  mdadm --stop /dev/md0 2>/dev/null || true
  
  # Zero superblocks
  for dev in "${NVME_DEVICES[@]}"; do
    mdadm --zero-superblock "$dev" 2>/dev/null || true
  done
  
  # Create RAID 0 array
  mdadm --create /dev/md0 \
    --level=0 \
    --raid-devices="$NUM_DEVICES" \
    "${NVME_DEVICES[@]}" \
    --force
  
  # Save RAID configuration
  mdadm --detail --scan >> /etc/mdadm.conf
  
  DEVICE="/dev/md0"
  echo "RAID 0 created: $DEVICE"
fi

# Format the device (RAID array or single NVMe)
echo "Formatting $DEVICE with XFS..."
mkfs.xfs -f "$DEVICE"

# Mount at /data
mkdir -p /data

# Add to fstab (use nofail for instance store since it won't survive reboot)
# For instance store, we use UUID to be safe
DEVICE_UUID=$(blkid -s UUID -o value "$DEVICE")
echo "UUID=$DEVICE_UUID /data xfs defaults,nofail,noatime,nodiratime 0 2" >> /etc/fstab
mount /data

echo "Mounted $DEVICE at /data"
df -h /data

# =============================================================================
# Create erigon user and directories
# =============================================================================
useradd -r -s /bin/false erigon || true
mkdir -p /data/erigon
chown -R erigon:erigon /data/erigon

# =============================================================================
# Download and install Erigon (ARM64)
# =============================================================================
ERIGON_VERSION="3.3.4"
echo "Downloading Erigon $ERIGON_VERSION (ARM64)..."
cd /tmp
wget "https://github.com/erigontech/erigon/releases/download/v${ERIGON_VERSION}/erigon_v${ERIGON_VERSION}_linux_arm64.tar.gz"
tar -xzf "erigon_v${ERIGON_VERSION}_linux_arm64.tar.gz"
mv "erigon_v${ERIGON_VERSION}_linux_arm64/erigon" /usr/local/bin/
chmod +x /usr/local/bin/erigon

# Verify installation
/usr/local/bin/erigon --version

# =============================================================================
# Create systemd service for Erigon - READ-ONLY MODE for bulk downloads
# Optimized for maximum RPC query throughput
# =============================================================================
cat > /etc/systemd/system/erigon-readonly.service << 'EOF'
[Unit]
Description=Erigon Ethereum Archive Node (Read-Only Mode)
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
  --http.api=eth,debug,net,trace,web3,erigon \
  --http.vhosts=* \
  --http.corsdomain=* \
  --private.api.addr=localhost:9090 \
  --txpool.disable \
  --nodiscover \
  --maxpeers=0 \
  --db.size.limit=4TB \
  --batchSize=512M \
  --rpc.batch.limit=2000 \
  --rpc.batch.concurrency=128 \
  --rpc.returndata.limit=1000000 \
  --db.read.concurrency=256 \
  --log.console.verbosity=info
Restart=always
RestartSec=10
LimitNOFILE=1048576
LimitMEMLOCK=infinity

[Install]
WantedBy=multi-user.target
EOF

# Also create the normal sync service (in case we need to sync more blocks)
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

systemctl daemon-reload
systemctl enable erigon
systemctl start erigon

echo "=== Setup complete ==="
echo ""
echo "c8gd.16xlarge NVMe RAID 0 setup complete!"
echo ""
echo "Storage: $(df -h /data | tail -1 | awk '{print $2}')"
echo ""
echo "Erigon is now syncing via BitTorrent snapshots."
echo "This will take several hours overnight."
echo ""
echo "Commands:"
echo "  Check status:    systemctl status erigon"
echo "  View logs:       journalctl -u erigon -f"
echo "  Switch to RO:    systemctl stop erigon && systemctl start erigon-readonly"
echo ""
