#!/bin/bash

# Log everything
exec > >(tee /var/log/user-data.log|logger -t user-data -s 2>/dev/console) 2>&1

echo "=== Starting Erigon Archive Node setup (c8gd with NVMe) ==="

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

# Update Tailscale to latest version (install.sh may install an older version from repos)
echo "=== Updating Tailscale to latest ==="
tailscale update --yes || echo "Tailscale update not needed or failed"

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
# c8gd instances have local NVMe instance store disks
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
  --state.cache=240GB \
  --bodies.cache=48GB \
  --batchSize=2GB \
  --db.size.limit=8TB \
  --rpc.batch.limit=10000 \
  --rpc.batch.concurrency=384 \
  --rpc.returndata.limit=10000000 \
  --rpc.evmtimeout=30m \
  --db.read.concurrency=512 \
  --nodiscover \
  --maxpeers=0 \
  --log.console.verbosity=warn
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
  --torrent.download.rate=1gb \
  --torrent.upload.rate=50mb \
  --db.size.limit=8TB \
  --batchSize=2GB \
  --rpc.batch.limit=1000 \
  --rpc.batch.concurrency=192 \
  --rpc.returndata.limit=1000000 \
  --db.read.concurrency=512 \
  --log.console.verbosity=info
Restart=always
RestartSec=10
LimitNOFILE=1048576
LimitMEMLOCK=infinity

[Install]
WantedBy=multi-user.target
EOF

# =============================================================================
# Kernel Tuning for Erigon
# =============================================================================
# Erigon uses MDBX (a fork of LMDB) as its database engine. MDBX memory-maps
# the entire database file, which on a full Ethereum archive node is ~2TB.
# These kernel settings are required/recommended for optimal performance.

cat > /etc/sysctl.d/99-erigon.conf << 'EOF'
# -----------------------------------------------------------------------------
# vm.max_map_count = 16777216
# -----------------------------------------------------------------------------
# Default: 65530
# 
# This controls the maximum number of memory-mapped regions a process can have.
# MDBX memory-maps the database file in chunks, and a 2TB+ database requires
# millions of mappings. The default of 65530 is far too low and will cause
# Erigon to fail with "mmap failed" errors.
#
# Why 16777216 (16M)?
# - Each mmap region can be up to 2MB (huge pages) or 4KB (regular pages)
# - For a 2TB database with 2MB pages: 2TB / 2MB = 1M mappings minimum
# - We set 16M to provide headroom for growth, fragmentation, and other
#   memory-mapped files (shared libraries, etc.)
# - This is a common setting for large MDBX/LMDB databases
#
# Memory impact: Minimal. This only controls the *number* of mappings allowed,
# not the amount of memory used. The actual memory usage is controlled by
# available RAM and how much of the database is actively accessed.
#
vm.max_map_count = 16777216

# -----------------------------------------------------------------------------
# vm.overcommit_memory = 1
# -----------------------------------------------------------------------------
# Default: 0 (heuristic overcommit)
#
# Controls how the kernel handles memory allocation requests:
#   0 = Heuristic: Kernel guesses if there's enough memory (can reject large mmaps)
#   1 = Always overcommit: Never refuse a memory allocation request
#   2 = Don't overcommit: Only allow allocations up to swap + (RAM * overcommit_ratio)
#
# Why set to 1?
# - MDBX requests a memory mapping for the entire database file upfront (~2TB)
# - With overcommit_memory=0, the kernel may reject this because 2TB > physical RAM
# - Setting to 1 tells the kernel "trust the application" - it will create the
#   mapping but only allocate physical pages when they're actually accessed
# - This is safe because Erigon only accesses a small portion of the database
#   at any given time (the "working set"), and Linux will page out unused data
#
# This is the recommended setting for:
# - Redis (similar large memory-mapped workloads)
# - Elasticsearch
# - Any application using LMDB/MDBX with large databases
#
vm.overcommit_memory = 1
EOF

# Apply the settings immediately
sysctl -p /etc/sysctl.d/99-erigon.conf

echo "Kernel tuning applied:"
sysctl vm.max_map_count vm.overcommit_memory

systemctl daemon-reload
systemctl enable erigon
systemctl start erigon

echo "=== Setup complete ==="
echo ""
echo "NVMe RAID 0 setup complete!"
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
