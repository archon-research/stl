#!/bin/bash
set -euo pipefail
# Updates kustomize image tags for a given overlay.
# Usage: ./update-image-tags.sh <overlay-dir> <registry> <env-prefix> <sha>
#
# Example:
#   ./update-image-tags.sh k8s/overlays/staging 579039992622.dkr.ecr.eu-west-1.amazonaws.com sentinelstaging abc123

OVERLAY=$1
REGISTRY=$2
PREFIX=$3
SHA=$4

cd "$OVERLAY"

# Map: kustomize image name → ECR image suffix
# Avalanche variants share images with their mainnet counterparts.
declare -A IMAGES=(
  [watcher]=watcher
  [backup-worker]=backup-worker
  [oracle-price-worker]=oracle-price-worker
  [sparklend-position-tracker]=sparklend-position-tracker
  [morpho-indexer]=morpho-indexer
  [allocation-tracker]=allocation-tracker
  [temporal-worker]=temporal-worker
  [prime-debt-indexer]=prime-debt-indexer
  [avalanche-watcher]=watcher
  [avalanche-backup-worker]=backup-worker
  [avalanche-sparklend-position-tracker]=sparklend-position-tracker
)

for name in "${!IMAGES[@]}"; do
  kustomize edit set image "$name=$REGISTRY/stl-$PREFIX-${IMAGES[$name]}:$SHA"
done
