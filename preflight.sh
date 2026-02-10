#!/bin/bash
# preflight.sh - Simplified Pre-flight Tool Check
# Validates CLI tools are installed before Terraform deployment
# Terraform itself handles credential and configuration validation
# Usage: ./preflight.sh

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Functions
print_header() {
  echo -e "\n${BLUE}============================================================${NC}"
  echo -e "${BLUE}$1${NC}"
  echo -e "${BLUE}============================================================${NC}\n"
}

print_success() {
  echo -e "${GREEN}✓${NC} $1"
}

print_error() {
  echo -e "${RED}✗${NC} $1"
}

print_info() {
  echo -e "${BLUE}ℹ${NC} $1"
}

# Check if command exists
command_exists() {
  command -v "$1" >/dev/null 2>&1
}

# ============================================================================
print_header "STL Infrastructure Pre-flight Check"
# ============================================================================

# 1. Check required CLI tools
print_info "Checking required CLI tools..."

MISSING_TOOLS=()
for tool in aws tofu curl jq; do
  if command_exists "$tool"; then
    print_success "$tool is installed"
  else
    print_error "$tool is not installed"
    MISSING_TOOLS+=("$tool")
  fi
done

if [ ${#MISSING_TOOLS[@]} -gt 0 ]; then
  print_error "Missing required tools: ${MISSING_TOOLS[*]}"
  echo ""
  echo "Installation instructions:"
  echo "  - awscli:   brew install awscli    # or visit https://aws.amazon.com/cli/"
  echo "  - opentofu: brew install opentofu  # or visit https://opentofu.org/docs/intro/install/"
  echo "  - curl:     brew install curl"
  echo "  - jq:       brew install jq"
  exit 1
fi

# 2. Check/create .env file
print_header "Environment Configuration"

if [ ! -f "$SCRIPT_DIR/.env" ]; then
  if [ ! -f "$SCRIPT_DIR/.env.example" ]; then
    print_error ".env.example not found - cannot create .env"
    exit 1
  fi
  
  print_info "Creating .env from .env.example..."
  cp "$SCRIPT_DIR/.env.example" "$SCRIPT_DIR/.env"
  print_success ".env created from .env.example"
  echo ""
  echo "⚠️  IMPORTANT: Edit .env and fill in your credentials:"
  echo "   - TigerData: TIGERDATA_PROJECT_ID, TIGERDATA_ACCESS_KEY, TIGERDATA_SECRET_KEY"
  echo "   - Alchemy:   ALCHEMY_API_KEY"
  echo "   - CoinGecko: COINGECKO_API_KEY"
  echo "   - Etherscan: ETHERSCAN_API_KEY"
  echo ""
else
  print_success ".env file exists"
fi

# 3. Quick guidance
print_header "Next Steps"

echo "Tools validated! Ready for Terraform deployment."
echo ""
echo "Deployment workflow:"
echo "  1. Edit .env with your API keys and credentials (if not done)"
echo "  2. make tf-bootstrap ENV=<env>  # Creates S3 backend + secrets"
echo "  3. make tf-init ENV=<env>       # Initializes Terraform"
echo "  4. make tf-plan ENV=<env>       # Preview changes (validates creds)"
echo "  5. make tf-apply ENV=<env>      # Deploy infrastructure"
echo ""
echo "Available environments:"
echo "  - sentineldev      (Development - minimal resources)"
echo "  - sentinelstaging  (Staging - with Tailscale bastion)"
echo "  - sentinelprod     (Production - full HA setup)"
echo ""
echo "Note: Terraform will validate AWS credentials, API keys, and"
echo "      configuration during the 'plan' step."

print_success "Pre-flight check complete!"
