#!/bin/bash
# preflight.sh - Infrastructure Deployment Pre-flight Validation Script
# Validates all prerequisites for infrastructure deployment (dev, staging, prod)
# Usage: ./preflight.sh [--skip-tailscale] [--env sentineldev|sentinelstaging|sentinelprod]

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Defaults
SKIP_TAILSCALE=false
TARGET_ENV=""
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Parse arguments
while [[ $# -gt 0 ]]; do
  case $1 in
    --skip-tailscale)
      SKIP_TAILSCALE=true
      shift
      ;;
    --env)
      TARGET_ENV="$2"
      shift 2
      ;;
    *)
      echo "Unknown option: $1"
      echo "Usage: $0 [--skip-tailscale] [--env sentineldev|sentinelstaging|sentinelprod]"
      exit 1
      ;;
  esac
done

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

print_warning() {
  echo -e "${YELLOW}⚠${NC} $1"
}

print_info() {
  echo -e "${BLUE}ℹ${NC} $1"
}

# Check if command exists
command_exists() {
  command -v "$1" >/dev/null 2>&1
}

# ============================================================================
print_header "STL Infrastructure Pre-flight Validation"
# ============================================================================

# 1. Check required tools
print_info "Checking required tools..."

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
  echo "Install missing tools:"
  echo "  - awscli: https://aws.amazon.com/cli/"
  echo "  - opentofu: https://opentofu.org/docs/intro/install/"
  echo "  - curl: brew install curl"
  echo "  - jq: brew install jq"
  exit 1
fi

# 2. Check .env file
print_header "Environment Configuration"

if [ ! -f "$SCRIPT_DIR/.env" ]; then
  if [ ! -f "$SCRIPT_DIR/.env.example" ]; then
    print_error ".env.example not found"
    exit 1
  fi
  
  print_info "Creating .env from .env.example..."
  cp "$SCRIPT_DIR/.env.example" "$SCRIPT_DIR/.env"
  print_success ".env created from .env.example"
fi

print_success ".env file exists"

# Source .env
source "$SCRIPT_DIR/.env"

# 3. Validate AWS credentials
print_header "AWS Credentials Validation"

if [ -z "$AWS_ACCESS_KEY_ID" ] && [ -z "$AWS_SECRET_ACCESS_KEY" ]; then
  print_info "Using AWS credentials from ~/.aws/credentials or environment"
else
  print_info "Using AWS credentials from .env file"
fi

if aws sts get-caller-identity >/dev/null 2>&1; then
  ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
  PRINCIPAL=$(aws sts get-caller-identity --query Arn --output text)
  print_success "AWS credentials valid"
  print_info "Account: $ACCOUNT_ID"
  print_info "Principal: $PRINCIPAL"
else
  print_error "AWS credentials invalid or not configured"
  echo "Configure AWS credentials:"
  echo "  1. Via AWS CLI: aws configure"
  echo "  2. Via environment: export AWS_ACCESS_KEY_ID=... AWS_SECRET_ACCESS_KEY=..."
  echo "  3. Via ~/.aws/credentials file"
  exit 1
fi

# 4. Validate TigerData credentials
print_header "TigerData Credentials Validation"

if [ -z "$TIGERDATA_PROJECT_ID" ]; then
  print_error "TIGERDATA_PROJECT_ID not set in .env"
  exit 1
fi
print_success "TIGERDATA_PROJECT_ID: $TIGERDATA_PROJECT_ID"

if [ -z "$TIGERDATA_ACCESS_KEY" ]; then
  print_error "TIGERDATA_ACCESS_KEY not set in .env"
  exit 1
fi
print_success "TIGERDATA_ACCESS_KEY is set"

if [ -z "$TIGERDATA_SECRET_KEY" ]; then
  print_error "TIGERDATA_SECRET_KEY not set in .env"
  exit 1
fi
print_success "TIGERDATA_SECRET_KEY is set"

# Optionally test TigerData credentials (requires curl)
if command_exists curl; then
  print_info "Testing TigerData API credentials..."
  if curl -s -u "${TIGERDATA_ACCESS_KEY}:${TIGERDATA_SECRET_KEY}" \
    "https://api.cloud.timescale.com/v1/projects" | jq . >/dev/null 2>&1; then
    print_success "TigerData API credentials valid"
  else
    print_warning "TigerData API credentials may be invalid (non-critical, will be tested during tf-apply)"
  fi
fi

# 5. Validate Alchemy API key
print_header "Alchemy API Key"

if [ -z "$ALCHEMY_API_KEY" ]; then
  print_error "ALCHEMY_API_KEY not set in .env"
  print_info "Get your key from: https://dashboard.alchemy.com/apps"
  exit 1
fi
print_success "ALCHEMY_API_KEY is set"

# 6. Tailscale Auth Key (optional, for staging/prod)
print_header "Tailscale Configuration"

if [ "$SKIP_TAILSCALE" = false ]; then
  if [ -z "$TAILSCALE_AUTH_KEY" ]; then
    if [ -n "$TAILSCALE_API_KEY" ]; then
      print_info "Generating Tailscale auth key from API..."
      TAILSCALE_RESPONSE=$(curl -s -X POST "https://api.tailscale.com/api/v2/tailnet/-/keys" \
        -H "Authorization: Bearer $TAILSCALE_API_KEY" \
        -H "Content-Type: application/json" \
        -d '{
          "capabilities": {
            "devices": {
              "create": {
                "reusable": false,
                "ephemeral": true,
                "preauthorized": true,
                "tags": ["tag:bastion"]
              }
            }
          },
          "expirySeconds": 86400
        }')
      
      if echo "$TAILSCALE_RESPONSE" | jq . >/dev/null 2>&1; then
        TAILSCALE_AUTH_KEY=$(echo "$TAILSCALE_RESPONSE" | jq -r '.key')
        if [ -n "$TAILSCALE_AUTH_KEY" ] && [ "$TAILSCALE_AUTH_KEY" != "null" ]; then
          print_success "Tailscale auth key generated and saved to .env"
          print_warning "This key expires in 24 hours and is single-use only"
          
          # Update .env with the new key
          if grep -q "^TAILSCALE_AUTH_KEY=" "$SCRIPT_DIR/.env"; then
            sed -i.bak "s|^TAILSCALE_AUTH_KEY=.*|TAILSCALE_AUTH_KEY=$TAILSCALE_AUTH_KEY|" "$SCRIPT_DIR/.env"
          else
            echo "TAILSCALE_AUTH_KEY=$TAILSCALE_AUTH_KEY" >> "$SCRIPT_DIR/.env"
          fi
          print_success "Updated TAILSCALE_AUTH_KEY in .env"
        else
          print_error "Failed to extract auth key from response"
          echo "$TAILSCALE_RESPONSE" | jq . >&2
          exit 1
        fi
      else
        print_error "Failed to generate Tailscale auth key"
        echo "$TAILSCALE_RESPONSE" >&2
        exit 1
      fi
    else
      print_warning "TAILSCALE_AUTH_KEY not set and TAILSCALE_API_KEY not provided (non-critical for dev)"
      print_info "Set TAILSCALE_AUTH_KEY in .env or provide TAILSCALE_API_KEY to auto-generate"
    fi
  else
    print_success "TAILSCALE_AUTH_KEY is already set"
  fi
else
  print_info "Tailscale validation skipped (--skip-tailscale)"
fi

# 7. Environment-specific summary
print_header "Deployment Summary"

if [ -n "$TARGET_ENV" ]; then
  echo "Target environment: $TARGET_ENV"
  TFVARS_FILE="$SCRIPT_DIR/infra/environments/${TARGET_ENV}.tfvars"
  if [ -f "$TFVARS_FILE" ]; then
    print_success "Found $TARGET_ENV configuration"
    echo "Configuration:"
    grep -E "^(bastion_enabled|tailscale_enabled|tigerdata_milli_cpu|redis_node_type)" "$TFVARS_FILE" || true
  else
    print_warning "$TARGET_ENV configuration not found at $TFVARS_FILE"
  fi
else
  print_info "Available environments:"
  ls -1 "$SCRIPT_DIR/infra/environments/"*.tfvars 2>/dev/null | xargs -I {} basename {} .tfvars || true
fi

# 8. Next steps
print_header "Next Steps"

echo "All prerequisites validated! Ready to deploy."
echo ""
echo "To bootstrap infrastructure:"
if [ -n "$TARGET_ENV" ]; then
  echo "  make tf-bootstrap ENV=$TARGET_ENV"
else
  echo "  make tf-bootstrap ENV=sentineldev        # Development (minimal)"
  echo "  make tf-bootstrap ENV=sentinelstaging    # Staging (with Tailscale)"
  echo "  make tf-bootstrap ENV=sentinelprod       # Production"
fi
echo ""
echo "Full deployment flow:"
echo "  make tf-bootstrap ENV=<env>"
echo "  make tf-init ENV=<env>"
echo "  make tf-validate ENV=<env>"
echo "  make tf-plan ENV=<env>"
echo "  make tf-apply ENV=<env>"
echo ""
echo "Documentation: $([ -f "$SCRIPT_DIR/DEPLOYMENT_WORKFLOW.md" ] && echo "see DEPLOYMENT_WORKFLOW.md" || echo "create DEPLOYMENT_WORKFLOW.md")"

print_success "Pre-flight validation complete!"
