# =============================================================================
# Bastion Host with Tailscale
# =============================================================================
# Small EC2 instance for accessing VPC resources (TigerData, Redis) from
# local machines via Tailscale VPN.

# -----------------------------------------------------------------------------
# Variables
# -----------------------------------------------------------------------------

variable "bastion_enabled" {
  description = "Whether to create the bastion host"
  type        = bool
  default     = false
}

variable "bastion_instance_type" {
  description = "EC2 instance type for bastion (t4g.nano is sufficient)"
  type        = string
  default     = "t4g.nano"
}

variable "tailscale_auth_key_secret_name" {
  description = "Name of the Secrets Manager secret containing the Tailscale auth key"
  type        = string
  default     = ""
}

variable "tailscale_enabled" {
  description = "Install and configure Tailscale on the bastion host (should only be enabled for sentinelstaging)"
  type        = bool
  default     = false
}

# -----------------------------------------------------------------------------
# Security Group
# -----------------------------------------------------------------------------

resource "aws_security_group" "bastion" {
  count = var.bastion_enabled ? 1 : 0

  name        = "${local.prefix}-bastion-sg"
  description = "Security group for Bastion host with Tailscale"
  vpc_id      = aws_vpc.main.id

  tags = {
    Name = "${local.prefix}-bastion-sg"
  }
}

# Egress: Allow all outbound (Tailscale, apt, etc.)
resource "aws_vpc_security_group_egress_rule" "bastion_all_out" {
  count = var.bastion_enabled ? 1 : 0

  security_group_id = aws_security_group.bastion[0].id
  description       = "All outbound traffic"
  ip_protocol       = "-1"
  cidr_ipv4         = "0.0.0.0/0"
}

# Egress: PostgreSQL to TigerData
resource "aws_vpc_security_group_egress_rule" "bastion_to_tigerdata" {
  count = var.bastion_enabled ? 1 : 0

  security_group_id = aws_security_group.bastion[0].id
  description       = "PostgreSQL to TigerData via VPC peering"
  ip_protocol       = "tcp"
  from_port         = 5432
  to_port           = 5432
  cidr_ipv4         = local.tigerdata_vpc_cidr
}

# -----------------------------------------------------------------------------
# IAM Role for Bastion
# -----------------------------------------------------------------------------

resource "aws_iam_role" "bastion" {
  count = var.bastion_enabled ? 1 : 0

  name = "${local.prefix}-bastion-role"

  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Action = "sts:AssumeRole"
        Effect = "Allow"
        Principal = {
          Service = "ec2.amazonaws.com"
        }
      }
    ]
  })

  tags = {
    Name = "${local.prefix}-bastion-role"
  }
}

# Allow reading Tailscale auth key from Secrets Manager
resource "aws_iam_role_policy" "bastion_secrets" {
  count = var.bastion_enabled && var.tailscale_enabled ? 1 : 0

  name = "${local.prefix}-bastion-secrets-policy"
  role = aws_iam_role.bastion[0].id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue"
        ]
        Resource = "arn:aws:secretsmanager:${var.aws_region}:${data.aws_caller_identity.current.account_id}:secret:${local.prefix}-tailscale-auth-key*"
      }
    ]
  })
}

# SSM for Session Manager access (fallback if Tailscale fails)
resource "aws_iam_role_policy_attachment" "bastion_ssm" {
  count = var.bastion_enabled ? 1 : 0

  role       = aws_iam_role.bastion[0].name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "bastion" {
  count = var.bastion_enabled ? 1 : 0

  name = "${local.prefix}-bastion-profile"
  role = aws_iam_role.bastion[0].name
}

# -----------------------------------------------------------------------------
# EC2 Instance
# -----------------------------------------------------------------------------

# Latest Amazon Linux 2023 ARM64 AMI
data "aws_ami" "amazon_linux_2023_arm64" {
  count = var.bastion_enabled ? 1 : 0

  most_recent = true
  owners      = ["amazon"]

  filter {
    name   = "name"
    values = ["al2023-ami-*-arm64"]
  }

  filter {
    name   = "virtualization-type"
    values = ["hvm"]
  }
}

resource "aws_instance" "bastion" {
  count = var.bastion_enabled ? 1 : 0

  ami                    = data.aws_ami.amazon_linux_2023_arm64[0].id
  instance_type          = var.bastion_instance_type
  subnet_id              = aws_subnet.private.id  # Private subnet has NAT gateway for internet access
  vpc_security_group_ids = [aws_security_group.bastion[0].id]
  iam_instance_profile   = aws_iam_instance_profile.bastion[0].name

  # No SSH key - use Tailscale or SSM Session Manager
  key_name = null

  user_data = <<-EOF
    #!/bin/bash
    set -e

    # Update system
    dnf update -y

    %{if var.tailscale_enabled}
    # Install Tailscale (only for sentinelstaging)
    dnf config-manager --add-repo https://pkgs.tailscale.com/stable/amazon-linux/2023/tailscale.repo
    dnf install -y tailscale

    # Enable and start Tailscale
    systemctl enable --now tailscaled
    %{endif}

    # Install PostgreSQL client for TigerData access
    dnf install -y postgresql15

    # Install useful tools
    dnf install -y htop jq

    %{if var.tailscale_enabled}
    # If auth key is in Secrets Manager, authenticate automatically
    # Use explicit secret name or construct from prefix
    SECRET_NAME="${var.tailscale_auth_key_secret_name != "" ? var.tailscale_auth_key_secret_name : "${local.prefix}-tailscale-auth-key"}"
    AUTH_KEY=$(aws secretsmanager get-secret-value \
      --secret-id "$SECRET_NAME" \
      --query SecretString --output text \
      --region ${var.aws_region} 2>/dev/null || echo "")
    
    if [ -n "$AUTH_KEY" ]; then
      tailscale up --authkey="$AUTH_KEY" --ssh --accept-dns=false --hostname="${local.prefix}-bastion"
    fi
    echo "Bastion setup complete with Tailscale. Run 'tailscale up' to authenticate if not done automatically."
    %{else}
    echo "Bastion setup complete. Use SSM Session Manager to connect."
    %{endif}
  EOF

  root_block_device {
    volume_size = 30
    volume_type = "gp3"
    encrypted   = true
  }

  metadata_options {
    http_endpoint               = "enabled"
    http_tokens                 = "required" # IMDSv2 only
    http_put_response_hop_limit = 1
  }

  tags = {
    Name = "${local.prefix}-bastion"
  }
}

# -----------------------------------------------------------------------------
# Outputs
# -----------------------------------------------------------------------------

output "bastion_instance_id" {
  description = "Bastion EC2 instance ID (use with SSM Session Manager)"
  value       = var.bastion_enabled ? aws_instance.bastion[0].id : null
}

output "bastion_private_ip" {
  description = "Bastion private IP address"
  value       = var.bastion_enabled ? aws_instance.bastion[0].private_ip : null
}

output "bastion_ssm_command" {
  description = "Command to connect via SSM Session Manager"
  value       = var.bastion_enabled ? "aws ssm start-session --target ${aws_instance.bastion[0].id} --region ${var.aws_region}" : null
}
