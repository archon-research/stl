# Get the latest Amazon Linux 2023 AMI (ARM)
data "aws_ami" "amazon_linux_2023" {
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

  filter {
    name   = "root-device-type"
    values = ["ebs"]
  }
}

# Security group for Erigon node (in main VPC private subnet)
# No ingress rules — private subnet has no direct internet ingress.
# SSH access via Tailscale only (outbound connection through NAT).
resource "aws_security_group" "erigon" {
  name        = "${var.project}-erigon-sg"
  description = "Security group for Erigon archive node"
  vpc_id      = data.aws_vpc.main.id

  # All outbound: covers Tailscale, NAT (torrent snapshot downloads, packages), S3 endpoint,
  # and TigerData access via VPC peering route.
  egress {
    from_port   = 0
    to_port     = 0
    protocol    = "-1"
    cidr_blocks = ["0.0.0.0/0"]
  }

  tags = {
    Name = "${var.project}-erigon-sg"
  }
}

# IAM role for EC2 (for S3 access)
resource "aws_iam_role" "erigon" {
  name = "${var.project}-erigon-role"

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
}

# S3 access policy for the specific bucket
resource "aws_iam_role_policy" "erigon_s3" {
  name = "${var.project}-erigon-s3-policy"
  role = aws_iam_role.erigon.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "ListBucket"
        Effect = "Allow"
        Action = [
          "s3:ListBucket",
          "s3:GetBucketLocation"
        ]
        Resource = "arn:aws:s3:::${var.s3_bucket_name}"
      },
      {
        Sid    = "ReadWriteObjects"
        Effect = "Allow"
        Action = [
          "s3:GetObject",
          "s3:PutObject",
          "s3:DeleteObject"
        ]
        Resource = "arn:aws:s3:::${var.s3_bucket_name}/*"
      }
    ]
  })
}

# Secrets Manager access for Tailscale auth key
resource "aws_iam_role_policy" "erigon_secrets" {
  name = "${var.project}-erigon-secrets-policy"
  role = aws_iam_role.erigon.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "GetTailscaleSecret"
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue"
        ]
        Resource = "arn:aws:secretsmanager:${var.aws_region}:*:secret:stl-erigon-tailscale-auth-key*"
      }
    ]
  })
}

# TigerData read/write credentials for oracle-pricing-backfill
resource "aws_iam_role_policy" "erigon_tigerdata" {
  name = "${var.project}-erigon-tigerdata-policy"
  role = aws_iam_role.erigon.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "GetTigerDataAppSecret"
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue"
        ]
        Resource = "arn:aws:secretsmanager:${var.aws_region}:*:secret:${var.main_infra_prefix}-tigerdata-app*"
      }
    ]
  })
}

# SSM access for Session Manager (optional, but useful for debugging)
resource "aws_iam_role_policy_attachment" "erigon_ssm" {
  role       = aws_iam_role.erigon.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "erigon" {
  name = "${var.project}-erigon-profile"
  role = aws_iam_role.erigon.name
}

# =============================================================================
# Erigon Instance (c8gd family — Graviton4 with local NVMe instance store)
# The user-data script auto-detects and RAIDs all NVMe instance store disks.
# =============================================================================

resource "aws_instance" "erigon_c8gd" {
  ami                    = data.aws_ami.amazon_linux_2023.id
  instance_type          = var.instance_type
  key_name               = var.key_name
  subnet_id              = data.aws_subnet.private.id
  vpc_security_group_ids = [aws_security_group.erigon.id]
  iam_instance_profile   = aws_iam_instance_profile.erigon.name

  root_block_device {
    volume_size = 50
    volume_type = "gp3"
  }

  # NVMe instance store disks are set up by user-data (RAID 0 if multiple)

  user_data = base64encode(file("${path.module}/user-data-c8gd.sh"))

  tags = {
    Name = "${var.project}-erigon-c8gd"
  }

  # Prevent accidental termination
  disable_api_termination = false

  # Enable detailed monitoring
  monitoring = true
}

