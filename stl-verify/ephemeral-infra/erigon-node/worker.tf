# =============================================================================
# Backfill Worker Instance
# Lightweight instance for running oracle-pricing-backfill and similar scripts.
# Makes HTTP requests to the Erigon node and writes to TigerData (TimescaleDB).
# =============================================================================

data "aws_caller_identity" "current" {}

# IAM role for the worker
resource "aws_iam_role" "worker" {
  name = "${var.project}-worker-role"

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

# Tailscale auth key access
resource "aws_iam_role_policy" "worker_secrets" {
  name = "${var.project}-worker-secrets-policy"
  role = aws_iam_role.worker.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "GetTailscaleSecret"
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue"
        ]
        Resource = "arn:aws:secretsmanager:${var.aws_region}:${data.aws_caller_identity.current.account_id}:secret:stl-erigon-tailscale-auth-key*"
      }
    ]
  })
}

# TigerData read/write credentials for oracle-pricing-backfill
resource "aws_iam_role_policy" "worker_tigerdata" {
  name = "${var.project}-worker-tigerdata-policy"
  role = aws_iam_role.worker.id

  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "GetTigerDataAppSecret"
        Effect = "Allow"
        Action = [
          "secretsmanager:GetSecretValue"
        ]
        Resource = "arn:aws:secretsmanager:${var.aws_region}:${data.aws_caller_identity.current.account_id}:secret:${var.main_infra_prefix}-tigerdata-app*"
      }
    ]
  })
}

# SSM access for Session Manager
resource "aws_iam_role_policy_attachment" "worker_ssm" {
  role       = aws_iam_role.worker.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "worker" {
  name = "${var.project}-worker-profile"
  role = aws_iam_role.worker.name
}

# Worker instance — small Graviton, no NVMe needed
resource "aws_instance" "worker" {
  ami                    = data.aws_ami.amazon_linux_2023.id
  instance_type          = var.worker_instance_type
  subnet_id              = data.aws_subnet.private.id
  vpc_security_group_ids = [aws_security_group.erigon.id]
  iam_instance_profile   = aws_iam_instance_profile.worker.name

  root_block_device {
    volume_size = 30
    volume_type = "gp3"
  }

  user_data = file("${path.module}/user-data-worker.sh")

  tags = {
    Name = "${var.project}-worker"
  }

  monitoring = true
}
