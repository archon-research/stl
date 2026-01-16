# =============================================================================
# VPC and Core Networking
# =============================================================================
# Based on networking_architecture.excalidraw
# Single-AZ deployment with 3-tier subnet architecture

locals {
  vpc_cidr = "10.0.0.0/16"

  # Single AZ for staging - extend for production multi-AZ
  availability_zone = "${var.aws_region}a"

  # Subnet CIDRs
  public_subnet_cidr   = "10.0.1.0/24"   # NAT Gateway, ALB, IGW
  private_subnet_cidr  = "10.0.10.0/24"  # ECS Fargate (Watcher, Worker, API)
  isolated_subnet_cidr = "10.0.100.0/24" # ElastiCache (no internet)

  # TigerData VPC CIDR (must not overlap with AWS VPC)
  tigerdata_vpc_cidr = "10.1.0.0/24"
}

# -----------------------------------------------------------------------------
# VPC
# -----------------------------------------------------------------------------

resource "aws_vpc" "main" {
  cidr_block           = local.vpc_cidr
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name = "${local.prefix}-vpc"
  }
}

# -----------------------------------------------------------------------------
# Internet Gateway
# -----------------------------------------------------------------------------

resource "aws_internet_gateway" "main" {
  vpc_id = aws_vpc.main.id

  tags = {
    Name = "${local.prefix}-igw"
  }
}

# -----------------------------------------------------------------------------
# Subnets
# -----------------------------------------------------------------------------

# Public Subnet - NAT Gateway, ALB
resource "aws_subnet" "public" {
  vpc_id                  = aws_vpc.main.id
  cidr_block              = local.public_subnet_cidr
  availability_zone       = local.availability_zone
  map_public_ip_on_launch = true

  tags = {
    Name = "${local.prefix}-public"
    Tier = "public"
  }
}

# Private Subnet - Application Tier (ECS Fargate)
resource "aws_subnet" "private" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = local.private_subnet_cidr
  availability_zone = local.availability_zone

  tags = {
    Name = "${local.prefix}-private"
    Tier = "private"
  }
}

# Isolated Subnet - Data Tier (RDS, ElastiCache) - NO internet access
resource "aws_subnet" "isolated" {
  vpc_id            = aws_vpc.main.id
  cidr_block        = local.isolated_subnet_cidr
  availability_zone = local.availability_zone

  tags = {
    Name = "${local.prefix}-isolated"
    Tier = "isolated"
  }
}

# -----------------------------------------------------------------------------
# NAT Gateway
# -----------------------------------------------------------------------------

resource "aws_eip" "nat" {
  domain = "vpc"

  tags = {
    Name = "${local.prefix}-nat-eip"
  }

  depends_on = [aws_internet_gateway.main]
}

resource "aws_nat_gateway" "main" {
  allocation_id = aws_eip.nat.id
  subnet_id     = aws_subnet.public.id

  tags = {
    Name = "${local.prefix}-nat"
  }

  depends_on = [aws_internet_gateway.main]
}

# -----------------------------------------------------------------------------
# Route Tables
# -----------------------------------------------------------------------------

# Public Route Table - routes to Internet Gateway
resource "aws_route_table" "public" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.main.id
  }

  tags = {
    Name = "${local.prefix}-public-rt"
  }
}

resource "aws_route_table_association" "public" {
  subnet_id      = aws_subnet.public.id
  route_table_id = aws_route_table.public.id
}

# Private Route Table - routes to NAT Gateway
resource "aws_route_table" "private" {
  vpc_id = aws_vpc.main.id

  route {
    cidr_block     = "0.0.0.0/0"
    nat_gateway_id = aws_nat_gateway.main.id
  }

  tags = {
    Name = "${local.prefix}-private-rt"
  }
}

resource "aws_route_table_association" "private" {
  subnet_id      = aws_subnet.private.id
  route_table_id = aws_route_table.private.id
}

# Isolated Route Table - NO internet routes (local VPC only)
resource "aws_route_table" "isolated" {
  vpc_id = aws_vpc.main.id

  # No routes - only local VPC traffic allowed

  tags = {
    Name = "${local.prefix}-isolated-rt"
  }
}

resource "aws_route_table_association" "isolated" {
  subnet_id      = aws_subnet.isolated.id
  route_table_id = aws_route_table.isolated.id
}

# -----------------------------------------------------------------------------
# VPC Endpoints
# -----------------------------------------------------------------------------

# S3 Gateway Endpoint - free, avoids NAT Gateway costs for S3 traffic
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.main.id
  service_name      = "com.amazonaws.${var.aws_region}.s3"
  vpc_endpoint_type = "Gateway"

  route_table_ids = [
    aws_route_table.private.id,
    aws_route_table.isolated.id,
  ]

  tags = {
    Name = "${local.prefix}-s3-endpoint"
  }
}
