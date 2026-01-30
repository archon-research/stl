# Isolated VPC for Erigon node
resource "aws_vpc" "erigon" {
  cidr_block           = var.vpc_cidr
  enable_dns_hostnames = true
  enable_dns_support   = true

  tags = {
    Name = "${var.project}-erigon-vpc"
  }
}

# Internet Gateway for outbound access
resource "aws_internet_gateway" "erigon" {
  vpc_id = aws_vpc.erigon.id

  tags = {
    Name = "${var.project}-erigon-igw"
  }
}

# Public subnet for the Erigon node
resource "aws_subnet" "erigon" {
  vpc_id                  = aws_vpc.erigon.id
  cidr_block              = var.subnet_cidr
  availability_zone       = "${var.aws_region}a"
  map_public_ip_on_launch = true

  tags = {
    Name = "${var.project}-erigon-subnet"
  }
}

# Route table for public subnet
resource "aws_route_table" "erigon" {
  vpc_id = aws_vpc.erigon.id

  route {
    cidr_block = "0.0.0.0/0"
    gateway_id = aws_internet_gateway.erigon.id
  }

  tags = {
    Name = "${var.project}-erigon-rt"
  }
}

resource "aws_route_table_association" "erigon" {
  subnet_id      = aws_subnet.erigon.id
  route_table_id = aws_route_table.erigon.id
}

# VPC Endpoint for S3 (Gateway type - free, no NAT needed)
resource "aws_vpc_endpoint" "s3" {
  vpc_id            = aws_vpc.erigon.id
  service_name      = "com.amazonaws.${var.aws_region}.s3"
  vpc_endpoint_type = "Gateway"
  route_table_ids   = [aws_route_table.erigon.id]

  tags = {
    Name = "${var.project}-erigon-s3-endpoint"
  }
}
