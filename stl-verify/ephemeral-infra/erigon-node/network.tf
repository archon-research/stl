# Look up the main infrastructure VPC and private subnet by tags.
# The Erigon node runs in the main VPC's private subnet, which already has:
# - NAT gateway for internet access (P2P sync, package downloads, Tailscale)
# - VPC peering to TigerData (TimescaleDB) for oracle price fetcher
# - S3 gateway endpoint for raw block data access

data "aws_vpc" "main" {
  filter {
    name   = "tag:Name"
    values = ["${var.main_infra_prefix}-vpc"]
  }
}

data "aws_subnet" "private" {
  vpc_id = data.aws_vpc.main.id

  filter {
    name   = "tag:Name"
    values = ["${var.main_infra_prefix}-private"]
  }
}
