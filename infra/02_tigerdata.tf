# =============================================================================
# TigerData (TimescaleDB) Configuration
# =============================================================================
# Managed TimescaleDB with VPC Peering for private connectivity

# -----------------------------------------------------------------------------
# TigerData VPC (for peering)
# -----------------------------------------------------------------------------

resource "timescale_vpcs" "main" {
  cidr        = local.tigerdata_vpc_cidr
  name        = "${local.prefix_lowercase}-vpc${local.resource_suffix}"
  region_code = var.aws_region
}

# -----------------------------------------------------------------------------
# TigerData Service
# -----------------------------------------------------------------------------

resource "timescale_service" "main" {
  name        = "${local.prefix_lowercase}-db${local.resource_suffix}"
  milli_cpu   = var.tigerdata_milli_cpu
  memory_gb   = var.tigerdata_memory_gb
  region_code = var.aws_region
  vpc_id      = timescale_vpcs.main.id

  # Connection pooling (PgBouncer)
  connection_pooler_enabled = true

  # High availability - enable for production
  ha_replicas   = var.tigerdata_ha_replicas
  sync_replicas = 0

  # Service requires active VPC peering and route before creation
  # Must wait for peering to be accepted and fully propagated on both AWS and TigerData sides
  depends_on = [
    timescale_peering_connection.to_aws,
    aws_vpc_peering_connection_accepter.tigerdata,
    time_sleep.peering_active,
    aws_route.private_to_tigerdata,
  ]
}

# -----------------------------------------------------------------------------
# VPC Peering: TigerData → AWS
# -----------------------------------------------------------------------------

# Peering request from TigerData side
resource "timescale_peering_connection" "to_aws" {
  peer_account_id  = data.aws_caller_identity.current.account_id
  peer_region_code = var.aws_region
  peer_vpc_id      = aws_vpc.main.id
  timescale_vpc_id = timescale_vpcs.main.id
}

# Accept peering on AWS side
resource "aws_vpc_peering_connection_accepter" "tigerdata" {
  vpc_peering_connection_id = timescale_peering_connection.to_aws.provisioned_id
  auto_accept               = true

  tags = {
    Name = "${local.prefix}-tigerdata-peering"
  }
}

# Wait for peering to fully activate before TigerData service tries to use it
# TigerData's backend needs time to propagate the peering connection state
resource "time_sleep" "peering_active" {
  depends_on      = [aws_vpc_peering_connection_accepter.tigerdata]
  create_duration = "300s" # 5 minutes to ensure peering is fully active on both TigerData and AWS sides
}

# -----------------------------------------------------------------------------
# Route from AWS VPC to TigerData VPC
# -----------------------------------------------------------------------------

resource "aws_route" "private_to_tigerdata" {
  route_table_id            = aws_route_table.private.id
  destination_cidr_block    = local.tigerdata_vpc_cidr
  vpc_peering_connection_id = aws_vpc_peering_connection_accepter.tigerdata.id
  
  depends_on = [time_sleep.peering_active]
}

# Note: data.aws_caller_identity.current is defined in monitoring.tf
