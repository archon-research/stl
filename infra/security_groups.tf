# =============================================================================
# Security Groups
# =============================================================================
# Based on networking_architecture.excalidraw security group rules

# -----------------------------------------------------------------------------
# ALB Security Group
# -----------------------------------------------------------------------------
# Inbound: 443 from internet
# Outbound: 80 to API

resource "aws_security_group" "alb" {
  name        = "${local.prefix}-alb-sg"
  description = "Security group for Application Load Balancer"
  vpc_id      = aws_vpc.main.id

  tags = {
    Name = "${local.prefix}-alb-sg"
  }
}

resource "aws_vpc_security_group_ingress_rule" "alb_https_in" {
  security_group_id = aws_security_group.alb.id
  description       = "HTTPS from internet"
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
  cidr_ipv4         = "0.0.0.0/0"
}

resource "aws_vpc_security_group_egress_rule" "alb_to_api" {
  security_group_id            = aws_security_group.alb.id
  description                  = "HTTP to API service"
  ip_protocol                  = "tcp"
  from_port                    = 80
  to_port                      = 80
  referenced_security_group_id = aws_security_group.api.id
}

# -----------------------------------------------------------------------------
# API Service Security Group
# -----------------------------------------------------------------------------
# Inbound: 80 from ALB only
# Outbound: 5432 to RDS

resource "aws_security_group" "api" {
  name        = "${local.prefix}-api-sg"
  description = "Security group for API service (ECS Fargate)"
  vpc_id      = aws_vpc.main.id

  tags = {
    Name = "${local.prefix}-api-sg"
  }
}

resource "aws_vpc_security_group_ingress_rule" "api_http_from_alb" {
  security_group_id            = aws_security_group.api.id
  description                  = "HTTP from ALB"
  ip_protocol                  = "tcp"
  from_port                    = 80
  to_port                      = 80
  referenced_security_group_id = aws_security_group.alb.id
}

resource "aws_vpc_security_group_egress_rule" "api_to_tigerdata" {
  security_group_id = aws_security_group.api.id
  description       = "PostgreSQL to TigerData via VPC peering"
  ip_protocol       = "tcp"
  from_port         = 5432
  to_port           = 5432
  cidr_ipv4         = local.tigerdata_vpc_cidr
}

# -----------------------------------------------------------------------------
# Watcher Service Security Group
# -----------------------------------------------------------------------------
# Outbound: 443 to internet (external RPC, SNS), 5432 to RDS, 6379 to Redis

resource "aws_security_group" "watcher" {
  name        = "${local.prefix}-watcher-sg"
  description = "Security group for Watcher service (ECS Fargate)"
  vpc_id      = aws_vpc.main.id

  tags = {
    Name = "${local.prefix}-watcher-sg"
  }
}

resource "aws_vpc_security_group_egress_rule" "watcher_to_internet" {
  security_group_id = aws_security_group.watcher.id
  description       = "HTTPS to external RPC providers and AWS SNS"
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
  cidr_ipv4         = "0.0.0.0/0"
}

resource "aws_vpc_security_group_egress_rule" "watcher_to_tigerdata" {
  security_group_id = aws_security_group.watcher.id
  description       = "PostgreSQL to TigerData via VPC peering"
  ip_protocol       = "tcp"
  from_port         = 5432
  to_port           = 5432
  cidr_ipv4         = local.tigerdata_vpc_cidr
}

resource "aws_vpc_security_group_egress_rule" "watcher_to_redis" {
  security_group_id            = aws_security_group.watcher.id
  description                  = "Watcher to Redis"
  ip_protocol                  = "tcp"
  from_port                    = 6379
  to_port                      = 6379
  referenced_security_group_id = aws_security_group.redis.id
}

# -----------------------------------------------------------------------------
# Worker Service Security Group
# -----------------------------------------------------------------------------
# Outbound: 443 to internet (AWS services), 5432 to RDS, 6379 to Redis

resource "aws_security_group" "worker" {
  name        = "${local.prefix}-worker-sg"
  description = "Security group for Worker service (ECS Fargate)"
  vpc_id      = aws_vpc.main.id

  tags = {
    Name = "${local.prefix}-worker-sg"
  }
}

resource "aws_vpc_security_group_egress_rule" "worker_to_internet" {
  security_group_id = aws_security_group.worker.id
  description       = "HTTPS to AWS services (SNS, SQS, S3)"
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
  cidr_ipv4         = "0.0.0.0/0"
}

resource "aws_vpc_security_group_egress_rule" "worker_to_tigerdata" {
  security_group_id = aws_security_group.worker.id
  description       = "PostgreSQL to TigerData via VPC peering"
  ip_protocol       = "tcp"
  from_port         = 5432
  to_port           = 5432
  cidr_ipv4         = local.tigerdata_vpc_cidr
}

resource "aws_vpc_security_group_egress_rule" "worker_to_redis" {
  security_group_id            = aws_security_group.worker.id
  description                  = "Worker to Redis"
  ip_protocol                  = "tcp"
  from_port                    = 6379
  to_port                      = 6379
  referenced_security_group_id = aws_security_group.redis.id
}

# -----------------------------------------------------------------------------
# RDS Security Group - REMOVED
# -----------------------------------------------------------------------------
# Database moved to TigerData (TimescaleDB) with VPC peering.
# See tigerdata.tf for configuration.

# -----------------------------------------------------------------------------
# ElastiCache Redis Security Group
# -----------------------------------------------------------------------------
# Inbound: 6379 from Watcher, Worker
# No outbound required

resource "aws_security_group" "redis" {
  name        = "${local.prefix}-redis-sg"
  description = "Security group for ElastiCache Redis"
  vpc_id      = aws_vpc.main.id

  tags = {
    Name = "${local.prefix}-redis-sg"
  }
}

resource "aws_vpc_security_group_ingress_rule" "redis_from_watcher" {
  security_group_id            = aws_security_group.redis.id
  description                  = "Redis from Watcher"
  ip_protocol                  = "tcp"
  from_port                    = 6379
  to_port                      = 6379
  referenced_security_group_id = aws_security_group.watcher.id
}

resource "aws_vpc_security_group_ingress_rule" "redis_from_worker" {
  security_group_id            = aws_security_group.redis.id
  description                  = "Redis from Worker"
  ip_protocol                  = "tcp"
  from_port                    = 6379
  to_port                      = 6379
  referenced_security_group_id = aws_security_group.worker.id
}
