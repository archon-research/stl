# STL Networking Architecture

## Overview

This document describes the secure, scalable, and maintainable AWS networking architecture for the STL Live Data Pipeline.

---

## Architecture Diagram

```
┌─────────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                              AWS REGION (eu-west-1)                                             │
│  ┌───────────────────────────────────────────────────────────────────────────────────────────────────────────┐  │
│  │                                              VPC (10.0.0.0/16)                                            │  │
│  │                                                                                                           │  │
│  │  ┌─────────────────────────────────────────────────────────────────────────────────────────────────────┐  │  │
│  │  │                                    AVAILABILITY ZONE 1 (eu-west-1a)                                 │  │  │
│  │  │  ┌──────────────────────┐  ┌──────────────────────┐  ┌────────────────────────────────────────────┐ │  │  │
│  │  │  │  PUBLIC SUBNET 1     │  │  PRIVATE SUBNET 1    │  │  ISOLATED SUBNET 1 (Data)                  │ │  │  │
│  │  │  │  10.0.1.0/24         │  │  10.0.10.0/24        │  │  10.0.100.0/24                             │ │  │  │
│  │  │  │  ┌────────────────┐  │  │  ┌────────────────┐  │  │  ┌──────────────┐  ┌──────────────────┐   │ │  │  │
│  │  │  │  │  NAT Gateway 1 │  │  │  │ ECS Fargate    │  │  │  │ RDS Primary  │  │ ElastiCache Node │   │ │  │  │
│  │  │  │  │  (Elastic IP)  │  │  │  │ ┌────────────┐ │  │  │  │ (PostgreSQL) │  │     (Redis)      │   │ │  │  │
│  │  │  │  └────────────────┘  │  │  │ │ Watcher    │ │  │  │  │  Multi-AZ    │  │                  │   │ │  │  │
│  │  │  │  ┌────────────────┐  │  │  │ │ Service    │ │  │  │  └──────────────┘  └──────────────────┘   │ │  │  │
│  │  │  │  │  ALB (if API)  │  │  │  │ └────────────┘ │  │  │                                           │ │  │  │
│  │  │  │  │  (optional)    │  │  │  │ ┌────────────┐ │  │  │                                           │ │  │  │
│  │  │  │  └────────────────┘  │  │  │ │ Worker     │ │  │  │                                           │ │  │  │
│  │  │  │                      │  │  │ │ Service    │ │  │  │                                           │ │  │  │
│  │  │  │                      │  │  │ └────────────┘ │  │  │                                           │ │  │  │
│  │  │  └──────────────────────┘  │  └────────────────┘  │  └────────────────────────────────────────────┘ │  │  │
│  │  └─────────────────────────────────────────────────────────────────────────────────────────────────────┘  │  │
│  │                                                                                                           │  │
│  │  ┌─────────────────────────────────────────────────────────────────────────────────────────────────────┐  │  │
│  │  │                                    AVAILABILITY ZONE 2 (eu-west-1b)                                 │  │  │
│  │  │  ┌──────────────────────┐  ┌──────────────────────┐  ┌────────────────────────────────────────────┐ │  │  │
│  │  │  │  PUBLIC SUBNET 2     │  │  PRIVATE SUBNET 2    │  │  ISOLATED SUBNET 2 (Data)                  │ │  │  │
│  │  │  │  10.0.2.0/24         │  │  10.0.20.0/24        │  │  10.0.200.0/24                             │ │  │  │
│  │  │  │  ┌────────────────┐  │  │  ┌────────────────┐  │  │  ┌──────────────┐  ┌──────────────────┐   │ │  │  │
│  │  │  │  │  NAT Gateway 2 │  │  │  │ ECS Fargate    │  │  │  │ RDS Standby  │  │ ElastiCache Node │   │ │  │  │
│  │  │  │  │  (Elastic IP)  │  │  │  │ ┌────────────┐ │  │  │  │ (PostgreSQL) │  │     (Redis)      │   │ │  │  │
│  │  │  │  └────────────────┘  │  │  │ │ Watcher    │ │  │  │  │  Multi-AZ    │  │   Replica        │   │ │  │  │
│  │  │  │                      │  │  │ │ Service    │ │  │  │  └──────────────┘  └──────────────────┘   │ │  │  │
│  │  │  │                      │  │  │ └────────────┘ │  │  │                                           │ │  │  │
│  │  │  │                      │  │  │ ┌────────────┐ │  │  │                                           │ │  │  │
│  │  │  │                      │  │  │ │ Worker     │ │  │  │                                           │ │  │  │
│  │  │  │                      │  │  │ │ Service    │ │  │  │                                           │ │  │  │
│  │  │  │                      │  │  │ └────────────┘ │  │  │                                           │ │  │  │
│  │  │  └──────────────────────┘  │  └────────────────┘  │  └────────────────────────────────────────────┘ │  │  │
│  │  └─────────────────────────────────────────────────────────────────────────────────────────────────────┘  │  │
│  │                                                                                                           │  │
│  │  ┌────────────────────────────────────────────────────────────────────────────────────────────────────┐   │  │
│  │  │                                    VPC ENDPOINTS (PrivateLink)                                     │   │  │
│  │  │  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────────┐ ┌──────────┐ │   │  │
│  │  │  │ S3 Gateway   │ │ ECR API      │ │ ECR Docker   │ │ CloudWatch   │ │ SNS          │ │ SQS      │ │   │  │
│  │  │  │ Endpoint     │ │ Endpoint     │ │ Endpoint     │ │ Logs         │ │ Endpoint     │ │ Endpoint │ │   │  │
│  │  │  └──────────────┘ └──────────────┘ └──────────────┘ └──────────────┘ └──────────────┘ └──────────┘ │   │  │
│  │  └────────────────────────────────────────────────────────────────────────────────────────────────────┘   │  │
│  │                                                                                                           │  │
│  └───────────────────────────────────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                                                 │
│  ┌───────────────────────────────────────────────────────────────────────────────────────────────────────────┐  │
│  │                                         AWS MANAGED SERVICES                                              │  │
│  │  ┌──────────────────────────────────────────┐  ┌──────────────────────────────────────────────────────┐   │  │
│  │  │          SNS FIFO Topics                 │  │                  SQS FIFO Queues                     │   │  │
│  │  │  ┌─────────┐ ┌─────────┐ ┌─────────┐     │  │  ┌────────────┐ ┌────────────┐ ┌────────────┐        │   │  │
│  │  │  │eth-block│ │arb-block│ │gno-block│ ... │  │  │ eth-trans  │ │ eth-raw    │ │ eth-dlq    │ ...    │   │  │
│  │  │  └─────────┘ └─────────┘ └─────────┘     │  │  └────────────┘ └────────────┘ └────────────┘        │   │  │
│  │  └──────────────────────────────────────────┘  └──────────────────────────────────────────────────────┘   │  │
│  │                                                                                                           │  │
│  │  ┌──────────────────────────────────────────┐  ┌──────────────────────────────────────────────────────┐   │  │
│  │  │          S3 Buckets (with VPC Endpoint)  │  │                  ECR (Container Registry)            │   │  │
│  │  │  ┌─────────────┐ ┌─────────────┐         │  │  ┌────────────────────────────────────────────────┐  │   │  │
│  │  │  │ eth-raw     │ │ arb-raw     │ ...     │  │  │ stl-watcher:latest  |  stl-worker:latest       │  │   │  │
│  │  │  └─────────────┘ └─────────────┘         │  │  └────────────────────────────────────────────────┘  │   │  │
│  │  └──────────────────────────────────────────┘  └──────────────────────────────────────────────────────┘   │  │
│  └───────────────────────────────────────────────────────────────────────────────────────────────────────────┘  │
│                                                                                                                 │
└────────────────────────────────────────────────────────────────────────────────────────────────────────────────┘

┌───────────────────────────────────────────────────────────────────────────────────────────────────────────────┐
│                                          EXTERNAL SERVICES (Internet)                                         │
│  ┌──────────────────────┐  ┌──────────────────────┐  ┌──────────────────────┐                                 │
│  │     Alchemy API      │  │     Erigon Node      │  │    Provider X        │                                 │
│  │  (WebSocket + HTTP)  │  │    (JSON-RPC)        │  │    (JSON-RPC)        │                                 │
│  │  wss://eth-mainnet   │  │                      │  │                      │                                 │
│  └──────────────────────┘  └──────────────────────┘  └──────────────────────┘                                 │
└───────────────────────────────────────────────────────────────────────────────────────────────────────────────┘
```

---

## Network Topology

### 1. VPC Design

| Component | CIDR Block | Purpose |
|-----------|------------|---------|
| VPC | `10.0.0.0/16` | Main VPC (65,536 addresses) |
| Public Subnet 1 | `10.0.1.0/24` | NAT Gateway, ALB (AZ-1) |
| Public Subnet 2 | `10.0.2.0/24` | NAT Gateway, ALB (AZ-2) |
| Private Subnet 1 | `10.0.10.0/24` | ECS Fargate Tasks (AZ-1) |
| Private Subnet 2 | `10.0.20.0/24` | ECS Fargate Tasks (AZ-2) |
| Isolated Subnet 1 | `10.0.100.0/24` | RDS, ElastiCache (AZ-1) |
| Isolated Subnet 2 | `10.0.200.0/24` | RDS, ElastiCache (AZ-2) |

### 2. Subnet Strategy (3-Tier)

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          INTERNET GATEWAY                                   │
└─────────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  PUBLIC SUBNETS (DMZ)                                                       │
│  • NAT Gateways (for private subnet egress)                                 │
│  • Application Load Balancer (if exposing API)                              │
│  • Bastion Host (optional, for emergency access)                            │
│  Route: 0.0.0.0/0 → Internet Gateway                                        │
└─────────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  PRIVATE SUBNETS (Application Tier)                                         │
│  • ECS Fargate: Watcher Services (per chain)                                │
│  • ECS Fargate: Worker Services (Transform/Raw backup)                      │
│  Route: 0.0.0.0/0 → NAT Gateway                                             │
│  Route: VPC Endpoints → Local                                               │
└─────────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  ISOLATED SUBNETS (Data Tier)                                               │
│  • RDS PostgreSQL (Multi-AZ)                                                │
│  • ElastiCache Redis Cluster                                                │
│  Route: NO Internet access (no NAT, no IGW)                                 │
│  Route: Only VPC local traffic                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Security Design

### Security Groups

```hcl
# 1. ALB Security Group (if API is exposed)
sg-alb:
  Inbound:
    - HTTPS (443) from 0.0.0.0/0  # Public access
  Outbound:
    - HTTP (8080) to sg-ecs-tasks  # Health checks

# 2. ECS Tasks Security Group (Watcher + Workers)
sg-ecs-tasks:
  Inbound:
    - HTTP (8080) from sg-alb      # Health check endpoint
    - All traffic from sg-ecs-tasks # Inter-service communication
  Outbound:
    - HTTPS (443) to 0.0.0.0/0     # External APIs (Alchemy, etc.)
    - WSS (443) to 0.0.0.0/0       # WebSocket connections
    - PostgreSQL (5432) to sg-rds  # Database access
    - Redis (6379) to sg-redis     # Cache access
    - HTTPS (443) to VPC Endpoints # AWS Services

# 3. RDS Security Group
sg-rds:
  Inbound:
    - PostgreSQL (5432) from sg-ecs-tasks
  Outbound:
    - (None required for RDS)

# 4. ElastiCache Security Group
sg-redis:
  Inbound:
    - Redis (6379) from sg-ecs-tasks
  Outbound:
    - (None required for ElastiCache)

# 5. VPC Endpoints Security Group
sg-vpc-endpoints:
  Inbound:
    - HTTPS (443) from sg-ecs-tasks
  Outbound:
    - (Managed by AWS)
```

### Network ACLs (Defense in Depth)

```hcl
# Public Subnets NACL
nacl-public:
  Inbound:
    - HTTPS (443) from 0.0.0.0/0 ALLOW
    - Ephemeral (1024-65535) from 0.0.0.0/0 ALLOW  # Return traffic
    - All else DENY
  Outbound:
    - All traffic to 10.0.0.0/16 ALLOW  # VPC internal
    - HTTPS (443) to 0.0.0.0/0 ALLOW
    - Ephemeral to 0.0.0.0/0 ALLOW

# Private Subnets NACL
nacl-private:
  Inbound:
    - All from 10.0.0.0/16 ALLOW  # VPC internal
    - Ephemeral from 0.0.0.0/0 ALLOW  # Return traffic via NAT
    - All else DENY
  Outbound:
    - All to 10.0.0.0/16 ALLOW
    - HTTPS (443) to 0.0.0.0/0 ALLOW  # Via NAT
    - WSS (443) to 0.0.0.0/0 ALLOW

# Isolated Subnets NACL
nacl-isolated:
  Inbound:
    - PostgreSQL (5432) from 10.0.10.0/24, 10.0.20.0/24 ALLOW
    - Redis (6379) from 10.0.10.0/24, 10.0.20.0/24 ALLOW
    - All else DENY
  Outbound:
    - Ephemeral to 10.0.10.0/24, 10.0.20.0/24 ALLOW
    - All else DENY
```

---

## VPC Endpoints Strategy

Using VPC Endpoints eliminates NAT Gateway data transfer costs for AWS services and keeps traffic private.

| Service | Endpoint Type | Purpose |
|---------|--------------|---------|
| S3 | Gateway | Raw data backup (free, no charges) |
| ECR API | Interface | Container image metadata |
| ECR DKR | Interface | Container image pull |
| CloudWatch Logs | Interface | Log shipping |
| SNS | Interface | Event publishing |
| SQS | Interface | Queue consumption |
| Secrets Manager | Interface | API keys, credentials |
| SSM | Interface | ECS Exec, Parameter Store |

### Cost Optimization

```
Without VPC Endpoints:
  NAT Gateway processing: $0.045/GB
  100GB/day SNS/SQS traffic = $4.50/day = $135/month

With VPC Endpoints:
  Interface Endpoint: ~$7.30/month each
  6 endpoints = $43.80/month
  Savings: ~$91/month + faster, more reliable
```

---

## High Availability Design

### Multi-AZ Deployment

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         ECS Service Distribution                        │
├─────────────────────────────────────────────────────────────────────────┤
│                                                                         │
│   AZ-1 (eu-west-1a)              AZ-2 (eu-west-1b)                     │
│   ┌───────────────────┐          ┌───────────────────┐                 │
│   │ watcher-eth (1)   │          │ watcher-eth (1)   │ ← Active/Active │
│   │ watcher-arb (1)   │          │ watcher-arb (1)   │                 │
│   │ watcher-gno (1)   │          │ watcher-gno (1)   │                 │
│   │ worker-transform  │          │ worker-transform  │                 │
│   │ worker-raw        │          │ worker-raw        │                 │
│   └───────────────────┘          └───────────────────┘                 │
│                                                                         │
│   ┌───────────────────┐          ┌───────────────────┐                 │
│   │ RDS Primary       │ ←sync──→ │ RDS Standby       │ ← Multi-AZ RDS  │
│   └───────────────────┘          └───────────────────┘                 │
│                                                                         │
│   ┌───────────────────┐          ┌───────────────────┐                 │
│   │ ElastiCache Node  │ ←sync──→ │ ElastiCache Node  │ ← Cluster Mode  │
│   └───────────────────┘          └───────────────────┘                 │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

### NAT Gateway Redundancy

- Deploy NAT Gateway in each AZ
- Route table per AZ pointing to its local NAT Gateway
- Prevents cross-AZ traffic costs ($0.01/GB)
- Provides AZ-level fault isolation

---

## Traffic Flow Patterns

### 1. Watcher → External RPC Providers

```
Watcher (Private Subnet)
    │
    ▼ [Security Group: Allow HTTPS/WSS outbound]
NAT Gateway (Public Subnet)
    │
    ▼ [Internet Gateway]
Internet
    │
    ▼
Alchemy/Erigon (wss://eth-mainnet.g.alchemy.com)
```

### 2. Watcher → SNS (Publishing Events)

```
Watcher (Private Subnet)
    │
    ▼ [Security Group: Allow HTTPS to VPC Endpoint]
VPC Endpoint (SNS)
    │
    ▼ [AWS PrivateLink - no internet]
SNS FIFO Topic
    │
    ▼ [Subscription]
SQS FIFO Queue (per chain, per data type)
```

### 3. Worker → Database/Cache

```
Worker (Private Subnet)
    │
    ├──▶ [SG: 5432] RDS PostgreSQL (Isolated Subnet)
    │
    └──▶ [SG: 6379] ElastiCache Redis (Isolated Subnet)
```

### 4. Worker → S3 (Raw Backup)

```
Worker (Private Subnet)
    │
    ▼ [S3 Gateway Endpoint - route table entry]
S3 Bucket (stl-{chain}-raw)
    │
    (No NAT charges, no internet)
```

---

## DNS and Service Discovery

### AWS Cloud Map (Service Discovery)

```hcl
# Private DNS namespace
namespace: stl.internal

# Service registrations
watcher-eth.stl.internal  → ECS Task IPs
watcher-arb.stl.internal  → ECS Task IPs
worker.stl.internal       → ECS Task IPs
postgres.stl.internal     → RDS Endpoint (CNAME)
redis.stl.internal        → ElastiCache Endpoint (CNAME)
```

### Route 53 Private Hosted Zone

For custom DNS resolution within VPC:
- `db.stl.internal` → RDS cluster endpoint
- `cache.stl.internal` → ElastiCache configuration endpoint

---

## Monitoring and Observability

### VPC Flow Logs

```hcl
resource "aws_flow_log" "main" {
  log_destination      = aws_cloudwatch_log_group.vpc_flow.arn
  log_destination_type = "cloud-watch-logs"
  traffic_type         = "ALL"  # or "REJECT" for security monitoring
  vpc_id               = aws_vpc.main.id

  # Enriched format for better analysis
  log_format = "$${version} $${account-id} $${interface-id} $${srcaddr} $${dstaddr} $${srcport} $${dstport} $${protocol} $${packets} $${bytes} $${start} $${end} $${action} $${log-status} $${vpc-id} $${subnet-id} $${instance-id} $${tcp-flags} $${type} $${pkt-srcaddr} $${pkt-dstaddr}"
}
```

### Network Metrics Dashboard

```
Metrics to monitor:
├── NAT Gateway
│   ├── BytesOutToDestination
│   ├── BytesOutToSource
│   ├── ConnectionAttemptCount
│   ├── ErrorPortAllocation
│   └── IdleTimeoutCount
├── VPC Endpoints
│   ├── BytesProcessed
│   ├── PacketsDropped
│   └── ActiveConnections
├── ECS Tasks
│   ├── NetworkRxBytes
│   ├── NetworkTxBytes
│   └── NetworkRxPackets
└── Connections
    ├── RDS ConnectionCount
    ├── ElastiCache CurrConnections
    └── ALB ActiveConnectionCount
```

---

## Cost Optimization

### NAT Gateway Cost Reduction

1. **Use VPC Endpoints** for AWS services (SNS, SQS, S3, ECR, CloudWatch)
2. **Same-AZ routing** - route through NAT in same AZ as task
3. **Consider NAT Instances** for dev/staging (EC2 with NAT AMI)

### Data Transfer Optimization

| Transfer Type | Cost | Optimization |
|---------------|------|--------------|
| NAT Gateway processing | $0.045/GB | Use VPC Endpoints |
| Cross-AZ | $0.01/GB | Prefer same-AZ placement |
| VPC Endpoint | ~$0.01/GB | Cheaper than NAT for AWS traffic |
| S3 Gateway Endpoint | Free | Always use for S3 |
| Internet egress | $0.09/GB | Cache, compress, batch |

### Estimated Monthly Costs (Production)

```
Component                        | Monthly Cost
---------------------------------|-------------
VPC (no charge)                  | $0
NAT Gateway (2x, multi-AZ)       | ~$65 (hourly) + data
VPC Endpoints (6x Interface)     | ~$44
Elastic IPs (2x)                 | ~$7
Data Transfer (est. 500GB)       | ~$50
---------------------------------|-------------
Total Network                    | ~$166/month
```

---

## Security Best Practices Checklist

- [ ] **No public IPs on ECS tasks** (Fargate with private subnets)
- [ ] **Database in isolated subnets** (no internet route)
- [ ] **VPC Endpoints for AWS services** (PrivateLink)
- [ ] **Security Groups with least privilege** (specific ports/sources)
- [ ] **NACLs as defense in depth** (subnet-level filtering)
- [ ] **VPC Flow Logs enabled** (CloudWatch or S3)
- [ ] **Encrypt data in transit** (TLS 1.2+)
- [ ] **Secrets in Secrets Manager** (not environment variables)
- [ ] **IAM roles with least privilege** (no access keys)
- [ ] **Enable GuardDuty** (threat detection)
- [ ] **Regular security audits** (AWS Config rules)

---

## Terraform Module Structure

```
infra/
├── modules/
│   └── networking/
│       ├── main.tf           # VPC, Subnets, IGW
│       ├── nat.tf            # NAT Gateways
│       ├── routes.tf         # Route Tables
│       ├── endpoints.tf      # VPC Endpoints
│       ├── security_groups.tf
│       ├── nacls.tf
│       ├── flow_logs.tf
│       ├── variables.tf
│       └── outputs.tf
├── environments/
│   ├── staging/
│   │   └── networking.tf     # Single NAT, smaller subnets
│   └── production/
│       └── networking.tf     # Multi-AZ, full redundancy
└── main.tf
```

---

## Next Steps

1. **Create Terraform networking module** with all components
2. **Set up VPC Flow Logs** for security monitoring
3. **Configure VPC Endpoints** to reduce NAT costs
4. **Deploy in staging** with single-AZ for cost savings
5. **Enable GuardDuty** for threat detection
6. **Set up CloudWatch alarms** for NAT/endpoint metrics

