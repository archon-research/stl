# STL Networking Architecture

## Overview

This document describes the secure, scalable, and maintainable AWS networking architecture for the STL blockchain data pipeline. The architecture follows a 3-tier subnet model with clear separation between public, application, and data layers.

---

## Architecture Diagram

See [networking_architecture.excalidraw](networking_architecture.excalidraw) for the visual diagram.

---

## Network Topology

### VPC Design

| Component | CIDR Block | Purpose |
|-----------|------------|---------|
| VPC | `10.0.0.0/16` | Main VPC (65,536 addresses) |
| Public Subnet | `10.0.1.0/24` | NAT Gateway, ALB |
| Private Subnet | `10.0.10.0/24` | ECS Fargate Tasks |
| Isolated Subnet | `10.0.100.0/24` | RDS, ElastiCache |

### 3-Tier Subnet Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          INTERNET                                           │
└─────────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  PUBLIC SUBNET                                                              │
│  • NAT Gateway                                                              │
│  • Internet Gateway                                                         │
│  • Application Load Balancer (for API service)                              │
│  Route: 0.0.0.0/0 → Internet Gateway                                        │
└─────────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  PRIVATE SUBNETS (Application Tier)                                         │
│  • ECS Fargate: Watcher Services (blockchain data ingestion)                │
│  • ECS Fargate: Worker Services (data processing)                           │
│  • ECS Fargate: API Service (REST API for data access)                      │
│  Route: 0.0.0.0/0 → NAT Gateway                                             │
└─────────────────────────────────────────────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────────┐
│  ISOLATED SUBNETS (Data Tier - No Internet Access)                          │
│  • RDS PostgreSQL (Multi-AZ)                                                │
│  • ElastiCache Redis Cluster                                                │
│  Route: NO Internet access (no NAT, no IGW)                                 │
│  Route: Only VPC local traffic                                              │
└─────────────────────────────────────────────────────────────────────────────┘
```

---

## Application Services

### Service Overview

| Service | Color Code | Description |
|---------|------------|-------------|
| **Watcher** | Purple | Ingests blockchain data from external RPC providers |
| **Worker** | Green | Processes queued events, transforms data |
| **API** | Blue | REST API for querying processed data |

### ECS Fargate Services

All application services run on ECS Fargate in private subnets:

- **Watcher Services**: One per blockchain (Ethereum, Arbitrum, Gnosis, etc.)
- **Worker Services**: Transform workers and raw backup workers
- **API Service**: REST API behind Application Load Balancer

---

## Data Flows

### Watcher Service

| Direction | Target | Protocol | Description |
|-----------|--------|----------|-------------|
| **READ** | External RPC Providers | HTTPS/WSS (443) | Blockchain data via Alchemy, Erigon, etc. |
| **WRITE** | SNS FIFO | HTTPS | Publish block events to SNS topics |
| **WRITE** | ElastiCache Redis | TCP (6379) | Cache block state |
| **WRITE** | RDS PostgreSQL | TCP (5432) | Store verified blocks |

### Worker Service

| Direction | Target | Protocol | Description |
|-----------|--------|----------|-------------|
| **READ** | SQS FIFO | HTTPS | Consume events from queues |
| **READ** | ElastiCache Redis | TCP (6379) | Read cached data |
| **WRITE** | RDS PostgreSQL | TCP (5432) | Store processed data |
| **WRITE** | S3 Buckets | HTTPS | Raw data backup |

### API Service

| Direction | Target | Protocol | Description |
|-----------|--------|----------|-------------|
| **READ** | Application Load Balancer | HTTP (80) | Receive API requests |
| **READ** | RDS PostgreSQL | TCP (5432) | Query processed data |

---

## AWS Managed Services

### Messaging (Purple)

| Service | Type | Purpose |
|---------|------|---------|
| SNS FIFO | Topics | Event publishing from Watchers |
| SQS FIFO | Queues | Event consumption by Workers |

### Storage

| Service | Color | Purpose |
|---------|-------|---------|
| RDS PostgreSQL | Orange | Primary data store |
| ElastiCache Redis | Yellow | Block state cache |
| S3 Buckets | Green | Raw data backup |

---

## External RPC Providers

Watcher services connect to external blockchain RPC providers via NAT Gateway:

| Provider | Protocol | Endpoint Example |
|----------|----------|------------------|
| Alchemy | WSS/HTTPS | `wss://eth-mainnet.g.alchemy.com/v2/...` |
| Erigon | JSON-RPC | Self-hosted or managed |
| Provider X | JSON-RPC | Additional redundancy |

---

## Security Groups

### Summary

| Security Group | Inbound | Outbound |
|----------------|---------|----------|
| **sg-alb** | 443 from internet | 80 to sg-api |
| **sg-api** | 80 from sg-alb only | 5432 to sg-rds |
| **sg-watcher** | (none) | 443 to internet, 5432 to sg-rds, 6379 to sg-redis |
| **sg-worker** | (none) | 5432 to sg-rds, 6379 to sg-redis, 443 to S3 |
| **sg-rds** | 5432 from app tier SGs | (none) |
| **sg-redis** | 6379 from app tier SGs | (none) |

### Detailed Rules

```hcl
# ALB Security Group
sg-alb:
  Inbound:
    - HTTPS (443) from 0.0.0.0/0
  Outbound:
    - HTTP (80) to sg-api

# API Service Security Group
sg-api:
  Inbound:
    - HTTP (80) from sg-alb
  Outbound:
    - PostgreSQL (5432) to sg-rds

# Watcher Security Group
sg-watcher:
  Outbound:
    - HTTPS (443) to 0.0.0.0/0     # External RPC providers
    - WSS (443) to 0.0.0.0/0       # WebSocket connections
    - PostgreSQL (5432) to sg-rds
    - Redis (6379) to sg-redis

# Worker Security Group
sg-worker:
  Outbound:
    - HTTPS (443) to 0.0.0.0/0     # AWS services (SNS, SQS, S3)
    - PostgreSQL (5432) to sg-rds
    - Redis (6379) to sg-redis

# RDS Security Group
sg-rds:
  Inbound:
    - PostgreSQL (5432) from sg-api
    - PostgreSQL (5432) from sg-watcher
    - PostgreSQL (5432) from sg-worker

# ElastiCache Security Group
sg-redis:
  Inbound:
    - Redis (6379) from sg-watcher
    - Redis (6379) from sg-worker
```

---

## Deployment

### Single AZ Architecture

- **NAT Gateway**: Single NAT Gateway in public subnet
- **ECS Services**: All services in one AZ
- **RDS**: Single-AZ deployment
- **ElastiCache**: Single node

```
eu-west-1a
┌─────────────────────┐
│ Public Subnet       │
│ ┌─────────────────┐ │
│ │ NAT Gateway     │ │
│ │ (Elastic IP)    │ │
│ └─────────────────┘ │
│ ┌─────────────────┐ │
│ │ ALB             │ │
│ └─────────────────┘ │
└─────────────────────┘
         │
         ▼
┌─────────────────────┐
│ Private Subnet      │
│ Route: 0.0.0.0/0    │
│   → NAT Gateway     │
└─────────────────────┘
         │
         ▼
┌─────────────────────┐
│ Isolated Subnet     │
│ RDS + Redis         │
└─────────────────────┘
```

---

## Cost Optimization

### Data Transfer Costs

| Transfer Type | Cost | Notes |
|---------------|------|-------|
| NAT Gateway processing | $0.045/GB | Optimize with caching |
| Internet egress | $0.09/GB | Cache, compress, batch |
| S3 in same region | Free | Use regional buckets |

### Estimated Monthly Costs

| Component | Monthly Cost |
|-----------|-------------|
| VPC | $0 |
| NAT Gateway (1x) | ~$32 |
| Elastic IP (1x) | ~$3.50 |
| Data Transfer (est. 500GB) | ~$50 |
| **Total Network** | **~$86/month** |

---

## Future Enhancements

Consider adding VPC Endpoints for cost optimization if AWS service traffic becomes significant:

| Service | Endpoint Type | Benefit |
|---------|--------------|---------|
| S3 | Gateway | Free, reduces NAT traffic |
| ECR | Interface | Faster container pulls |
| SNS/SQS | Interface | Private AWS traffic |
| CloudWatch | Interface | Log shipping |

---

## Legend

| Color | Component Type |
|-------|---------------|
| 🟣 Purple | Watcher service / Messaging (SNS, SQS) |
| 🟢 Green | Worker service / Storage (S3) |
| 🔵 Blue | API service / Load Balancer |
| 🟠 Orange | Database (RDS PostgreSQL) |
| 🟡 Yellow | Cache (ElastiCache Redis) |

