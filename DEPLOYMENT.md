# Deployment Guide

Deploy the Multi-Protocol Lending Indexer to production.

## Overview

This indexer tracks Sparklend (Mainnet + Gnosis) and Aave V3 (Core + Horizon) in real-time, storing normalized data in PostgreSQL with GraphQL and REST APIs.

## Prerequisites

- **RPC URLs**: 
  - Mainnet: For Sparklend Mainnet + Aave V3
  - Gnosis: For Sparklend Gnosis
  - Provider: [Alchemy](https://www.alchemy.com/)
- **PostgreSQL database** (v14+)

## Local Development with Docker Compose

### Quick Start

1. Create a `.env` file:

```env
PONDER_RPC_URL_1=https://eth-mainnet.g.alchemy.com/v2/YOUR_API_KEY
PONDER_RPC_URL_100=https://rpc.gnosischain.com
DATABASE_SCHEMA=public
```

2. Start services:

```bash
docker-compose up -d
```

3. Access the app:
- GraphQL: http://localhost:42069/graphql
- Health: http://localhost:42069/health

4. Stop services:

```bash
docker-compose down
```

## Railway Deployment (Production)

### Prerequisites

1. [Railway](https://railway.app/) account
2. RPC URLs (Mainnet + Gnosis)
3. Repository on GitHub

### Steps

#### 1. Create Railway Project

1. Go to [Railway Dashboard](https://railway.app/dashboard)
2. Click **"New Project"**
3. Select **"Deploy from GitHub repo"**
4. Select this repository

#### 2. Add PostgreSQL

1. Click **"New"** → **"Database"** → **"Add PostgreSQL"**
2. Railway automatically creates and links `DATABASE_URL`

#### 3. Configure Environment Variables

Add these to your **app service** in Railway:

| Variable | Example |
|----------|---------|
| `PONDER_RPC_URL_1` | `https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY` |
| `PONDER_RPC_URL_100` | `https://rpc.gnosischain.com` |
| `DATABASE_SCHEMA` | `public` |

#### 4. Deploy

Railway will automatically build and deploy. Initial sync time depends on your RPC provider speed.

### Verifying Deployment

Once deployed, access:

- **GraphQL API**: `https://your-app.railway.app/graphql`
- **Health check**: `https://your-app.railway.app/health`

## Running Snapshot Jobs (Optional)

Snapshot calculations run separately from real-time indexing. Use Railway's shell:

```bash
pnpm job snapshot-calculation mainnet 16776401 18000000 7200
pnpm job price-capture mainnet 16776401 18000000 7200
```

See [`src/scripts/README.md`](./src/scripts/README.md) for details.

## Troubleshooting

### "RPC URL not found"

Ensure `PONDER_RPC_URL_1` and `PONDER_RPC_URL_100` are set in Railway environment variables.

### Database Connection Errors

- Verify PostgreSQL is added to Railway project
- Check `DATABASE_URL` is set automatically
- Ensure `DATABASE_SCHEMA` is set (default: `public`)

## Updating

Push to `main` branch. Railway rebuilds automatically.

## Environment Variables Reference

### Required

- `PONDER_RPC_URL_1` - Ethereum Mainnet RPC
- `PONDER_RPC_URL_100` - Gnosis Chain RPC
- `DATABASE_SCHEMA` - PostgreSQL schema (e.g., `public`)

### Optional

- `PONDER_LOG_LEVEL` - `info` | `debug` | `warn` (default: `info`)
- `PORT` - Railway sets this automatically

### Auto-Set by Railway

- `DATABASE_URL` - PostgreSQL connection string

