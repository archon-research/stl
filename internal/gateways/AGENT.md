# Agent Instructions: Gateways

## Purpose
Shared infrastructure clients for external systems. These are used by all subsystems.

## Structure
```
gateways/
├── ethereum/   # Ethereum JSON-RPC client
├── arbitrum/   # Arbitrum JSON-RPC client
└── postgres/   # Shared database connection
```

## Guidelines
- Keep clients generic and reusable
- Do NOT put business logic here
- Clients should be stateless or use connection pooling
- Return raw data; let subsystems transform it

## Ethereum/Arbitrum Clients
- Wrap JSON-RPC calls
- Handle retries and rate limiting
- Return block data, transaction data, etc.

## Postgres
- Provides `Connect(dsn)` for getting a DB connection
- Subsystem-specific queries belong in subsystem adapters, not here

## Usage
```go
import "github.com/archon-research/stl/internal/gateways/ethereum"

client := ethereum.NewClient("https://mainnet.infura.io/v3/...")
block, err := client.GetLatestBlock()
```
