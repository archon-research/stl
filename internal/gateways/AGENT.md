# Gateways

Shared infrastructure clients for external systems. Used by all subsystems.

## Contents
- `ethereum/` - Ethereum JSON-RPC client
- `arbitrum/` - Arbitrum JSON-RPC client
- `postgres/` - Shared database connection utilities

## Guidelines
- Keep clients generic and reusable
- Do NOT put business logic here
- Clients should be stateless or use connection pooling
- Return raw data; let subsystems transform it
- Subsystem-specific queries belong in subsystem adapters, not here

## Usage
```go
import "github.com/archon-research/stl/internal/gateways/ethereum"

client := ethereum.NewClient("https://mainnet.infura.io/v3/...")
block, err := client.GetLatestBlock()
```
