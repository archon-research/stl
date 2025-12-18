# Health Factor Query

Query `getUserAccountData(user).healthFactor` for a user across multiple blocks using JSON-RPC batch calls.

## Prerequisites

- Ethereum archive node RPC URL (e.g., Alchemy, Infura, or local Erigon)

## Usage

```bash
# Query health factor for a user from block range
go run main.go -rpc "https://eth-mainnet.g.alchemy.com/v2/YOUR_KEY" \
    -user 0x000000093e55f433fb57a32aa5d5fe717b3f7ab1 \
    -start 18779216 \
    -end 18779316

# Query specific blocks from a file (one block per line)
go run main.go -rpc "https://..." -user 0x... -blocks blocks.txt

# Adjust batch size (default 100)
go run main.go -rpc "https://..." -user 0x... -start 18779216 -end 18780216 -batch 50
```

## Output

CSV with columns:
- `block_number`
- `health_factor` (in wei, divide by 1e18 for human readable)
- `health_factor_decimal` (human readable)

## How it works

1. Encodes `getUserAccountData(address)` call
2. Batches multiple `eth_call` requests into single HTTP request (JSON-RPC batching)
3. Decodes healthFactor (6th return value) from each response
4. Outputs results to CSV
