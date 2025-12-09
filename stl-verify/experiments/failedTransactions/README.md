# SparkLend Failed Transaction Scanner

This tool scans Ethereum blockchain history to identify and store all **failed** transactions that attempted to interact with SparkLend protocol functions.

## Tracked Functions

- `supply` - Supply assets to SparkLend
- `borrow` - Borrow assets from SparkLend
- `repay` - Repay borrowed assets
- `withdraw` - Withdraw supplied assets
- `liquidationCall` - Liquidate undercollateralized positions

## Features

- **Checkpoint System**: Saves progress every 50,000 blocks to `failed_tx_sync_state` table
- **Resume on Restart**: Automatically resumes from the last checkpoint
- **Revert Reason Extraction**: Re-simulates failed calls to extract revert reasons
- **Graceful Shutdown**: Handles SIGINT/SIGTERM and saves state before exiting

## Database Schema

```sql
CREATE TABLE failed_transactions (
  block_number INTEGER,
  block_timestamp INTEGER,
  transaction_hash TEXT UNIQUE,
  from_address TEXT,
  to_address TEXT,
  function_name TEXT,          -- 'supply', 'borrow', etc.
  function_args TEXT,          -- JSON with decoded params
  gas_used TEXT,
  revert_reason TEXT           -- why it failed
);

CREATE TABLE failed_tx_sync_state (
  id INTEGER PRIMARY KEY CHECK (id = 1),
  last_processed_block INTEGER NOT NULL,
  updated_at INTEGER NOT NULL
);
```

## Usage

### Build

```bash
go build -o failed-tx-scanner .
```

### Run

```bash
# Use defaults (Ethereum mainnet, SparkLend Pool)
./failed-tx-scanner

# Custom RPC endpoint
./failed-tx-scanner -rpc "https://your-rpc-endpoint.com"

# Custom block range
./failed-tx-scanner -start 17203646 -end 18000000

# Custom database path
./failed-tx-scanner -db "/path/to/database.db"

# All options
./failed-tx-scanner \
  -rpc "https://eth.llamarpc.com" \
  -db "failed_transactions.db" \
  -contract "0xC13e21B648A5Ee794902342038FF3aDAB66BE987" \
  -start 17203646 \
  -end 0
```

### Command Line Options

| Flag | Default | Description |
|------|---------|-------------|
| `-rpc` | `https://eth.llamarpc.com` | Ethereum RPC endpoint |
| `-db` | `failed_transactions.db` | SQLite database file path |
| `-contract` | `0xC13e21B648A5Ee794902342038FF3aDAB66BE987` | SparkLend Pool contract address |
| `-start` | `17203646` | Start block number (SparkLend deployment) |
| `-end` | `0` | End block number (0 = latest) |

## Output

The scanner logs progress every 1,000 blocks and saves checkpoints every 50,000 blocks:

```
SparkLend Failed Transaction Scanner
=====================================
RPC Endpoint: https://eth.llamarpc.com
Database: failed_transactions.db
Contract: 0xC13e21B648A5Ee794902342038FF3aDAB66BE987
Start Block: 17203646
End Block: latest
=====================================
Connected to chain ID: 1
Existing failed transactions in database: 0
Scanning blocks 17203646 to 21000000 for failed SparkLend transactions
Progress: block 17204000/21000000 (0.01%), failed txs found: 0
...
Found failed supply tx: 0x123... at block 17205000
...
Checkpoint saved at block 17253646
...
Scan complete. Total failed transactions found: 42
```

## Querying Results

After scanning, you can query the SQLite database:

```bash
# Open database
sqlite3 failed_transactions.db

# Count by function
SELECT function_name, COUNT(*) FROM failed_transactions GROUP BY function_name;

# Get recent failures
SELECT * FROM failed_transactions ORDER BY block_number DESC LIMIT 10;

# Find specific revert reasons
SELECT * FROM failed_transactions WHERE revert_reason LIKE '%insufficient%';

# Export to JSON
.mode json
.output failed_transactions.json
SELECT * FROM failed_transactions;
```

## Dependencies

- `github.com/ethereum/go-ethereum` - Ethereum client library
- `github.com/mattn/go-sqlite3` - SQLite driver (requires CGO)

## Building Notes

This project requires CGO for SQLite support:

```bash
CGO_ENABLED=1 go build -o failed-tx-scanner .
```

On Linux, you may need to install SQLite development libraries:

```bash
# Debian/Ubuntu
apt-get install libsqlite3-dev

# Alpine
apk add sqlite-dev gcc musl-dev
```
