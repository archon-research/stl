# Direct Erigon Database Access

This tool directly accesses Erigon's MDBX database to read blockchain data without going through RPC.

## Why Direct Database Access?

- **Speed**: No RPC overhead, direct memory-mapped access
- **No rate limits**: Read as fast as your disk allows
- **Full access**: Read any data Erigon stores, not just what RPC exposes

## Requirements

1. **Erigon node** with accessible chaindata directory
2. **Read access** to the chaindata folder
3. Go 1.22+ with CGO enabled (MDBX requires C bindings)

## Setup

**Option 1: Use local Erigon source (Recommended)**

If you have Erigon cloned locally, update `go.mod` to use a replace directive:

```go
replace github.com/erigontech/erigon/v3 => /path/to/your/erigon
```

Then:
```bash
go mod tidy
go build -o erigon-direct .
```

**Option 2: Fetch from GitHub**

```bash
# Get the latest main branch
go get github.com/erigontech/erigon/v3@main
go mod tidy
go build -o erigon-direct .
```

Note: This may take a while as Erigon has many dependencies.

## Usage

```bash
# Set the path to your Erigon data directory
export ERIGON_DATA_DIR=/mnt/data

# Run
./erigon-direct
```

## Important Notes

1. **Read-only access**: This tool opens the database in read-only mode, safe to run while Erigon is running

2. **Must run on same machine**: You need direct filesystem access to the chaindata directory

3. **Path structure**: Erigon stores chaindata at:
   ```
   $ERIGON_DATA_DIR/
   └── chaindata/
       ├── mdbx.dat
       └── mdbx.lck
   ```

## Troubleshooting

### Module errors

If you get module resolution errors, the easiest fix is to clone Erigon and use a replace directive:

```bash
git clone https://github.com/erigontech/erigon.git /tmp/erigon
```

Then in `go.mod`:
```go
replace github.com/erigontech/erigon/v3 => /tmp/erigon
```

### CGO errors

MDBX requires CGO. Make sure you have:
- GCC or Clang installed
- `CGO_ENABLED=1` (default on most systems)

## Available Data

Using `rawdb` package, you can read:
- Block headers: `rawdb.ReadHeader(tx, hash, number)`
- Block bodies: `rawdb.ReadBody(tx, hash, number)`
- Receipts: `rawdb.ReadReceipts(tx, hash, number)`
- Current block: `rawdb.ReadCurrentBlockNumber(tx)`
- Canonical hashes: `rawdb.ReadCanonicalHash(tx, number)`
