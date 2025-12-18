# Dune Query - SparkLend Supply Events

Query SparkLend supply events for a specific user via Dune Analytics.

## Usage

```bash
export DUNE_API_KEY="your_api_key"

# Query default user
go run main.go

# Query specific user
go run main.go -user 0x1234...

# Custom output file
go run main.go -output my_events.csv
```

## Output

CSV file with columns:
- `block_number`
- `block_time`
- `tx_hash`
- `reserve` - Asset supplied
- `user` - User address
- `amount` - Amount supplied (raw, no decimals)
