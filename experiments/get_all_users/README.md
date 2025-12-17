# SparkLend Active Users Query

This tool queries Dune Analytics to get all users who have ever interacted with SparkLend protocol.

## Prerequisites

1. **Dune API Key** - Get one from [Dune Analytics](https://dune.com/settings/api)

## Usage

```bash
# Set your Dune API key
export DUNE_API_KEY="your_api_key_here"

# Run the query
go run main.go

# Or build and run
go build -o get_all_users
./get_all_users
```

## Output

The tool outputs a CSV file `sparklend_users.csv` with the following columns:
- `user` - The user's Ethereum address
- `first_interaction` - Timestamp of their first interaction
- `last_interaction` - Timestamp of their last interaction
- `total_supplies` - Number of supply transactions
- `total_borrows` - Number of borrow transactions
- `total_withdrawals` - Number of withdrawal transactions
- `total_repays` - Number of repay transactions

## Query

The underlying Dune SQL query aggregates all user interactions from:
- `Supply` events
- `Borrow` events
- `Withdraw` events
- `Repay` events
- `LiquidationCall` events (as liquidated user)

## Notes

- SparkLend Pool: `0xC13e21B648A5Ee794902342038FF3aDAB66BE987`
- Deployment Block: `17203646` (May 2023)
