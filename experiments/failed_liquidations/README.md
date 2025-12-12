# Failed Liquidations Scanner

This tool scans the Ethereum blockchain (using a local Erigon database) for `liquidationCall` transactions on the SparkLend Pool contract. It re-executes transactions using a custom tracer to capture details about the liquidation calls, including whether they failed or succeeded.

## Features

- **Direct DB Access**: Connects directly to Erigon's MDBX database for high performance.
- **Custom Tracer**: Uses a custom EVM tracer to capture internal calls and their arguments.
- **Efficient Scanning**: Uses Erigon's `TracesToIdx` (if available) to quickly identify relevant transactions, avoiding the need to trace every single transaction.
- **Resumable**: Can resume scanning from the last processed block.
- **Streaming Output**: Writes results to CSV in real-time.
- **Parallel Execution**: Includes a script (`parallel_scan.sh`) to run multiple instances for faster processing.

## Prerequisites

- **Go 1.21+**
- **Erigon Node**: A fully synced Erigon node (archive mode recommended for full historical access).
- **Access to Data Directory**: The tool needs read access to the Erigon data directory (e.g., `/mnt/data/chaindata`).

## Usage

### Basic Usage

```bash
go run main.go --data-dir /path/to/erigon/datadir
```

### Common Flags

| Flag | Description | Default |
|------|-------------|---------|
| `--data-dir` | Path to Erigon data directory | `/mnt/data` |
| `--contract` | SparkLend Pool contract address | `0xC13e21B648A5Ee794902342038FF3aDAB66BE987` |
| `--start` | Start block number | `17203646` (SparkLend deployment) |
| `--end` | End block number (0 for latest) | `0` |
| `--output` | Output CSV file | `liquidation_calls.csv` |
| `--failed-only` | Only output failed liquidations | `true` |
| `--resume` | Resume from last processed block in output file | `false` |
| `--workers` | Number of internal parallel workers | `1` |

### Example: Scan for failed liquidations in a specific range

```bash
go run main.go \
  --data-dir /var/lib/erigon \
  --start 18000000 \
  --end 18100000 \
  --failed-only=true \
  --output failed_liquidations_18m.csv
```

### Parallel Scanning

For faster processing of large block ranges, use the `parallel_scan.sh` script. It splits the block range into chunks and runs multiple instances of the scanner.

```bash
./parallel_scan.sh \
  --data-dir /var/lib/erigon \
  --start 17203646 \
  --processes 8 \
  --output-dir ./results
```

## Output Format

The tool outputs a CSV file with the following columns:

- `block_number`: Block number where the call occurred.
- `block_timestamp`: Timestamp of the block.
- `transaction_hash`: Hash of the transaction.
- `tx_index`: Index of the transaction in the block.
- `from_address`: Sender address.
- `to_address`: Target address (usually the contract).
- `collateral_asset`: Address of the collateral asset.
- `debt_asset`: Address of the debt asset.
- `user`: Address of the user being liquidated.
- `debt_to_cover`: Amount of debt to cover.
- `receive_atoken`: Whether to receive aTokens.
- `gas_limit`: Gas limit of the transaction.
- `value`: ETH value sent with the transaction.
- `call_depth`: Depth of the call stack.
- `failed`: `true` if the liquidation call reverted.
- `revert_reason`: Reason for reversion (if available).
- `internal_call_index`: Index of the internal call within the transaction.

## How it Works

1. **Block Iteration**: Iterates through blocks in the specified range.
2. **Transaction Filtering**: 
   - Uses Erigon's `TracesToIdx` (if available) to find transactions that touched the SparkLend contract.
   - Alternatively, scans calldata for the `liquidationCall` selector (`00a718a9`).
3. **Tracing**: Re-executes candidate transactions using a custom `LiquidationTracer`.
4. **Capture**: The tracer hooks into `OnEnter` to detect calls to the SparkLend contract with the correct selector. It decodes the arguments and records the call details.
5. **Result**: If the call reverts (detected in `OnExit`), it is marked as failed.
