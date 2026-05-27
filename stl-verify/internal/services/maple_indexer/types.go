package maple_indexer

import (
	"fmt"

	"github.com/archon-research/stl/stl-verify/internal/services/shared"
)

// mapleSyrupDeployBlocks maps chain IDs to the earliest meaningful block
// for Syrup ERC-4626 indexing. Used as the deploy_block when seeding the
// `maple-syrup-v1` protocol row and as the lower bound for any backfill
// that pulls historical state.
//
// Confirm exact deploy block on etherscan if updated; values here are the
// SyrupUSDC genesis block.
var mapleSyrupDeployBlocks = map[int64]int64{
	1: 20231245, // Ethereum mainnet
}

// MapleSyrupDeployBlock returns the deploy block for the given chain ID,
// or an error if the chain is not supported by this indexer.
func MapleSyrupDeployBlock(chainID int64) (int64, error) {
	block, ok := mapleSyrupDeployBlocks[chainID]
	if !ok {
		return 0, fmt.Errorf("unsupported chain ID %d for Maple Syrup: no known deploy block", chainID)
	}
	return block, nil
}

// Config holds service configuration for the maple-indexer worker.
type Config struct {
	shared.SQSConsumerConfig
	Telemetry *Telemetry // optional, nil-safe
}

// ConfigDefaults returns default configuration values.
func ConfigDefaults() Config {
	return Config{
		SQSConsumerConfig: shared.SQSConsumerConfigDefaults(),
	}
}
