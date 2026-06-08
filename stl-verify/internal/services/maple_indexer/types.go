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

// DefaultMulticallChunkSize bounds how many balanceOf / convertToAssets calls
// go into a single multicall3 aggregate in FetchUserPositions. A high-churn
// block can touch thousands of users; one unbounded aggregate would exceed the
// node's eth_call gas cap or the RPC provider's compute limit and revert
// wholesale, stalling the block forever. 100 keeps each aggregate well under
// those limits while still amortising round-trips. Exported so the worker's
// env-var fallback names a single source of truth.
const DefaultMulticallChunkSize = 100

// Config holds service configuration for the maple-indexer worker.
type Config struct {
	shared.SQSConsumerConfig
	Telemetry *Telemetry // optional, nil-safe

	// MulticallChunkSize caps the per-multicall call count in
	// FetchUserPositions. A value <= 0 falls back to DefaultMulticallChunkSize.
	MulticallChunkSize int
}

// ConfigDefaults returns default configuration values.
func ConfigDefaults() Config {
	return Config{
		SQSConsumerConfig:  shared.SQSConsumerConfigDefaults(),
		MulticallChunkSize: DefaultMulticallChunkSize,
	}
}

// ApplyDefaults fills in zero-value fields with defaults, delegating the SQS
// fields to the embedded config so every field in Config defaults through the
// same mechanism.
func (c *Config) ApplyDefaults() {
	c.SQSConsumerConfig.ApplyDefaults()
	if c.MulticallChunkSize <= 0 {
		c.MulticallChunkSize = DefaultMulticallChunkSize
	}
}
