// Package entity contains the core domain entities for the Sentinel risk data layer.
// These entities represent the fundamental business objects and have no external dependencies.
package entity

import "fmt"

// Chain represents a blockchain network.
type Chain struct {
	ChainID int
	Name    string
}

// NewChain creates a new Chain entity with validation.
func NewChain(chainID int, name string) (*Chain, error) {
	c := &Chain{
		ChainID: chainID,
		Name:    name,
	}
	if err := c.Validate(); err != nil {
		return nil, fmt.Errorf("NewChain: %w", err)
	}
	return c, nil
}

// validate checks that all fields have valid values.
func (c *Chain) Validate() error {
	if c.ChainID <= 0 {
		return fmt.Errorf("chainID must be positive, got %d", c.ChainID)
	}
	if c.Name == "" {
		return fmt.Errorf("name must not be empty")
	}
	return nil
}

// ChainIDToName maps chain IDs to internal chain names used by service configs.
var ChainIDToName = map[int64]string{
	1:     "mainnet",
	10:    "optimism",
	130:   "unichain",
	8453:  "base",
	42161: "arbitrum",
	43114: "avalanche-c",
}

// ChainName returns the internal chain name for a chain ID (the same value used
// in service configs and emitted as the `chain` metric label). It errors on an
// unrecognised chain ID so callers fail hard at startup rather than silently
// emitting an empty `chain` label, which is what left the Vector alerts showing
// an empty chain.
func ChainName(chainID int64) (string, error) {
	name, ok := ChainIDToName[chainID]
	if !ok {
		return "", fmt.Errorf("unknown chain ID %d", chainID)
	}
	return name, nil
}

// ChainIDToS3Bucket maps chain IDs to canonical names expected in S3 bucket names.
// Example: stl-sentinelstaging-ethereum-raw, stl-sentinelstaging-avalanche-raw.
var ChainIDToS3Bucket = map[int64]string{
	1:     "ethereum",
	10:    "optimism",
	130:   "unichain",
	8453:  "base",
	42161: "arbitrum",
	43114: "avalanche",
}
