// Package blockverifier selects and constructs the right outbound.BlockVerifier
// for a given chain. It is the chain-routing seam for the data validator: the
// service depends only on the BlockVerifier port, and this factory decides which
// concrete adapter backs that port for each chain we ingest. Adding a future
// chain (including a non-EVM one) is a registry entry plus, if needed, a new
// adapter behind the same port; the service never changes.
package blockverifier

import (
	"fmt"
	"log/slog"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/etherscan"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Kind identifies which canonical-source adapter backs a chain.
type Kind string

const (
	// KindEtherscan uses the Etherscan V2 multichain API: one endpoint, one key,
	// chain selected via the chainid query parameter.
	KindEtherscan Kind = "etherscan"
)

// chainKind maps each chain we ingest to the adapter kind that verifies it.
// A chain not present here is rejected at startup so a misconfigured deployment
// fails loudly rather than silently validating nothing.
var chainKind = map[int64]Kind{
	1:     KindEtherscan, // Ethereum mainnet
	10:    KindEtherscan, // Optimism
	130:   KindEtherscan, // Unichain
	8453:  KindEtherscan, // Base
	42161: KindEtherscan, // Arbitrum One
	43114: KindEtherscan, // Avalanche C-Chain
}

// Options carries the credentials and overrides the factory needs to build a
// verifier. A field is used only by the adapter kinds that require it.
type Options struct {
	// EtherscanAPIKey is the Etherscan V2 API key (shared across all chains).
	EtherscanAPIKey string

	// EtherscanBaseURL overrides the Etherscan V2 endpoint. Empty uses the
	// adapter default. Tests set this to a mock server URL.
	EtherscanBaseURL string

	// Logger is the structured logger passed to the adapter.
	Logger *slog.Logger
}

// New returns the BlockVerifier for chainID, or an error if the chain is not
// configured or required credentials are missing.
func New(chainID int64, opts Options) (outbound.BlockVerifier, error) {
	kind, ok := chainKind[chainID]
	if !ok {
		return nil, fmt.Errorf("no block verifier configured for chain ID %d", chainID)
	}

	switch kind {
	case KindEtherscan:
		return newEtherscanVerifier(chainID, opts)
	default:
		// Unreachable while every chainKind entry uses a declared Kind constant.
		// Kept so a future Kind added to the registry without a matching switch
		// arm fails loudly instead of silently.
		return nil, fmt.Errorf("unsupported verifier kind %q for chain ID %d", kind, chainID)
	}
}

func newEtherscanVerifier(chainID int64, opts Options) (outbound.BlockVerifier, error) {
	// Validate before NewClient so the error names the chain (NewClient also
	// rejects an empty key, but without the chain ID).
	if opts.EtherscanAPIKey == "" {
		return nil, fmt.Errorf("etherscan API key required for chain ID %d", chainID)
	}
	client, err := etherscan.NewClient(etherscan.ClientConfig{
		APIKey:  opts.EtherscanAPIKey,
		ChainID: chainID,
		BaseURL: opts.EtherscanBaseURL, // empty => adapter default
		Logger:  opts.Logger,
	})
	if err != nil {
		return nil, fmt.Errorf("creating etherscan verifier for chain ID %d: %w", chainID, err)
	}
	return client, nil
}
