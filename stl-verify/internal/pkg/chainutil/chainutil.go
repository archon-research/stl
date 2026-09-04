// Package chainutil provides utilities for working with blockchain chain IDs and
// names, and for cross-checking a deployment's configured chain against its wiring.
package chainutil

import (
	"context"
	"fmt"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/env"
)

// BlockDataExpectation declares which block data types a chain's watcher fetches
// and caches for every block. It is a chain fact with two readers: a tool that
// re-publishes a block must produce exactly this set, and a consumer of the
// block feed may expect to find exactly this set.
type BlockDataExpectation struct {
	// ExpectReceipts indicates receipts data is required for this chain.
	ExpectReceipts bool
	// ExpectTraces indicates traces data is required for this chain.
	ExpectTraces bool
	// ExpectBlobs indicates blob sidecars are required for this chain.
	ExpectBlobs bool
}

// DefaultChainExpectations returns the expectations for known chains. These MUST
// mirror what each chain's watcher actually caches: receipts are always fetched,
// but traces only when the watcher runs without --enable-traces=false. Today only
// the ethereum watcher fetches traces; every other chain's watcher sets
// --enable-traces=false (avalanche and arbitrum have no trace_block on Alchemy at
// all; base/optimism/unichain support it but the watcher still does not fetch
// it). Blobs are not fetched anywhere (--enable-blobs is false).
func DefaultChainExpectations() map[int64]BlockDataExpectation {
	return map[int64]BlockDataExpectation{
		1:     {ExpectReceipts: true, ExpectTraces: true, ExpectBlobs: false},  // Ethereum Mainnet
		43114: {ExpectReceipts: true, ExpectTraces: false, ExpectBlobs: false}, // Avalanche C-Chain
		8453:  {ExpectReceipts: true, ExpectTraces: false, ExpectBlobs: false}, // Base
		10:    {ExpectReceipts: true, ExpectTraces: false, ExpectBlobs: false}, // Optimism
		130:   {ExpectReceipts: true, ExpectTraces: false, ExpectBlobs: false}, // Unichain
		42161: {ExpectReceipts: true, ExpectTraces: false, ExpectBlobs: false}, // Arbitrum
		4663:  {ExpectReceipts: true, ExpectTraces: false, ExpectBlobs: false}, // Robinhood Chain
	}
}

// ValidateS3BucketForChain checks that the S3 bucket name has the expected prefix
// for the given chain ID and deployment environment. This prevents accidentally
// reading from or writing to the wrong chain's bucket.
//
// The expected prefix format is: stl-sentinel{environment}-{chainName}-raw
// For example, chain ID 1 (Ethereum) in "staging" requires the bucket name to
// start with "stl-sentinelstaging-ethereum-raw", such as
// "stl-sentinelstaging-ethereum-raw" or "stl-sentinelstaging-ethereum-raw-89d540d0".
//
// Returns an error if:
//   - environment is empty
//   - The chain ID is not recognized
//   - The bucket name does not have the expected prefix
func ValidateS3BucketForChain(chainID int64, bucket string, environment string) error {
	if environment == "" {
		return fmt.Errorf("environment must not be empty")
	}

	chainName, ok := entity.ChainIDToS3Bucket[chainID]
	if !ok {
		return fmt.Errorf("unknown chain ID %d: cannot validate bucket name", chainID)
	}

	expectedPrefix := fmt.Sprintf("stl-sentinel%s-%s-raw", environment, chainName)
	if !strings.HasPrefix(strings.ToLower(bucket), strings.ToLower(expectedPrefix)) {
		return fmt.Errorf("bucket %q does not have expected prefix %q for chain ID %d", bucket, expectedPrefix, chainID)
	}

	return nil
}

// ValidateSNSTopicForChain checks that the SNS topic ARN's topic name has the
// expected suffix for the given chain ID and deployment environment. This
// prevents accidentally publishing chain-X BlockEvents to chain-Y's topic.
//
// The expected suffix is: stl-sentinel{environment}-{chainName}-blocks.fifo
// matching the watcher's per-chain topic naming. For chain ID 1 (Ethereum) in
// "staging" the ARN must end with "stl-sentinelstaging-ethereum-blocks.fifo".
//
// Returns an error if:
//   - environment is empty
//   - The chain ID is not recognized
//   - The topic ARN does not have the expected suffix
func ValidateSNSTopicForChain(chainID int64, topicARN string, environment string) error {
	if environment == "" {
		return fmt.Errorf("environment must not be empty")
	}

	chainName, ok := entity.ChainIDToS3Bucket[chainID]
	if !ok {
		return fmt.Errorf("unknown chain ID %d: cannot validate sns topic", chainID)
	}

	expectedSuffix := fmt.Sprintf(":stl-sentinel%s-%s-blocks.fifo", environment, chainName)
	if !strings.HasSuffix(strings.ToLower(topicARN), strings.ToLower(expectedSuffix)) {
		return fmt.Errorf("sns topic %q does not have expected suffix %q for chain ID %d", topicARN, expectedSuffix, chainID)
	}

	return nil
}

// EnvironmentFromBucket extracts the deployment environment (e.g. "staging",
// "prod") from a stl-sentinel-prefixed bucket name. Use this to derive a
// single env value that downstream validators (ValidateS3BucketForChain,
// ValidateSNSTopicForChain) can share, so an operator who passes a
// staging bucket and a prod topic gets a clear error at startup.
//
// Returns an error if bucket does not start with "stl-sentinel" or has no
// segment following it.
func EnvironmentFromBucket(bucket string) (string, error) {
	const prefix = "stl-sentinel"
	lower := strings.ToLower(bucket)
	if !strings.HasPrefix(lower, prefix) {
		return "", fmt.Errorf("bucket %q does not start with %q", bucket, prefix)
	}
	rest := lower[len(prefix):]
	dash := strings.Index(rest, "-")
	if dash < 1 {
		return "", fmt.Errorf("bucket %q malformed; expected stl-sentinel{env}-{chain}-raw[-suffix]", bucket)
	}
	return rest[:dash], nil
}

// RequireChainID reads CHAIN_ID from the environment and parses it as an int.
// Returns an error if the variable is unset or not a valid integer.
func RequireChainID() (int, error) {
	s, err := env.Require("CHAIN_ID")
	if err != nil {
		return 0, err
	}
	id, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("CHAIN_ID must be a valid integer: %w", err)
	}
	return id, nil
}

const (
	ethereumMainnetChainID int64 = 1
	defaultAlchemyHTTPURL        = "https://eth-mainnet.g.alchemy.com/v2"
)

// AlchemyRPCURL joins ALCHEMY_HTTP_URL and ALCHEMY_API_KEY into the node URL the
// indexers dial. The built-in endpoint default is mainnet-only, so every other
// chain must set ALCHEMY_HTTP_URL explicitly rather than silently index mainnet.
func AlchemyRPCURL(chainID int64) (string, error) {
	apiKey, err := env.Require("ALCHEMY_API_KEY")
	if err != nil {
		return "", err
	}
	baseURL := env.Get("ALCHEMY_HTTP_URL", "")
	if baseURL == "" && chainID != ethereumMainnetChainID {
		return "", fmt.Errorf("ALCHEMY_HTTP_URL is required for chain %d (the default endpoint is mainnet-only)", chainID)
	}
	if baseURL == "" {
		baseURL = defaultAlchemyHTTPURL
	}
	return strings.TrimRight(baseURL, "/") + "/" + apiKey, nil
}

// ChainIDReader is the one node method AssertChainID needs, so the check is
// testable without an *ethclient.Client.
type ChainIDReader interface {
	ChainID(ctx context.Context) (*big.Int, error)
}

// chainIDProbeTimeout bounds the startup probe on its own: the dialers' budgets
// (60s to 5m, with retries) are sized for heavy calls, not for failing a sick node fast.
const chainIDProbeTimeout = 15 * time.Second

// AssertChainID refuses a node that disagrees with the configured chain. Every
// block number and contract address a job handles is meaningless on another
// chain, and the mismatch would otherwise surface far downstream (missing S3
// keys, mainnet state written under another chain id) rather than as itself.
func AssertChainID(ctx context.Context, node ChainIDReader, want int64) error {
	ctx, cancel := context.WithTimeout(ctx, chainIDProbeTimeout)
	defer cancel()

	got, err := node.ChainID(ctx)
	if err != nil {
		return fmt.Errorf("fetching RPC chain ID: %w", err)
	}
	if got == nil || !got.IsInt64() || got.Int64() != want {
		return fmt.Errorf("RPC chain ID mismatch: RPC reports %s, config says %d", got, want)
	}
	return nil
}
