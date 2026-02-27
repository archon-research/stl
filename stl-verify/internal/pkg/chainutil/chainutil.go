// Package chainutil provides utilities for working with blockchain chain IDs and names.
package chainutil

import (
	"fmt"
	"strings"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// ValidateS3BucketForChain checks that the S3 bucket name contains the expected
// chain name for the given chain ID. This prevents accidentally reading from or
// writing to the wrong chain's bucket.
//
// For example, chain ID 1 (Ethereum) requires the bucket name to contain "ethereum",
// such as "stl-sentinelstaging-ethereum-raw".
//
// Returns an error if:
//   - The chain ID is not recognized
//   - The bucket name does not contain the expected chain name
func ValidateS3BucketForChain(chainID int64, bucket string) error {
	chainName, ok := entity.ChainIDToName[chainID]
	if !ok {
		return fmt.Errorf("unknown chain ID %d: cannot validate bucket name", chainID)
	}

	if !strings.Contains(strings.ToLower(bucket), chainName) {
		return fmt.Errorf("bucket %q does not contain expected chain name %q for chain ID %d", bucket, chainName, chainID)
	}

	return nil
}
