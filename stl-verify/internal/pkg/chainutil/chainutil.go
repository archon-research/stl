// Package chainutil provides utilities for working with blockchain chain IDs and names.
package chainutil

import (
	"fmt"
	"strings"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

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
