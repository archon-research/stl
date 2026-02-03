// Package partition provides utilities for S3 partition key generation.
package partition

import "fmt"

// BlockRangeSize is the number of blocks per S3 partition.
// Each partition contains exactly 1000 blocks.
const BlockRangeSize = 1000

// GetPartition returns the partition string for a block number.
// Each partition contains exactly BlockRangeSize (1000) blocks:
// Block 0-999 -> "0-999", block 1000-1999 -> "1000-1999", etc.
func GetPartition(blockNumber int64) string {
	partitionIndex := blockNumber / BlockRangeSize
	start := partitionIndex * BlockRangeSize
	end := start + BlockRangeSize - 1
	return fmt.Sprintf("%d-%d", start, end)
}
