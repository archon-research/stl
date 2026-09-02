// Package s3key provides construction and parsing of S3 keys for block data files.
//
// Key format: {partition}/{blockNumber}_{version}_{dataType}.json.gz
//
// Example: "21000000-21000999/21000042_1_block.json.gz"
package s3key

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/archon-research/stl/stl-verify/internal/pkg/partition"
)

// DataType identifies the kind of block data stored in an S3 object.
type DataType string

const (
	Block    DataType = "block"
	Receipts DataType = "receipts"
	Traces   DataType = "traces"
	Blobs    DataType = "blobs"
)

const suffix = ".json.gz"

// Key represents a parsed S3 key for block data.
type Key struct {
	Partition   string
	BlockNumber int64
	Version     int
	DataType    DataType
}

// Build constructs the full S3 key for a block data file.
// It computes the partition from the block number automatically.
// Format: {partition}/{blockNumber}_{version}_{dataType}.json.gz
func Build(blockNumber int64, version int, dataType DataType) string {
	return BuildWithPartition(partition.GetPartition(blockNumber), blockNumber, version, dataType)
}

// BuildWithPartition constructs an S3 key using a pre-computed partition string.
// Use this when the partition is already known to avoid recomputing it.
func BuildWithPartition(partitionStr string, blockNumber int64, version int, dataType DataType) string {
	return fmt.Sprintf("%s/%d_%d_%s%s", partitionStr, blockNumber, version, dataType, suffix)
}

// Parse extracts block number, version, and data type from an S3 key.
// Returns the parsed Key and true on success, or zero Key and false
// if the key doesn't match the expected format.
func Parse(key string) (Key, bool) {
	// Split into partition and filename on the last slash.
	slash := strings.LastIndex(key, "/")
	if slash < 0 {
		return Key{}, false
	}
	partStr := key[:slash]
	filename := key[slash+1:]

	// Strip the .json.gz suffix.
	if !strings.HasSuffix(filename, suffix) {
		return Key{}, false
	}
	stem := filename[:len(filename)-len(suffix)] // e.g. "21000042_1_block"

	// Split stem into exactly 3 parts: blockNumber, version, dataType.
	parts := strings.SplitN(stem, "_", 3)
	if len(parts) != 3 {
		return Key{}, false
	}

	blockNumber, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || blockNumber < 0 {
		return Key{}, false
	}

	version, err := strconv.Atoi(parts[1])
	if err != nil || version < 0 {
		return Key{}, false
	}

	dt := DataType(parts[2])
	if !validDataType(dt) {
		return Key{}, false
	}

	return Key{
		Partition:   partStr,
		BlockNumber: blockNumber,
		Version:     version,
		DataType:    dt,
	}, true
}

func validDataType(dt DataType) bool {
	switch dt {
	case Block, Receipts, Traces, Blobs:
		return true
	}
	return false
}

// HeightPrefix is the key prefix every object for one height shares, for a
// listing narrowed to that height. The trailing underscore is what keeps a
// longer height sharing the partition — 10 against 1 — out of the listing.
func HeightPrefix(blockNumber int64) string {
	return fmt.Sprintf("%s/%d_", partition.GetPartition(blockNumber), blockNumber)
}

// FirstCorrectionVersion is the lowest version a correction may occupy: version
// 0 is the slot being corrected.
const FirstCorrectionVersion = 1

// HighestVersion returns the highest version the given keys carry for
// blockNumber, and whether any of them names that height at all. An object at a
// version occupies the slot whatever data type it holds, so a height archived
// only halfway still counts as taken. Keys naming another height, another
// partition, or nothing this package recognises are ignored.
func HighestVersion(keys []string, blockNumber int64) (int, bool) {
	part := partition.GetPartition(blockNumber)

	highest, found := 0, false
	for _, key := range keys {
		parsed, ok := Parse(key)
		if !ok || parsed.BlockNumber != blockNumber || parsed.Partition != part {
			continue
		}
		if !found || parsed.Version > highest {
			highest, found = parsed.Version, true
		}
	}
	return highest, found
}

// NextVersion returns the version a correction for a height must be written
// under: one past the highest version the archive already holds, and the first
// correction slot where it holds nothing. Never 0 — that slot carries the data
// being corrected.
func NextVersion(highest int, found bool) int {
	if !found {
		return FirstCorrectionVersion
	}
	return max(highest+1, FirstCorrectionVersion)
}
