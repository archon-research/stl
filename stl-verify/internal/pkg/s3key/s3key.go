// Package s3key provides construction and parsing of S3 keys for block data files.
//
// Key format: {partition}/{blockNumber}_{version}_{dataType}.json.gz
//
// Example: "21000000-21000999/21000042_1_block.json.gz"
package s3key

import (
	"errors"
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

// PartitionPrefix is the key prefix every object in one partition shares, for a
// listing of the whole partition.
func PartitionPrefix(partitionStr string) string {
	return partitionStr + "/"
}

// ErrUnrecognisedKey marks an object whose name carries no
// {blockNumber}_{version}_ stem. Nothing under an archive prefix should look
// like that, and a caller cannot tell whether such an object occupies a version,
// so it stops rather than plan around a slot it cannot read.
var ErrUnrecognisedKey = errors.New("archive key carries no {blockNumber}_{version}_ stem")

// Occupancy is what a listing holds at one height: the highest version any
// object carries, and the data types stored under that version that this binary
// recognises. An object of a type added after this binary shipped occupies its
// version without appearing in DataTypes.
type Occupancy struct {
	Version   int
	DataTypes map[DataType]bool
}

// slot is the height and version an object occupies, whatever it stores there.
type slot struct {
	Partition   string
	BlockNumber int64
	Version     int
	DataType    DataType
}

// parseSlot reads the {blockNumber}_{version}_ stem every archived object's name
// starts with. What follows decides DataType only: a suffix this binary does not
// know leaves it empty, because occupancy is the stem's business and the version
// is taken either way.
func parseSlot(key string) (slot, bool) {
	slash := strings.LastIndex(key, "/")
	if slash < 0 {
		return slot{}, false
	}
	parts := strings.SplitN(key[slash+1:], "_", 3)
	if len(parts) != 3 || parts[2] == "" {
		return slot{}, false
	}

	blockNumber, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || blockNumber < 0 {
		return slot{}, false
	}
	version, err := strconv.Atoi(parts[1])
	if err != nil || version < 0 {
		return slot{}, false
	}

	found := slot{Partition: key[:slash], BlockNumber: blockNumber, Version: version}
	if stem, ok := strings.CutSuffix(parts[2], suffix); ok && validDataType(DataType(stem)) {
		found.DataType = DataType(stem)
	}
	return found, true
}

// Occupancies folds listed keys into what each height holds. A key filed under a
// partition that is not its own height's is ignored; one this package cannot
// read at all fails, naming the key.
func Occupancies(keys []string) (map[int64]Occupancy, error) {
	index := make(map[int64]Occupancy)
	for _, key := range keys {
		parsed, ok := parseSlot(key)
		if !ok {
			return nil, fmt.Errorf("%q: %w", key, ErrUnrecognisedKey)
		}
		if parsed.Partition != partition.GetPartition(parsed.BlockNumber) {
			continue
		}

		top, seen := index[parsed.BlockNumber]
		switch {
		case !seen || parsed.Version > top.Version:
			index[parsed.BlockNumber] = Occupancy{Version: parsed.Version, DataTypes: dataTypeSet(parsed.DataType)}
		case parsed.Version == top.Version && parsed.DataType != "":
			top.DataTypes[parsed.DataType] = true
		}
	}
	return index, nil
}

func dataTypeSet(dataType DataType) map[DataType]bool {
	set := make(map[DataType]bool, 1)
	if dataType != "" {
		set[dataType] = true
	}
	return set
}

// FirstCorrectionVersion is the lowest version a correction may occupy: version
// 0 is the slot being corrected.
const FirstCorrectionVersion = 1

// HighestVersion returns the highest version the given keys carry for
// blockNumber, and whether any of them names that height at all. An object at a
// version occupies the slot whatever it stores there, so a height archived only
// halfway, or holding a type this binary does not know, still counts as taken.
func HighestVersion(keys []string, blockNumber int64) (int, bool, error) {
	index, err := Occupancies(keys)
	if err != nil {
		return 0, false, err
	}
	top, found := index[blockNumber]
	return top.Version, found, nil
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
