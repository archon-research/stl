// Package shared provides shared utilities for application services.
package shared

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// ParseBlockNumber parses a hex-encoded block number string to int64.
func ParseBlockNumber(hexNum string) (int64, error) {
	hexNum = strings.TrimPrefix(hexNum, "0x")
	return strconv.ParseInt(hexNum, 16, 64)
}

// ParseBlockHeader unmarshals JSON data into a BlockHeader.
func ParseBlockHeader(data []byte, header *outbound.BlockHeader) error {
	return json.Unmarshal(data, header)
}

// CacheKey generates the cache key for a given data type.
// Format: stl:{chainID}:{blockNumber}:{version}:{dataType}
// The version is incremented each time a block at the same height is reorged.
func CacheKey(chainID, blockNumber int64, version int, dataType string) string {
	return fmt.Sprintf("stl:%d:%d:%d:%s", chainID, blockNumber, version, dataType)
}
