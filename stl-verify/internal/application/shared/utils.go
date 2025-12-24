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
// Format: {chainID}:{blockNumber}:{dataType}
func CacheKey(chainID, blockNumber int64, dataType string) string {
	return fmt.Sprintf("%d:%d:%s", chainID, blockNumber, dataType)
}
