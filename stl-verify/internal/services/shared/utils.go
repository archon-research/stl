// Package shared provides shared utilities for application services.
package shared

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

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

// ParseCompressedJSON decompresses gzipped data (if needed) and unmarshals it
func ParseCompressedJSON(data []byte, v interface{}) error {
	// Check for gzip magic bytes (0x1f 0x8b)
	if len(data) > 2 && data[0] == 0x1f && data[1] == 0x8b {
		gr, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return fmt.Errorf("failed to create gzip reader: %w", err)
		}

		decompressed, err := io.ReadAll(gr)
		closeErr := gr.Close()

		if err != nil {
			return fmt.Errorf("failed to decompress: %w", err)
		}

		if closeErr != nil {
			return fmt.Errorf("failed to close gzip reader: %w", closeErr)
		}

		data = decompressed
	}

	return json.Unmarshal(data, v)
}
