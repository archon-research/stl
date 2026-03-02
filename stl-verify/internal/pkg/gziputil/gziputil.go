// Package gziputil provides shared gzip compression utilities.
package gziputil

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
)

// Decompress decompresses gzip data if compressed, otherwise returns data as-is.
// This provides backward compatibility with uncompressed data.
func Decompress(data []byte) ([]byte, error) {
	if !IsGzipped(data) {
		return data, nil
	}
	r, err := gzip.NewReader(bytes.NewReader(data))
	if err != nil {
		return nil, fmt.Errorf("failed to create gzip reader: %w", err)
	}
	defer r.Close()
	return io.ReadAll(r)
}

// IsGzipped reports whether data begins with the gzip magic bytes (0x1f 0x8b).
func IsGzipped(data []byte) bool {
	return len(data) >= 2 && data[0] == 0x1f && data[1] == 0x8b
}
