package s3

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"testing"
)

// gzipBytes stores a payload the way the archive holds it.
func gzipBytes(t *testing.T, payload []byte) []byte {
	t.Helper()

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(payload); err != nil {
		t.Fatalf("gzip write: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("gzip close: %v", err)
	}
	return buf.Bytes()
}

// gzippedBlock is a stored block payload: readers take only its first kilobytes, so it
// must decompress from a prefix rather than from the whole object.
func gzippedBlock(t *testing.T, hash string) []byte {
	t.Helper()
	return gzipBytes(t, fmt.Appendf(nil, `{"hash":%q,"number":"0x1836b83"}`, hash))
}
