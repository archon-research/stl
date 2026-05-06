package memory

import (
	"context"
	"fmt"
	"io"
	"sync"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// S3Writer is an in-memory implementation of outbound.S3Writer for tests. It
// keeps every written payload in a sync.Map keyed by "bucket/key" so test
// assertions can read back what was written.
type S3Writer struct {
	mu      sync.Mutex
	files   map[string][]byte
	failKey string // optional: if set, WriteFileIfNotExists returns this error for matching keys
	failErr error
}

var _ outbound.S3Writer = (*S3Writer)(nil)

// NewS3Writer returns an in-memory S3Writer suitable for tests.
func NewS3Writer() *S3Writer {
	return &S3Writer{files: make(map[string][]byte)}
}

// WriteFileIfNotExists implements outbound.S3Writer. Returns true on first
// write and false on subsequent writes for the same (bucket, key).
func (w *S3Writer) WriteFileIfNotExists(ctx context.Context, bucket, key string, content io.Reader, _ bool) (bool, error) {
	if err := ctx.Err(); err != nil {
		return false, err
	}
	w.mu.Lock()
	defer w.mu.Unlock()

	full := bucket + "/" + key
	if w.failKey != "" && (w.failKey == "*" || w.failKey == full) {
		return false, w.failErr
	}
	if _, ok := w.files[full]; ok {
		return false, nil
	}
	body, err := io.ReadAll(content)
	if err != nil {
		return false, fmt.Errorf("memory.S3Writer: read body: %w", err)
	}
	w.files[full] = body
	return true, nil
}

// FileExists implements outbound.S3Writer.
func (w *S3Writer) FileExists(_ context.Context, bucket, key string) (bool, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	_, ok := w.files[bucket+"/"+key]
	return ok, nil
}

// Get returns the bytes stored for a key, or nil if absent.
func (w *S3Writer) Get(bucket, key string) []byte {
	w.mu.Lock()
	defer w.mu.Unlock()
	out, ok := w.files[bucket+"/"+key]
	if !ok {
		return nil
	}
	cp := make([]byte, len(out))
	copy(cp, out)
	return cp
}

// Keys returns the set of keys stored under bucket. Returned in arbitrary
// order — tests should sort before comparing.
func (w *S3Writer) Keys(bucket string) []string {
	w.mu.Lock()
	defer w.mu.Unlock()
	prefix := bucket + "/"
	var out []string
	for k := range w.files {
		if len(k) > len(prefix) && k[:len(prefix)] == prefix {
			out = append(out, k[len(prefix):])
		}
	}
	return out
}

// Count returns the number of objects stored under bucket.
func (w *S3Writer) Count(bucket string) int {
	return len(w.Keys(bucket))
}

// FailNext makes subsequent WriteFileIfNotExists calls return err. If key is
// "*", every call fails; otherwise only writes whose "bucket/key" match.
func (w *S3Writer) FailNext(key string, err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.failKey = key
	w.failErr = err
}

// ClearFail disables the failure injection set by FailNext.
func (w *S3Writer) ClearFail() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.failKey = ""
	w.failErr = nil
}
