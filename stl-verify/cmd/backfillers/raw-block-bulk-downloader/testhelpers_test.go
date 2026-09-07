package main

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math/rand/v2"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/pkg/retry"
	"github.com/archon-research/stl/stl-verify/internal/pkg/s3key"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// fixtureFiller is seeded so a fixture is byte-identical from run to run, and
// mutex-guarded because a fake RPC server builds fixtures on its own goroutines.
var (
	fixtureFillerMu sync.Mutex
	fixtureFiller   = rand.New(rand.NewPCG(1, 2))
)

const (
	canonicalHash = "0xf327fc5c0000000000000000000000000000000000000000000000000000cafe"
	forkHash      = "0x20383ba20000000000000000000000000000000000000000000000000000beef"
)

// fakeListReader serves a fixed partition listing so the archive index can be
// built without S3. Only ListPrefix is used.
type fakeListReader struct {
	keys  []string
	delay time.Duration
	err   error
	// failures is how many leading calls err answers; zero means every call.
	failures int

	mu    sync.Mutex
	calls int
}

func (f *fakeListReader) ListFiles(context.Context, string, string) ([]outbound.S3File, error) {
	return nil, errors.New("not used")
}

func (f *fakeListReader) ListPrefix(_ context.Context, _, prefix string) ([]string, error) {
	f.mu.Lock()
	f.calls++
	failing := f.err != nil && (f.failures == 0 || f.calls <= f.failures)
	f.mu.Unlock()

	time.Sleep(f.delay)
	if failing {
		return nil, f.err
	}
	var out []string
	for _, k := range f.keys {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	return out, nil
}

func (f *fakeListReader) StreamFile(context.Context, string, string) (io.ReadCloser, error) {
	return nil, errors.New("not used")
}

// fakeRangeReader serves objects as stored and records the ranges asked for, so
// a test can tell a ranged read from a whole-object one.
type fakeRangeReader struct {
	objects map[string][]byte
	ranges  map[string]int64
	err     error
}

func newFakeRangeReader(objects map[string][]byte) *fakeRangeReader {
	return &fakeRangeReader{objects: objects, ranges: map[string]int64{}}
}

func (f *fakeRangeReader) ReadRange(_ context.Context, _, key string, start, end int64) ([]byte, error) {
	if f.err != nil {
		return nil, f.err
	}
	body, ok := f.objects[key]
	if !ok {
		return nil, fmt.Errorf("%s: %w", key, outbound.ErrObjectNotFound)
	}
	f.ranges[key] = end - start + 1
	if start >= int64(len(body)) {
		return nil, nil
	}
	return body[start:min(end+1, int64(len(body)))], nil
}

func (f *fakeListReader) listings() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func gzipped(t *testing.T, payload []byte) []byte {
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

// blockJSON builds an eth_getBlockByNumber payload whose transaction list is far
// longer than the prefix a hash read fetches.
func blockJSON(hash string, transactions int) []byte {
	var b strings.Builder
	fmt.Fprintf(&b, `{"hash":%q,"number":"0x1830003","parentHash":%q,"transactions":[`, hash, forkHash)
	for i := range transactions {
		if i > 0 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, `{"hash":"0x%064x","blockHash":%q,"input":%q}`, i, hash, randomHex(512))
	}
	b.WriteString(`]}`)
	return []byte(b.String())
}

// receiptsJSON builds an eth_getBlockReceipts payload of the same scale.
func receiptsJSON(blockHash string, receipts int) []byte {
	var b strings.Builder
	b.WriteString("[")
	for i := range receipts {
		if i > 0 {
			b.WriteString(",")
		}
		fmt.Fprintf(&b, `{"blockHash":%q,"transactionHash":"0x%064x","logs":[{"data":%q}]}`, blockHash, i, randomHex(512))
	}
	b.WriteString("]")
	return []byte(b.String())
}

// randomHex returns incompressible filler, so a fixture object is as large
// stored as a real one and a prefix read is really a partial one.
func randomHex(bytes int) string {
	fixtureFillerMu.Lock()
	defer fixtureFillerMu.Unlock()

	buf := make([]byte, bytes)
	for i := range buf {
		buf[i] = byte(fixtureFiller.Uint32())
	}
	return "0x" + hex.EncodeToString(buf)
}

func archivedObjects(t *testing.T, blockNum int64, version int, hash string) map[string][]byte {
	t.Helper()

	return map[string][]byte{
		s3key.Build(blockNum, version, s3key.Block):    gzipped(t, blockJSON(hash, 40)),
		s3key.Build(blockNum, version, s3key.Receipts): gzipped(t, receiptsJSON(hash, 40)),
	}
}

// captureLogger records at Debug so a test can assert the level a line was
// written at, not only its text.
func captureLogger() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	return slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})), &buf
}

// newTestPartitionCache gives the cache a retry schedule a test can wait out.
func newTestPartitionCache(reader outbound.S3Reader) *PartitionCache {
	cache := NewPartitionCache(reader, "bucket", testutil.DiscardLogger())
	cache.listRetry = retry.Config{
		MaxRetries:     listRetryAttempts - 1,
		InitialBackoff: time.Millisecond,
		MaxBackoff:     2 * time.Millisecond,
		BackoffFactor:  2,
	}
	return cache
}

// ethereumTypes is the data type set of the chain most of these tests plan
// against; chainDataTypes is what a run derives it from.
func ethereumTypes() []s3key.DataType {
	return []s3key.DataType{s3key.Block, s3key.Receipts, s3key.Traces}
}

// newTestPlanner wires a planner over a fake partition listing and fake object
// bodies, with no S3 and no RPC behind it.
func newTestPlanner(t *testing.T, chainID int64, keys []string, objects map[string][]byte) (*blockPlanner, *Stats) {
	t.Helper()

	types, err := chainDataTypes(chainID)
	if err != nil {
		t.Fatalf("chainDataTypes(%d): %v", chainID, err)
	}

	stats := &Stats{}
	cache := newTestPartitionCache(&fakeListReader{keys: keys})
	return &blockPlanner{
		cache:  cache,
		reader: newFakeRangeReader(objects),
		bucket: "bucket",
		types:  types,
		stats:  stats,
	}, stats
}

// failingSink refuses every write and every close, the way a full disk does
// halfway through a run.
type failingSink struct{ err error }

func (f failingSink) Write([]byte) (int, error) { return 0, f.err }
func (f failingSink) Close() error              { return f.err }

// unbufferedReport writes each line straight through — a one-byte buffer is
// smaller than any line — so a sink that refuses a write fails the record that
// reaches it rather than a flush much later.
func unbufferedReport(path string, sink io.WriteCloser) *decisionReport {
	writer := bufio.NewWriterSize(sink, 1)
	return &decisionReport{path: path, sink: sink, writer: writer, enc: json.NewEncoder(writer)}
}
