package archiveblock

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"math/rand/v2"
	"strings"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/pkg/s3key"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

const (
	testBlock = int64(25395651)
	testHash  = "0x4d1c1a52b1f5e5a0c6f0b0a0d9e8c7b6a594837261504f3e2d1c0b9a8f7e6d5c"
)

// fakeObjects serves objects as stored and records the ranges asked for, so a
// test can tell a ranged read from a whole-object one. A key it does not hold
// answers the way S3 does.
type fakeObjects struct {
	objects map[string][]byte
	ranges  map[string]int64
	err     error
}

func newFakeObjects(objects map[string][]byte) *fakeObjects {
	return &fakeObjects{objects: objects, ranges: map[string]int64{}}
}

func (f *fakeObjects) ReadRange(_ context.Context, _, key string, start, end int64) ([]byte, error) {
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

// blockJSON is a block payload whose incompressible filler makes the stored
// object larger than a prefix read fetches.
func blockJSON(hash string, transactions int) []byte {
	body := fmt.Appendf(nil, `{"hash":%q,"number":"0x1830003","transactions":[`, hash)
	for i := range transactions {
		if i > 0 {
			body = append(body, ',')
		}
		body = fmt.Appendf(body, `{"hash":%q,"input":%q}`, randomHex(32), randomHex(512))
	}
	return append(body, ']', '}')
}

func receiptsJSON(blockHash string, receipts int) []byte {
	body := []byte("[")
	for i := range receipts {
		if i > 0 {
			body = append(body, ',')
		}
		body = fmt.Appendf(body, `{"blockHash":%q,"logsBloom":%q}`, blockHash, randomHex(256))
	}
	return append(body, ']')
}

// blockJSONWithLateHash puts the hash behind more incompressible filler than a
// prefix read fetches.
func blockJSONWithLateHash(hash string) []byte {
	return fmt.Appendf(nil, `{"extraData":%q,"hash":%q,"number":"0x1830003"}`, randomHex(64<<10), hash)
}

func randomHex(size int) string {
	buf := make([]byte, size)
	for i := range buf {
		buf[i] = byte(rand.Uint32())
	}
	return "0x" + hex.EncodeToString(buf)
}

func archivedObjects(t *testing.T, version int, hash string) map[string][]byte {
	t.Helper()

	return map[string][]byte{
		s3key.Build(testBlock, version, s3key.Block):    gzipped(t, blockJSON(hash, 40)),
		s3key.Build(testBlock, version, s3key.Receipts): gzipped(t, receiptsJSON(hash, 40)),
	}
}

func TestHash_ReadsTheBlockHashFromATruncatedObject(t *testing.T) {
	objects := archivedObjects(t, 0, testHash)
	reader := newFakeObjects(objects)

	got, found, err := Hash(context.Background(), reader, "bucket", testBlock, 0)
	if err != nil {
		t.Fatalf("Hash: %v", err)
	}
	if !found || got != testHash {
		t.Errorf("Hash = %q (found %v), want %q", got, found, testHash)
	}

	key := s3key.Build(testBlock, 0, s3key.Block)
	if asked := reader.ranges[key]; asked != PrefixBytes {
		t.Errorf("requested %d bytes of %s, want a %d-byte prefix", asked, key, PrefixBytes)
	}
	if int64(len(objects[key])) <= PrefixBytes {
		t.Fatalf("fixture object is %d bytes: too small to prove the read was partial", len(objects[key]))
	}
}

func TestHash_FallsBackToReceiptsWhenTheBlockObjectIsMissing(t *testing.T) {
	objects := archivedObjects(t, 0, testHash)
	delete(objects, s3key.Build(testBlock, 0, s3key.Block))

	got, found, err := Hash(context.Background(), newFakeObjects(objects), "bucket", testBlock, 0)
	if err != nil {
		t.Fatalf("Hash: %v", err)
	}
	if !found || got != testHash {
		t.Errorf("Hash = %q (found %v), want the blockHash of the first receipt %q", got, found, testHash)
	}
}

func TestHash_NotFoundWhenTheVersionHoldsNoObjectThatCarriesOne(t *testing.T) {
	objects := map[string][]byte{s3key.Build(testBlock, 0, s3key.Traces): gzipped(t, []byte(`[]`))}

	got, found, err := Hash(context.Background(), newFakeObjects(objects), "bucket", testBlock, 0)
	if err != nil {
		t.Fatalf("Hash: %v", err)
	}
	if found || got != "" {
		t.Errorf("Hash = %q (found %v), want nothing from an archive that cannot answer", got, found)
	}
}

func TestHash_AReadableObjectWithNoHashIsNotFoundRatherThanAFailure(t *testing.T) {
	tests := []struct {
		name     string
		dataType s3key.DataType
		body     string
	}{
		{name: "the empty receipt list of a zero-tx block", dataType: s3key.Receipts, body: `[]`},
		{name: "a null receipts payload", dataType: s3key.Receipts, body: `null`},
		{name: "a null block payload", dataType: s3key.Block, body: `null`},
		{name: "an empty list where the block object should be", dataType: s3key.Block, body: `[]`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := s3key.Build(testBlock, 0, tt.dataType)
			reader := newFakeObjects(map[string][]byte{key: gzipped(t, []byte(tt.body))})

			got, found, err := Hash(context.Background(), reader, "bucket", testBlock, 0)
			if err != nil {
				t.Fatalf("Hash: %v, want the height planned rather than failed on every run", err)
			}
			if found || got != "" {
				t.Errorf("Hash = %q (found %v), want nothing from an object that carries no hash", got, found)
			}
		})
	}
}

func TestHash_FallsBackToReceiptsWhenTheBlockObjectCarriesNoHash(t *testing.T) {
	objects := archivedObjects(t, 0, testHash)
	objects[s3key.Build(testBlock, 0, s3key.Block)] = gzipped(t, []byte(`null`))

	got, found, err := Hash(context.Background(), newFakeObjects(objects), "bucket", testBlock, 0)
	if err != nil {
		t.Fatalf("Hash: %v", err)
	}
	if !found || got != testHash {
		t.Errorf("Hash = %q (found %v), want the receipts to answer for a block object that cannot", got, found)
	}
}

// A hash the prefix could not reach must not read as a losing fork: that would
// republish over a height whose archive is already canonical. No later attempt
// reaches it either, so the caller must be able to stop rather than retry.
func TestHash_ErrorsWhenTheHashIsBeyondThePrefix(t *testing.T) {
	key := s3key.Build(testBlock, 0, s3key.Block)
	reader := newFakeObjects(map[string][]byte{key: gzipped(t, blockJSONWithLateHash(testHash))})

	_, _, err := Hash(context.Background(), reader, "bucket", testBlock, 0)

	if !errors.Is(err, ErrUnreadable) {
		t.Fatalf("error = %v, want ErrUnreadable", err)
	}
	if !strings.Contains(err.Error(), key) {
		t.Errorf("error = %v, want it to name the key", err)
	}
}

func TestHash_ErrorsOnAnObjectItCannotDecompress(t *testing.T) {
	tests := []struct {
		name string
		body func(t *testing.T) []byte
	}{
		{
			name: "not gzip at all",
			body: func(*testing.T) []byte { return []byte("not gzip at all") },
		},
		{
			// A gzip header the deflate stream behind it does not match: the read
			// fails past the header, not at it.
			name: "a corrupt deflate stream",
			body: func(t *testing.T) []byte {
				stored := gzipped(t, blockJSON(testHash, 40))
				for i := 10; i < 200; i++ {
					stored[i] = 0xff
				}
				return stored
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := s3key.Build(testBlock, 0, s3key.Block)
			reader := newFakeObjects(map[string][]byte{key: tt.body(t)})

			_, _, err := Hash(context.Background(), reader, "bucket", testBlock, 0)

			if !errors.Is(err, ErrUnreadable) {
				t.Fatalf("error = %v, want ErrUnreadable", err)
			}
			if !strings.Contains(err.Error(), key) {
				t.Errorf("error = %v, want it to name the key", err)
			}
		})
	}
}

// S3 refuses a ranged read of a zero-byte object rather than answering with an
// empty body, and that object names no block on any attempt either.
func TestHash_MarksAZeroByteObjectUnreadable(t *testing.T) {
	key := s3key.Build(testBlock, 0, s3key.Block)
	reader := newFakeObjects(nil)
	reader.err = fmt.Errorf("bucket/%s: %w", key, outbound.ErrObjectEmpty)

	_, _, err := Hash(context.Background(), reader, "bucket", testBlock, 0)

	if !errors.Is(err, ErrUnreadable) {
		t.Fatalf("error = %v, want ErrUnreadable", err)
	}
	if !strings.Contains(err.Error(), key) {
		t.Errorf("error = %v, want it to name the key", err)
	}
}

// A read that failed says nothing about the height, so it must surface rather
// than read as "the archive holds no hash".
func TestHash_AReadFailureIsNotAnUnknownHash(t *testing.T) {
	reader := newFakeObjects(archivedObjects(t, 0, testHash))
	reader.err = errors.New("503 SlowDown")

	_, found, err := Hash(context.Background(), reader, "bucket", testBlock, 0)

	if err == nil {
		t.Fatal("Hash succeeded against a failing read")
	}
	if found {
		t.Error("reported a hash it never read")
	}
	if errors.Is(err, ErrUnreadable) {
		t.Errorf("error = %v, want a throttled read left retryable", err)
	}
}

func TestHashFromPayload(t *testing.T) {
	tests := []struct {
		name      string
		payload   string
		want      string
		wantFound bool
	}{
		{name: "a header", payload: `{"number":"0x1","hash":"0xabc"}`, want: "0xabc", wantFound: true},
		{name: "a payload carrying no hash", payload: `{"number":"0x1"}`},
		{name: "a null payload", payload: `null`},
		{name: "a nested hash is not the block's", payload: `{"transactions":[{"hash":"0xdead"}]}`},
		{name: "a hash that is not a string", payload: `{"hash":123}`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, found := HashFromPayload([]byte(tt.payload))

			if found != tt.wantFound {
				t.Fatalf("HashFromPayload found = %v, want %v", found, tt.wantFound)
			}
			if got != tt.want {
				t.Errorf("HashFromPayload = %q, want %q", got, tt.want)
			}
		})
	}
}
