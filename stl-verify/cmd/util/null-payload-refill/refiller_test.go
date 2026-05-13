package main

import (
	"bytes"
	"compress/gzip"
	"context"
	"encoding/json"
	"errors"
	"io"
	"path/filepath"
	"sync"
	"testing"

	"github.com/aws/aws-sdk-go-v2/service/s3/types"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

type fakeS3Reader struct {
	mu         sync.Mutex
	objects    map[string][]byte
	errs       map[string]error
	calls      int
	streamHook func(ctx context.Context) (io.ReadCloser, error)
}

func newFakeS3Reader() *fakeS3Reader {
	return &fakeS3Reader{
		objects: make(map[string][]byte),
		errs:    make(map[string]error),
	}
}

func (f *fakeS3Reader) StreamFile(ctx context.Context, bucket, key string) (io.ReadCloser, error) {
	f.mu.Lock()
	hook := f.streamHook
	f.calls++
	if err, ok := f.errs[bucket+"/"+key]; ok {
		f.mu.Unlock()
		return nil, err
	}
	data, ok := f.objects[bucket+"/"+key]
	f.mu.Unlock()
	if hook != nil {
		return hook(ctx)
	}
	if !ok {
		return nil, errors.New("not found")
	}
	return io.NopCloser(bytes.NewReader(data)), nil
}

func (f *fakeS3Reader) ListFiles(_ context.Context, _, _ string) ([]outbound.S3File, error) {
	panic("fakeS3Reader.ListFiles: not used by tests")
}

func (f *fakeS3Reader) ListPrefix(_ context.Context, _, _ string) ([]string, error) {
	panic("fakeS3Reader.ListPrefix: not used by tests")
}

func (f *fakeS3Reader) putGzipped(bucket, key string, content []byte) {
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(content); err != nil {
		panic(err)
	}
	if err := w.Close(); err != nil {
		panic(err)
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[bucket+"/"+key] = buf.Bytes()
}

func (f *fakeS3Reader) putRaw(bucket, key string, content []byte) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.objects[bucket+"/"+key] = content
}

func (f *fakeS3Reader) putErr(bucket, key string, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.errs[bucket+"/"+key] = err
}

func (f *fakeS3Reader) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

var _ outbound.S3Overwriter = (*fakeS3Overwriter)(nil)

type fakeS3Overwriter struct {
	mu      sync.Mutex
	written map[string][]byte
	err     error
	calls   int
}

func newFakeS3Overwriter() *fakeS3Overwriter {
	return &fakeS3Overwriter{written: make(map[string][]byte)}
}

func (f *fakeS3Overwriter) WriteFile(_ context.Context, bucket, key string, content io.Reader, _ bool) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls++
	if f.err != nil {
		return f.err
	}
	body, err := io.ReadAll(content)
	if err != nil {
		return err
	}
	f.written[bucket+"/"+key] = body
	return nil
}

func (f *fakeS3Overwriter) callCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.calls
}

func (f *fakeS3Overwriter) get(bucket, key string) []byte {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.written[bucket+"/"+key]
}

type fakeRPCClient struct {
	mu            sync.Mutex
	response      outbound.BlockData
	err           error
	byNumber      map[int64]json.RawMessage
	byNumberErr   error
	byNumberCalls []int64
	dataCalls     []rpcCall
}

type rpcCall struct {
	BlockNum int64
	Hash     string
}

func newFakeRPCClient() *fakeRPCClient {
	return &fakeRPCClient{byNumber: make(map[int64]json.RawMessage)}
}

func (f *fakeRPCClient) GetBlockDataByHash(_ context.Context, blockNum int64, hash string, _ bool) (outbound.BlockData, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.dataCalls = append(f.dataCalls, rpcCall{BlockNum: blockNum, Hash: hash})
	if f.err != nil {
		return outbound.BlockData{}, f.err
	}
	return f.response, nil
}

func (f *fakeRPCClient) GetBlockByNumber(_ context.Context, blockNum int64, _ bool) (json.RawMessage, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.byNumberCalls = append(f.byNumberCalls, blockNum)
	if f.byNumberErr != nil {
		return nil, f.byNumberErr
	}
	if v, ok := f.byNumber[blockNum]; ok {
		return v, nil
	}
	return json.RawMessage("null"), nil
}

func (f *fakeRPCClient) GetBlockByHash(_ context.Context, _ string, _ bool) (*outbound.BlockHeader, error) {
	panic("fakeRPCClient.GetBlockByHash: not used by tests")
}

func (f *fakeRPCClient) GetFullBlockByHash(_ context.Context, _ string, _ bool) (json.RawMessage, error) {
	panic("fakeRPCClient.GetFullBlockByHash: not used by tests")
}

func (f *fakeRPCClient) GetBlockReceipts(_ context.Context, _ int64) (json.RawMessage, error) {
	panic("fakeRPCClient.GetBlockReceipts: not used by tests")
}

func (f *fakeRPCClient) GetBlockReceiptsByHash(_ context.Context, _ string) (json.RawMessage, error) {
	panic("fakeRPCClient.GetBlockReceiptsByHash: not used by tests")
}

func (f *fakeRPCClient) GetBlockTraces(_ context.Context, _ int64) (json.RawMessage, error) {
	panic("fakeRPCClient.GetBlockTraces: not used by tests")
}

func (f *fakeRPCClient) GetBlockTracesByHash(_ context.Context, _ string) (json.RawMessage, error) {
	panic("fakeRPCClient.GetBlockTracesByHash: not used by tests")
}

func (f *fakeRPCClient) GetBlobSidecars(_ context.Context, _ int64) (json.RawMessage, error) {
	panic("fakeRPCClient.GetBlobSidecars: not used by tests")
}

func (f *fakeRPCClient) GetBlobSidecarsByHash(_ context.Context, _ string) (json.RawMessage, error) {
	panic("fakeRPCClient.GetBlobSidecarsByHash: not used by tests")
}

func (f *fakeRPCClient) GetCurrentBlockNumber(_ context.Context) (int64, error) {
	panic("fakeRPCClient.GetCurrentBlockNumber: not used by tests")
}

func (f *fakeRPCClient) GetBlocksBatch(_ context.Context, _ []int64, _ bool) ([]outbound.BlockData, error) {
	panic("fakeRPCClient.GetBlocksBatch: not used by tests")
}

func (f *fakeRPCClient) dataCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.dataCalls)
}

func (f *fakeRPCClient) byNumberCallCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.byNumberCalls)
}

type fakePublisher struct {
	mu     sync.Mutex
	events []outbound.Event
	err    error
}

func (f *fakePublisher) Publish(_ context.Context, event outbound.Event) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.err != nil {
		return f.err
	}
	f.events = append(f.events, event)
	return nil
}

func (f *fakePublisher) Close() error { return nil }

func (f *fakePublisher) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.events)
}

func (f *fakePublisher) lastEvent() outbound.BlockEvent {
	f.mu.Lock()
	defer f.mu.Unlock()
	if len(f.events) == 0 {
		return outbound.BlockEvent{}
	}
	return f.events[len(f.events)-1].(outbound.BlockEvent)
}

type harness struct {
	bucket       string
	chainID      int64
	r            *Refiller
	state        *State
	statePath    string
	s3Reader     *fakeS3Reader
	s3Overwriter *fakeS3Overwriter
	rpc          *fakeRPCClient
	publisher    *fakePublisher
}

func newHarness(t *testing.T, opts ...func(*RefillerOptions)) *harness {
	t.Helper()
	path := filepath.Join(t.TempDir(), "state.jsonl")
	state, err := Load(path)
	if err != nil {
		t.Fatalf("Load state: %v", err)
	}
	t.Cleanup(func() { _ = state.Close() })

	h := &harness{
		bucket:       "test-bucket",
		chainID:      43114,
		state:        state,
		statePath:    path,
		s3Reader:     newFakeS3Reader(),
		s3Overwriter: newFakeS3Overwriter(),
		rpc:          newFakeRPCClient(),
		publisher:    &fakePublisher{},
	}

	options := RefillerOptions{
		Bucket:       h.bucket,
		ChainID:      h.chainID,
		S3Reader:     h.s3Reader,
		S3Overwriter: h.s3Overwriter,
		RPCClient:    h.rpc,
		Publisher:    h.publisher,
		State:        h.state,
	}
	for _, opt := range opts {
		opt(&options)
	}

	r, err := NewRefiller(options)
	if err != nil {
		t.Fatalf("NewRefiller: %v", err)
	}
	h.r = r
	return h
}

func withDryRun(opts *RefillerOptions) { opts.DryRun = true }

const testBlockKey = "85149000-85149999/85149017_0_block.json.gz"
const testReceiptsKey = "85149000-85149999/85149017_0_receipts.json.gz"
const testVersion1BlockKey = "85149000-85149999/85149017_1_block.json.gz"

// canonicalHeader is the eth_getBlockByNumber JSON response used across
// happy-path tests. The hash/parentHash/timestamp are the values the refiller
// extracts into the synthetic *outbound.BlockState that drives the BlockEvent.
const canonicalHeader = `{"hash":"0xabc","parentHash":"0xdef","timestamp":"0x3e7"}`

func TestProcess_NotNullInS3_RecordsSkip(t *testing.T) {
	h := newHarness(t)
	h.s3Reader.putGzipped(h.bucket, testBlockKey, []byte(`{"number":"0x5"}`))

	out := h.r.Process(context.Background(), testBlockKey)
	if out.Stage != StageSkip {
		t.Errorf("stage = %q, want %q", out.Stage, StageSkip)
	}
	if out.Reason != "already-healed" {
		t.Errorf("reason = %q, want already-healed", out.Reason)
	}
	if h.rpc.dataCallCount() != 0 {
		t.Errorf("rpc data calls = %d, want 0", h.rpc.dataCallCount())
	}
	if h.rpc.byNumberCallCount() != 0 {
		t.Errorf("rpc byNumber calls = %d, want 0", h.rpc.byNumberCallCount())
	}
	if h.publisher.count() != 0 {
		t.Errorf("publish calls = %d, want 0", h.publisher.count())
	}
	if h.s3Overwriter.callCount() != 0 {
		t.Errorf("write calls = %d, want 0", h.s3Overwriter.callCount())
	}
}

func TestProcess_RpcStillReturnsNull_RecordsFail(t *testing.T) {
	h := newHarness(t)
	h.s3Reader.putGzipped(h.bucket, testBlockKey, []byte("null"))
	h.rpc.byNumber[85149017] = json.RawMessage(canonicalHeader)
	h.rpc.response = outbound.BlockData{Block: json.RawMessage("null")}

	out := h.r.Process(context.Background(), testBlockKey)
	if out.Stage != StageFail {
		t.Errorf("stage = %q, want %q", out.Stage, StageFail)
	}
	if out.Reason != "rpc-still-null" {
		t.Errorf("reason = %q, want rpc-still-null", out.Reason)
	}
	if !out.Fatal {
		t.Errorf("Fatal = false, want true (rpc-still-null is unexpected, run must abort)")
	}
	if !errors.Is(out.Err, errRPCStillNull) {
		t.Errorf("Err chain missing errRPCStillNull, got %v", out.Err)
	}
	if h.s3Overwriter.callCount() != 0 {
		t.Errorf("unexpected s3 write")
	}
	if h.publisher.count() != 0 {
		t.Errorf("unexpected publish")
	}
}

func TestProcess_HappyPath_BlockKey(t *testing.T) {
	h := newHarness(t)
	h.s3Reader.putGzipped(h.bucket, testBlockKey, []byte("null"))
	h.rpc.byNumber[85149017] = json.RawMessage(canonicalHeader)
	validBlock := json.RawMessage(`{"number":"0x512f1c9","hash":"0xabc"}`)
	h.rpc.response = outbound.BlockData{Block: validBlock, Receipts: json.RawMessage(`[]`)}

	out := h.r.Process(context.Background(), testBlockKey)
	if out.Stage != StageSNS {
		t.Errorf("stage = %q, want %q", out.Stage, StageSNS)
	}

	if h.s3Overwriter.callCount() != 1 {
		t.Errorf("s3 write calls = %d, want 1", h.s3Overwriter.callCount())
	}
	if got := h.s3Overwriter.get(h.bucket, testBlockKey); !bytes.Equal(got, validBlock) {
		t.Errorf("written content = %q, want %q", got, validBlock)
	}
	if h.publisher.count() != 1 {
		t.Fatalf("publisher calls = %d, want 1", h.publisher.count())
	}
	ev := h.publisher.lastEvent()
	if ev.ChainID != h.chainID || ev.BlockNumber != 85149017 || ev.Version != 0 || ev.BlockHash != "0xabc" {
		t.Errorf("unexpected event: %+v", ev)
	}
	if ev.ParentHash != "0xdef" {
		t.Errorf("event parent hash = %q, want 0xdef", ev.ParentHash)
	}
	if ev.BlockTimestamp != 0x3e7 {
		t.Errorf("event block timestamp = %d, want %d", ev.BlockTimestamp, 0x3e7)
	}

	stage, _ := h.state.Lookup(testBlockKey)
	if stage != StageSNS {
		t.Errorf("state stage = %q, want %q", stage, StageSNS)
	}
}

func TestProcess_HappyPath_ReceiptsKey(t *testing.T) {
	h := newHarness(t)
	h.s3Reader.putGzipped(h.bucket, testReceiptsKey, []byte("null"))
	h.rpc.byNumber[85149017] = json.RawMessage(canonicalHeader)
	validReceipts := json.RawMessage(`[{"transactionHash":"0xttt"}]`)
	h.rpc.response = outbound.BlockData{Block: json.RawMessage(`{}`), Receipts: validReceipts}

	out := h.r.Process(context.Background(), testReceiptsKey)
	if out.Stage != StageSNS {
		t.Errorf("stage = %q, want %q", out.Stage, StageSNS)
	}
	if got := h.s3Overwriter.get(h.bucket, testReceiptsKey); !bytes.Equal(got, validReceipts) {
		t.Errorf("written receipts = %q, want %q", got, validReceipts)
	}
}

func TestProcess_ResumeFromS3Stage_OnlyPublishesSNS(t *testing.T) {
	h := newHarness(t)
	if err := h.state.Record(testBlockKey, StageS3, ""); err != nil {
		t.Fatalf("preload state: %v", err)
	}
	h.rpc.byNumber[85149017] = json.RawMessage(canonicalHeader)
	h.rpc.response = outbound.BlockData{Block: json.RawMessage(`{"hash":"0xabc"}`)}

	out := h.r.Process(context.Background(), testBlockKey)
	if out.Stage != StageSNS {
		t.Errorf("stage = %q, want %q", out.Stage, StageSNS)
	}
	if h.s3Reader.callCount() != 0 {
		t.Errorf("s3 read calls = %d, want 0 (resume from s3 must skip verify)", h.s3Reader.callCount())
	}
	if h.s3Overwriter.callCount() != 0 {
		t.Errorf("s3 write calls = %d, want 0", h.s3Overwriter.callCount())
	}
	if h.publisher.count() != 1 {
		t.Errorf("publisher calls = %d, want 1", h.publisher.count())
	}
}

func TestProcess_ResumeFromSNSStage_SkipsEntirely(t *testing.T) {
	h := newHarness(t)
	if err := h.state.Record(testBlockKey, StageSNS, ""); err != nil {
		t.Fatalf("preload state: %v", err)
	}

	out := h.r.Process(context.Background(), testBlockKey)
	if out.Stage != StageSNS || !out.Skipped {
		t.Errorf("outcome = %+v, want stage=sns skipped=true", out)
	}
	if h.s3Reader.callCount() != 0 {
		t.Errorf("s3 read calls = %d, want 0", h.s3Reader.callCount())
	}
	if h.rpc.dataCallCount() != 0 {
		t.Errorf("rpc data calls = %d, want 0", h.rpc.dataCallCount())
	}
	if h.rpc.byNumberCallCount() != 0 {
		t.Errorf("rpc byNumber calls = %d, want 0", h.rpc.byNumberCallCount())
	}
	if h.s3Overwriter.callCount() != 0 {
		t.Errorf("s3 write calls = %d, want 0", h.s3Overwriter.callCount())
	}
	if h.publisher.count() != 0 {
		t.Errorf("publisher calls = %d, want 0", h.publisher.count())
	}
}

func TestProcess_DryRun_NoWritesNoPublish(t *testing.T) {
	h := newHarness(t, withDryRun)
	h.s3Reader.putGzipped(h.bucket, testBlockKey, []byte("null"))
	h.rpc.byNumber[85149017] = json.RawMessage(canonicalHeader)
	h.rpc.response = outbound.BlockData{Block: json.RawMessage(`{}`)}

	out := h.r.Process(context.Background(), testBlockKey)
	if out.Stage != StageDryRun {
		t.Errorf("stage = %q, want %q", out.Stage, StageDryRun)
	}
	if h.s3Overwriter.callCount() != 0 {
		t.Errorf("s3 write should not happen on dry-run")
	}
	if h.publisher.count() != 0 {
		t.Errorf("publish should not happen on dry-run")
	}
	stage, _ := h.state.Lookup(testBlockKey)
	if stage != StageDryRun {
		t.Errorf("state stage = %q, want %q", stage, StageDryRun)
	}
}

// TestProcess_VersionGt0_SkipsCannotDetermineOrphan asserts that a key with
// version > 0 is skipped without any RPC call: eth_getBlockByNumber can only
// return the canonical block, so we cannot recover orphan-chain hashes from
// RPC alone. The DB-free design accepts this limitation because no version > 0
// nulls exist in the wild.
func TestProcess_VersionGt0_SkipsCannotDetermineOrphan(t *testing.T) {
	h := newHarness(t)
	h.s3Reader.putGzipped(h.bucket, testVersion1BlockKey, []byte("null"))

	out := h.r.Process(context.Background(), testVersion1BlockKey)
	if out.Stage != StageSkip || out.Reason != "cannot-determine-orphan-hash" {
		t.Errorf("outcome = %+v, want stage=skip reason=cannot-determine-orphan-hash", out)
	}
	if out.Fatal {
		t.Errorf("Fatal = true, want false (skipped, not fatal)")
	}
	if h.rpc.byNumberCallCount() != 0 {
		t.Errorf("rpc byNumber calls = %d, want 0 (version>0 skip must not call RPC)", h.rpc.byNumberCallCount())
	}
	if h.rpc.dataCallCount() != 0 {
		t.Errorf("rpc data calls = %d, want 0", h.rpc.dataCallCount())
	}
	if h.s3Overwriter.callCount() != 0 {
		t.Errorf("s3 writes = %d, want 0", h.s3Overwriter.callCount())
	}
	stage, reason := h.state.Lookup(testVersion1BlockKey)
	if stage != StageSkip {
		t.Errorf("state stage = %q, want %q", stage, StageSkip)
	}
	if reason != "cannot-determine-orphan-hash" {
		t.Errorf("state reason = %q, want cannot-determine-orphan-hash", reason)
	}
}

func TestProcess_InvalidKey_RecordsFail(t *testing.T) {
	h := newHarness(t)
	out := h.r.Process(context.Background(), "not-a-valid-key")
	if out.Stage != StageFail {
		t.Errorf("stage = %q, want %q", out.Stage, StageFail)
	}
	if out.Reason != "invalid-key-format" {
		t.Errorf("reason = %q, want invalid-key-format", out.Reason)
	}
	if !out.Fatal {
		t.Errorf("Fatal = false, want true (invalid-key-format is run-level fatal)")
	}
}

func TestProcess_RpcError_RecordsFail(t *testing.T) {
	h := newHarness(t)
	h.s3Reader.putGzipped(h.bucket, testBlockKey, []byte("null"))
	h.rpc.byNumber[85149017] = json.RawMessage(canonicalHeader)
	h.rpc.err = errors.New("rpc boom")

	out := h.r.Process(context.Background(), testBlockKey)
	if out.Stage != StageFail {
		t.Errorf("stage = %q, want %q", out.Stage, StageFail)
	}
	if !out.Fatal {
		t.Errorf("Fatal = false, want true (rpc-error is run-level fatal)")
	}
	if h.s3Overwriter.callCount() != 0 || h.publisher.count() != 0 {
		t.Errorf("no writes/publishes on rpc error")
	}
}

func TestProcess_RpcError_PreservesErrorInOutcome(t *testing.T) {
	h := newHarness(t)
	h.s3Reader.putGzipped(h.bucket, testBlockKey, []byte("null"))
	h.rpc.byNumber[85149017] = json.RawMessage(canonicalHeader)
	expectedErr := errors.New("rpc boom")
	h.rpc.err = expectedErr

	out := h.r.Process(context.Background(), testBlockKey)
	if out.Stage != StageFail {
		t.Errorf("stage = %q, want %q", out.Stage, StageFail)
	}
	if out.Reason != "rpc-error" {
		t.Errorf("reason = %q, want rpc-error", out.Reason)
	}
	if !out.Fatal {
		t.Errorf("Fatal = false, want true (rpc-error is run-level fatal)")
	}
	if out.Err == nil {
		t.Fatalf("out.Err is nil, want chain-preserving error")
	}
	if !errors.Is(out.Err, expectedErr) {
		t.Errorf("errors.Is(out.Err, expectedErr) = false, want true (chain not preserved)")
	}
}

func TestProcess_S3WriteError_RecordsFail(t *testing.T) {
	h := newHarness(t)
	h.s3Reader.putGzipped(h.bucket, testBlockKey, []byte("null"))
	h.rpc.byNumber[85149017] = json.RawMessage(canonicalHeader)
	h.rpc.response = outbound.BlockData{Block: json.RawMessage(`{"hash":"0xabc"}`)}
	h.s3Overwriter.err = errors.New("s3 boom")

	out := h.r.Process(context.Background(), testBlockKey)
	if out.Stage != StageFail {
		t.Errorf("stage = %q, want %q", out.Stage, StageFail)
	}
	if !out.Fatal {
		t.Errorf("Fatal = false, want true (s3-write-error is run-level fatal)")
	}
	if h.publisher.count() != 0 {
		t.Errorf("must not publish after failed write")
	}
}

func TestProcess_SNSPublishError_RecordsFail(t *testing.T) {
	h := newHarness(t)
	h.s3Reader.putGzipped(h.bucket, testBlockKey, []byte("null"))
	h.rpc.byNumber[85149017] = json.RawMessage(canonicalHeader)
	h.rpc.response = outbound.BlockData{Block: json.RawMessage(`{"hash":"0xabc"}`)}
	h.publisher.err = errors.New("sns boom")

	out := h.r.Process(context.Background(), testBlockKey)
	if out.Stage != StageFail {
		t.Errorf("stage = %q, want %q", out.Stage, StageFail)
	}
	if !out.Fatal {
		t.Errorf("Fatal = false, want true (sns-error is run-level fatal)")
	}
	if h.s3Overwriter.callCount() != 1 {
		t.Errorf("expected s3 write before sns error")
	}
	stage, _ := h.state.Lookup(testBlockKey)
	if stage != StageFail {
		t.Errorf("expected last state to be fail (after recording s3 then fail), got %q", stage)
	}
	if got := h.s3Overwriter.get(h.bucket, testBlockKey); len(got) == 0 {
		t.Errorf("expected something to be written before sns failure")
	}
}

func TestProcess_S3KeyNotFound_RecordsNoSuchKey(t *testing.T) {
	h := newHarness(t)
	h.s3Reader.putErr(h.bucket, testBlockKey, &types.NoSuchKey{})

	out := h.r.Process(context.Background(), testBlockKey)
	if out.Stage != StageSkip {
		t.Errorf("stage = %q, want %q", out.Stage, StageSkip)
	}
	if out.Reason != "no-such-key" {
		t.Errorf("reason = %q, want no-such-key", out.Reason)
	}
	if h.rpc.dataCallCount() != 0 || h.rpc.byNumberCallCount() != 0 {
		t.Errorf("rpc calls (data=%d byNumber=%d), want 0/0", h.rpc.dataCallCount(), h.rpc.byNumberCallCount())
	}
	if h.s3Overwriter.callCount() != 0 {
		t.Errorf("s3 writes = %d, want 0", h.s3Overwriter.callCount())
	}
	if h.publisher.count() != 0 {
		t.Errorf("publishes = %d, want 0", h.publisher.count())
	}
	stage, reason := h.state.Lookup(testBlockKey)
	if stage != StageSkip {
		t.Errorf("state stage = %q, want %q", stage, StageSkip)
	}
	if reason != "no-such-key" {
		t.Errorf("state reason = %q, want no-such-key", reason)
	}
}

func TestProcess_ContextCancelledDuringS3Read_DoesNotRecordFail(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithCancel(context.Background())
	released := make(chan struct{})
	h.s3Reader.streamHook = func(streamCtx context.Context) (io.ReadCloser, error) {
		cancel()
		<-streamCtx.Done()
		close(released)
		return nil, streamCtx.Err()
	}

	out := h.r.Process(ctx, testBlockKey)
	<-released

	if out.Stage != "" {
		t.Errorf("stage = %q, want \"\" (cancellation must not produce a recorded stage)", out.Stage)
	}
	if out.Reason != "" {
		t.Errorf("reason = %q, want \"\"", out.Reason)
	}
	if out.Fatal {
		t.Errorf("Fatal = true, want false (cancellation is interrupted, not fatal)")
	}
	if stage, _ := h.state.Lookup(testBlockKey); stage != "" {
		t.Errorf("state stage = %q, want empty (no record on cancellation)", stage)
	}
	if h.s3Overwriter.callCount() != 0 || h.publisher.count() != 0 {
		t.Errorf("expected no writes or publishes on cancellation")
	}
}

func TestProcess_RawNullBytesInS3_StillTreatedAsNull(t *testing.T) {
	h := newHarness(t)
	h.s3Reader.putRaw(h.bucket, testBlockKey, []byte("null"))
	h.rpc.byNumber[85149017] = json.RawMessage(canonicalHeader)
	h.rpc.response = outbound.BlockData{Block: json.RawMessage(`{"hash":"0xabc"}`)}

	out := h.r.Process(context.Background(), testBlockKey)
	if out.Stage != StageSNS {
		t.Errorf("stage = %q, want %q (raw 'null' must be treated as null)", out.Stage, StageSNS)
	}
}

// TestProcess_GetBlockByNumberError_IsFatal asserts that an RPC failure during
// the canonical-block lookup is classified as run-level fatal: Outcome.Fatal
// must be true and Err must wrap the cause.
func TestProcess_GetBlockByNumberError_IsFatal(t *testing.T) {
	h := newHarness(t)
	h.s3Reader.putGzipped(h.bucket, testBlockKey, []byte("null"))
	rpcErr := errors.New("rpc unreachable")
	h.rpc.byNumberErr = rpcErr

	out := h.r.Process(context.Background(), testBlockKey)
	if out.Stage != StageFail {
		t.Errorf("stage = %q, want %q", out.Stage, StageFail)
	}
	if out.Reason != "rpc-error" {
		t.Errorf("reason = %q, want rpc-error", out.Reason)
	}
	if !out.Fatal {
		t.Errorf("Fatal = false, want true (rpc-error during canonical lookup is run-level fatal)")
	}
	if !errors.Is(out.Err, rpcErr) {
		t.Errorf("Err does not wrap rpcErr: %v", out.Err)
	}
}

// TestProcess_GetBlockByNumberReturnsNull_IsFatal asserts a still-null
// canonical-block lookup (operator pointed at an out-of-sync node, wrong
// endpoint, or block genuinely missing) aborts the run with rpc-still-null.
func TestProcess_GetBlockByNumberReturnsNull_IsFatal(t *testing.T) {
	h := newHarness(t)
	h.s3Reader.putGzipped(h.bucket, testBlockKey, []byte("null"))
	h.rpc.byNumber[85149017] = json.RawMessage("null")

	out := h.r.Process(context.Background(), testBlockKey)
	if out.Stage != StageFail || out.Reason != "rpc-still-null" {
		t.Errorf("outcome = %+v, want stage=fail reason=rpc-still-null", out)
	}
	if !out.Fatal {
		t.Errorf("Fatal = false, want true (rpc-still-null at canonical lookup is fatal)")
	}
	if !errors.Is(out.Err, errRPCStillNull) {
		t.Errorf("Err chain missing errRPCStillNull, got %v", out.Err)
	}
}

// TestProcess_GetBlockByNumberMalformedJSON_IsFatal asserts that an
// undecodable header response is fatal so the operator notices.
func TestProcess_GetBlockByNumberMalformedJSON_IsFatal(t *testing.T) {
	h := newHarness(t)
	h.s3Reader.putGzipped(h.bucket, testBlockKey, []byte("null"))
	h.rpc.byNumber[85149017] = json.RawMessage(`{"hash":}`)

	out := h.r.Process(context.Background(), testBlockKey)
	if out.Stage != StageFail || out.Reason != "block-decode-error" {
		t.Errorf("outcome = %+v, want stage=fail reason=block-decode-error", out)
	}
	if !out.Fatal {
		t.Errorf("Fatal = false, want true (block-decode-error is run-level fatal)")
	}
}

// TestProcess_GetBlockByNumberEmptyHash_IsFatal asserts a decode-success with
// empty hash is treated as malformed and is fatal: we cannot proceed without a
// canonical hash to drive GetBlockDataByHash.
func TestProcess_GetBlockByNumberEmptyHash_IsFatal(t *testing.T) {
	h := newHarness(t)
	h.s3Reader.putGzipped(h.bucket, testBlockKey, []byte("null"))
	h.rpc.byNumber[85149017] = json.RawMessage(`{"hash":"","parentHash":"0xdef","timestamp":"0x3e7"}`)

	out := h.r.Process(context.Background(), testBlockKey)
	if out.Stage != StageFail || out.Reason != "block-decode-error" {
		t.Errorf("outcome = %+v, want stage=fail reason=block-decode-error", out)
	}
	if !out.Fatal {
		t.Errorf("Fatal = false, want true")
	}
}

// TestProcess_GetBlockByNumberBadTimestamp_IsFatal asserts an unparseable
// timestamp (non-hex) is fatal.
func TestProcess_GetBlockByNumberBadTimestamp_IsFatal(t *testing.T) {
	h := newHarness(t)
	h.s3Reader.putGzipped(h.bucket, testBlockKey, []byte("null"))
	h.rpc.byNumber[85149017] = json.RawMessage(`{"hash":"0xabc","parentHash":"0xdef","timestamp":"not-hex"}`)

	out := h.r.Process(context.Background(), testBlockKey)
	if out.Stage != StageFail || out.Reason != "block-timestamp-decode-error" {
		t.Errorf("outcome = %+v, want stage=fail reason=block-timestamp-decode-error", out)
	}
	if !out.Fatal {
		t.Errorf("Fatal = false, want true")
	}
}

// TestProcess_RpcError_IsFatal asserts that an RPC failure (during
// GetBlockDataByHash) is fatal.
func TestProcess_RpcError_IsFatal(t *testing.T) {
	h := newHarness(t)
	h.s3Reader.putGzipped(h.bucket, testBlockKey, []byte("null"))
	h.rpc.byNumber[85149017] = json.RawMessage(canonicalHeader)
	rpcErr := errors.New("rpc boom")
	h.rpc.err = rpcErr

	out := h.r.Process(context.Background(), testBlockKey)
	if out.Stage != StageFail {
		t.Errorf("stage = %q, want %q", out.Stage, StageFail)
	}
	if !out.Fatal {
		t.Errorf("Fatal = false, want true (rpc-error is run-level fatal)")
	}
	if !errors.Is(out.Err, rpcErr) {
		t.Errorf("Err does not wrap rpcErr: %v", out.Err)
	}
}

// TestProcess_S3WriteError_IsFatal asserts S3 write failures are fatal.
func TestProcess_S3WriteError_IsFatal(t *testing.T) {
	h := newHarness(t)
	h.s3Reader.putGzipped(h.bucket, testBlockKey, []byte("null"))
	h.rpc.byNumber[85149017] = json.RawMessage(canonicalHeader)
	h.rpc.response = outbound.BlockData{Block: json.RawMessage(`{"hash":"0xabc"}`)}
	s3Err := errors.New("s3 boom")
	h.s3Overwriter.err = s3Err

	out := h.r.Process(context.Background(), testBlockKey)
	if out.Stage != StageFail {
		t.Errorf("stage = %q, want %q", out.Stage, StageFail)
	}
	if out.Reason != "s3-write-error" {
		t.Errorf("reason = %q, want s3-write-error", out.Reason)
	}
	if !out.Fatal {
		t.Errorf("Fatal = false, want true (s3-write-error is run-level fatal)")
	}
	if !errors.Is(out.Err, s3Err) {
		t.Errorf("Err does not wrap s3Err: %v", out.Err)
	}
}

// TestProcess_SNSError_IsFatal asserts SNS publish failures are fatal.
func TestProcess_SNSError_IsFatal(t *testing.T) {
	h := newHarness(t)
	h.s3Reader.putGzipped(h.bucket, testBlockKey, []byte("null"))
	h.rpc.byNumber[85149017] = json.RawMessage(canonicalHeader)
	h.rpc.response = outbound.BlockData{Block: json.RawMessage(`{"hash":"0xabc"}`)}
	snsErr := errors.New("sns boom")
	h.publisher.err = snsErr

	out := h.r.Process(context.Background(), testBlockKey)
	if out.Stage != StageFail || out.Reason != "sns-error" {
		t.Errorf("outcome = %+v, want stage=fail reason=sns-error", out)
	}
	if !out.Fatal {
		t.Errorf("Fatal = false, want true (sns-error is run-level fatal)")
	}
	if !errors.Is(out.Err, snsErr) {
		t.Errorf("Err does not wrap snsErr: %v", out.Err)
	}
}

// TestProcess_InvalidKeyFormat_IsFatal asserts malformed keys abort the run.
func TestProcess_InvalidKeyFormat_IsFatal(t *testing.T) {
	h := newHarness(t)

	out := h.r.Process(context.Background(), "not-a-valid-key")
	if out.Stage != StageFail || out.Reason != "invalid-key-format" {
		t.Errorf("outcome = %+v, want stage=fail reason=invalid-key-format", out)
	}
	if !out.Fatal {
		t.Errorf("Fatal = false, want true (invalid-key-format is run-level fatal)")
	}
}

// TestProcess_S3VerifyError_IsFatal asserts a non-NoSuchKey S3 verify error
// is fatal (e.g. permissions or network outage).
func TestProcess_S3VerifyError_IsFatal(t *testing.T) {
	h := newHarness(t)
	s3Err := errors.New("permission denied")
	h.s3Reader.putErr(h.bucket, testBlockKey, s3Err)

	out := h.r.Process(context.Background(), testBlockKey)
	if out.Stage != StageFail || out.Reason != "s3-verify-error" {
		t.Errorf("outcome = %+v, want stage=fail reason=s3-verify-error", out)
	}
	if !out.Fatal {
		t.Errorf("Fatal = false, want true (s3-verify-error is run-level fatal)")
	}
	if !errors.Is(out.Err, s3Err) {
		t.Errorf("Err does not wrap s3Err: %v", out.Err)
	}
}

// TestProcess_RpcStillNull_IsFatal asserts a still-null GetBlockDataByHash
// response aborts the run.
func TestProcess_RpcStillNull_IsFatal(t *testing.T) {
	h := newHarness(t)
	h.s3Reader.putGzipped(h.bucket, testBlockKey, []byte("null"))
	h.rpc.byNumber[85149017] = json.RawMessage(canonicalHeader)
	h.rpc.response = outbound.BlockData{Block: json.RawMessage("null")}

	out := h.r.Process(context.Background(), testBlockKey)
	if out.Stage != StageFail || out.Reason != "rpc-still-null" {
		t.Errorf("outcome = %+v, want stage=fail reason=rpc-still-null", out)
	}
	if !out.Fatal {
		t.Errorf("Fatal = false, want true (rpc-still-null is unexpected, run must abort)")
	}
	if !errors.Is(out.Err, errRPCStillNull) {
		t.Errorf("Err chain missing errRPCStillNull, got %v", out.Err)
	}
}

// TestProcess_ContextCancelled_IsNotFatal: a SIGINT mid-S3-call must NOT be
// classified as fatal. The empty Outcome signals "interrupted, will retry".
func TestProcess_ContextCancelled_IsNotFatal(t *testing.T) {
	h := newHarness(t)
	ctx, cancel := context.WithCancel(context.Background())
	released := make(chan struct{})
	h.s3Reader.streamHook = func(streamCtx context.Context) (io.ReadCloser, error) {
		cancel()
		<-streamCtx.Done()
		close(released)
		return nil, streamCtx.Err()
	}

	out := h.r.Process(ctx, testBlockKey)
	<-released

	if out.Stage != "" {
		t.Errorf("stage = %q, want \"\" (cancellation must not produce a recorded stage)", out.Stage)
	}
	if out.Fatal {
		t.Errorf("Fatal = true, want false (cancellation is not fatal)")
	}
}
