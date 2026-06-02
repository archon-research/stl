package rawdatabackup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/pkg/rpcutil"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// =============================================================================
// Mock Implementations
// =============================================================================

// mockSQSConsumer is a mock implementation of outbound.SQSConsumer.
type mockSQSConsumer struct {
	mu              sync.Mutex
	messages        []outbound.SQSMessage
	deletedHandles  []string
	receiveErr      error
	deleteErr       error
	receiveCount    int
	receiveCalled   atomic.Int32
	deleteCalled    atomic.Int32
	receiveDelay    time.Duration
	closed          bool
	receiveCallback func(ctx context.Context, maxMessages int) ([]outbound.SQSMessage, error)
}

func newMockSQSConsumer() *mockSQSConsumer {
	return &mockSQSConsumer{
		messages:       []outbound.SQSMessage{},
		deletedHandles: []string{},
	}
}

func (m *mockSQSConsumer) AddMessage(msg outbound.SQSMessage) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages = append(m.messages, msg)
}

func (m *mockSQSConsumer) ReceiveMessages(ctx context.Context, maxMessages int) ([]outbound.SQSMessage, error) {
	m.receiveCalled.Add(1)

	if m.receiveCallback != nil {
		return m.receiveCallback(ctx, maxMessages)
	}

	if m.receiveDelay > 0 {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(m.receiveDelay):
		}
	}

	if m.receiveErr != nil {
		return nil, m.receiveErr
	}

	m.mu.Lock()
	defer m.mu.Unlock()

	m.receiveCount++

	if len(m.messages) == 0 {
		return []outbound.SQSMessage{}, nil
	}

	// Return up to maxMessages
	count := min(maxMessages, len(m.messages))

	result := m.messages[:count]
	m.messages = m.messages[count:]
	return result, nil
}

func (m *mockSQSConsumer) DeleteMessage(ctx context.Context, receiptHandle string) error {
	m.deleteCalled.Add(1)

	if m.deleteErr != nil {
		return m.deleteErr
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.deletedHandles = append(m.deletedHandles, receiptHandle)
	return nil
}

func (m *mockSQSConsumer) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

func (m *mockSQSConsumer) GetDeletedHandles() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]string, len(m.deletedHandles))
	copy(result, m.deletedHandles)
	return result
}

// mockBlockCache is a mock implementation of outbound.BlockCache.
type mockBlockCache struct {
	mu        sync.RWMutex
	blocks    map[string]json.RawMessage
	receipts  map[string]json.RawMessage
	traces    map[string]json.RawMessage
	blobs     map[string]json.RawMessage
	getErrors map[string]error
	closed    bool
}

func newMockBlockCache() *mockBlockCache {
	return &mockBlockCache{
		blocks:    make(map[string]json.RawMessage),
		receipts:  make(map[string]json.RawMessage),
		traces:    make(map[string]json.RawMessage),
		blobs:     make(map[string]json.RawMessage),
		getErrors: make(map[string]error),
	}
}

func (m *mockBlockCache) key(chainID, blockNumber int64, version int) string {
	return fmt.Sprintf("%d:%d:%d", chainID, blockNumber, version)
}

func (m *mockBlockCache) SetBlock(ctx context.Context, chainID, blockNumber int64, version int, data json.RawMessage) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.blocks[m.key(chainID, blockNumber, version)] = data
	return nil
}

func (m *mockBlockCache) SetReceipts(ctx context.Context, chainID, blockNumber int64, version int, data json.RawMessage) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.receipts[m.key(chainID, blockNumber, version)] = data
	return nil
}

func (m *mockBlockCache) SetTraces(ctx context.Context, chainID, blockNumber int64, version int, data json.RawMessage) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.traces[m.key(chainID, blockNumber, version)] = data
	return nil
}

func (m *mockBlockCache) SetBlobs(ctx context.Context, chainID, blockNumber int64, version int, data json.RawMessage) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.blobs[m.key(chainID, blockNumber, version)] = data
	return nil
}

func (m *mockBlockCache) SetBlockData(ctx context.Context, chainID, blockNumber int64, version int, data outbound.BlockDataInput) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := m.key(chainID, blockNumber, version)
	m.blocks[key] = data.Block
	m.receipts[key] = data.Receipts
	m.traces[key] = data.Traces
	if data.Blobs != nil {
		m.blobs[key] = data.Blobs
	}
	return nil
}

func (m *mockBlockCache) GetBlock(ctx context.Context, chainID, blockNumber int64, version int) (json.RawMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	key := m.key(chainID, blockNumber, version)
	if err, ok := m.getErrors["block:"+key]; ok {
		return nil, err
	}
	return m.blocks[key], nil
}

func (m *mockBlockCache) GetReceipts(ctx context.Context, chainID, blockNumber int64, version int) (json.RawMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	key := m.key(chainID, blockNumber, version)
	if err, ok := m.getErrors["receipts:"+key]; ok {
		return nil, err
	}
	return m.receipts[key], nil
}

func (m *mockBlockCache) GetTraces(ctx context.Context, chainID, blockNumber int64, version int) (json.RawMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	key := m.key(chainID, blockNumber, version)
	if err, ok := m.getErrors["traces:"+key]; ok {
		return nil, err
	}
	return m.traces[key], nil
}

func (m *mockBlockCache) GetBlobs(ctx context.Context, chainID, blockNumber int64, version int) (json.RawMessage, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	key := m.key(chainID, blockNumber, version)
	if err, ok := m.getErrors["blobs:"+key]; ok {
		return nil, err
	}
	return m.blobs[key], nil
}

func (m *mockBlockCache) DeleteBlock(ctx context.Context, chainID, blockNumber int64, version int) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := m.key(chainID, blockNumber, version)
	delete(m.blocks, key)
	delete(m.receipts, key)
	delete(m.traces, key)
	delete(m.blobs, key)
	return nil
}

func (m *mockBlockCache) Close() error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.closed = true
	return nil
}

func (m *mockBlockCache) SetGetError(dataType string, chainID, blockNumber int64, version int, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	key := fmt.Sprintf("%s:%d:%d:%d", dataType, chainID, blockNumber, version)
	m.getErrors[key] = err
}

// mockS3Writer is a mock implementation of outbound.S3Writer.
type mockS3Writer struct {
	mu           sync.Mutex
	files        map[string][]byte
	existsErrors map[string]error
	writeErrors  map[string]error
	writeCalled  atomic.Int32
	existsCalled atomic.Int32
}

var _ outbound.S3Writer = (*mockS3Writer)(nil)

func newMockS3Writer() *mockS3Writer {
	return &mockS3Writer{
		files:        make(map[string][]byte),
		existsErrors: make(map[string]error),
		writeErrors:  make(map[string]error),
	}
}

func (m *mockS3Writer) WriteFileIfNotExists(ctx context.Context, bucket, key string, content io.Reader, compressGzip bool) (bool, error) {
	m.writeCalled.Add(1)

	fullKey := bucket + "/" + key

	m.mu.Lock()
	defer m.mu.Unlock()

	if err, ok := m.writeErrors[fullKey]; ok {
		return false, err
	}
	if err, ok := m.writeErrors[key]; ok {
		return false, err
	}

	if _, exists := m.files[fullKey]; exists {
		return false, nil
	}

	data, err := io.ReadAll(content)
	if err != nil {
		return false, err
	}

	m.files[fullKey] = data
	return true, nil
}

func (m *mockS3Writer) FileExists(ctx context.Context, bucket, key string) (bool, error) {
	m.existsCalled.Add(1)

	fullKey := bucket + "/" + key

	m.mu.Lock()
	defer m.mu.Unlock()

	if err, ok := m.existsErrors[fullKey]; ok {
		return false, err
	}
	if err, ok := m.existsErrors[key]; ok {
		return false, err
	}

	_, exists := m.files[fullKey]
	return exists, nil
}

func (m *mockS3Writer) GetFile(bucket, key string) ([]byte, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.files[bucket+"/"+key]
	return data, ok
}

func (m *mockS3Writer) GetAllKeys() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	keys := make([]string, 0, len(m.files))
	for k := range m.files {
		keys = append(keys, k)
	}
	return keys
}

func (m *mockS3Writer) SetWriteError(key string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.writeErrors[key] = err
}

func (m *mockS3Writer) SetExistsError(key string, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.existsErrors[key] = err
}

func (m *mockS3Writer) PresetFileExists(bucket, key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.files[bucket+"/"+key] = []byte{}
}

// mockDeadLetterPublisher is a mock implementation of outbound.DeadLetterPublisher.
type mockDeadLetterPublisher struct {
	mu         sync.Mutex
	published  []dlqPublish
	publishErr error
	calls      atomic.Int32
}

type dlqPublish struct {
	body    string
	groupID string
}

var _ outbound.DeadLetterPublisher = (*mockDeadLetterPublisher)(nil)

func newMockDeadLetterPublisher() *mockDeadLetterPublisher {
	return &mockDeadLetterPublisher{}
}

func (m *mockDeadLetterPublisher) Publish(ctx context.Context, body string, groupID string) error {
	m.calls.Add(1)
	if m.publishErr != nil {
		return m.publishErr
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.published = append(m.published, dlqPublish{body: body, groupID: groupID})
	return nil
}

func (m *mockDeadLetterPublisher) Published() []dlqPublish {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]dlqPublish, len(m.published))
	copy(result, m.published)
	return result
}

// mockBlockchainClient is a configurable stub of outbound.BlockchainClient used
// to drive the RPC-fallback path. Only GetBlockDataByHash is exercised by the
// backup service; the remaining methods are unimplemented and must not be called.
type mockBlockchainClient struct {
	mu         sync.Mutex
	calls      atomic.Int32
	data       outbound.BlockData // returned when callErr is nil
	callErr    error              // top-level (network/batch) error, takes precedence
	lastHash   string
	lastNumber int64
}

func newMockBlockchainClient() *mockBlockchainClient {
	return &mockBlockchainClient{}
}

func (m *mockBlockchainClient) GetBlockDataByHash(ctx context.Context, blockNum int64, hash string, fullTx bool) (outbound.BlockData, error) {
	m.calls.Add(1)
	m.mu.Lock()
	m.lastHash = hash
	m.lastNumber = blockNum
	m.mu.Unlock()
	if m.callErr != nil {
		return outbound.BlockData{}, m.callErr
	}
	return m.data, nil
}

func (m *mockBlockchainClient) Calls() int32 {
	return m.calls.Load()
}

func (m *mockBlockchainClient) GetBlockByNumber(ctx context.Context, blockNum int64, fullTx bool) (json.RawMessage, error) {
	panic("GetBlockByNumber must not be called by the backup service")
}

func (m *mockBlockchainClient) GetBlockByHash(ctx context.Context, hash string, fullTx bool) (*outbound.BlockHeader, error) {
	panic("GetBlockByHash must not be called by the backup service")
}

func (m *mockBlockchainClient) GetFullBlockByHash(ctx context.Context, hash string, fullTx bool) (json.RawMessage, error) {
	panic("GetFullBlockByHash must not be called by the backup service")
}

func (m *mockBlockchainClient) GetBlockReceipts(ctx context.Context, blockNum int64) (json.RawMessage, error) {
	panic("GetBlockReceipts must not be called by the backup service")
}

func (m *mockBlockchainClient) GetBlockReceiptsByHash(ctx context.Context, hash string) (json.RawMessage, error) {
	panic("GetBlockReceiptsByHash must not be called by the backup service")
}

func (m *mockBlockchainClient) GetBlockTraces(ctx context.Context, blockNum int64) (json.RawMessage, error) {
	panic("GetBlockTraces must not be called by the backup service")
}

func (m *mockBlockchainClient) GetBlockTracesByHash(ctx context.Context, hash string) (json.RawMessage, error) {
	panic("GetBlockTracesByHash must not be called by the backup service")
}

func (m *mockBlockchainClient) GetBlobSidecars(ctx context.Context, blockNum int64) (json.RawMessage, error) {
	panic("GetBlobSidecars must not be called by the backup service")
}

func (m *mockBlockchainClient) GetBlobSidecarsByHash(ctx context.Context, hash string) (json.RawMessage, error) {
	panic("GetBlobSidecarsByHash must not be called by the backup service")
}

func (m *mockBlockchainClient) GetCurrentBlockNumber(ctx context.Context) (int64, error) {
	panic("GetCurrentBlockNumber must not be called by the backup service")
}

func (m *mockBlockchainClient) GetBlocksBatch(ctx context.Context, blockNums []int64, fullTx bool) ([]outbound.BlockData, error) {
	panic("GetBlocksBatch must not be called by the backup service")
}

var _ outbound.BlockchainClient = (*mockBlockchainClient)(nil)

// =============================================================================
// Helper Functions
// =============================================================================

// newTestServiceWithClient builds a Service with mocks for the public-API driven
// tests, with an explicit blockchain client so tests can drive the RPC-fallback
// path. It wires a discard-logger and a no-op dead-letter publisher unless the
// caller supplies its own via the Config/arguments.
func newTestServiceWithClient(t *testing.T, config Config, consumer outbound.SQSConsumer, cache outbound.BlockCache, writer outbound.S3Writer, deadLetter outbound.DeadLetterPublisher, client outbound.BlockchainClient) *Service {
	t.Helper()
	if config.Logger == nil {
		config.Logger = testutil.DiscardLogger()
	}
	if deadLetter == nil {
		deadLetter = newMockDeadLetterPublisher()
	}
	svc, err := NewService(config, consumer, cache, writer, deadLetter, client)
	if err != nil {
		t.Fatalf("failed to create service: %v", err)
	}
	return svc
}

// blockOnlyExpectations returns chain expectations where only block data is required.
// Use this for unit tests that only set block data in cache.
func blockOnlyExpectations() map[int64]ChainExpectation {
	return map[int64]ChainExpectation{
		1:     {ExpectReceipts: false, ExpectTraces: false, ExpectBlobs: false},
		43114: {ExpectReceipts: false, ExpectTraces: false, ExpectBlobs: false},
	}
}

func createBlockEvent(chainID, blockNumber int64, version int) outbound.BlockEvent {
	return outbound.BlockEvent{
		ChainID:        chainID,
		BlockNumber:    blockNumber,
		Version:        version,
		BlockHash:      fmt.Sprintf("0x%064x", blockNumber),
		ParentHash:     fmt.Sprintf("0x%064x", blockNumber-1),
		BlockTimestamp: time.Now().Unix(),
		ReceivedAt:     time.Now(),
	}
}

func createSQSMessage(id string, event outbound.BlockEvent) outbound.SQSMessage {
	body, _ := json.Marshal(event)
	return outbound.SQSMessage{
		MessageID:     id,
		ReceiptHandle: "receipt-" + id,
		Body:          string(body),
	}
}

// =============================================================================
// Tests: NewService
// =============================================================================

func TestNewService_Success(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	svc, err := NewService(Config{
		ChainID: 1,
		Bucket:  "test-bucket",
		Workers: 2,
		Logger:  testutil.DiscardLogger(),
	}, consumer, cache, writer, newMockDeadLetterPublisher(), newMockBlockchainClient())

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if svc == nil {
		t.Fatal("expected service to be created")
	}
}

func TestNewService_NilConsumer(t *testing.T) {
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	_, err := NewService(Config{
		Bucket: "test-bucket",
	}, nil, cache, writer, newMockDeadLetterPublisher(), newMockBlockchainClient())

	if err == nil {
		t.Fatal("expected error for nil consumer")
	}
	if !strings.Contains(err.Error(), "consumer is required") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestNewService_NilCache(t *testing.T) {
	consumer := newMockSQSConsumer()
	writer := newMockS3Writer()

	_, err := NewService(Config{
		Bucket: "test-bucket",
	}, consumer, nil, writer, newMockDeadLetterPublisher(), newMockBlockchainClient())

	if err == nil {
		t.Fatal("expected error for nil cache")
	}
	if !strings.Contains(err.Error(), "cache is required") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestNewService_NilWriter(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()

	_, err := NewService(Config{
		Bucket: "test-bucket",
	}, consumer, cache, nil, newMockDeadLetterPublisher(), newMockBlockchainClient())

	if err == nil {
		t.Fatal("expected error for nil writer")
	}
	if !strings.Contains(err.Error(), "writer is required") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestNewService_NilDeadLetter(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	_, err := NewService(Config{
		Bucket: "test-bucket",
	}, consumer, cache, writer, nil, newMockBlockchainClient())

	if err == nil {
		t.Fatal("expected error for nil dead-letter publisher")
	}
	if !strings.Contains(err.Error(), "dead-letter publisher is required") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestNewService_NilClient(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	_, err := NewService(Config{
		Bucket: "test-bucket",
	}, consumer, cache, writer, newMockDeadLetterPublisher(), nil)

	if err == nil {
		t.Fatal("expected error for nil blockchain client")
	}
	if !strings.Contains(err.Error(), "blockchain client is required") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestNewService_EmptyBucket(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	_, err := NewService(Config{
		Bucket: "",
	}, consumer, cache, writer, newMockDeadLetterPublisher(), newMockBlockchainClient())

	if err == nil {
		t.Fatal("expected error for empty bucket")
	}
	if !strings.Contains(err.Error(), "bucket is required") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestNewService_DefaultsApplied(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	svc, err := NewService(Config{
		Bucket:  "test-bucket",
		Workers: 0, // Should use default
	}, consumer, cache, writer, newMockDeadLetterPublisher(), newMockBlockchainClient())

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	defaults := ConfigDefaults()
	if svc.config.Workers != defaults.Workers {
		t.Errorf("expected workers=%d, got %d", defaults.Workers, svc.config.Workers)
	}
	if svc.config.BatchSize != defaults.BatchSize {
		t.Errorf("expected batchSize=%d, got %d", defaults.BatchSize, svc.config.BatchSize)
	}
}

// =============================================================================
// Tests: processMessage
// =============================================================================

func TestProcessMessage_Success(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	// Use default Ethereum expectations: receipts + traces expected, blobs not expected
	svc, _ := NewService(Config{
		ChainID: 1,
		Bucket:  "test-bucket",
		Logger:  testutil.DiscardLogger(),
	}, consumer, cache, writer, newMockDeadLetterPublisher(), newMockBlockchainClient())

	// Set up cache with block data
	event := createBlockEvent(1, 100, 0)
	blockData := json.RawMessage(`{"number": 100}`)
	receiptsData := json.RawMessage(`[{"transactionHash": "0x123"}]`)
	tracesData := json.RawMessage(`[{"type": "call"}]`)

	ctx := context.Background()
	_ = cache.SetBlock(ctx, 1, 100, 0, blockData)
	_ = cache.SetReceipts(ctx, 1, 100, 0, receiptsData)
	_ = cache.SetTraces(ctx, 1, 100, 0, tracesData)

	msg := createSQSMessage("msg1", event)
	_, err := svc.processMessage(ctx, msg)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify expected files were written (block + receipts + traces, no blobs for Ethereum)
	keys := writer.GetAllKeys()
	if len(keys) != 3 {
		t.Errorf("expected 3 files, got %d: %v", len(keys), keys)
	}

	// Check specific keys
	expectedKeys := []string{
		"test-bucket/0-999/100_0_block.json.gz",
		"test-bucket/0-999/100_0_receipts.json.gz",
		"test-bucket/0-999/100_0_traces.json.gz",
	}

	for _, expectedKey := range expectedKeys {
		found := slices.Contains(keys, expectedKey)
		if !found {
			t.Errorf("expected key %s not found in %v", expectedKey, keys)
		}
	}
}

func TestProcessMessage_AllDataTypesWithExplicitExpectations(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	// Explicitly expect all data types including blobs
	svc, _ := NewService(Config{
		ChainID: 1,
		Bucket:  "test-bucket",
		ChainExpectations: map[int64]ChainExpectation{
			1: {ExpectReceipts: true, ExpectTraces: true, ExpectBlobs: true},
		},
		Logger: testutil.DiscardLogger(),
	}, consumer, cache, writer, newMockDeadLetterPublisher(), newMockBlockchainClient())

	event := createBlockEvent(1, 100, 0)
	ctx := context.Background()
	_ = cache.SetBlock(ctx, 1, 100, 0, json.RawMessage(`{"number": 100}`))
	_ = cache.SetReceipts(ctx, 1, 100, 0, json.RawMessage(`[{"transactionHash": "0x123"}]`))
	_ = cache.SetTraces(ctx, 1, 100, 0, json.RawMessage(`[{"type": "call"}]`))
	_ = cache.SetBlobs(ctx, 1, 100, 0, json.RawMessage(`[{"commitment": "0xabc"}]`))

	msg := createSQSMessage("msg1", event)
	_, err := svc.processMessage(ctx, msg)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	keys := writer.GetAllKeys()
	if len(keys) != 4 {
		t.Errorf("expected 4 files, got %d: %v", len(keys), keys)
	}
}

func TestProcessMessage_BlockOnlyNoOptionalData(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	svc, _ := NewService(Config{
		ChainID:           1,
		Bucket:            "test-bucket",
		ChainExpectations: blockOnlyExpectations(),
		Logger:            testutil.DiscardLogger(),
	}, consumer, cache, writer, newMockDeadLetterPublisher(), newMockBlockchainClient())

	// Set up cache with ONLY block data (no receipts, traces, blobs)
	event := createBlockEvent(1, 100, 0)
	blockData := json.RawMessage(`{"number": 100}`)

	ctx := context.Background()
	_ = cache.SetBlock(ctx, 1, 100, 0, blockData)

	msg := createSQSMessage("msg1", event)
	_, err := svc.processMessage(ctx, msg)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify only block file was written
	keys := writer.GetAllKeys()
	if len(keys) != 1 {
		t.Errorf("expected 1 file, got %d: %v", len(keys), keys)
	}
}

// TestProcessMessage_BlockNotInCache_RPCFallbackBacksUp verifies that a cache
// miss is no longer terminal: the worker falls back to an RPC fetch by hash and,
// on full data, backs the block up. Default Ethereum expectations require block +
// receipts + traces, so the stub must return all three.
func TestProcessMessage_BlockNotInCache_RPCFallbackBacksUp(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	client := newMockBlockchainClient()
	client.data = outbound.BlockData{
		BlockNumber: 100,
		Block:       json.RawMessage(`{"number":"0x64","hash":"0xrpc"}`),
		Receipts:    json.RawMessage(`[{"transactionHash":"0xdead"}]`),
		Traces:      json.RawMessage(`[{"type":"call"}]`),
	}

	svc := newTestServiceWithClient(t, Config{
		ChainID:             1,
		Bucket:              "test-bucket",
		CacheMissMaxRetries: 0,
	}, consumer, cache, writer, newMockDeadLetterPublisher(), client)

	// Nothing in cache and nothing in S3 -> confirmed miss -> RPC fallback.
	event := createBlockEvent(1, 100, 0)
	msg := createSQSMessage("msg1", event)

	status, err := svc.processMessage(context.Background(), msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if status != statusRPCFallback {
		t.Errorf("expected status %q, got %q", statusRPCFallback, status)
	}
	if calls := client.Calls(); calls != 1 {
		t.Errorf("expected exactly 1 RPC call, got %d", calls)
	}
	// Block fetch must be by the event's block hash (TOCTOU-safe).
	if client.lastHash != event.BlockHash {
		t.Errorf("expected RPC fetch by hash %q, got %q", event.BlockHash, client.lastHash)
	}

	// Block + receipts + traces should have been written from RPC data.
	keys := writer.GetAllKeys()
	if len(keys) != 3 {
		t.Errorf("expected 3 files written from RPC, got %d: %v", len(keys), keys)
	}
}

// TestProcessMessage_CacheMiss_RPCNullBlock_Permanent verifies that a cache miss
// whose RPC re-fetch returns a null/empty block is permanent (the block was
// reorged away or never existed and cannot be recovered by redelivery).
func TestProcessMessage_CacheMiss_RPCNullBlock_Permanent(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	client := newMockBlockchainClient()
	client.data = outbound.BlockData{BlockNumber: 100, Block: json.RawMessage("null")}

	svc := newTestServiceWithClient(t, Config{
		ChainID:             1,
		Bucket:              "test-bucket",
		CacheMissMaxRetries: 0,
		ChainExpectations:   blockOnlyExpectations(),
	}, consumer, cache, writer, newMockDeadLetterPublisher(), client)

	event := createBlockEvent(1, 100, 0)
	msg := createSQSMessage("msg1", event)

	_, err := svc.processMessage(context.Background(), msg)
	if err == nil {
		t.Fatal("expected error for null RPC block")
	}
	if !strings.Contains(err.Error(), "RPC returned null/empty block") {
		t.Errorf("unexpected error message: %v", err)
	}
	if !errors.Is(err, ErrPermanent) {
		t.Errorf("expected null RPC block to be permanent, got: %v", err)
	}
}

// TestProcessMessage_CacheMiss_RPCUpstreamNull_Permanent verifies the REAL adapter
// signal for a null-by-hash result (ErrUpstreamNullResult, surfaced as a per-type
// error) is permanent for an aged-out block: routed to the DLQ rather than retried,
// since the block has been reorged out or is absent.
func TestProcessMessage_CacheMiss_RPCUpstreamNull_Permanent(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	client := newMockBlockchainClient()
	client.data = outbound.BlockData{
		BlockNumber: 100,
		BlockErr:    fmt.Errorf("eth_getBlockByHash 0xabc: %w", rpcutil.ErrUpstreamNullResult),
	}

	svc := newTestServiceWithClient(t, Config{
		ChainID:             1,
		Bucket:              "test-bucket",
		CacheMissMaxRetries: 0,
		ChainExpectations:   blockOnlyExpectations(),
	}, consumer, cache, writer, newMockDeadLetterPublisher(), client)

	event := createBlockEvent(1, 100, 0)
	msg := createSQSMessage("msg1", event)

	_, err := svc.processMessage(context.Background(), msg)
	if err == nil {
		t.Fatal("expected error for upstream-null RPC block")
	}
	if !strings.Contains(err.Error(), "RPC returned null block") {
		t.Errorf("unexpected error message: %v", err)
	}
	if !errors.Is(err, ErrPermanent) {
		t.Errorf("expected upstream-null RPC block to be permanent, got: %v", err)
	}
}

// TestProcessMessage_CacheMiss_EmptyBlockHash_Permanent verifies that a cache miss
// for an event with no block hash is permanent without any RPC call: the fallback
// re-fetches by hash, so a hash-less event can never be recovered.
func TestProcessMessage_CacheMiss_EmptyBlockHash_Permanent(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	client := newMockBlockchainClient()

	svc := newTestServiceWithClient(t, Config{
		ChainID:             1,
		Bucket:              "test-bucket",
		CacheMissMaxRetries: 0,
		ChainExpectations:   blockOnlyExpectations(),
	}, consumer, cache, writer, newMockDeadLetterPublisher(), client)

	event := createBlockEvent(1, 100, 0)
	event.BlockHash = ""
	msg := createSQSMessage("msg1", event)

	_, err := svc.processMessage(context.Background(), msg)
	if err == nil {
		t.Fatal("expected error for event with no block hash")
	}
	if !errors.Is(err, ErrPermanent) {
		t.Errorf("expected missing block hash to be permanent, got: %v", err)
	}
	if client.Calls() != 0 {
		t.Errorf("expected no RPC call for a hash-less event, got %d", client.Calls())
	}
}

// TestProcessMessage_CacheMiss_RPCCallError_Transient verifies that a top-level
// RPC error (network/batch failure) is transient: not permanent, so SQS can
// redeliver once the upstream recovers.
func TestProcessMessage_CacheMiss_RPCCallError_Transient(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	client := newMockBlockchainClient()
	client.callErr = errors.New("HTTP 429: rate limited")

	svc := newTestServiceWithClient(t, Config{
		ChainID:             1,
		Bucket:              "test-bucket",
		CacheMissMaxRetries: 0,
		ChainExpectations:   blockOnlyExpectations(),
	}, consumer, cache, writer, newMockDeadLetterPublisher(), client)

	event := createBlockEvent(1, 100, 0)
	msg := createSQSMessage("msg1", event)

	_, err := svc.processMessage(context.Background(), msg)
	if err == nil {
		t.Fatal("expected error for RPC call failure")
	}
	if !strings.Contains(err.Error(), "RPC fetch by hash failed") {
		t.Errorf("unexpected error message: %v", err)
	}
	if errors.Is(err, ErrPermanent) {
		t.Error("a top-level RPC error must be transient, not ErrPermanent")
	}
}

// TestProcessMessage_CacheMiss_RPCPerTypeError_Transient verifies that a per-type
// RPC error (e.g. receipts fetch failed) is transient.
func TestProcessMessage_CacheMiss_RPCPerTypeError_Transient(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	client := newMockBlockchainClient()
	client.data = outbound.BlockData{
		BlockNumber: 100,
		Block:       json.RawMessage(`{"number":"0x64"}`),
		ReceiptsErr: errors.New("receipts upstream timeout"),
	}

	svc := newTestServiceWithClient(t, Config{
		ChainID:             1,
		Bucket:              "test-bucket",
		CacheMissMaxRetries: 0,
		ChainExpectations: map[int64]ChainExpectation{
			1: {ExpectReceipts: true, ExpectTraces: false, ExpectBlobs: false},
		},
	}, consumer, cache, writer, newMockDeadLetterPublisher(), client)

	event := createBlockEvent(1, 100, 0)
	msg := createSQSMessage("msg1", event)

	_, err := svc.processMessage(context.Background(), msg)
	if err == nil {
		t.Fatal("expected error for per-type RPC failure")
	}
	if !strings.Contains(err.Error(), "RPC receipts fetch failed") {
		t.Errorf("unexpected error message: %v", err)
	}
	if errors.Is(err, ErrPermanent) {
		t.Error("a per-type RPC error must be transient, not ErrPermanent")
	}
}

// TestProcessMessage_CacheMiss_RPCAllTypes_BacksUp verifies the RPC fallback
// validates and writes every expected type when all are expected (block,
// receipts, traces, blobs), exercising the traces and blobs assign branches.
func TestProcessMessage_CacheMiss_RPCAllTypes_BacksUp(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	client := newMockBlockchainClient()
	client.data = outbound.BlockData{
		BlockNumber: 100,
		Block:       json.RawMessage(`{"number":"0x64"}`),
		Receipts:    json.RawMessage(`[{"transactionHash":"0xdead"}]`),
		Traces:      json.RawMessage(`[{"type":"call"}]`),
		Blobs:       json.RawMessage(`[{"commitment":"0xabc"}]`),
	}

	svc := newTestServiceWithClient(t, Config{
		ChainID:             1,
		Bucket:              "test-bucket",
		CacheMissMaxRetries: 0,
		ChainExpectations: map[int64]ChainExpectation{
			1: {ExpectReceipts: true, ExpectTraces: true, ExpectBlobs: true},
		},
	}, consumer, cache, writer, newMockDeadLetterPublisher(), client)

	event := createBlockEvent(1, 100, 0)
	msg := createSQSMessage("msg1", event)

	status, err := svc.processMessage(context.Background(), msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if status != statusRPCFallback {
		t.Errorf("expected status %q, got %q", statusRPCFallback, status)
	}
	if keys := writer.GetAllKeys(); len(keys) != 4 {
		t.Errorf("expected 4 files from RPC fallback, got %d: %v", len(keys), keys)
	}
}

// TestProcessMessage_CacheMiss_RPCTracesError_Transient verifies a traces per-type
// RPC error is transient when traces are expected.
func TestProcessMessage_CacheMiss_RPCTracesError_Transient(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	client := newMockBlockchainClient()
	client.data = outbound.BlockData{
		BlockNumber: 100,
		Block:       json.RawMessage(`{"number":"0x64"}`),
		Receipts:    json.RawMessage(`[{"transactionHash":"0xdead"}]`),
		TracesErr:   errors.New("trace_block upstream error"),
	}

	svc := newTestServiceWithClient(t, Config{
		ChainID:             1,
		Bucket:              "test-bucket",
		CacheMissMaxRetries: 0,
		ChainExpectations: map[int64]ChainExpectation{
			1: {ExpectReceipts: true, ExpectTraces: true, ExpectBlobs: false},
		},
	}, consumer, cache, writer, newMockDeadLetterPublisher(), client)

	event := createBlockEvent(1, 100, 0)
	msg := createSQSMessage("msg1", event)

	_, err := svc.processMessage(context.Background(), msg)
	if err == nil {
		t.Fatal("expected error for traces RPC failure")
	}
	if !strings.Contains(err.Error(), "RPC traces fetch failed") {
		t.Errorf("unexpected error message: %v", err)
	}
	if errors.Is(err, ErrPermanent) {
		t.Error("a per-type RPC traces error must be transient, not ErrPermanent")
	}
}

// TestProcessMessage_CacheMiss_RPCNullBlobs_Permanent verifies a null/empty blobs
// payload from the RPC fallback is permanent when blobs are expected.
func TestProcessMessage_CacheMiss_RPCNullBlobs_Permanent(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	client := newMockBlockchainClient()
	client.data = outbound.BlockData{
		BlockNumber: 100,
		Block:       json.RawMessage(`{"number":"0x64"}`),
		Receipts:    json.RawMessage(`[{"transactionHash":"0xdead"}]`),
		Blobs:       json.RawMessage("null"),
	}

	svc := newTestServiceWithClient(t, Config{
		ChainID:             1,
		Bucket:              "test-bucket",
		CacheMissMaxRetries: 0,
		ChainExpectations: map[int64]ChainExpectation{
			1: {ExpectReceipts: true, ExpectTraces: false, ExpectBlobs: true},
		},
	}, consumer, cache, writer, newMockDeadLetterPublisher(), client)

	event := createBlockEvent(1, 100, 0)
	msg := createSQSMessage("msg1", event)

	_, err := svc.processMessage(context.Background(), msg)
	if err == nil {
		t.Fatal("expected error for null RPC blobs")
	}
	if !strings.Contains(err.Error(), "RPC returned null/empty blobs") {
		t.Errorf("unexpected error message: %v", err)
	}
	if !errors.Is(err, ErrPermanent) {
		t.Errorf("expected null RPC blobs to be permanent, got: %v", err)
	}
}

// TestProcessMessage_CacheMiss_AlreadyInS3_NoOp verifies that a cache miss whose
// files all already exist in S3 is a no-op: status already_backed_up, no RPC call.
func TestProcessMessage_CacheMiss_AlreadyInS3_NoOp(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	client := newMockBlockchainClient()

	svc := newTestServiceWithClient(t, Config{
		ChainID:             1,
		Bucket:              "test-bucket",
		CacheMissMaxRetries: 0,
		ChainExpectations:   blockOnlyExpectations(),
	}, consumer, cache, writer, newMockDeadLetterPublisher(), client)

	// Block file already in S3, nothing in cache.
	writer.PresetFileExists("test-bucket", "0-999/100_0_block.json.gz")

	event := createBlockEvent(1, 100, 0)
	msg := createSQSMessage("msg1", event)

	status, err := svc.processMessage(context.Background(), msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if status != statusAlreadyBackedUp {
		t.Errorf("expected status %q, got %q", statusAlreadyBackedUp, status)
	}
	if calls := client.Calls(); calls != 0 {
		t.Errorf("expected 0 RPC calls when already backed up, got %d", calls)
	}
	if writer.writeCalled.Load() != 0 {
		t.Errorf("expected 0 write calls (already in S3), got %d", writer.writeCalled.Load())
	}
}

func TestProcessMessage_InvalidJSON(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	svc, _ := NewService(Config{
		ChainID: 1,
		Bucket:  "test-bucket",
		Logger:  testutil.DiscardLogger(),
	}, consumer, cache, writer, newMockDeadLetterPublisher(), newMockBlockchainClient())

	msg := outbound.SQSMessage{
		MessageID:     "msg1",
		ReceiptHandle: "receipt1",
		Body:          "not valid json",
	}

	_, err := svc.processMessage(context.Background(), msg)

	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
	if !strings.Contains(err.Error(), "failed to parse block event") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestProcessMessage_ChainIDMismatch(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	// Service configured for chain 1
	svc, _ := NewService(Config{
		ChainID: 1,
		Bucket:  "test-bucket",
		Logger:  testutil.DiscardLogger(),
	}, consumer, cache, writer, newMockDeadLetterPublisher(), newMockBlockchainClient())

	// Event comes from chain 137 (Polygon)
	event := createBlockEvent(137, 100, 0)
	msg := createSQSMessage("msg1", event)

	_, err := svc.processMessage(context.Background(), msg)

	if err == nil {
		t.Fatal("expected error for chain ID mismatch")
	}
	if !strings.Contains(err.Error(), "chain ID mismatch") {
		t.Errorf("unexpected error message: %v", err)
	}
	if !strings.Contains(err.Error(), "event has 137") {
		t.Errorf("error should include event chain ID: %v", err)
	}
	if !strings.Contains(err.Error(), "expected 1") {
		t.Errorf("error should include expected chain ID: %v", err)
	}

	// Verify nothing was written to S3
	keys := writer.GetAllKeys()
	if len(keys) != 0 {
		t.Errorf("expected no files written, got %d: %v", len(keys), keys)
	}
}

func TestProcessMessage_CacheGetBlockError(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	svc, _ := NewService(Config{
		ChainID: 1,
		Bucket:  "test-bucket",
		Logger:  testutil.DiscardLogger(),
	}, consumer, cache, writer, newMockDeadLetterPublisher(), newMockBlockchainClient())

	event := createBlockEvent(1, 100, 0)
	cache.SetGetError("block", 1, 100, 0, errors.New("redis connection failed"))

	msg := createSQSMessage("msg1", event)
	_, err := svc.processMessage(context.Background(), msg)

	if err == nil {
		t.Fatal("expected error for cache failure")
	}
	if !strings.Contains(err.Error(), "failed to get block from cache") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestProcessMessage_CacheGetReceiptsError(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	svc, _ := NewService(Config{
		ChainID: 1,
		Bucket:  "test-bucket",
		Logger:  testutil.DiscardLogger(),
	}, consumer, cache, writer, newMockDeadLetterPublisher(), newMockBlockchainClient())

	event := createBlockEvent(1, 100, 0)
	ctx := context.Background()
	_ = cache.SetBlock(ctx, 1, 100, 0, json.RawMessage(`{}`))
	cache.SetGetError("receipts", 1, 100, 0, errors.New("redis timeout"))

	msg := createSQSMessage("msg1", event)
	_, err := svc.processMessage(ctx, msg)

	if err == nil {
		t.Fatal("expected error for cache failure")
	}
	if !strings.Contains(err.Error(), "failed to get receipts from cache") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestProcessMessage_CacheGetTracesError(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	// Explicitly expect traces so the cache fetch is triggered
	svc, _ := NewService(Config{
		ChainID: 1,
		Bucket:  "test-bucket",
		ChainExpectations: map[int64]ChainExpectation{
			1: {ExpectReceipts: false, ExpectTraces: true, ExpectBlobs: false},
		},
		Logger: testutil.DiscardLogger(),
	}, consumer, cache, writer, newMockDeadLetterPublisher(), newMockBlockchainClient())

	event := createBlockEvent(1, 100, 0)
	ctx := context.Background()
	_ = cache.SetBlock(ctx, 1, 100, 0, json.RawMessage(`{}`))
	cache.SetGetError("traces", 1, 100, 0, errors.New("redis error"))

	msg := createSQSMessage("msg1", event)
	_, err := svc.processMessage(ctx, msg)

	if err == nil {
		t.Fatal("expected error for cache failure")
	}
	if !strings.Contains(err.Error(), "failed to get traces from cache") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestProcessMessage_CacheGetBlobsError(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	// Explicitly expect blobs so the cache fetch is triggered
	svc, _ := NewService(Config{
		ChainID: 1,
		Bucket:  "test-bucket",
		ChainExpectations: map[int64]ChainExpectation{
			1: {ExpectReceipts: false, ExpectTraces: false, ExpectBlobs: true},
		},
		Logger: testutil.DiscardLogger(),
	}, consumer, cache, writer, newMockDeadLetterPublisher(), newMockBlockchainClient())

	event := createBlockEvent(1, 100, 0)
	ctx := context.Background()
	_ = cache.SetBlock(ctx, 1, 100, 0, json.RawMessage(`{}`))
	cache.SetGetError("blobs", 1, 100, 0, errors.New("redis error"))

	msg := createSQSMessage("msg1", event)
	_, err := svc.processMessage(ctx, msg)

	if err == nil {
		t.Fatal("expected error for cache failure")
	}
	if !strings.Contains(err.Error(), "failed to get blobs from cache") {
		t.Errorf("unexpected error message: %v", err)
	}
	if errors.Is(err, ErrPermanent) {
		t.Error("a cache getter error must be transient, not ErrPermanent")
	}
}

func TestProcessMessage_S3WriteError(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	svc, _ := NewService(Config{
		ChainID:           1,
		Bucket:            "test-bucket",
		ChainExpectations: blockOnlyExpectations(),
		Logger:            testutil.DiscardLogger(),
	}, consumer, cache, writer, newMockDeadLetterPublisher(), newMockBlockchainClient())

	event := createBlockEvent(1, 100, 0)
	ctx := context.Background()
	_ = cache.SetBlock(ctx, 1, 100, 0, json.RawMessage(`{}`))

	writer.SetWriteError("test-bucket/0-999/100_0_block.json.gz", errors.New("S3 access denied"))

	msg := createSQSMessage("msg1", event)
	_, err := svc.processMessage(ctx, msg)

	if err == nil {
		t.Fatal("expected error for S3 write failure")
	}
	if !strings.Contains(err.Error(), "failed to write block to S3") {
		t.Errorf("unexpected error message: %v", err)
	}
	if errors.Is(err, ErrPermanent) {
		t.Error("an S3 write error must be transient, not ErrPermanent")
	}
}

func TestProcessMessage_S3ExistsError(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	svc, _ := NewService(Config{
		ChainID:           1,
		Bucket:            "test-bucket",
		ChainExpectations: blockOnlyExpectations(),
		Logger:            testutil.DiscardLogger(),
	}, consumer, cache, writer, newMockDeadLetterPublisher(), newMockBlockchainClient())

	event := createBlockEvent(1, 100, 0)
	ctx := context.Background()
	_ = cache.SetBlock(ctx, 1, 100, 0, json.RawMessage(`{}`))

	writer.SetExistsError("test-bucket/0-999/100_0_block.json.gz", errors.New("S3 service unavailable"))

	msg := createSQSMessage("msg1", event)
	_, err := svc.processMessage(ctx, msg)

	if err == nil {
		t.Fatal("expected error for S3 exists check failure")
	}
	if !strings.Contains(err.Error(), "failed to check") {
		t.Errorf("unexpected error message: %v", err)
	}
	if errors.Is(err, ErrPermanent) {
		t.Error("an S3 exists-check error must be transient, not ErrPermanent")
	}
}

func TestProcessMessage_Idempotent_SkipsExistingFile(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	svc, _ := NewService(Config{
		ChainID:           1,
		Bucket:            "test-bucket",
		ChainExpectations: blockOnlyExpectations(),
		Logger:            testutil.DiscardLogger(),
	}, consumer, cache, writer, newMockDeadLetterPublisher(), newMockBlockchainClient())

	event := createBlockEvent(1, 100, 0)
	ctx := context.Background()
	_ = cache.SetBlock(ctx, 1, 100, 0, json.RawMessage(`{}`))

	// Pre-populate S3 with existing file
	writer.PresetFileExists("test-bucket", "0-999/100_0_block.json.gz")

	msg := createSQSMessage("msg1", event)
	_, err := svc.processMessage(ctx, msg)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Since the file already exists, FileExists short-circuits the flow and
	// WriteFileIfNotExists must not be invoked.
	if writer.writeCalled.Load() != 0 {
		t.Errorf("expected 0 write calls (file existed), got %d", writer.writeCalled.Load())
	}
}

func TestProcessMessage_DifferentVersionsAreSeparate(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	svc, _ := NewService(Config{
		ChainID:           1,
		Bucket:            "test-bucket",
		ChainExpectations: blockOnlyExpectations(),
		Logger:            testutil.DiscardLogger(),
	}, consumer, cache, writer, newMockDeadLetterPublisher(), newMockBlockchainClient())

	ctx := context.Background()

	// Version 0
	event0 := createBlockEvent(1, 100, 0)
	_ = cache.SetBlock(ctx, 1, 100, 0, json.RawMessage(`{"version": 0}`))
	msg0 := createSQSMessage("msg0", event0)
	_, _ = svc.processMessage(ctx, msg0)

	// Version 1 (reorg)
	event1 := createBlockEvent(1, 100, 1)
	_ = cache.SetBlock(ctx, 1, 100, 1, json.RawMessage(`{"version": 1}`))
	msg1 := createSQSMessage("msg1", event1)
	_, _ = svc.processMessage(ctx, msg1)

	// Both files should exist
	_, exists0 := writer.GetFile("test-bucket", "0-999/100_0_block.json.gz")
	_, exists1 := writer.GetFile("test-bucket", "0-999/100_1_block.json.gz")

	if !exists0 {
		t.Error("expected version 0 file to exist")
	}
	if !exists1 {
		t.Error("expected version 1 file to exist")
	}
}

// TestProcessMessage_SameBlockNumberDifferentBucket verifies that the same block number
// from different chains would use the same key pattern (since each chain has its own bucket).
func TestProcessMessage_SameBlockNumberSameKeyPattern(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	// A service configured for mainnet
	svc, _ := NewService(Config{
		ChainID:           1,
		Bucket:            "mainnet-bucket",
		ChainExpectations: blockOnlyExpectations(),
		Logger:            testutil.DiscardLogger(),
	}, consumer, cache, writer, newMockDeadLetterPublisher(), newMockBlockchainClient())

	ctx := context.Background()

	// Process a mainnet block
	event1 := createBlockEvent(1, 100, 0)
	_ = cache.SetBlock(ctx, 1, 100, 0, json.RawMessage(`{"chain": 1}`))
	msg1 := createSQSMessage("msg1", event1)
	_, _ = svc.processMessage(ctx, msg1)

	// Verify the key structure doesn't include chain ID
	// (chain separation is done via different buckets)
	_, existsMainnet := writer.GetFile("mainnet-bucket", "0-999/100_0_block.json.gz")

	if !existsMainnet {
		t.Error("expected file with partition-based key (no chain ID prefix)")
	}

	// Confirm no file with old chain-prefixed format exists
	_, existsOldFormat := writer.GetFile("mainnet-bucket", "1/0-999/100_0_block.json.gz")
	if existsOldFormat {
		t.Error("unexpected file with chain ID prefix - format should be {partition}/{block}_{version}_{type}.json.gz")
	}
}

// =============================================================================
// Tests: Run and Stop
// =============================================================================

func TestRun_ProcessesMessages(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	svc, _ := NewService(Config{
		ChainID:           1,
		Bucket:            "test-bucket",
		Workers:           1,
		BatchSize:         1,
		ChainExpectations: blockOnlyExpectations(),
		Logger:            testutil.DiscardLogger(),
	}, consumer, cache, writer, newMockDeadLetterPublisher(), newMockBlockchainClient())

	ctx := context.Background()

	// Add message to queue
	event := createBlockEvent(1, 100, 0)
	_ = cache.SetBlock(ctx, 1, 100, 0, json.RawMessage(`{}`))
	consumer.AddMessage(createSQSMessage("msg1", event))

	// Run with timeout
	ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	// Make consumer return empty after first message
	consumer.receiveCallback = func(ctx context.Context, maxMessages int) ([]outbound.SQSMessage, error) {
		consumer.mu.Lock()
		defer consumer.mu.Unlock()
		if len(consumer.messages) == 0 {
			// Block until context cancelled
			<-ctx.Done()
			return nil, ctx.Err()
		}
		msg := consumer.messages[0]
		consumer.messages = consumer.messages[1:]
		return []outbound.SQSMessage{msg}, nil
	}

	err := svc.Run(ctx)

	if err != context.DeadlineExceeded {
		t.Errorf("expected context deadline exceeded, got: %v", err)
	}

	// Verify message was processed
	deletedHandles := consumer.GetDeletedHandles()
	if len(deletedHandles) != 1 {
		t.Errorf("expected 1 deleted message, got %d", len(deletedHandles))
	}

	// Verify file was written
	keys := writer.GetAllKeys()
	if len(keys) != 1 {
		t.Errorf("expected 1 file, got %d", len(keys))
	}
}

func TestRun_StopsOnContextCancel(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	svc, _ := NewService(Config{
		ChainID: 1,
		Bucket:  "test-bucket",
		Workers: 1,
		Logger:  testutil.DiscardLogger(),
	}, consumer, cache, writer, newMockDeadLetterPublisher(), newMockBlockchainClient())

	// Make consumer block on receive
	consumer.receiveDelay = 10 * time.Second

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- svc.Run(ctx)
	}()

	// Give it time to start
	time.Sleep(50 * time.Millisecond)

	// Cancel context
	cancel()

	select {
	case err := <-done:
		if err != context.Canceled {
			t.Errorf("expected context.Canceled, got: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("service did not stop in time")
	}
}

func TestRun_StopsOnStopSignal(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	svc, _ := NewService(Config{
		ChainID: 1,
		Bucket:  "test-bucket",
		Workers: 1,
		Logger:  testutil.DiscardLogger(),
	}, consumer, cache, writer, newMockDeadLetterPublisher(), newMockBlockchainClient())

	// Make consumer return empty immediately
	consumer.receiveCallback = func(ctx context.Context, maxMessages int) ([]outbound.SQSMessage, error) {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(100 * time.Millisecond):
			return []outbound.SQSMessage{}, nil
		}
	}

	ctx := context.Background()

	done := make(chan error, 1)
	go func() {
		done <- svc.Run(ctx)
	}()

	// Give it time to start
	time.Sleep(50 * time.Millisecond)

	// Stop the service
	svc.Stop()

	select {
	case err := <-done:
		if err != nil {
			t.Errorf("expected nil error on stop, got: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("service did not stop in time")
	}
}

func TestRun_ContinuesOnReceiveError(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	svc, _ := NewService(Config{
		ChainID: 1,
		Bucket:  "test-bucket",
		Workers: 1,
		Logger:  testutil.DiscardLogger(),
	}, consumer, cache, writer, newMockDeadLetterPublisher(), newMockBlockchainClient())

	errorReturned := make(chan struct{})
	consumer.receiveCallback = func(ctx context.Context, maxMessages int) ([]outbound.SQSMessage, error) {
		select {
		case <-errorReturned:
			// After error was returned, block until cancelled - service is still running
			<-ctx.Done()
			return nil, ctx.Err()
		default:
			close(errorReturned)
			return nil, errors.New("temporary network error")
		}
	}

	ctx, cancel := context.WithTimeout(context.Background(), 100*time.Millisecond)
	defer cancel()

	// The key test: Run should NOT exit immediately when ReceiveMessages returns an error
	// It should continue running until context is cancelled
	start := time.Now()
	err := svc.Run(ctx)
	elapsed := time.Since(start)

	// Service should have run for close to the full timeout duration
	// (not exit immediately on error)
	if err != context.DeadlineExceeded {
		t.Errorf("expected context.DeadlineExceeded, got: %v", err)
	}

	// Elapsed time should be close to timeout, not immediate
	if elapsed < 80*time.Millisecond {
		t.Errorf("service exited too quickly (%v), expected to wait for timeout", elapsed)
	}
}

func TestRun_DoesNotDeleteMessageOnTransientError(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()
	dlq := newMockDeadLetterPublisher()

	svc, _ := NewService(Config{
		ChainID:   1,
		Bucket:    "test-bucket",
		Workers:   1,
		BatchSize: 1,
		Logger:    testutil.DiscardLogger(),
	}, consumer, cache, writer, dlq, newMockBlockchainClient())

	ctx := context.Background()

	// A transient Redis error on the block read must NOT delete or dead-letter.
	event := createBlockEvent(1, 100, 0)
	cache.SetGetError("block", 1, 100, 0, errors.New("redis connection refused"))
	consumer.AddMessage(createSQSMessage("msg1", event))

	callCount := atomic.Int32{}
	consumer.receiveCallback = func(ctx context.Context, maxMessages int) ([]outbound.SQSMessage, error) {
		count := callCount.Add(1)
		if count == 1 {
			consumer.mu.Lock()
			defer consumer.mu.Unlock()
			if len(consumer.messages) > 0 {
				msg := consumer.messages[0]
				consumer.messages = consumer.messages[1:]
				return []outbound.SQSMessage{msg}, nil
			}
		}
		// Block until cancelled
		<-ctx.Done()
		return nil, ctx.Err()
	}

	ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()

	_ = svc.Run(ctx)

	// Message should NOT be deleted because the failure is transient.
	deletedHandles := consumer.GetDeletedHandles()
	if len(deletedHandles) != 0 {
		t.Errorf("expected 0 deleted messages (transient failure), got %d", len(deletedHandles))
	}
	// And it must NOT be dead-lettered.
	if calls := dlq.calls.Load(); calls != 0 {
		t.Errorf("expected 0 DLQ publishes for transient failure, got %d", calls)
	}
}

func TestRun_DeletesMessageOnSuccess(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	svc, _ := NewService(Config{
		ChainID:           1,
		Bucket:            "test-bucket",
		Workers:           1,
		BatchSize:         1,
		ChainExpectations: blockOnlyExpectations(),
		Logger:            testutil.DiscardLogger(),
	}, consumer, cache, writer, newMockDeadLetterPublisher(), newMockBlockchainClient())

	ctx := context.Background()

	// Add message WITH block data
	event := createBlockEvent(1, 100, 0)
	_ = cache.SetBlock(ctx, 1, 100, 0, json.RawMessage(`{}`))
	consumer.AddMessage(createSQSMessage("msg1", event))

	callCount := atomic.Int32{}
	consumer.receiveCallback = func(ctx context.Context, maxMessages int) ([]outbound.SQSMessage, error) {
		count := callCount.Add(1)
		if count == 1 {
			consumer.mu.Lock()
			defer consumer.mu.Unlock()
			if len(consumer.messages) > 0 {
				msg := consumer.messages[0]
				consumer.messages = consumer.messages[1:]
				return []outbound.SQSMessage{msg}, nil
			}
		}
		// Block until cancelled
		<-ctx.Done()
		return nil, ctx.Err()
	}

	ctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()

	_ = svc.Run(ctx)

	// Message should be deleted
	deletedHandles := consumer.GetDeletedHandles()
	if len(deletedHandles) != 1 {
		t.Errorf("expected 1 deleted message, got %d", len(deletedHandles))
	}
	if deletedHandles[0] != "receipt-msg1" {
		t.Errorf("expected receipt-msg1, got %s", deletedHandles[0])
	}
}

func TestRun_HandlesDeleteMessageError(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	svc, _ := NewService(Config{
		ChainID:           1,
		Bucket:            "test-bucket",
		Workers:           1,
		BatchSize:         1,
		ChainExpectations: blockOnlyExpectations(),
		Logger:            testutil.DiscardLogger(),
	}, consumer, cache, writer, newMockDeadLetterPublisher(), newMockBlockchainClient())

	ctx := context.Background()

	// Add message WITH block data
	event := createBlockEvent(1, 100, 0)
	_ = cache.SetBlock(ctx, 1, 100, 0, json.RawMessage(`{}`))
	consumer.AddMessage(createSQSMessage("msg1", event))

	// Make delete fail
	consumer.deleteErr = errors.New("SQS delete failed")

	callCount := atomic.Int32{}
	consumer.receiveCallback = func(ctx context.Context, maxMessages int) ([]outbound.SQSMessage, error) {
		count := callCount.Add(1)
		if count == 1 {
			consumer.mu.Lock()
			defer consumer.mu.Unlock()
			if len(consumer.messages) > 0 {
				msg := consumer.messages[0]
				consumer.messages = consumer.messages[1:]
				return []outbound.SQSMessage{msg}, nil
			}
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}

	ctx, cancel := context.WithTimeout(ctx, 200*time.Millisecond)
	defer cancel()

	// Should not panic even with delete error
	err := svc.Run(ctx)

	// Service should continue running despite delete error
	if err != context.DeadlineExceeded {
		t.Errorf("expected deadline exceeded, got: %v", err)
	}

	// File should still be written (processing succeeded)
	keys := writer.GetAllKeys()
	if len(keys) != 1 {
		t.Errorf("expected 1 file, got %d", len(keys))
	}
}

// =============================================================================
// Tests: Concurrent Workers
// =============================================================================

func TestRun_MultipleWorkersProcessConcurrently(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	svc, _ := NewService(Config{
		ChainID:           1,
		Bucket:            "test-bucket",
		Workers:           4,
		BatchSize:         10,
		ChainExpectations: blockOnlyExpectations(),
		Logger:            testutil.DiscardLogger(),
	}, consumer, cache, writer, newMockDeadLetterPublisher(), newMockBlockchainClient())

	ctx := context.Background()

	// Add multiple messages
	for i := range 10 {
		event := createBlockEvent(1, int64(100+i), 0)
		_ = cache.SetBlock(ctx, 1, int64(100+i), 0, json.RawMessage(fmt.Sprintf(`{"block": %d}`, 100+i)))
		consumer.AddMessage(createSQSMessage(fmt.Sprintf("msg%d", i), event))
	}

	processed := atomic.Int32{}
	consumer.receiveCallback = func(ctx context.Context, maxMessages int) ([]outbound.SQSMessage, error) {
		consumer.mu.Lock()
		if len(consumer.messages) == 0 {
			consumer.mu.Unlock()
			// Wait for processing to complete
			for processed.Load() < 10 {
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				case <-time.After(10 * time.Millisecond):
				}
			}
			<-ctx.Done()
			return nil, ctx.Err()
		}

		count := min(maxMessages, len(consumer.messages))
		msgs := consumer.messages[:count]
		consumer.messages = consumer.messages[count:]
		consumer.mu.Unlock()

		processed.Add(int32(count))
		return msgs, nil
	}

	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()

	_ = svc.Run(ctx)

	// All messages should be processed
	deletedHandles := consumer.GetDeletedHandles()
	if len(deletedHandles) != 10 {
		t.Errorf("expected 10 deleted messages, got %d", len(deletedHandles))
	}

	// All files should be written
	keys := writer.GetAllKeys()
	if len(keys) != 10 {
		t.Errorf("expected 10 files, got %d", len(keys))
	}
}

// =============================================================================
// Tests: Edge Cases
// =============================================================================

func TestProcessMessage_ZeroBlockNumber(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	svc, _ := NewService(Config{
		ChainID:           1,
		Bucket:            "test-bucket",
		ChainExpectations: blockOnlyExpectations(),
		Logger:            testutil.DiscardLogger(),
	}, consumer, cache, writer, newMockDeadLetterPublisher(), newMockBlockchainClient())

	event := createBlockEvent(1, 0, 0)
	ctx := context.Background()
	_ = cache.SetBlock(ctx, 1, 0, 0, json.RawMessage(`{"block": 0}`))

	msg := createSQSMessage("msg0", event)
	_, err := svc.processMessage(ctx, msg)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should be in partition 0-999
	_, exists := writer.GetFile("test-bucket", "0-999/0_0_block.json.gz")
	if !exists {
		t.Error("expected block 0 file to exist")
	}
}

func TestProcessMessage_LargeBlockNumber(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	svc, _ := NewService(Config{
		ChainID:           1,
		Bucket:            "test-bucket",
		ChainExpectations: blockOnlyExpectations(),
		Logger:            testutil.DiscardLogger(),
	}, consumer, cache, writer, newMockDeadLetterPublisher(), newMockBlockchainClient())

	blockNum := int64(20000000) // Block 20 million
	event := createBlockEvent(1, blockNum, 0)
	ctx := context.Background()
	_ = cache.SetBlock(ctx, 1, blockNum, 0, json.RawMessage(`{}`))

	msg := createSQSMessage("msg1", event)
	_, err := svc.processMessage(ctx, msg)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Check partition calculation (20000000 / 1000 = 20000, start = 20000000, end = 20000999)
	expectedPartition := "20000000-20000999"
	expectedKey := fmt.Sprintf("%s/%d_0_block.json.gz", expectedPartition, blockNum)
	_, exists := writer.GetFile("test-bucket", expectedKey)
	if !exists {
		t.Errorf("expected file at %s", expectedKey)
	}
}

func TestProcessMessage_HighVersion(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	svc, _ := NewService(Config{
		ChainID:           1,
		Bucket:            "test-bucket",
		ChainExpectations: blockOnlyExpectations(),
		Logger:            testutil.DiscardLogger(),
	}, consumer, cache, writer, newMockDeadLetterPublisher(), newMockBlockchainClient())

	// High version number (many reorgs)
	event := createBlockEvent(1, 100, 999)
	ctx := context.Background()
	_ = cache.SetBlock(ctx, 1, 100, 999, json.RawMessage(`{}`))

	msg := createSQSMessage("msg1", event)
	_, err := svc.processMessage(ctx, msg)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, exists := writer.GetFile("test-bucket", "0-999/100_999_block.json.gz")
	if !exists {
		t.Error("expected high version file to exist")
	}
}

func TestProcessMessage_EmptyBlockData(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	svc, _ := NewService(Config{
		ChainID:           1,
		Bucket:            "test-bucket",
		ChainExpectations: blockOnlyExpectations(),
		Logger:            testutil.DiscardLogger(),
	}, consumer, cache, writer, newMockDeadLetterPublisher(), newMockBlockchainClient())

	event := createBlockEvent(1, 100, 0)
	ctx := context.Background()
	// Empty but valid JSON
	_ = cache.SetBlock(ctx, 1, 100, 0, json.RawMessage(`{}`))

	msg := createSQSMessage("msg1", event)
	_, err := svc.processMessage(ctx, msg)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestProcessMessage_LargeBlockData(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	svc, _ := NewService(Config{
		ChainID:           1,
		Bucket:            "test-bucket",
		ChainExpectations: blockOnlyExpectations(),
		Logger:            testutil.DiscardLogger(),
	}, consumer, cache, writer, newMockDeadLetterPublisher(), newMockBlockchainClient())

	event := createBlockEvent(1, 100, 0)
	ctx := context.Background()

	// Create large block data (10MB)
	largeData := make([]byte, 10*1024*1024)
	for i := range largeData {
		largeData[i] = 'x'
	}
	blockJSON := fmt.Sprintf(`{"data": "%s"}`, string(largeData))

	_ = cache.SetBlock(ctx, 1, 100, 0, json.RawMessage(blockJSON))

	msg := createSQSMessage("msg1", event)
	_, err := svc.processMessage(ctx, msg)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	data, exists := writer.GetFile("test-bucket", "0-999/100_0_block.json.gz")
	if !exists {
		t.Error("expected file to exist")
	}
	if len(data) == 0 {
		t.Error("expected data to be written")
	}
}

func TestStop_CanBeCalledMultipleTimes(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	svc, _ := NewService(Config{
		ChainID: 1,
		Bucket:  "test-bucket",
		Logger:  testutil.DiscardLogger(),
	}, consumer, cache, writer, newMockDeadLetterPublisher(), newMockBlockchainClient())

	// Should not panic
	svc.Stop()
	svc.Stop()
	svc.Stop()
}

func TestConfigDefaults(t *testing.T) {
	defaults := ConfigDefaults()

	if defaults.ChainID != 1 {
		t.Errorf("expected ChainID=1, got %d", defaults.ChainID)
	}
	if defaults.Workers != 4 {
		t.Errorf("expected Workers=4, got %d", defaults.Workers)
	}
	if defaults.BatchSize != 10 {
		t.Errorf("expected BatchSize=10, got %d", defaults.BatchSize)
	}
	if defaults.Logger == nil {
		t.Error("expected Logger to be set")
	}
}

// =============================================================================
// Tests: Avalanche C-Chain
// =============================================================================

func TestDefaultChainExpectations_IncludesAvalanche(t *testing.T) {
	expectations := DefaultChainExpectations()

	avax, ok := expectations[43114]
	if !ok {
		t.Fatal("expected Avalanche C-Chain (43114) in default expectations")
	}
	if !avax.ExpectReceipts {
		t.Error("expected Avalanche to expect receipts")
	}
	if avax.ExpectTraces {
		t.Error("expected Avalanche to NOT expect traces")
	}
	if avax.ExpectBlobs {
		t.Error("expected Avalanche to NOT expect blobs")
	}
}

// TestDefaultChainExpectations_L2sBackUpReceipts guards against the L2 backup gap:
// base/optimism/unichain/arbitrum watchers run with --enable-traces=false but always
// cache receipts, so their backups must expect receipts (not block-only) and must not
// expect traces or blobs. A missing entry silently degrades to block-only.
func TestDefaultChainExpectations_L2sBackUpReceipts(t *testing.T) {
	expectations := DefaultChainExpectations()
	for _, chainID := range []int64{8453, 10, 130, 42161} {
		exp, ok := expectations[chainID]
		if !ok {
			t.Errorf("chain %d missing from default expectations (would back up block only, skipping receipts)", chainID)
			continue
		}
		if !exp.ExpectReceipts {
			t.Errorf("chain %d: expected receipts to be backed up", chainID)
		}
		if exp.ExpectTraces {
			t.Errorf("chain %d: must NOT expect traces (watcher runs --enable-traces=false)", chainID)
		}
		if exp.ExpectBlobs {
			t.Errorf("chain %d: must NOT expect blobs", chainID)
		}
	}
}

func TestProcessMessage_AvalancheSkipsTracesAndBlobs(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	svc, _ := NewService(Config{
		ChainID: 43114,
		Bucket:  "avax-bucket",
		Logger:  testutil.DiscardLogger(),
	}, consumer, cache, writer, newMockDeadLetterPublisher(), newMockBlockchainClient())

	event := createBlockEvent(43114, 500, 0)
	ctx := context.Background()

	// Set block and receipts (expected for Avalanche), but NOT traces or blobs
	_ = cache.SetBlock(ctx, 43114, 500, 0, json.RawMessage(`{"number": 500}`))
	_ = cache.SetReceipts(ctx, 43114, 500, 0, json.RawMessage(`[{"status": "0x1"}]`))

	msg := createSQSMessage("avax-msg1", event)
	_, err := svc.processMessage(ctx, msg)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should write block + receipts (2 files), NOT traces or blobs
	keys := writer.GetAllKeys()
	if len(keys) != 2 {
		t.Errorf("expected 2 files (block + receipts), got %d: %v", len(keys), keys)
	}

	expectedKeys := []string{
		"avax-bucket/0-999/500_0_block.json.gz",
		"avax-bucket/0-999/500_0_receipts.json.gz",
	}
	for _, expectedKey := range expectedKeys {
		found := slices.Contains(keys, expectedKey)
		if !found {
			t.Errorf("expected key %s not found in %v", expectedKey, keys)
		}
	}
}

func TestProcessMessage_AvalancheDoesNotFetchTraces(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	svc, _ := NewService(Config{
		ChainID: 43114,
		Bucket:  "avax-bucket",
		Logger:  testutil.DiscardLogger(),
	}, consumer, cache, writer, newMockDeadLetterPublisher(), newMockBlockchainClient())

	event := createBlockEvent(43114, 500, 0)
	ctx := context.Background()

	_ = cache.SetBlock(ctx, 43114, 500, 0, json.RawMessage(`{"number": 500}`))
	_ = cache.SetReceipts(ctx, 43114, 500, 0, json.RawMessage(`[]`))

	// Set a traces error - should NOT be triggered since Avalanche doesn't expect traces
	cache.SetGetError("traces", 43114, 500, 0, errors.New("should not be called"))

	msg := createSQSMessage("avax-msg1", event)
	_, err := svc.processMessage(ctx, msg)

	if err != nil {
		t.Fatalf("unexpected error (traces fetch should be skipped): %v", err)
	}
}

// TestProcessMessage_AvalancheMissingReceipts_RPCFallback verifies that a
// receipts cache miss on Avalanche (block present, receipts absent) is no longer
// permanent: the whole block is re-fetched by hash via RPC and backed up. The
// stub returns block + receipts (Avalanche does not expect traces or blobs).
func TestProcessMessage_AvalancheMissingReceipts_RPCFallback(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	client := newMockBlockchainClient()
	client.data = outbound.BlockData{
		BlockNumber: 500,
		Block:       json.RawMessage(`{"number":"0x1f4"}`),
		Receipts:    json.RawMessage(`[{"status":"0x1"}]`),
	}

	svc := newTestServiceWithClient(t, Config{
		ChainID:             43114,
		Bucket:              "avax-bucket",
		CacheMissMaxRetries: 0,
	}, consumer, cache, writer, newMockDeadLetterPublisher(), client)

	event := createBlockEvent(43114, 500, 0)
	ctx := context.Background()

	// Set block but NOT receipts (which Avalanche expects) -> receipts cache miss.
	_ = cache.SetBlock(ctx, 43114, 500, 0, json.RawMessage(`{"number": 500}`))

	msg := createSQSMessage("avax-msg1", event)
	status, err := svc.processMessage(ctx, msg)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if status != statusRPCFallback {
		t.Errorf("expected status %q, got %q", statusRPCFallback, status)
	}
	if calls := client.Calls(); calls != 1 {
		t.Errorf("expected 1 RPC call, got %d", calls)
	}

	// Block + receipts written from RPC (no traces/blobs for Avalanche).
	keys := writer.GetAllKeys()
	if len(keys) != 2 {
		t.Errorf("expected 2 files (block + receipts) from RPC, got %d: %v", len(keys), keys)
	}
}

// =============================================================================
// Benchmark Tests
// =============================================================================

func BenchmarkProcessMessage(b *testing.B) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	svc, _ := NewService(Config{
		ChainID:           1,
		Bucket:            "test-bucket",
		ChainExpectations: blockOnlyExpectations(),
		Logger:            testutil.DiscardLogger(),
	}, consumer, cache, writer, newMockDeadLetterPublisher(), newMockBlockchainClient())

	ctx := context.Background()
	event := createBlockEvent(1, 100, 0)
	_ = cache.SetBlock(ctx, 1, 100, 0, json.RawMessage(`{"block": 100}`))

	msg := createSQSMessage("bench", event)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		// Reset writer for each iteration (or use different block numbers)
		writer.mu.Lock()
		writer.files = make(map[string][]byte)
		writer.mu.Unlock()

		_, _ = svc.processMessage(ctx, msg)
	}
}

// =============================================================================
// Tests: Additional Edge Cases for Complete Coverage
// =============================================================================

func TestProcessMessage_ReceiptsWriteError(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	svc, _ := NewService(Config{
		ChainID: 1,
		Bucket:  "test-bucket",
		ChainExpectations: map[int64]ChainExpectation{
			1: {ExpectReceipts: true, ExpectTraces: false, ExpectBlobs: false},
		},
		Logger: testutil.DiscardLogger(),
	}, consumer, cache, writer, newMockDeadLetterPublisher(), newMockBlockchainClient())

	event := createBlockEvent(1, 100, 0)
	ctx := context.Background()
	_ = cache.SetBlock(ctx, 1, 100, 0, json.RawMessage(`{}`))
	_ = cache.SetReceipts(ctx, 1, 100, 0, json.RawMessage(`[]`))

	writer.SetWriteError("test-bucket/0-999/100_0_receipts.json.gz", errors.New("S3 error"))

	msg := createSQSMessage("msg1", event)
	_, err := svc.processMessage(ctx, msg)

	if err == nil {
		t.Fatal("expected error for receipts write failure")
	}
	if !strings.Contains(err.Error(), "failed to write receipts to S3") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestProcessMessage_TracesWriteError(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	svc, _ := NewService(Config{
		ChainID: 1,
		Bucket:  "test-bucket",
		ChainExpectations: map[int64]ChainExpectation{
			1: {ExpectReceipts: false, ExpectTraces: true, ExpectBlobs: false},
		},
		Logger: testutil.DiscardLogger(),
	}, consumer, cache, writer, newMockDeadLetterPublisher(), newMockBlockchainClient())

	event := createBlockEvent(1, 100, 0)
	ctx := context.Background()
	_ = cache.SetBlock(ctx, 1, 100, 0, json.RawMessage(`{}`))
	_ = cache.SetTraces(ctx, 1, 100, 0, json.RawMessage(`[]`))

	writer.SetWriteError("test-bucket/0-999/100_0_traces.json.gz", errors.New("S3 error"))

	msg := createSQSMessage("msg1", event)
	_, err := svc.processMessage(ctx, msg)

	if err == nil {
		t.Fatal("expected error for traces write failure")
	}
	if !strings.Contains(err.Error(), "failed to write traces to S3") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestProcessMessage_BlobsWriteError(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	svc, _ := NewService(Config{
		ChainID: 1,
		Bucket:  "test-bucket",
		ChainExpectations: map[int64]ChainExpectation{
			1: {ExpectReceipts: false, ExpectTraces: false, ExpectBlobs: true},
		},
		Logger: testutil.DiscardLogger(),
	}, consumer, cache, writer, newMockDeadLetterPublisher(), newMockBlockchainClient())

	event := createBlockEvent(1, 100, 0)
	ctx := context.Background()
	_ = cache.SetBlock(ctx, 1, 100, 0, json.RawMessage(`{}`))
	_ = cache.SetBlobs(ctx, 1, 100, 0, json.RawMessage(`[]`))

	writer.SetWriteError("test-bucket/0-999/100_0_blobs.json.gz", errors.New("S3 error"))

	msg := createSQSMessage("msg1", event)
	_, err := svc.processMessage(ctx, msg)

	if err == nil {
		t.Fatal("expected error for blobs write failure")
	}
	if !strings.Contains(err.Error(), "failed to write blobs to S3") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestRun_ContextCancelledDuringMessageSend(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	svc, _ := NewService(Config{
		ChainID:   1,
		Bucket:    "test-bucket",
		Workers:   1,
		BatchSize: 100,
		Logger:    testutil.DiscardLogger(),
	}, consumer, cache, writer, newMockDeadLetterPublisher(), newMockBlockchainClient())

	// Create many messages
	for i := range 100 {
		event := createBlockEvent(1, int64(i), 0)
		consumer.AddMessage(createSQSMessage(fmt.Sprintf("msg%d", i), event))
	}

	// Block workers so messages queue up
	workerBlocker := make(chan struct{})
	originalCallback := consumer.receiveCallback
	consumer.receiveCallback = func(ctx context.Context, maxMessages int) ([]outbound.SQSMessage, error) {
		if originalCallback != nil {
			return originalCallback(ctx, maxMessages)
		}
		consumer.mu.Lock()
		msgs := make([]outbound.SQSMessage, len(consumer.messages))
		copy(msgs, consumer.messages)
		consumer.messages = nil
		consumer.mu.Unlock()
		<-workerBlocker // Block after receiving
		return msgs, nil
	}

	ctx, cancel := context.WithCancel(context.Background())

	done := make(chan error, 1)
	go func() {
		done <- svc.Run(ctx)
	}()

	// Cancel quickly
	time.Sleep(20 * time.Millisecond)
	cancel()
	close(workerBlocker)

	select {
	case err := <-done:
		if err != context.Canceled {
			t.Errorf("expected context.Canceled, got: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("service did not stop in time")
	}
}

func TestRun_ReceiveMessagesReturnsContextError(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	svc, _ := NewService(Config{
		ChainID: 1,
		Bucket:  "test-bucket",
		Workers: 1,
		Logger:  testutil.DiscardLogger(),
	}, consumer, cache, writer, newMockDeadLetterPublisher(), newMockBlockchainClient())

	ctx, cancel := context.WithCancel(context.Background())

	consumer.receiveCallback = func(ctx context.Context, maxMessages int) ([]outbound.SQSMessage, error) {
		cancel() // Cancel context during receive
		return nil, ctx.Err()
	}

	err := svc.Run(ctx)

	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got: %v", err)
	}
}

func TestProcessMessage_AllDataTypesWithContent(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	// Default Ethereum expectations: receipts + traces, no blobs
	svc, _ := NewService(Config{
		ChainID: 1,
		Bucket:  "test-bucket",
		Logger:  testutil.DiscardLogger(),
	}, consumer, cache, writer, newMockDeadLetterPublisher(), newMockBlockchainClient())

	event := createBlockEvent(1, 100, 0)
	ctx := context.Background()

	// Set expected data types with actual content (block + receipts + traces)
	_ = cache.SetBlock(ctx, 1, 100, 0, json.RawMessage(`{"number": "0x64", "hash": "0x123"}`))
	_ = cache.SetReceipts(ctx, 1, 100, 0, json.RawMessage(`[{"transactionIndex": "0x0"}]`))
	_ = cache.SetTraces(ctx, 1, 100, 0, json.RawMessage(`[{"action": {"callType": "call"}}]`))

	msg := createSQSMessage("msg1", event)
	_, err := svc.processMessage(ctx, msg)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify 3 files written (block + receipts + traces, blobs not expected for Ethereum)
	keys := writer.GetAllKeys()
	if len(keys) != 3 {
		t.Errorf("expected 3 files, got %d: %v", len(keys), keys)
	}

	// Check content was written
	for _, key := range keys {
		parts := strings.Split(key, "/")
		if len(parts) > 0 {
			bucket := parts[0]
			restOfKey := strings.Join(parts[1:], "/")
			data, exists := writer.GetFile(bucket, restOfKey)
			if !exists || len(data) == 0 {
				t.Errorf("expected non-empty data for %s", key)
			}
		}
	}
}

// =============================================================================
// Tests: writeToS3 null-payload guard (VEC-241)
// =============================================================================

func TestProcessMessage_RejectsNullBlockPayload(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	svc, _ := NewService(Config{
		ChainID: 1,
		Bucket:  "test-bucket",
		Logger:  testutil.DiscardLogger(),
	}, consumer, cache, writer, newMockDeadLetterPublisher(), newMockBlockchainClient())

	event := createBlockEvent(1, 100, 0)
	ctx := context.Background()

	_ = cache.SetBlock(ctx, 1, 100, 0, json.RawMessage("null"))
	_ = cache.SetReceipts(ctx, 1, 100, 0, json.RawMessage(`[{"transactionIndex": "0x0"}]`))
	_ = cache.SetTraces(ctx, 1, 100, 0, json.RawMessage(`[{"action": {"callType": "call"}}]`))

	msg := createSQSMessage("msg1", event)
	_, err := svc.processMessage(ctx, msg)
	if err == nil {
		t.Fatal("expected error for null block payload, got nil")
	}
	if !strings.Contains(err.Error(), "refusing to write null/empty") {
		t.Errorf("error missing expected text 'refusing to write null/empty': %v", err)
	}
	if !strings.Contains(err.Error(), "block") {
		t.Errorf("error should mention dataType 'block': %v", err)
	}
	if !strings.Contains(err.Error(), "100") {
		t.Errorf("error should include block number 100: %v", err)
	}
	if !strings.Contains(err.Error(), "chain=1") {
		t.Errorf("error should include chain=1: %v", err)
	}
	if !strings.Contains(err.Error(), "version=0") {
		t.Errorf("error should include version=0: %v", err)
	}

	if _, exists := writer.GetFile("test-bucket", "0-999/100_0_block.json.gz"); exists {
		t.Error("block key should NOT have been written")
	}
}

func TestProcessMessage_RejectsEmptyBlockPayload(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	svc, _ := NewService(Config{
		ChainID: 1,
		Bucket:  "test-bucket",
		Logger:  testutil.DiscardLogger(),
	}, consumer, cache, writer, newMockDeadLetterPublisher(), newMockBlockchainClient())

	event := createBlockEvent(1, 100, 0)
	ctx := context.Background()

	_ = cache.SetBlock(ctx, 1, 100, 0, json.RawMessage(""))
	_ = cache.SetReceipts(ctx, 1, 100, 0, json.RawMessage(`[{"transactionIndex": "0x0"}]`))
	_ = cache.SetTraces(ctx, 1, 100, 0, json.RawMessage(`[{"action": {"callType": "call"}}]`))

	msg := createSQSMessage("msg1", event)
	_, err := svc.processMessage(ctx, msg)
	if err == nil {
		t.Fatal("expected error for empty block payload, got nil")
	}
	if !strings.Contains(err.Error(), "refusing to write null/empty") {
		t.Errorf("error missing expected text 'refusing to write null/empty': %v", err)
	}

	if _, exists := writer.GetFile("test-bucket", "0-999/100_0_block.json.gz"); exists {
		t.Error("block key should NOT have been written")
	}
}

func TestProcessMessage_RejectsNullReceiptsPayload(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	svc, _ := NewService(Config{
		ChainID: 1,
		Bucket:  "test-bucket",
		Logger:  testutil.DiscardLogger(),
	}, consumer, cache, writer, newMockDeadLetterPublisher(), newMockBlockchainClient())

	event := createBlockEvent(1, 100, 0)
	ctx := context.Background()

	_ = cache.SetBlock(ctx, 1, 100, 0, json.RawMessage(`{"number": "0x64"}`))
	_ = cache.SetReceipts(ctx, 1, 100, 0, json.RawMessage("null"))
	_ = cache.SetTraces(ctx, 1, 100, 0, json.RawMessage(`[{"action": {"callType": "call"}}]`))

	msg := createSQSMessage("msg1", event)
	_, err := svc.processMessage(ctx, msg)
	if err == nil {
		t.Fatal("expected error for null receipts payload, got nil")
	}
	if !strings.Contains(err.Error(), "refusing to write null/empty") {
		t.Errorf("error missing expected text 'refusing to write null/empty': %v", err)
	}
	if !strings.Contains(err.Error(), "receipts") {
		t.Errorf("error should mention dataType 'receipts': %v", err)
	}
	if !strings.Contains(err.Error(), "100") {
		t.Errorf("error should include block number 100: %v", err)
	}
	if !strings.Contains(err.Error(), "chain=1") {
		t.Errorf("error should include chain=1: %v", err)
	}
	if !strings.Contains(err.Error(), "version=0") {
		t.Errorf("error should include version=0: %v", err)
	}

	if _, exists := writer.GetFile("test-bucket", "0-999/100_0_receipts.json.gz"); exists {
		t.Error("receipts key should NOT have been written")
	}
}

func TestProcessMessage_RejectsEmptyReceiptsPayload(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	svc, _ := NewService(Config{
		ChainID: 1,
		Bucket:  "test-bucket",
		Logger:  testutil.DiscardLogger(),
	}, consumer, cache, writer, newMockDeadLetterPublisher(), newMockBlockchainClient())

	event := createBlockEvent(1, 100, 0)
	ctx := context.Background()

	_ = cache.SetBlock(ctx, 1, 100, 0, json.RawMessage(`{"number": "0x64"}`))
	_ = cache.SetReceipts(ctx, 1, 100, 0, json.RawMessage(""))
	_ = cache.SetTraces(ctx, 1, 100, 0, json.RawMessage(`[{"action": {"callType": "call"}}]`))

	msg := createSQSMessage("msg1", event)
	_, err := svc.processMessage(ctx, msg)
	if err == nil {
		t.Fatal("expected error for empty receipts payload, got nil")
	}
	if !strings.Contains(err.Error(), "refusing to write null/empty") {
		t.Errorf("error missing expected text 'refusing to write null/empty': %v", err)
	}

	if _, exists := writer.GetFile("test-bucket", "0-999/100_0_receipts.json.gz"); exists {
		t.Error("receipts key should NOT have been written")
	}
}

// =============================================================================
// Tests: dead-letter routing (permanent vs transient classification)
// =============================================================================

// runUntilProcessed drives the service via its public Run API for a single
// message and waits for the worker to reach a terminal action: either it
// deleted the message (success or dead-lettered+deleted) or it called the DLQ
// publisher (covers the publish-fails case where no delete happens). It is used
// by the tests where the worker is expected to act on the message.
func runUntilProcessed(t *testing.T, svc *Service, consumer *mockSQSConsumer, dlq *mockDeadLetterPublisher) {
	t.Helper()
	runAndWait(t, svc, consumer, func() bool {
		return consumer.deleteCalled.Load() > 0 || dlq.calls.Load() > 0
	})
}

// runUntilTransient drives the service via its public Run API for a single
// message that fails transiently: the worker leaves the message untouched (no
// delete, no DLQ publish). We detect completion via the second fetcher poll,
// which only happens after the single worker has finished its only message.
func runUntilTransient(t *testing.T, svc *Service, consumer *mockSQSConsumer) {
	t.Helper()
	runAndWait(t, svc, consumer, func() bool {
		return consumer.receiveCalled.Load() >= 2
	})
}

func runAndWait(t *testing.T, svc *Service, consumer *mockSQSConsumer, done func() bool) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	delivered := atomic.Bool{}
	consumer.receiveCallback = func(ctx context.Context, maxMessages int) ([]outbound.SQSMessage, error) {
		if delivered.CompareAndSwap(false, true) {
			consumer.mu.Lock()
			defer consumer.mu.Unlock()
			count := min(maxMessages, len(consumer.messages))
			msgs := consumer.messages[:count]
			consumer.messages = consumer.messages[count:]
			return msgs, nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}

	finished := make(chan struct{})
	go func() {
		_ = svc.Run(ctx)
		close(finished)
	}()

	testutil.WaitForCondition(t, 6*time.Second, done, "worker to reach a terminal state")

	// Give the worker a brief moment to finish bookkeeping after the terminal action.
	time.Sleep(50 * time.Millisecond)

	svc.Stop()
	cancel()
	<-finished
}

func newDLQTestService(t *testing.T, config Config, consumer outbound.SQSConsumer, cache outbound.BlockCache, writer outbound.S3Writer, deadLetter outbound.DeadLetterPublisher) *Service {
	t.Helper()
	return newDLQTestServiceWithClient(t, config, consumer, cache, writer, deadLetter, newMockBlockchainClient())
}

// newDLQTestServiceWithClient is newDLQTestService with an explicit blockchain
// client so DLQ-routing tests can drive the cache-miss RPC fallback.
func newDLQTestServiceWithClient(t *testing.T, config Config, consumer outbound.SQSConsumer, cache outbound.BlockCache, writer outbound.S3Writer, deadLetter outbound.DeadLetterPublisher, client outbound.BlockchainClient) *Service {
	t.Helper()
	if config.Bucket == "" {
		config.Bucket = "test-bucket"
	}
	if config.ChainID == 0 {
		config.ChainID = 1
	}
	config.Workers = 1
	config.BatchSize = 1
	return newTestServiceWithClient(t, config, consumer, cache, writer, deadLetter, client)
}

// TestRun_CacheMiss_RPCNull_PublishesToDLQAndDeletes verifies that a cache miss
// whose RPC re-fetch returns a null block is permanent: routed to the DLQ and
// deleted from the source queue. (A real block would self-heal via RPC; a null
// body cannot be recovered.)
func TestRun_CacheMiss_RPCNull_PublishesToDLQAndDeletes(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()
	dlq := newMockDeadLetterPublisher()

	client := newMockBlockchainClient()
	client.data = outbound.BlockData{BlockNumber: 100, Block: json.RawMessage("null")}

	svc := newDLQTestServiceWithClient(t, Config{
		ChainID:             43114,
		Bucket:              "avax-bucket",
		ChainExpectations:   blockOnlyExpectations(),
		CacheMissMaxRetries: 0, // fail fast on miss
	}, consumer, cache, writer, dlq, client)

	// No block data in cache, RPC returns null -> permanent.
	event := createBlockEvent(43114, 100, 0)
	consumer.AddMessage(createSQSMessage("msg1", event))

	runUntilProcessed(t, svc, consumer, dlq)

	published := dlq.Published()
	if len(published) != 1 {
		t.Fatalf("expected 1 DLQ publish, got %d", len(published))
	}
	if published[0].groupID != "43114" {
		t.Errorf("expected groupID 43114, got %q", published[0].groupID)
	}
	if deleted := consumer.GetDeletedHandles(); len(deleted) != 1 {
		t.Errorf("expected 1 deleted message after DLQ publish, got %d", len(deleted))
	}
}

// TestRun_CacheMiss_RPCFallback_BacksUpAndDeletes verifies the self-heal path:
// a cache miss whose RPC re-fetch returns full data is backed up to S3, the
// message deleted, and nothing dead-lettered.
func TestRun_CacheMiss_RPCFallback_BacksUpAndDeletes(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()
	dlq := newMockDeadLetterPublisher()

	client := newMockBlockchainClient()
	client.data = outbound.BlockData{
		BlockNumber: 100,
		Block:       json.RawMessage(`{"number":"0x64","hash":"0xrpc"}`),
	}

	svc := newDLQTestServiceWithClient(t, Config{
		ChainExpectations:   blockOnlyExpectations(),
		CacheMissMaxRetries: 0,
	}, consumer, cache, writer, dlq, client)

	event := createBlockEvent(1, 100, 0)
	consumer.AddMessage(createSQSMessage("msg1", event))

	runUntilProcessed(t, svc, consumer, dlq)

	if calls := dlq.calls.Load(); calls != 0 {
		t.Errorf("expected 0 DLQ publishes for RPC fallback success, got %d", calls)
	}
	if deleted := consumer.GetDeletedHandles(); len(deleted) != 1 {
		t.Errorf("expected 1 deleted message after RPC fallback success, got %d", len(deleted))
	}
	if keys := writer.GetAllKeys(); len(keys) != 1 {
		t.Errorf("expected 1 file written from RPC fallback, got %d", len(keys))
	}
}

// TestRun_CacheMiss_AlreadyInS3_NoOpDeletes verifies that a cache miss whose
// files already exist in S3 is a no-op: the message is deleted, nothing is
// dead-lettered, and the RPC fallback is never invoked.
func TestRun_CacheMiss_AlreadyInS3_NoOpDeletes(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()
	dlq := newMockDeadLetterPublisher()

	client := newMockBlockchainClient()

	svc := newDLQTestServiceWithClient(t, Config{
		ChainExpectations:   blockOnlyExpectations(),
		CacheMissMaxRetries: 0,
	}, consumer, cache, writer, dlq, client)

	// File already in S3, nothing in cache -> no-op.
	writer.PresetFileExists("test-bucket", "0-999/100_0_block.json.gz")

	event := createBlockEvent(1, 100, 0)
	consumer.AddMessage(createSQSMessage("msg1", event))

	runUntilProcessed(t, svc, consumer, dlq)

	if calls := dlq.calls.Load(); calls != 0 {
		t.Errorf("expected 0 DLQ publishes for no-op, got %d", calls)
	}
	if deleted := consumer.GetDeletedHandles(); len(deleted) != 1 {
		t.Errorf("expected 1 deleted message for no-op, got %d", len(deleted))
	}
	if calls := client.Calls(); calls != 0 {
		t.Errorf("expected 0 RPC calls for no-op, got %d", calls)
	}
}

// TestRun_CacheMiss_RPCTransient_NoPublishNoDelete verifies that a cache miss
// whose RPC re-fetch fails transiently (network/batch error) leaves the message
// on the queue: not deleted, not dead-lettered, so SQS can redeliver.
func TestRun_CacheMiss_RPCTransient_NoPublishNoDelete(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()
	dlq := newMockDeadLetterPublisher()

	client := newMockBlockchainClient()
	client.callErr = errors.New("HTTP 503: upstream unavailable")

	svc := newDLQTestServiceWithClient(t, Config{
		ChainExpectations:   blockOnlyExpectations(),
		CacheMissMaxRetries: 0,
	}, consumer, cache, writer, dlq, client)

	event := createBlockEvent(1, 100, 0)
	consumer.AddMessage(createSQSMessage("msg1", event))

	runUntilTransient(t, svc, consumer)

	if calls := dlq.calls.Load(); calls != 0 {
		t.Errorf("expected 0 DLQ publishes for transient RPC error, got %d", calls)
	}
	if deleted := consumer.GetDeletedHandles(); len(deleted) != 0 {
		t.Errorf("expected 0 deleted messages for transient RPC error, got %d", len(deleted))
	}
}

func TestRun_TransientRedisError_NoPublishNoDelete(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()
	dlq := newMockDeadLetterPublisher()
	client := newMockBlockchainClient()

	svc := newDLQTestServiceWithClient(t, Config{
		ChainExpectations:   blockOnlyExpectations(),
		CacheMissMaxRetries: 3,
	}, consumer, cache, writer, dlq, client)

	event := createBlockEvent(1, 100, 0)
	// Redis error on every attempt -> transient, must NOT be dead-lettered.
	cache.SetGetError("block", 1, 100, 0, errors.New("redis connection refused"))
	consumer.AddMessage(createSQSMessage("msg1", event))

	runUntilTransient(t, svc, consumer)

	if calls := dlq.calls.Load(); calls != 0 {
		t.Errorf("expected 0 DLQ publishes for transient error, got %d", calls)
	}
	if deleted := consumer.GetDeletedHandles(); len(deleted) != 0 {
		t.Errorf("expected 0 deleted messages for transient error, got %d", len(deleted))
	}
	// A transient Redis getter error must not be mistaken for a miss, so the RPC
	// fallback must NOT fire.
	if calls := client.Calls(); calls != 0 {
		t.Errorf("expected 0 RPC calls for transient Redis error, got %d", calls)
	}
}

func TestRun_PublishFails_MessagePreserved(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()
	dlq := newMockDeadLetterPublisher()
	dlq.publishErr = errors.New("dlq send failed")

	client := newMockBlockchainClient()
	client.data = outbound.BlockData{BlockNumber: 100, Block: json.RawMessage("null")}

	svc := newDLQTestServiceWithClient(t, Config{
		ChainExpectations:   blockOnlyExpectations(),
		CacheMissMaxRetries: 0,
	}, consumer, cache, writer, dlq, client)

	// Confirmed miss -> RPC null -> permanent -> tries to publish, publish fails.
	event := createBlockEvent(1, 100, 0)
	consumer.AddMessage(createSQSMessage("msg1", event))

	runUntilProcessed(t, svc, consumer, dlq)

	if calls := dlq.calls.Load(); calls != 1 {
		t.Errorf("expected 1 DLQ publish attempt, got %d", calls)
	}
	// Publish failed -> message must NOT be deleted (preserved for redelivery).
	if deleted := consumer.GetDeletedHandles(); len(deleted) != 0 {
		t.Errorf("expected 0 deleted messages when DLQ publish fails, got %d", len(deleted))
	}
}

func TestRun_Success_DeletesOnlyNoPublish(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()
	dlq := newMockDeadLetterPublisher()

	svc := newDLQTestService(t, Config{
		ChainExpectations: blockOnlyExpectations(),
	}, consumer, cache, writer, dlq)

	ctx := context.Background()
	event := createBlockEvent(1, 100, 0)
	_ = cache.SetBlock(ctx, 1, 100, 0, json.RawMessage(`{"number":100}`))
	consumer.AddMessage(createSQSMessage("msg1", event))

	runUntilProcessed(t, svc, consumer, dlq)

	if calls := dlq.calls.Load(); calls != 0 {
		t.Errorf("expected 0 DLQ publishes on success, got %d", calls)
	}
	if deleted := consumer.GetDeletedHandles(); len(deleted) != 1 {
		t.Errorf("expected 1 deleted message on success, got %d", len(deleted))
	}
}

func TestRun_JSONParseFailure_PublishesToDLQAndDeletes(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()
	dlq := newMockDeadLetterPublisher()

	svc := newDLQTestService(t, Config{}, consumer, cache, writer, dlq)

	consumer.AddMessage(outbound.SQSMessage{
		MessageID:     "msg1",
		ReceiptHandle: "receipt-msg1",
		Body:          "this is not json",
	})

	runUntilProcessed(t, svc, consumer, dlq)

	if calls := dlq.calls.Load(); calls != 1 {
		t.Errorf("expected 1 DLQ publish for malformed JSON, got %d", calls)
	}
	if deleted := consumer.GetDeletedHandles(); len(deleted) != 1 {
		t.Errorf("expected 1 deleted message for malformed JSON, got %d", len(deleted))
	}
}

func TestRun_ChainMismatch_PublishesToDLQAndDeletes(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()
	dlq := newMockDeadLetterPublisher()

	svc := newDLQTestService(t, Config{ChainID: 1}, consumer, cache, writer, dlq)

	// Event for chain 137 hits a service configured for chain 1.
	event := createBlockEvent(137, 100, 0)
	consumer.AddMessage(createSQSMessage("msg1", event))

	runUntilProcessed(t, svc, consumer, dlq)

	if calls := dlq.calls.Load(); calls != 1 {
		t.Errorf("expected 1 DLQ publish for chain mismatch, got %d", calls)
	}
	published := dlq.Published()
	if len(published) == 1 && published[0].groupID != "1" {
		t.Errorf("expected groupID to be the service chain ID 1, got %q", published[0].groupID)
	}
	if deleted := consumer.GetDeletedHandles(); len(deleted) != 1 {
		t.Errorf("expected 1 deleted message for chain mismatch, got %d", len(deleted))
	}
}

func TestRun_NullPayload_PublishesToDLQAndDeletes(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()
	dlq := newMockDeadLetterPublisher()

	svc := newDLQTestService(t, Config{
		ChainExpectations: blockOnlyExpectations(),
	}, consumer, cache, writer, dlq)

	ctx := context.Background()
	event := createBlockEvent(1, 100, 0)
	// Block is "present" in cache but null -> permanent null-payload guard.
	_ = cache.SetBlock(ctx, 1, 100, 0, json.RawMessage("null"))
	consumer.AddMessage(createSQSMessage("msg1", event))

	runUntilProcessed(t, svc, consumer, dlq)

	if calls := dlq.calls.Load(); calls != 1 {
		t.Errorf("expected 1 DLQ publish for null payload, got %d", calls)
	}
	if deleted := consumer.GetDeletedHandles(); len(deleted) != 1 {
		t.Errorf("expected 1 deleted message for null payload, got %d", len(deleted))
	}
}

// TestRun_MissThenHitAcrossRetry exercises the app-level retry: the first cache
// reads are misses, a later read succeeds. The race must be absorbed and the
// message processed successfully (no DLQ).
func TestRun_MissThenHitAcrossRetry(t *testing.T) {
	consumer := newMockSQSConsumer()
	writer := newMockS3Writer()
	dlq := newMockDeadLetterPublisher()

	cache := newMissThenHitCache(2) // miss twice, then hit
	cache.blockData = json.RawMessage(`{"number":100}`)

	svc := newDLQTestService(t, Config{
		ChainExpectations:   blockOnlyExpectations(),
		CacheMissMaxRetries: 5,
	}, consumer, cache, writer, dlq)

	event := createBlockEvent(1, 100, 0)
	consumer.AddMessage(createSQSMessage("msg1", event))

	runUntilProcessed(t, svc, consumer, dlq)

	if calls := dlq.calls.Load(); calls != 0 {
		t.Errorf("expected 0 DLQ publishes when retry absorbs the miss, got %d", calls)
	}
	if deleted := consumer.GetDeletedHandles(); len(deleted) != 1 {
		t.Errorf("expected 1 deleted message after retry success, got %d", len(deleted))
	}
	if keys := writer.GetAllKeys(); len(keys) != 1 {
		t.Errorf("expected 1 file written after retry success, got %d", len(keys))
	}
}

// missThenHitCache returns nil (a miss) for the first missCount GetBlock calls
// and then the configured block data. Other getters return nothing.
type missThenHitCache struct {
	mu        sync.Mutex
	missCount int
	calls     int
	blockData json.RawMessage
}

var _ outbound.BlockCache = (*missThenHitCache)(nil)

func newMissThenHitCache(missCount int) *missThenHitCache {
	return &missThenHitCache{missCount: missCount}
}

func (m *missThenHitCache) GetBlock(ctx context.Context, chainID, blockNumber int64, version int) (json.RawMessage, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls++
	if m.calls <= m.missCount {
		return nil, nil
	}
	return m.blockData, nil
}

func (m *missThenHitCache) GetReceipts(ctx context.Context, chainID, blockNumber int64, version int) (json.RawMessage, error) {
	return nil, nil
}

func (m *missThenHitCache) GetTraces(ctx context.Context, chainID, blockNumber int64, version int) (json.RawMessage, error) {
	return nil, nil
}

func (m *missThenHitCache) GetBlobs(ctx context.Context, chainID, blockNumber int64, version int) (json.RawMessage, error) {
	return nil, nil
}

func (m *missThenHitCache) SetBlock(ctx context.Context, chainID, blockNumber int64, version int, data json.RawMessage) error {
	return nil
}

func (m *missThenHitCache) SetReceipts(ctx context.Context, chainID, blockNumber int64, version int, data json.RawMessage) error {
	return nil
}

func (m *missThenHitCache) SetTraces(ctx context.Context, chainID, blockNumber int64, version int, data json.RawMessage) error {
	return nil
}

func (m *missThenHitCache) SetBlobs(ctx context.Context, chainID, blockNumber int64, version int, data json.RawMessage) error {
	return nil
}

func (m *missThenHitCache) SetBlockData(ctx context.Context, chainID, blockNumber int64, version int, data outbound.BlockDataInput) error {
	return nil
}

func (m *missThenHitCache) DeleteBlock(ctx context.Context, chainID, blockNumber int64, version int) error {
	return nil
}

func (m *missThenHitCache) Close() error { return nil }

func TestConfigDefaults_CacheMissRetries(t *testing.T) {
	defaults := ConfigDefaults()
	if defaults.CacheMissMaxRetries != 3 {
		t.Errorf("expected CacheMissMaxRetries=3, got %d", defaults.CacheMissMaxRetries)
	}
}

// =============================================================================
// Tests: metrics recording
// =============================================================================

// mockMetrics records the status labels passed to the metrics recorder.
type mockMetrics struct {
	mu        sync.Mutex
	processed []string
	latencies []string
}

var _ outbound.BackupMetricsRecorder = (*mockMetrics)(nil)

func (m *mockMetrics) RecordProcessingLatency(ctx context.Context, duration time.Duration, status string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.latencies = append(m.latencies, status)
}

func (m *mockMetrics) RecordBlockProcessed(ctx context.Context, status string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.processed = append(m.processed, status)
}

func (m *mockMetrics) ProcessedStatuses() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, len(m.processed))
	copy(out, m.processed)
	return out
}

func (m *mockMetrics) LatencyStatuses() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := make([]string, len(m.latencies))
	copy(out, m.latencies)
	return out
}

func TestRun_Metrics_SuccessStatus(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()
	dlq := newMockDeadLetterPublisher()
	metrics := &mockMetrics{}

	svc := newDLQTestService(t, Config{
		ChainExpectations: blockOnlyExpectations(),
		Metrics:           metrics,
	}, consumer, cache, writer, dlq)

	ctx := context.Background()
	event := createBlockEvent(1, 100, 0)
	_ = cache.SetBlock(ctx, 1, 100, 0, json.RawMessage(`{"number":100}`))
	consumer.AddMessage(createSQSMessage("msg1", event))

	runUntilProcessed(t, svc, consumer, dlq)

	if !slices.Contains(metrics.ProcessedStatuses(), "success") {
		t.Errorf("expected a success processed metric, got %v", metrics.ProcessedStatuses())
	}
	if !slices.Contains(metrics.LatencyStatuses(), "success") {
		t.Errorf("expected a success latency metric, got %v", metrics.LatencyStatuses())
	}
}

func TestRun_Metrics_DeadLetteredStatus(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()
	dlq := newMockDeadLetterPublisher()
	metrics := &mockMetrics{}

	client := newMockBlockchainClient()
	client.data = outbound.BlockData{BlockNumber: 100, Block: json.RawMessage("null")}

	svc := newDLQTestServiceWithClient(t, Config{
		ChainExpectations:   blockOnlyExpectations(),
		CacheMissMaxRetries: 0,
		Metrics:             metrics,
	}, consumer, cache, writer, dlq, client)

	event := createBlockEvent(1, 100, 0)
	consumer.AddMessage(createSQSMessage("msg1", event))

	runUntilProcessed(t, svc, consumer, dlq)

	if !slices.Contains(metrics.ProcessedStatuses(), "dead_lettered") {
		t.Errorf("expected a dead_lettered processed metric, got %v", metrics.ProcessedStatuses())
	}
	if !slices.Contains(metrics.LatencyStatuses(), "error") {
		t.Errorf("expected an error latency metric, got %v", metrics.LatencyStatuses())
	}
}

func TestRun_Metrics_RPCFallbackStatus(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()
	dlq := newMockDeadLetterPublisher()
	metrics := &mockMetrics{}

	client := newMockBlockchainClient()
	client.data = outbound.BlockData{
		BlockNumber: 100,
		Block:       json.RawMessage(`{"number":"0x64"}`),
	}

	svc := newDLQTestServiceWithClient(t, Config{
		ChainExpectations:   blockOnlyExpectations(),
		CacheMissMaxRetries: 0,
		Metrics:             metrics,
	}, consumer, cache, writer, dlq, client)

	event := createBlockEvent(1, 100, 0)
	consumer.AddMessage(createSQSMessage("msg1", event))

	runUntilProcessed(t, svc, consumer, dlq)

	if !slices.Contains(metrics.ProcessedStatuses(), statusRPCFallback) {
		t.Errorf("expected a %s processed metric, got %v", statusRPCFallback, metrics.ProcessedStatuses())
	}
}

func TestRun_Metrics_AlreadyBackedUpStatus(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()
	dlq := newMockDeadLetterPublisher()
	metrics := &mockMetrics{}

	svc := newDLQTestService(t, Config{
		ChainExpectations: blockOnlyExpectations(),
		Metrics:           metrics,
	}, consumer, cache, writer, dlq)

	// File already in S3 (with a cache hit too) -> no-op already_backed_up.
	ctx := context.Background()
	_ = cache.SetBlock(ctx, 1, 100, 0, json.RawMessage(`{"number":100}`))
	writer.PresetFileExists("test-bucket", "0-999/100_0_block.json.gz")

	event := createBlockEvent(1, 100, 0)
	consumer.AddMessage(createSQSMessage("msg1", event))

	runUntilProcessed(t, svc, consumer, dlq)

	if !slices.Contains(metrics.ProcessedStatuses(), statusAlreadyBackedUp) {
		t.Errorf("expected a %s processed metric, got %v", statusAlreadyBackedUp, metrics.ProcessedStatuses())
	}
}

func TestRun_Metrics_TransientStatus(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()
	dlq := newMockDeadLetterPublisher()
	metrics := &mockMetrics{}

	svc := newDLQTestService(t, Config{
		ChainExpectations:   blockOnlyExpectations(),
		CacheMissMaxRetries: 0,
		Metrics:             metrics,
	}, consumer, cache, writer, dlq)

	event := createBlockEvent(1, 100, 0)
	cache.SetGetError("block", 1, 100, 0, errors.New("redis down"))
	consumer.AddMessage(createSQSMessage("msg1", event))

	runUntilTransient(t, svc, consumer)

	if !slices.Contains(metrics.ProcessedStatuses(), "transient_error") {
		t.Errorf("expected a transient_error processed metric, got %v", metrics.ProcessedStatuses())
	}
}

func TestRun_Metrics_DLQPublishFailedStatus(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()
	dlq := newMockDeadLetterPublisher()
	dlq.publishErr = errors.New("dlq unavailable")
	metrics := &mockMetrics{}

	svc := newDLQTestService(t, Config{
		ChainExpectations:   blockOnlyExpectations(),
		CacheMissMaxRetries: 0,
		Metrics:             metrics,
	}, consumer, cache, writer, dlq)

	event := createBlockEvent(1, 100, 0)
	consumer.AddMessage(createSQSMessage("msg1", event))

	runUntilProcessed(t, svc, consumer, dlq)

	if !slices.Contains(metrics.ProcessedStatuses(), "dlq_publish_failed") {
		t.Errorf("expected a dlq_publish_failed processed metric, got %v", metrics.ProcessedStatuses())
	}
}
