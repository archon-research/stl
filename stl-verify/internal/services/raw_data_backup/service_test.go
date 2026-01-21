package rawdatabackup

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
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
	count := maxMessages
	if count > len(m.messages) {
		count = len(m.messages)
	}

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

func newMockS3Writer() *mockS3Writer {
	return &mockS3Writer{
		files:        make(map[string][]byte),
		existsErrors: make(map[string]error),
		writeErrors:  make(map[string]error),
	}
}

func (m *mockS3Writer) WriteFile(ctx context.Context, bucket, key string, content io.Reader, compressGzip bool) error {
	m.writeCalled.Add(1)

	fullKey := bucket + "/" + key

	m.mu.Lock()
	defer m.mu.Unlock()

	if err, ok := m.writeErrors[fullKey]; ok {
		return err
	}
	if err, ok := m.writeErrors[key]; ok {
		return err
	}

	data, err := io.ReadAll(content)
	if err != nil {
		return err
	}

	m.files[fullKey] = data
	return nil
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

// =============================================================================
// Helper Functions
// =============================================================================

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
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

func createSNSWrappedMessage(id string, event outbound.BlockEvent) outbound.SQSMessage {
	innerBody, _ := json.Marshal(event)
	wrapper := struct {
		Message string `json:"Message"`
	}{
		Message: string(innerBody),
	}
	body, _ := json.Marshal(wrapper)
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
		Logger:  testLogger(),
	}, consumer, cache, writer)

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
	}, nil, cache, writer)

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
	}, consumer, nil, writer)

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
	}, consumer, cache, nil)

	if err == nil {
		t.Fatal("expected error for nil writer")
	}
	if !strings.Contains(err.Error(), "writer is required") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestNewService_EmptyBucket(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	_, err := NewService(Config{
		Bucket: "",
	}, consumer, cache, writer)

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
	}, consumer, cache, writer)

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
// Tests: getPartition
// =============================================================================

func TestGetPartition(t *testing.T) {
	svc := &Service{}

	tests := []struct {
		blockNumber int64
		expected    string
	}{
		// First partition: 0-1000
		{0, "0-1000"},
		{1, "0-1000"},
		{500, "0-1000"},
		{999, "0-1000"},
		{1000, "0-1000"},

		// Second partition: 1001-2000
		{1001, "1001-2000"},
		{1500, "1001-2000"},
		{2000, "1001-2000"},

		// Third partition: 2001-3000
		{2001, "2001-3000"},
		{3000, "2001-3000"},

		// Large block numbers
		{10000, "9001-10000"},
		{10001, "10001-11000"},
		{1000000, "999001-1000000"},
		{1000001, "1000001-1001000"},

		// Edge cases
		{BlockRangeSize, "0-1000"},
		{BlockRangeSize + 1, "1001-2000"},
	}

	for _, tt := range tests {
		t.Run(fmt.Sprintf("block_%d", tt.blockNumber), func(t *testing.T) {
			result := svc.getPartition(tt.blockNumber)
			if result != tt.expected {
				t.Errorf("getPartition(%d) = %s, expected %s", tt.blockNumber, result, tt.expected)
			}
		})
	}
}

// =============================================================================
// Tests: processMessage
// =============================================================================

func TestProcessMessage_Success(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	svc, _ := NewService(Config{
		ChainID: 1,
		Bucket:  "test-bucket",
		Logger:  testLogger(),
	}, consumer, cache, writer)

	// Set up cache with block data
	event := createBlockEvent(1, 100, 0)
	blockData := json.RawMessage(`{"number": 100}`)
	receiptsData := json.RawMessage(`[{"transactionHash": "0x123"}]`)
	tracesData := json.RawMessage(`[{"type": "call"}]`)
	blobsData := json.RawMessage(`[{"commitment": "0xabc"}]`)

	ctx := context.Background()
	_ = cache.SetBlock(ctx, 1, 100, 0, blockData)
	_ = cache.SetReceipts(ctx, 1, 100, 0, receiptsData)
	_ = cache.SetTraces(ctx, 1, 100, 0, tracesData)
	_ = cache.SetBlobs(ctx, 1, 100, 0, blobsData)

	msg := createSQSMessage("msg1", event)
	err := svc.processMessage(ctx, msg)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify all files were written
	keys := writer.GetAllKeys()
	if len(keys) != 4 {
		t.Errorf("expected 4 files, got %d: %v", len(keys), keys)
	}

	// Check specific keys
	expectedKeys := []string{
		"test-bucket/1/0-1000/100_0_block.json.gz",
		"test-bucket/1/0-1000/100_0_receipts.json.gz",
		"test-bucket/1/0-1000/100_0_traces.json.gz",
		"test-bucket/1/0-1000/100_0_blobs.json.gz",
	}

	for _, expectedKey := range expectedKeys {
		found := false
		for _, key := range keys {
			if key == expectedKey {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("expected key %s not found in %v", expectedKey, keys)
		}
	}
}

func TestProcessMessage_SNSWrapped(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	svc, _ := NewService(Config{
		ChainID: 1,
		Bucket:  "test-bucket",
		Logger:  testLogger(),
	}, consumer, cache, writer)

	// Set up cache with block data
	event := createBlockEvent(1, 100, 0)
	blockData := json.RawMessage(`{"number": 100}`)

	ctx := context.Background()
	_ = cache.SetBlock(ctx, 1, 100, 0, blockData)

	// Create SNS-wrapped message
	msg := createSNSWrappedMessage("msg1", event)
	err := svc.processMessage(ctx, msg)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify file was written
	_, exists := writer.GetFile("test-bucket", "1/0-1000/100_0_block.json.gz")
	if !exists {
		t.Error("expected block file to be written")
	}
}

func TestProcessMessage_BlockOnlyNoOptionalData(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	svc, _ := NewService(Config{
		ChainID: 1,
		Bucket:  "test-bucket",
		Logger:  testLogger(),
	}, consumer, cache, writer)

	// Set up cache with ONLY block data (no receipts, traces, blobs)
	event := createBlockEvent(1, 100, 0)
	blockData := json.RawMessage(`{"number": 100}`)

	ctx := context.Background()
	_ = cache.SetBlock(ctx, 1, 100, 0, blockData)

	msg := createSQSMessage("msg1", event)
	err := svc.processMessage(ctx, msg)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify only block file was written
	keys := writer.GetAllKeys()
	if len(keys) != 1 {
		t.Errorf("expected 1 file, got %d: %v", len(keys), keys)
	}
}

func TestProcessMessage_BlockNotInCache(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	svc, _ := NewService(Config{
		ChainID: 1,
		Bucket:  "test-bucket",
		Logger:  testLogger(),
	}, consumer, cache, writer)

	// Don't put any data in cache
	event := createBlockEvent(1, 100, 0)
	msg := createSQSMessage("msg1", event)

	err := svc.processMessage(context.Background(), msg)

	if err == nil {
		t.Fatal("expected error for missing block data")
	}
	if !strings.Contains(err.Error(), "block data not found in cache") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestProcessMessage_InvalidJSON(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	svc, _ := NewService(Config{
		ChainID: 1,
		Bucket:  "test-bucket",
		Logger:  testLogger(),
	}, consumer, cache, writer)

	msg := outbound.SQSMessage{
		MessageID:     "msg1",
		ReceiptHandle: "receipt1",
		Body:          "not valid json",
	}

	err := svc.processMessage(context.Background(), msg)

	if err == nil {
		t.Fatal("expected error for invalid JSON")
	}
	if !strings.Contains(err.Error(), "failed to parse block event") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestProcessMessage_CacheGetBlockError(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	svc, _ := NewService(Config{
		ChainID: 1,
		Bucket:  "test-bucket",
		Logger:  testLogger(),
	}, consumer, cache, writer)

	event := createBlockEvent(1, 100, 0)
	cache.SetGetError("block", 1, 100, 0, errors.New("redis connection failed"))

	msg := createSQSMessage("msg1", event)
	err := svc.processMessage(context.Background(), msg)

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
		Logger:  testLogger(),
	}, consumer, cache, writer)

	event := createBlockEvent(1, 100, 0)
	ctx := context.Background()
	_ = cache.SetBlock(ctx, 1, 100, 0, json.RawMessage(`{}`))
	cache.SetGetError("receipts", 1, 100, 0, errors.New("redis timeout"))

	msg := createSQSMessage("msg1", event)
	err := svc.processMessage(ctx, msg)

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

	svc, _ := NewService(Config{
		ChainID: 1,
		Bucket:  "test-bucket",
		Logger:  testLogger(),
	}, consumer, cache, writer)

	event := createBlockEvent(1, 100, 0)
	ctx := context.Background()
	_ = cache.SetBlock(ctx, 1, 100, 0, json.RawMessage(`{}`))
	cache.SetGetError("traces", 1, 100, 0, errors.New("redis error"))

	msg := createSQSMessage("msg1", event)
	err := svc.processMessage(ctx, msg)

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

	svc, _ := NewService(Config{
		ChainID: 1,
		Bucket:  "test-bucket",
		Logger:  testLogger(),
	}, consumer, cache, writer)

	event := createBlockEvent(1, 100, 0)
	ctx := context.Background()
	_ = cache.SetBlock(ctx, 1, 100, 0, json.RawMessage(`{}`))
	cache.SetGetError("blobs", 1, 100, 0, errors.New("redis error"))

	msg := createSQSMessage("msg1", event)
	err := svc.processMessage(ctx, msg)

	if err == nil {
		t.Fatal("expected error for cache failure")
	}
	if !strings.Contains(err.Error(), "failed to get blobs from cache") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestProcessMessage_S3WriteError(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	svc, _ := NewService(Config{
		ChainID: 1,
		Bucket:  "test-bucket",
		Logger:  testLogger(),
	}, consumer, cache, writer)

	event := createBlockEvent(1, 100, 0)
	ctx := context.Background()
	_ = cache.SetBlock(ctx, 1, 100, 0, json.RawMessage(`{}`))

	writer.SetWriteError("test-bucket/1/0-1000/100_0_block.json.gz", errors.New("S3 access denied"))

	msg := createSQSMessage("msg1", event)
	err := svc.processMessage(ctx, msg)

	if err == nil {
		t.Fatal("expected error for S3 write failure")
	}
	if !strings.Contains(err.Error(), "failed to write block to S3") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestProcessMessage_S3ExistsError(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	svc, _ := NewService(Config{
		ChainID: 1,
		Bucket:  "test-bucket",
		Logger:  testLogger(),
	}, consumer, cache, writer)

	event := createBlockEvent(1, 100, 0)
	ctx := context.Background()
	_ = cache.SetBlock(ctx, 1, 100, 0, json.RawMessage(`{}`))

	writer.SetExistsError("test-bucket/1/0-1000/100_0_block.json.gz", errors.New("S3 service unavailable"))

	msg := createSQSMessage("msg1", event)
	err := svc.processMessage(ctx, msg)

	if err == nil {
		t.Fatal("expected error for S3 exists check failure")
	}
	if !strings.Contains(err.Error(), "failed to check if") {
		t.Errorf("unexpected error message: %v", err)
	}
}

func TestProcessMessage_Idempotent_SkipsExistingFile(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	svc, _ := NewService(Config{
		ChainID: 1,
		Bucket:  "test-bucket",
		Logger:  testLogger(),
	}, consumer, cache, writer)

	event := createBlockEvent(1, 100, 0)
	ctx := context.Background()
	_ = cache.SetBlock(ctx, 1, 100, 0, json.RawMessage(`{}`))

	// Pre-populate S3 with existing file
	writer.PresetFileExists("test-bucket", "1/0-1000/100_0_block.json.gz")

	msg := createSQSMessage("msg1", event)
	err := svc.processMessage(ctx, msg)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// WriteFile should only be called for FileExists check, not for actual write
	// Since file exists, no new write should happen
	if writer.writeCalled.Load() != 0 {
		t.Errorf("expected 0 write calls (file existed), got %d", writer.writeCalled.Load())
	}
}

func TestProcessMessage_DifferentVersionsAreSeparate(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	svc, _ := NewService(Config{
		ChainID: 1,
		Bucket:  "test-bucket",
		Logger:  testLogger(),
	}, consumer, cache, writer)

	ctx := context.Background()

	// Version 0
	event0 := createBlockEvent(1, 100, 0)
	_ = cache.SetBlock(ctx, 1, 100, 0, json.RawMessage(`{"version": 0}`))
	msg0 := createSQSMessage("msg0", event0)
	_ = svc.processMessage(ctx, msg0)

	// Version 1 (reorg)
	event1 := createBlockEvent(1, 100, 1)
	_ = cache.SetBlock(ctx, 1, 100, 1, json.RawMessage(`{"version": 1}`))
	msg1 := createSQSMessage("msg1", event1)
	_ = svc.processMessage(ctx, msg1)

	// Both files should exist
	_, exists0 := writer.GetFile("test-bucket", "1/0-1000/100_0_block.json.gz")
	_, exists1 := writer.GetFile("test-bucket", "1/0-1000/100_1_block.json.gz")

	if !exists0 {
		t.Error("expected version 0 file to exist")
	}
	if !exists1 {
		t.Error("expected version 1 file to exist")
	}
}

func TestProcessMessage_DifferentChainsAreSeparate(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	svc, _ := NewService(Config{
		ChainID: 1,
		Bucket:  "test-bucket",
		Logger:  testLogger(),
	}, consumer, cache, writer)

	ctx := context.Background()

	// Mainnet (chain 1)
	event1 := createBlockEvent(1, 100, 0)
	_ = cache.SetBlock(ctx, 1, 100, 0, json.RawMessage(`{"chain": 1}`))
	msg1 := createSQSMessage("msg1", event1)
	_ = svc.processMessage(ctx, msg1)

	// Arbitrum (chain 42161)
	event42161 := createBlockEvent(42161, 100, 0)
	_ = cache.SetBlock(ctx, 42161, 100, 0, json.RawMessage(`{"chain": 42161}`))
	msg42161 := createSQSMessage("msg42161", event42161)
	_ = svc.processMessage(ctx, msg42161)

	// Both files should exist in different chain directories
	_, existsMainnet := writer.GetFile("test-bucket", "1/0-1000/100_0_block.json.gz")
	_, existsArbitrum := writer.GetFile("test-bucket", "42161/0-1000/100_0_block.json.gz")

	if !existsMainnet {
		t.Error("expected mainnet file to exist")
	}
	if !existsArbitrum {
		t.Error("expected arbitrum file to exist")
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
		ChainID:   1,
		Bucket:    "test-bucket",
		Workers:   1,
		BatchSize: 1,
		Logger:    testLogger(),
	}, consumer, cache, writer)

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
		Logger:  testLogger(),
	}, consumer, cache, writer)

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
		Logger:  testLogger(),
	}, consumer, cache, writer)

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
		Logger:  testLogger(),
	}, consumer, cache, writer)

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

func TestRun_DoesNotDeleteMessageOnProcessError(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	svc, _ := NewService(Config{
		ChainID:   1,
		Bucket:    "test-bucket",
		Workers:   1,
		BatchSize: 1,
		Logger:    testLogger(),
	}, consumer, cache, writer)

	ctx := context.Background()

	// Add message but DON'T add block data to cache - this will cause process error
	event := createBlockEvent(1, 100, 0)
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

	// Message should NOT be deleted because processing failed
	deletedHandles := consumer.GetDeletedHandles()
	if len(deletedHandles) != 0 {
		t.Errorf("expected 0 deleted messages (process failed), got %d", len(deletedHandles))
	}
}

func TestRun_DeletesMessageOnSuccess(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	svc, _ := NewService(Config{
		ChainID:   1,
		Bucket:    "test-bucket",
		Workers:   1,
		BatchSize: 1,
		Logger:    testLogger(),
	}, consumer, cache, writer)

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
		ChainID:   1,
		Bucket:    "test-bucket",
		Workers:   1,
		BatchSize: 1,
		Logger:    testLogger(),
	}, consumer, cache, writer)

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
		ChainID:   1,
		Bucket:    "test-bucket",
		Workers:   4,
		BatchSize: 10,
		Logger:    testLogger(),
	}, consumer, cache, writer)

	ctx := context.Background()

	// Add multiple messages
	for i := 0; i < 10; i++ {
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

		count := maxMessages
		if count > len(consumer.messages) {
			count = len(consumer.messages)
		}
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
		ChainID: 1,
		Bucket:  "test-bucket",
		Logger:  testLogger(),
	}, consumer, cache, writer)

	event := createBlockEvent(1, 0, 0)
	ctx := context.Background()
	_ = cache.SetBlock(ctx, 1, 0, 0, json.RawMessage(`{"block": 0}`))

	msg := createSQSMessage("msg0", event)
	err := svc.processMessage(ctx, msg)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Should be in partition 0-1000
	_, exists := writer.GetFile("test-bucket", "1/0-1000/0_0_block.json.gz")
	if !exists {
		t.Error("expected block 0 file to exist")
	}
}

func TestProcessMessage_LargeBlockNumber(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	svc, _ := NewService(Config{
		ChainID: 1,
		Bucket:  "test-bucket",
		Logger:  testLogger(),
	}, consumer, cache, writer)

	blockNum := int64(20000000) // Block 20 million
	event := createBlockEvent(1, blockNum, 0)
	ctx := context.Background()
	_ = cache.SetBlock(ctx, 1, blockNum, 0, json.RawMessage(`{}`))

	msg := createSQSMessage("msg1", event)
	err := svc.processMessage(ctx, msg)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Check partition calculation
	expectedPartition := "19999001-20000000"
	expectedKey := fmt.Sprintf("1/%s/%d_0_block.json.gz", expectedPartition, blockNum)
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
		ChainID: 1,
		Bucket:  "test-bucket",
		Logger:  testLogger(),
	}, consumer, cache, writer)

	// High version number (many reorgs)
	event := createBlockEvent(1, 100, 999)
	ctx := context.Background()
	_ = cache.SetBlock(ctx, 1, 100, 999, json.RawMessage(`{}`))

	msg := createSQSMessage("msg1", event)
	err := svc.processMessage(ctx, msg)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, exists := writer.GetFile("test-bucket", "1/0-1000/100_999_block.json.gz")
	if !exists {
		t.Error("expected high version file to exist")
	}
}

func TestProcessMessage_EmptyBlockData(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	svc, _ := NewService(Config{
		ChainID: 1,
		Bucket:  "test-bucket",
		Logger:  testLogger(),
	}, consumer, cache, writer)

	event := createBlockEvent(1, 100, 0)
	ctx := context.Background()
	// Empty but valid JSON
	_ = cache.SetBlock(ctx, 1, 100, 0, json.RawMessage(`{}`))

	msg := createSQSMessage("msg1", event)
	err := svc.processMessage(ctx, msg)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestProcessMessage_LargeBlockData(t *testing.T) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	svc, _ := NewService(Config{
		ChainID: 1,
		Bucket:  "test-bucket",
		Logger:  testLogger(),
	}, consumer, cache, writer)

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
	err := svc.processMessage(ctx, msg)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	data, exists := writer.GetFile("test-bucket", "1/0-1000/100_0_block.json.gz")
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
		Logger:  testLogger(),
	}, consumer, cache, writer)

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
// Benchmark Tests
// =============================================================================

func BenchmarkGetPartition(b *testing.B) {
	svc := &Service{}
	blockNumbers := []int64{0, 500, 1000, 1001, 5000, 1000000, 20000000}

	for _, bn := range blockNumbers {
		b.Run(fmt.Sprintf("block_%d", bn), func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				_ = svc.getPartition(bn)
			}
		})
	}
}

func BenchmarkProcessMessage(b *testing.B) {
	consumer := newMockSQSConsumer()
	cache := newMockBlockCache()
	writer := newMockS3Writer()

	svc, _ := NewService(Config{
		ChainID: 1,
		Bucket:  "test-bucket",
		Logger:  testLogger(),
	}, consumer, cache, writer)

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

		_ = svc.processMessage(ctx, msg)
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
		Logger:  testLogger(),
	}, consumer, cache, writer)

	event := createBlockEvent(1, 100, 0)
	ctx := context.Background()
	_ = cache.SetBlock(ctx, 1, 100, 0, json.RawMessage(`{}`))
	_ = cache.SetReceipts(ctx, 1, 100, 0, json.RawMessage(`[]`))

	writer.SetWriteError("test-bucket/1/0-1000/100_0_receipts.json.gz", errors.New("S3 error"))

	msg := createSQSMessage("msg1", event)
	err := svc.processMessage(ctx, msg)

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
		Logger:  testLogger(),
	}, consumer, cache, writer)

	event := createBlockEvent(1, 100, 0)
	ctx := context.Background()
	_ = cache.SetBlock(ctx, 1, 100, 0, json.RawMessage(`{}`))
	_ = cache.SetTraces(ctx, 1, 100, 0, json.RawMessage(`[]`))

	writer.SetWriteError("test-bucket/1/0-1000/100_0_traces.json.gz", errors.New("S3 error"))

	msg := createSQSMessage("msg1", event)
	err := svc.processMessage(ctx, msg)

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
		Logger:  testLogger(),
	}, consumer, cache, writer)

	event := createBlockEvent(1, 100, 0)
	ctx := context.Background()
	_ = cache.SetBlock(ctx, 1, 100, 0, json.RawMessage(`{}`))
	_ = cache.SetBlobs(ctx, 1, 100, 0, json.RawMessage(`[]`))

	writer.SetWriteError("test-bucket/1/0-1000/100_0_blobs.json.gz", errors.New("S3 error"))

	msg := createSQSMessage("msg1", event)
	err := svc.processMessage(ctx, msg)

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
		Logger:    testLogger(),
	}, consumer, cache, writer)

	// Create many messages
	for i := 0; i < 100; i++ {
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
		Logger:  testLogger(),
	}, consumer, cache, writer)

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

	svc, _ := NewService(Config{
		ChainID: 1,
		Bucket:  "test-bucket",
		Logger:  testLogger(),
	}, consumer, cache, writer)

	event := createBlockEvent(1, 100, 0)
	ctx := context.Background()

	// Set all data types with actual content
	_ = cache.SetBlock(ctx, 1, 100, 0, json.RawMessage(`{"number": "0x64", "hash": "0x123"}`))
	_ = cache.SetReceipts(ctx, 1, 100, 0, json.RawMessage(`[{"transactionIndex": "0x0"}]`))
	_ = cache.SetTraces(ctx, 1, 100, 0, json.RawMessage(`[{"action": {"callType": "call"}}]`))
	_ = cache.SetBlobs(ctx, 1, 100, 0, json.RawMessage(`[{"commitment": "0xabc"}]`))

	msg := createSQSMessage("msg1", event)
	err := svc.processMessage(ctx, msg)

	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Verify all 4 files written
	keys := writer.GetAllKeys()
	if len(keys) != 4 {
		t.Errorf("expected 4 files, got %d: %v", len(keys), keys)
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

func TestProcessMessage_PartitionBoundariesComprehensive(t *testing.T) {
	svc := &Service{}

	// Test all boundary conditions thoroughly
	testCases := []struct {
		blockNumber int64
		expected    string
	}{
		// First partition boundaries
		{0, "0-1000"},
		{1, "0-1000"},
		{999, "0-1000"},
		{1000, "0-1000"},

		// Second partition boundaries
		{1001, "1001-2000"},
		{1002, "1001-2000"},
		{1999, "1001-2000"},
		{2000, "1001-2000"},

		// Third partition boundaries
		{2001, "2001-3000"},
		{2002, "2001-3000"},
		{2999, "2001-3000"},
		{3000, "2001-3000"},

		// Large number boundaries
		{9999999, "9999001-10000000"},
		{10000000, "9999001-10000000"},
		{10000001, "10000001-10001000"},
	}

	for _, tc := range testCases {
		result := svc.getPartition(tc.blockNumber)
		if result != tc.expected {
			t.Errorf("getPartition(%d) = %s, expected %s", tc.blockNumber, result, tc.expected)
		}
	}
}
