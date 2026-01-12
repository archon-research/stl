//go:build integration

package redis

import (
	"context"
	"encoding/json"
	"fmt"
	"testing"
	"time"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// setupRedis creates a Redis container and returns a connected BlockCache.
func setupRedis(t *testing.T, ttl time.Duration) (*BlockCache, func()) {
	t.Helper()
	ctx := context.Background()

	req := testcontainers.ContainerRequest{
		Image:        "redis:7-alpine",
		ExposedPorts: []string{"6379/tcp"},
		WaitingFor:   wait.ForLog("Ready to accept connections").WithStartupTimeout(60 * time.Second),
	}

	container, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: req,
		Started:          true,
	})
	if err != nil {
		t.Fatalf("failed to start Redis container: %v", err)
	}

	host, err := container.Host(ctx)
	if err != nil {
		t.Fatalf("failed to get container host: %v", err)
	}

	port, err := container.MappedPort(ctx, "6379")
	if err != nil {
		t.Fatalf("failed to get container port: %v", err)
	}

	cfg := Config{
		Addr:      fmt.Sprintf("%s:%s", host, port.Port()),
		Password:  "",
		DB:        0,
		TTL:       ttl,
		KeyPrefix: "test",
	}

	cache, err := NewBlockCache(cfg, nil)
	if err != nil {
		t.Fatalf("failed to create block cache: %v", err)
	}

	// Wait for connection
	for i := 0; i < 30; i++ {
		if err := cache.Ping(ctx); err == nil {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}

	cleanup := func() {
		cache.Close()
		container.Terminate(ctx)
	}

	return cache, cleanup
}

// --- Test: Ping ---

func TestPing_Success(t *testing.T) {
	cache, cleanup := setupRedis(t, 24*time.Hour)
	defer cleanup()

	ctx := context.Background()
	if err := cache.Ping(ctx); err != nil {
		t.Fatalf("ping failed: %v", err)
	}
}

// --- Test: SetBlock and GetBlock ---

func TestSetBlock_AndGetBlock_RoundTrip(t *testing.T) {
	cache, cleanup := setupRedis(t, 24*time.Hour)
	defer cleanup()

	ctx := context.Background()
	chainID := int64(1)
	blockNumber := int64(12345)
	version := 1
	blockData := json.RawMessage(`{"number":"0x3039","hash":"0xabc123","transactions":[]}`)

	// Set block
	if err := cache.SetBlock(ctx, chainID, blockNumber, version, blockData); err != nil {
		t.Fatalf("SetBlock failed: %v", err)
	}

	// Get block
	retrieved, err := cache.GetBlock(ctx, chainID, blockNumber, version)
	if err != nil {
		t.Fatalf("GetBlock failed: %v", err)
	}

	if string(retrieved) != string(blockData) {
		t.Errorf("expected block data=%s, got %s", blockData, retrieved)
	}
}

func TestGetBlock_NotFound_ReturnsNil(t *testing.T) {
	cache, cleanup := setupRedis(t, 24*time.Hour)
	defer cleanup()

	ctx := context.Background()
	retrieved, err := cache.GetBlock(ctx, 1, 99999, 1)
	if err != nil {
		t.Fatalf("GetBlock failed: %v", err)
	}

	if retrieved != nil {
		t.Errorf("expected nil for non-existent block, got %s", retrieved)
	}
}

// --- Test: SetReceipts and GetReceipts ---

func TestSetReceipts_AndGetReceipts_RoundTrip(t *testing.T) {
	cache, cleanup := setupRedis(t, 24*time.Hour)
	defer cleanup()

	ctx := context.Background()
	chainID := int64(1)
	blockNumber := int64(12345)
	version := 1
	receiptsData := json.RawMessage(`[{"transactionHash":"0xabc","status":"0x1","logs":[]}]`)

	// Set receipts
	if err := cache.SetReceipts(ctx, chainID, blockNumber, version, receiptsData); err != nil {
		t.Fatalf("SetReceipts failed: %v", err)
	}

	// Get receipts
	retrieved, err := cache.GetReceipts(ctx, chainID, blockNumber, version)
	if err != nil {
		t.Fatalf("GetReceipts failed: %v", err)
	}

	if string(retrieved) != string(receiptsData) {
		t.Errorf("expected receipts data=%s, got %s", receiptsData, retrieved)
	}
}

func TestGetReceipts_NotFound_ReturnsNil(t *testing.T) {
	cache, cleanup := setupRedis(t, 24*time.Hour)
	defer cleanup()

	ctx := context.Background()
	retrieved, err := cache.GetReceipts(ctx, 1, 99999, 1)
	if err != nil {
		t.Fatalf("GetReceipts failed: %v", err)
	}

	if retrieved != nil {
		t.Errorf("expected nil for non-existent receipts, got %s", retrieved)
	}
}

// --- Test: SetTraces and GetTraces ---

func TestSetTraces_AndGetTraces_RoundTrip(t *testing.T) {
	cache, cleanup := setupRedis(t, 24*time.Hour)
	defer cleanup()

	ctx := context.Background()
	chainID := int64(1)
	blockNumber := int64(12345)
	version := 1
	tracesData := json.RawMessage(`[{"type":"call","from":"0x123","to":"0x456"}]`)

	// Set traces
	if err := cache.SetTraces(ctx, chainID, blockNumber, version, tracesData); err != nil {
		t.Fatalf("SetTraces failed: %v", err)
	}

	// Get traces
	retrieved, err := cache.GetTraces(ctx, chainID, blockNumber, version)
	if err != nil {
		t.Fatalf("GetTraces failed: %v", err)
	}

	if string(retrieved) != string(tracesData) {
		t.Errorf("expected traces data=%s, got %s", tracesData, retrieved)
	}
}

func TestGetTraces_NotFound_ReturnsNil(t *testing.T) {
	cache, cleanup := setupRedis(t, 24*time.Hour)
	defer cleanup()

	ctx := context.Background()
	retrieved, err := cache.GetTraces(ctx, 1, 99999, 1)
	if err != nil {
		t.Fatalf("GetTraces failed: %v", err)
	}

	if retrieved != nil {
		t.Errorf("expected nil for non-existent traces, got %s", retrieved)
	}
}

// --- Test: SetBlobs and GetBlobs ---

func TestSetBlobs_AndGetBlobs_RoundTrip(t *testing.T) {
	cache, cleanup := setupRedis(t, 24*time.Hour)
	defer cleanup()

	ctx := context.Background()
	chainID := int64(1)
	blockNumber := int64(18000000)
	version := 1
	blobsData := json.RawMessage(`[{"index":"0x0","blob":"0x0102..."}]`)

	// Set blobs
	if err := cache.SetBlobs(ctx, chainID, blockNumber, version, blobsData); err != nil {
		t.Fatalf("SetBlobs failed: %v", err)
	}

	// Get blobs
	retrieved, err := cache.GetBlobs(ctx, chainID, blockNumber, version)
	if err != nil {
		t.Fatalf("GetBlobs failed: %v", err)
	}

	if string(retrieved) != string(blobsData) {
		t.Errorf("expected blobs data=%s, got %s", blobsData, retrieved)
	}
}

func TestGetBlobs_NotFound_ReturnsNil(t *testing.T) {
	cache, cleanup := setupRedis(t, 24*time.Hour)
	defer cleanup()

	ctx := context.Background()
	retrieved, err := cache.GetBlobs(ctx, 1, 99999, 1)
	if err != nil {
		t.Fatalf("GetBlobs failed: %v", err)
	}

	if retrieved != nil {
		t.Errorf("expected nil for non-existent blobs, got %s", retrieved)
	}
}

// --- Test: DeleteBlock ---

func TestDeleteBlock_RemovesAllDataTypes(t *testing.T) {
	cache, cleanup := setupRedis(t, 24*time.Hour)
	defer cleanup()

	ctx := context.Background()
	chainID := int64(1)
	blockNumber := int64(12345)
	version := 1

	// Set all data types
	cache.SetBlock(ctx, chainID, blockNumber, version, json.RawMessage(`{"block":true}`))
	cache.SetReceipts(ctx, chainID, blockNumber, version, json.RawMessage(`{"receipts":true}`))
	cache.SetTraces(ctx, chainID, blockNumber, version, json.RawMessage(`{"traces":true}`))
	cache.SetBlobs(ctx, chainID, blockNumber, version, json.RawMessage(`{"blobs":true}`))

	// Delete block
	if err := cache.DeleteBlock(ctx, chainID, blockNumber, version); err != nil {
		t.Fatalf("DeleteBlock failed: %v", err)
	}

	// Verify all data types are deleted
	block, _ := cache.GetBlock(ctx, chainID, blockNumber, version)
	if block != nil {
		t.Errorf("expected block to be deleted, got %s", block)
	}

	receipts, _ := cache.GetReceipts(ctx, chainID, blockNumber, version)
	if receipts != nil {
		t.Errorf("expected receipts to be deleted, got %s", receipts)
	}

	traces, _ := cache.GetTraces(ctx, chainID, blockNumber, version)
	if traces != nil {
		t.Errorf("expected traces to be deleted, got %s", traces)
	}

	blobs, _ := cache.GetBlobs(ctx, chainID, blockNumber, version)
	if blobs != nil {
		t.Errorf("expected blobs to be deleted, got %s", blobs)
	}
}

func TestDeleteBlock_OnlyDeletesSpecificVersion(t *testing.T) {
	cache, cleanup := setupRedis(t, 24*time.Hour)
	defer cleanup()

	ctx := context.Background()
	chainID := int64(1)
	blockNumber := int64(12345)

	// Set two versions
	cache.SetBlock(ctx, chainID, blockNumber, 1, json.RawMessage(`{"version":1}`))
	cache.SetBlock(ctx, chainID, blockNumber, 2, json.RawMessage(`{"version":2}`))

	// Delete version 1 only
	if err := cache.DeleteBlock(ctx, chainID, blockNumber, 1); err != nil {
		t.Fatalf("DeleteBlock failed: %v", err)
	}

	// Version 1 should be gone
	block1, _ := cache.GetBlock(ctx, chainID, blockNumber, 1)
	if block1 != nil {
		t.Errorf("expected version 1 to be deleted, got %s", block1)
	}

	// Version 2 should still exist
	block2, _ := cache.GetBlock(ctx, chainID, blockNumber, 2)
	if block2 == nil {
		t.Error("expected version 2 to still exist, got nil")
	}
}

func TestDeleteBlock_NonExistent_NoError(t *testing.T) {
	cache, cleanup := setupRedis(t, 24*time.Hour)
	defer cleanup()

	ctx := context.Background()

	// Delete non-existent block should not error
	err := cache.DeleteBlock(ctx, 1, 99999, 1)
	if err != nil {
		t.Errorf("expected no error for non-existent block, got %v", err)
	}
}

// --- Test: Version isolation ---

func TestBlockCache_VersionIsolation(t *testing.T) {
	cache, cleanup := setupRedis(t, 24*time.Hour)
	defer cleanup()

	ctx := context.Background()
	chainID := int64(1)
	blockNumber := int64(12345)

	// Set same block with different versions (simulating reorgs)
	v1Data := json.RawMessage(`{"hash":"0xoriginal"}`)
	v2Data := json.RawMessage(`{"hash":"0xreorged"}`)

	cache.SetBlock(ctx, chainID, blockNumber, 1, v1Data)
	cache.SetBlock(ctx, chainID, blockNumber, 2, v2Data)

	// Retrieve and verify both versions exist independently
	retrieved1, _ := cache.GetBlock(ctx, chainID, blockNumber, 1)
	if string(retrieved1) != string(v1Data) {
		t.Errorf("version 1: expected %s, got %s", v1Data, retrieved1)
	}

	retrieved2, _ := cache.GetBlock(ctx, chainID, blockNumber, 2)
	if string(retrieved2) != string(v2Data) {
		t.Errorf("version 2: expected %s, got %s", v2Data, retrieved2)
	}
}

// --- Test: Chain isolation ---

func TestBlockCache_ChainIsolation(t *testing.T) {
	cache, cleanup := setupRedis(t, 24*time.Hour)
	defer cleanup()

	ctx := context.Background()
	blockNumber := int64(12345)
	version := 1

	// Set same block number on different chains
	mainnetData := json.RawMessage(`{"chain":"mainnet"}`)
	arbitrumData := json.RawMessage(`{"chain":"arbitrum"}`)

	cache.SetBlock(ctx, 1, blockNumber, version, mainnetData)      // Mainnet
	cache.SetBlock(ctx, 42161, blockNumber, version, arbitrumData) // Arbitrum

	// Retrieve and verify isolation
	mainnet, _ := cache.GetBlock(ctx, 1, blockNumber, version)
	if string(mainnet) != string(mainnetData) {
		t.Errorf("mainnet: expected %s, got %s", mainnetData, mainnet)
	}

	arbitrum, _ := cache.GetBlock(ctx, 42161, blockNumber, version)
	if string(arbitrum) != string(arbitrumData) {
		t.Errorf("arbitrum: expected %s, got %s", arbitrumData, arbitrum)
	}
}

// --- Test: Data type isolation ---

func TestBlockCache_DataTypeIsolation(t *testing.T) {
	cache, cleanup := setupRedis(t, 24*time.Hour)
	defer cleanup()

	ctx := context.Background()
	chainID := int64(1)
	blockNumber := int64(12345)
	version := 1

	// Set different data types for same block
	blockData := json.RawMessage(`{"type":"block"}`)
	receiptsData := json.RawMessage(`{"type":"receipts"}`)
	tracesData := json.RawMessage(`{"type":"traces"}`)
	blobsData := json.RawMessage(`{"type":"blobs"}`)

	cache.SetBlock(ctx, chainID, blockNumber, version, blockData)
	cache.SetReceipts(ctx, chainID, blockNumber, version, receiptsData)
	cache.SetTraces(ctx, chainID, blockNumber, version, tracesData)
	cache.SetBlobs(ctx, chainID, blockNumber, version, blobsData)

	// Verify each data type is isolated
	block, _ := cache.GetBlock(ctx, chainID, blockNumber, version)
	if string(block) != string(blockData) {
		t.Errorf("block: expected %s, got %s", blockData, block)
	}

	receipts, _ := cache.GetReceipts(ctx, chainID, blockNumber, version)
	if string(receipts) != string(receiptsData) {
		t.Errorf("receipts: expected %s, got %s", receiptsData, receipts)
	}

	traces, _ := cache.GetTraces(ctx, chainID, blockNumber, version)
	if string(traces) != string(tracesData) {
		t.Errorf("traces: expected %s, got %s", tracesData, traces)
	}

	blobs, _ := cache.GetBlobs(ctx, chainID, blockNumber, version)
	if string(blobs) != string(blobsData) {
		t.Errorf("blobs: expected %s, got %s", blobsData, blobs)
	}
}

// --- Test: TTL expiration ---

func TestBlockCache_TTLExpiration(t *testing.T) {
	// Use a very short TTL for testing
	cache, cleanup := setupRedis(t, 1*time.Second)
	defer cleanup()

	ctx := context.Background()
	chainID := int64(1)
	blockNumber := int64(12345)
	version := 1
	blockData := json.RawMessage(`{"test":"ttl"}`)

	// Set block
	if err := cache.SetBlock(ctx, chainID, blockNumber, version, blockData); err != nil {
		t.Fatalf("SetBlock failed: %v", err)
	}

	// Immediately should be retrievable
	retrieved, _ := cache.GetBlock(ctx, chainID, blockNumber, version)
	if retrieved == nil {
		t.Fatal("block should exist immediately after setting")
	}

	// Wait for TTL to expire
	time.Sleep(2 * time.Second)

	// Should be gone after TTL
	retrieved, err := cache.GetBlock(ctx, chainID, blockNumber, version)
	if err != nil {
		t.Fatalf("GetBlock failed: %v", err)
	}
	if retrieved != nil {
		t.Errorf("expected block to be expired, got %s", retrieved)
	}
}

// --- Test: Overwrite existing data ---

func TestBlockCache_OverwriteExisting(t *testing.T) {
	cache, cleanup := setupRedis(t, 24*time.Hour)
	defer cleanup()

	ctx := context.Background()
	chainID := int64(1)
	blockNumber := int64(12345)
	version := 1

	originalData := json.RawMessage(`{"original":true}`)
	updatedData := json.RawMessage(`{"updated":true}`)

	// Set original
	cache.SetBlock(ctx, chainID, blockNumber, version, originalData)

	// Overwrite with new data
	if err := cache.SetBlock(ctx, chainID, blockNumber, version, updatedData); err != nil {
		t.Fatalf("SetBlock overwrite failed: %v", err)
	}

	// Retrieve and verify it's updated
	retrieved, _ := cache.GetBlock(ctx, chainID, blockNumber, version)
	if string(retrieved) != string(updatedData) {
		t.Errorf("expected updated data=%s, got %s", updatedData, retrieved)
	}
}

// --- Test: Large data ---

func TestBlockCache_LargeData(t *testing.T) {
	cache, cleanup := setupRedis(t, 24*time.Hour)
	defer cleanup()

	ctx := context.Background()
	chainID := int64(1)
	blockNumber := int64(12345)
	version := 1

	// Create a large JSON payload (simulating a block with many transactions)
	largePayload := make([]byte, 1024*100) // 100KB
	for i := range largePayload {
		largePayload[i] = 'x'
	}
	largeData := json.RawMessage(fmt.Sprintf(`{"data":"%s"}`, string(largePayload)))

	// Set large block
	if err := cache.SetBlock(ctx, chainID, blockNumber, version, largeData); err != nil {
		t.Fatalf("SetBlock with large data failed: %v", err)
	}

	// Retrieve and verify
	retrieved, err := cache.GetBlock(ctx, chainID, blockNumber, version)
	if err != nil {
		t.Fatalf("GetBlock with large data failed: %v", err)
	}

	if len(retrieved) != len(largeData) {
		t.Errorf("expected data length=%d, got %d", len(largeData), len(retrieved))
	}
}

// --- Test: Empty data ---

func TestBlockCache_EmptyData(t *testing.T) {
	cache, cleanup := setupRedis(t, 24*time.Hour)
	defer cleanup()

	ctx := context.Background()
	chainID := int64(1)
	blockNumber := int64(12345)
	version := 1

	emptyData := json.RawMessage(`{}`)

	// Set empty block
	if err := cache.SetBlock(ctx, chainID, blockNumber, version, emptyData); err != nil {
		t.Fatalf("SetBlock with empty data failed: %v", err)
	}

	// Retrieve and verify
	retrieved, err := cache.GetBlock(ctx, chainID, blockNumber, version)
	if err != nil {
		t.Fatalf("GetBlock with empty data failed: %v", err)
	}

	if string(retrieved) != string(emptyData) {
		t.Errorf("expected empty data={}, got %s", retrieved)
	}
}

// --- Test: Concurrent access ---

func TestBlockCache_ConcurrentAccess(t *testing.T) {
	cache, cleanup := setupRedis(t, 24*time.Hour)
	defer cleanup()

	ctx := context.Background()
	chainID := int64(1)
	version := 1

	// Spawn multiple goroutines writing to different blocks
	done := make(chan bool)
	for i := 0; i < 10; i++ {
		go func(blockNum int64) {
			data := json.RawMessage(fmt.Sprintf(`{"block":%d}`, blockNum))
			cache.SetBlock(ctx, chainID, blockNum, version, data)
			cache.GetBlock(ctx, chainID, blockNum, version)
			done <- true
		}(int64(i))
	}

	// Wait for all goroutines
	for i := 0; i < 10; i++ {
		<-done
	}

	// Verify all blocks were written correctly
	for i := int64(0); i < 10; i++ {
		retrieved, err := cache.GetBlock(ctx, chainID, i, version)
		if err != nil {
			t.Errorf("block %d: GetBlock failed: %v", i, err)
			continue
		}
		expected := json.RawMessage(fmt.Sprintf(`{"block":%d}`, i))
		if string(retrieved) != string(expected) {
			t.Errorf("block %d: expected %s, got %s", i, expected, retrieved)
		}
	}
}
