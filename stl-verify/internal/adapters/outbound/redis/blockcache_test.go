package redis

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
)

// --- Test: NewBlockCache ---

func TestNewBlockCache_CreatesWithConfig(t *testing.T) {
	cfg := Config{
		Addr:      "localhost:6379",
		Password:  "secret",
		DB:        1,
		TTL:       1 * time.Hour,
		KeyPrefix: "test",
	}

	cache, err := NewBlockCache(cfg, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer cache.Close()

	if cache.ttl != cfg.TTL {
		t.Errorf("expected TTL=%v, got %v", cfg.TTL, cache.ttl)
	}
	if cache.keyPrefix != cfg.KeyPrefix {
		t.Errorf("expected keyPrefix=%s, got %s", cfg.KeyPrefix, cache.keyPrefix)
	}
	if cache.client == nil {
		t.Fatal("expected client, got nil")
	}
	if cache.logger == nil {
		t.Fatal("expected logger, got nil")
	}
}

func TestNewBlockCache_EmptyAddrReturnsError(t *testing.T) {
	_, err := NewBlockCache(Config{}, nil)
	if err == nil {
		t.Fatal("expected error for empty addr, got nil")
	}
	if !strings.Contains(err.Error(), "redis address is required") {
		t.Errorf("expected 'redis address is required' error, got %v", err)
	}
}

func TestNewBlockCache_UsesDefaultLogger(t *testing.T) {
	cache, err := NewBlockCache(Config{Addr: "localhost:6379"}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer cache.Close()

	if cache.logger == nil {
		t.Fatal("expected default logger to be set, got nil")
	}
}

// --- Test: ConfigDefaults ---

func TestConfigDefaults_ReturnsDefaults(t *testing.T) {
	defaults := ConfigDefaults()

	if defaults.Addr != "localhost:6379" {
		t.Errorf("expected Addr=localhost:6379, got %s", defaults.Addr)
	}
	if defaults.Password != "" {
		t.Errorf("expected Password=empty, got %s", defaults.Password)
	}
	if defaults.DB != 0 {
		t.Errorf("expected DB=0, got %d", defaults.DB)
	}
	if defaults.TTL != 24*time.Hour {
		t.Errorf("expected TTL=24h, got %v", defaults.TTL)
	}
	if defaults.KeyPrefix != "stl" {
		t.Errorf("expected KeyPrefix=stl, got %s", defaults.KeyPrefix)
	}
}

// --- Test: key generation ---

func TestBlockCache_KeyFormat(t *testing.T) {
	cache, err := NewBlockCache(Config{Addr: "localhost:6379", KeyPrefix: "test"}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer cache.Close()

	tests := []struct {
		name        string
		chainID     int64
		blockNumber int64
		version     int
		dataType    string
		expected    string
	}{
		{
			name:        "block key",
			chainID:     1,
			blockNumber: 12345,
			version:     1,
			dataType:    "block",
			expected:    "test:1:12345:1:block",
		},
		{
			name:        "receipts key",
			chainID:     1,
			blockNumber: 12345,
			version:     2,
			dataType:    "receipts",
			expected:    "test:1:12345:2:receipts",
		},
		{
			name:        "traces key",
			chainID:     42161,
			blockNumber: 99999999,
			version:     1,
			dataType:    "traces",
			expected:    "test:42161:99999999:1:traces",
		},
		{
			name:        "blobs key",
			chainID:     1,
			blockNumber: 18000000,
			version:     3,
			dataType:    "blobs",
			expected:    "test:1:18000000:3:blobs",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := cache.key(tt.chainID, tt.blockNumber, tt.version, tt.dataType)
			if key != tt.expected {
				t.Errorf("expected key=%s, got %s", tt.expected, key)
			}
		})
	}
}

func TestBlockCache_KeyWithEmptyPrefix(t *testing.T) {
	cache, err := NewBlockCache(Config{Addr: "localhost:6379", KeyPrefix: ""}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer cache.Close()

	key := cache.key(1, 100, 1, "block")
	if key != ":1:100:1:block" {
		t.Errorf("expected key=:1:100:1:block, got %s", key)
	}
}

// --- Test: Interface compliance ---

func TestBlockCache_ImplementsInterface(t *testing.T) {
	// This is a compile-time check, but we can also verify at runtime
	cache, err := NewBlockCache(Config{Addr: "localhost:6379"}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer cache.Close()

	// The var _ outbound.BlockCache = (*BlockCache)(nil) in the source
	// ensures this, but this test documents the expectation
	if cache == nil {
		t.Fatal("expected cache, got nil")
	}
}

// --- Test: Close ---

func TestBlockCache_Close(t *testing.T) {
	cache, err := NewBlockCache(Config{Addr: "localhost:6379"}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	err = cache.Close()
	if err != nil {
		t.Errorf("expected no error on close, got %v", err)
	}
}

// --- Test: Set/Get method signatures (compile checks) ---

func TestBlockCache_MethodSignatures(t *testing.T) {
	cache, err := NewBlockCache(Config{Addr: "localhost:6379"}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer cache.Close()

	ctx := context.Background()
	var chainID int64 = 1
	var blockNumber int64 = 100
	version := 1
	data := json.RawMessage(`{"test": true}`)

	// These are compile-time checks for method signatures
	// We don't actually connect to Redis here
	_ = func() error { return cache.SetBlock(ctx, chainID, blockNumber, version, data) }
	_ = func() error { return cache.SetReceipts(ctx, chainID, blockNumber, version, data) }
	_ = func() error { return cache.SetTraces(ctx, chainID, blockNumber, version, data) }
	_ = func() error { return cache.SetBlobs(ctx, chainID, blockNumber, version, data) }
	_ = func() (json.RawMessage, error) { return cache.GetBlock(ctx, chainID, blockNumber, version) }
	_ = func() (json.RawMessage, error) { return cache.GetReceipts(ctx, chainID, blockNumber, version) }
	_ = func() (json.RawMessage, error) { return cache.GetTraces(ctx, chainID, blockNumber, version) }
	_ = func() (json.RawMessage, error) { return cache.GetBlobs(ctx, chainID, blockNumber, version) }
	_ = func() error { return cache.DeleteBlock(ctx, chainID, blockNumber, version) }
	_ = func() error { return cache.Ping(ctx) }
	_ = func() error { return cache.Close() }
}

// --- Test: Data type validation ---

func TestBlockCache_DataTypes(t *testing.T) {
	cache, err := NewBlockCache(Config{Addr: "localhost:6379", KeyPrefix: "test"}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer cache.Close()

	dataTypes := []string{"block", "receipts", "traces", "blobs"}
	for _, dt := range dataTypes {
		key := cache.key(1, 100, 1, dt)
		if !strings.Contains(key, dt) {
			t.Errorf("expected key to contain %s, got %s", dt, key)
		}
	}
}

// --- Test: TTL configuration ---

func TestBlockCache_TTLConfiguration(t *testing.T) {
	tests := []struct {
		name     string
		ttl      time.Duration
		expected time.Duration
	}{
		{"1 hour TTL", 1 * time.Hour, 1 * time.Hour},
		{"24 hour TTL", 24 * time.Hour, 24 * time.Hour},
		{"1 minute TTL", 1 * time.Minute, 1 * time.Minute},
		{"zero TTL", 0, 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache, err := NewBlockCache(Config{Addr: "localhost:6379", TTL: tt.ttl}, nil)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			defer cache.Close()

			if cache.ttl != tt.expected {
				t.Errorf("expected TTL=%v, got %v", tt.expected, cache.ttl)
			}
		})
	}
}

// --- Test: Different chain IDs and block numbers ---

func TestBlockCache_KeyUniqueness(t *testing.T) {
	cache, err := NewBlockCache(Config{Addr: "localhost:6379", KeyPrefix: "test"}, nil)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	defer cache.Close()

	keys := make(map[string]bool)

	// Generate keys for different combinations
	testCases := []struct {
		chainID     int64
		blockNumber int64
		version     int
		dataType    string
	}{
		{1, 100, 1, "block"},
		{1, 100, 1, "receipts"},
		{1, 100, 2, "block"},
		{1, 101, 1, "block"},
		{42161, 100, 1, "block"},
	}

	for _, tc := range testCases {
		key := cache.key(tc.chainID, tc.blockNumber, tc.version, tc.dataType)
		if keys[key] {
			t.Errorf("duplicate key generated: %s", key)
		}
		keys[key] = true
	}

	if len(keys) != len(testCases) {
		t.Errorf("expected %d unique keys, got %d", len(testCases), len(keys))
	}
}

// --- Test: Config validation ---

func TestConfig_Validation(t *testing.T) {
	tests := []struct {
		name      string
		config    Config
		expectErr bool
		errMsg    string
	}{
		{
			name:      "empty address",
			config:    Config{},
			expectErr: true,
			errMsg:    "redis address is required",
		},
		{
			name:      "valid minimal config",
			config:    Config{Addr: "localhost:6379"},
			expectErr: false,
		},
		{
			name: "full config",
			config: Config{
				Addr:      "redis.example.com:6379",
				Password:  "secret",
				DB:        5,
				TTL:       1 * time.Hour,
				KeyPrefix: "myapp",
			},
			expectErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cache, err := NewBlockCache(tt.config, nil)
			if tt.expectErr {
				if err == nil {
					t.Error("expected error, got nil")
				} else if !strings.Contains(err.Error(), tt.errMsg) {
					t.Errorf("expected error containing %q, got %v", tt.errMsg, err)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error: %v", err)
				} else {
					cache.Close()
				}
			}
		})
	}
}

// --- Test: Compression functions ---

func TestCompress_ProducesGzipData(t *testing.T) {
	input := []byte(`{"block": {"number": "0x12345"}, "transactions": []}`)

	compressed, err := compress(input)
	if err != nil {
		t.Fatalf("compress failed: %v", err)
	}

	// Verify gzip magic bytes
	if len(compressed) < 2 {
		t.Fatalf("compressed data too short: %d bytes", len(compressed))
	}
	if compressed[0] != 0x1f || compressed[1] != 0x8b {
		t.Errorf("expected gzip magic bytes 0x1f 0x8b, got 0x%02x 0x%02x", compressed[0], compressed[1])
	}

	// Verify compression actually reduced size (for typical JSON)
	if len(compressed) >= len(input) {
		t.Logf("warning: compressed size (%d) >= original size (%d)", len(compressed), len(input))
	}
}

func TestDecompress_DecompressesGzipData(t *testing.T) {
	original := []byte(`{"block": {"number": "0x12345"}, "transactions": [1,2,3]}`)

	compressed, err := compress(original)
	if err != nil {
		t.Fatalf("compress failed: %v", err)
	}

	decompressed, err := decompress(compressed)
	if err != nil {
		t.Fatalf("decompress failed: %v", err)
	}

	if string(decompressed) != string(original) {
		t.Errorf("round-trip failed: got %q, want %q", decompressed, original)
	}
}

func TestDecompress_BackwardCompatibility_UncompressedData(t *testing.T) {
	// Simulate uncompressed JSON data that might exist in Redis before compression rollout
	uncompressedJSON := []byte(`{"block": {"number": "0x12345"}, "receipts": []}`)

	// decompress should detect it's not gzipped and return as-is
	result, err := decompress(uncompressedJSON)
	if err != nil {
		t.Fatalf("decompress should not fail on uncompressed data: %v", err)
	}

	if string(result) != string(uncompressedJSON) {
		t.Errorf("expected uncompressed data returned as-is, got %q, want %q", result, uncompressedJSON)
	}
}

func TestIsGzipped_DetectsMagicBytes(t *testing.T) {
	tests := []struct {
		name     string
		data     []byte
		expected bool
	}{
		{"gzip data", []byte{0x1f, 0x8b, 0x08, 0x00}, true},
		{"json data", []byte(`{"key": "value"}`), false},
		{"empty data", []byte{}, false},
		{"single byte", []byte{0x1f}, false},
		{"wrong magic", []byte{0x1f, 0x00}, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			result := isGzipped(tt.data)
			if result != tt.expected {
				t.Errorf("isGzipped(%v) = %v, want %v", tt.data, result, tt.expected)
			}
		})
	}
}

func TestCompressDecompress_LargeData(t *testing.T) {
	// Create a large JSON-like payload similar to actual block data
	largeData := make([]byte, 100000)
	copy(largeData, `{"transactions":[`)
	for i := 17; i < 99990; i++ {
		largeData[i] = 'x'
	}
	copy(largeData[99990:], `]}`)

	compressed, err := compress(largeData)
	if err != nil {
		t.Fatalf("compress failed: %v", err)
	}

	// Large repetitive data should compress very well
	compressionRatio := float64(len(compressed)) / float64(len(largeData))
	t.Logf("Compression ratio: %.2f%% (original: %d, compressed: %d)",
		compressionRatio*100, len(largeData), len(compressed))

	if compressionRatio > 0.1 {
		t.Logf("warning: compression ratio %.2f%% is higher than expected for repetitive data", compressionRatio*100)
	}

	// Verify round-trip
	decompressed, err := decompress(compressed)
	if err != nil {
		t.Fatalf("decompress failed: %v", err)
	}

	if len(decompressed) != len(largeData) {
		t.Errorf("round-trip size mismatch: got %d, want %d", len(decompressed), len(largeData))
	}
}
