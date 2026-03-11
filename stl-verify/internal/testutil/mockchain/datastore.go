// Implements the thread-safe in-memory store for block headers and associated data (block, receipts, traces, blobs).
package mockchain

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"slices"
	"strconv"
	"strings"
	"sync"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// s3Lister is a subset of outbound.S3Reader used by LoadFromS3.
// *s3.Reader from internal/adapters/outbound/s3 satisfies this interface.
type s3Lister interface {
	ListFiles(ctx context.Context, bucket, prefix string) ([]outbound.S3File, error)
	StreamFile(ctx context.Context, bucket, key string) (io.ReadCloser, error)
}

// DataStore is a thread-safe in-memory store for block headers and associated
// per-block data (block body, receipts, traces, blobs), keyed by block index.
type DataStore struct {
	mu      sync.RWMutex
	data    map[int]map[string]json.RawMessage
	headers []outbound.BlockHeader

	// reorgData and reorgHeaders hold data for reorg simulation blocks,
	// keyed by derived reorg hash.
	reorgData    map[string]map[string]json.RawMessage
	reorgHeaders map[string]outbound.BlockHeader
}

// NewDataStore returns an empty DataStore.
func NewDataStore() *DataStore {
	return &DataStore{
		data:         make(map[int]map[string]json.RawMessage),
		reorgData:    make(map[string]map[string]json.RawMessage),
		reorgHeaders: make(map[string]outbound.BlockHeader),
	}
}

// Add stores raw JSON data of the given dataType ("block", "receipts", "traces", "blobs")
// for the block at index. Overwrites any existing entry for that index/dataType pair.
func (ds *DataStore) Add(index int, dataType string, raw json.RawMessage) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if ds.data[index] == nil {
		ds.data[index] = make(map[string]json.RawMessage)
	}
	cp := make(json.RawMessage, len(raw))
	copy(cp, raw)
	ds.data[index][dataType] = cp
}

// AddHeader appends a block header to the ordered list of headers.
func (ds *DataStore) AddHeader(header outbound.BlockHeader) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	ds.headers = append(ds.headers, header)
}

// Get retrieves the raw JSON data for the given dataType at block index.
// Returns false if no data is stored for that index/dataType pair.
func (ds *DataStore) Get(index int, dataType string) (json.RawMessage, bool) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	inner, ok := ds.data[index]
	if !ok {
		return nil, false
	}
	raw, ok := inner[dataType]
	if !ok {
		return nil, false
	}
	cp := make(json.RawMessage, len(raw))
	copy(cp, raw)
	return cp, true
}

// Headers returns a copy of all block headers in insertion order.
func (ds *DataStore) Headers() []outbound.BlockHeader {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	result := make([]outbound.BlockHeader, len(ds.headers))
	copy(result, ds.headers)
	return result
}

// Len returns the number of block headers in the store.
func (ds *DataStore) Len() int {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	return len(ds.headers)
}

// AddReorgBlock stores raw JSON data for a reorg simulation block, keyed by derived hash.
func (ds *DataStore) AddReorgBlock(hash string, dataType string, raw json.RawMessage) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if ds.reorgData[hash] == nil {
		ds.reorgData[hash] = make(map[string]json.RawMessage)
	}
	cp := make(json.RawMessage, len(raw))
	copy(cp, raw)
	ds.reorgData[hash][dataType] = cp
}

// AddReorgHeader stores a block header for a reorg simulation block, keyed by derived hash.
func (ds *DataStore) AddReorgHeader(hash string, header outbound.BlockHeader) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	ds.reorgHeaders[hash] = header
}

// GetReorgBlock retrieves raw JSON data for a reorg simulation block.
// Returns false if not found.
func (ds *DataStore) GetReorgBlock(hash, dataType string) (json.RawMessage, bool) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	inner, ok := ds.reorgData[hash]
	if !ok {
		return nil, false
	}
	raw, ok := inner[dataType]
	if !ok {
		return nil, false
	}
	cp := make(json.RawMessage, len(raw))
	copy(cp, raw)
	return cp, true
}

// GetReorgHeader retrieves the block header for a reorg simulation block.
// Returns false if not found.
func (ds *DataStore) GetReorgHeader(hash string) (outbound.BlockHeader, bool) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	h, ok := ds.reorgHeaders[hash]
	return h, ok
}

// ClearReorgData removes all stored reorg simulation blocks.
func (ds *DataStore) ClearReorgData() {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	ds.reorgData = make(map[string]map[string]json.RawMessage)
	ds.reorgHeaders = make(map[string]outbound.BlockHeader)
}

// LoadFromS3 loads block data from an S3 bucket into the store.
//
// Keys must follow the format: {prefix}/{blockNumber}/{dataType}.json
// where dataType is one of "block", "receipts", "traces", "blobs".
//
// Blocks are loaded in ascending block number order. The "block" dataType
// is used to extract the BlockHeader for each block. S3 connection is used
// only during this call; no ongoing AWS connection is held after it returns.
func (ds *DataStore) LoadFromS3(ctx context.Context, getter s3Lister, bucket, prefix string) error {
	files, err := getter.ListFiles(ctx, bucket, prefix)
	if err != nil {
		return fmt.Errorf("loading from S3: listing files: %w", err)
	}

	// Group keys by block number: blockNum → dataType → key
	type blockEntry struct {
		keys map[string]string // dataType → S3 key
	}
	blocks := make(map[int64]*blockEntry)
	for _, f := range files {
		blockNum, dataType, ok := parseS3Key(f.Key, prefix)
		if !ok {
			continue
		}
		if blocks[blockNum] == nil {
			blocks[blockNum] = &blockEntry{keys: make(map[string]string)}
		}
		blocks[blockNum].keys[dataType] = f.Key
	}

	// Sort block numbers ascending
	blockNums := make([]int64, 0, len(blocks))
	for n := range blocks {
		blockNums = append(blockNums, n)
	}
	slices.Sort(blockNums)

	for idx, blockNum := range blockNums {
		entry := blocks[blockNum]
		if _, ok := entry.keys["block"]; !ok {
			return fmt.Errorf("loading from S3: block %d: missing block.json", blockNum)
		}
		for _, dataType := range []string{"block", "receipts", "traces", "blobs"} {
			key, ok := entry.keys[dataType]
			if !ok {
				continue
			}
			raw, err := streamAll(ctx, getter, bucket, key)
			if err != nil {
				return fmt.Errorf("loading from S3: block %d %s: %w", blockNum, dataType, err)
			}
			ds.Add(idx, dataType, raw)

			if dataType == "block" {
				var header outbound.BlockHeader
				if err := json.Unmarshal(raw, &header); err != nil {
					return fmt.Errorf("loading from S3: block %d: unmarshalling header: %w", blockNum, err)
				}
				ds.AddHeader(header)
			}
		}
	}

	return nil
}

// parseS3Key parses a key of the form "{prefix}/{blockNumber}/{dataType}.json".
// Returns the block number, dataType, and true on success; false if the key
// does not match the expected format.
func parseS3Key(key, prefix string) (int64, string, bool) {
	rel := strings.TrimPrefix(key, prefix+"/")
	if rel == key {
		// prefix not present
		return 0, "", false
	}
	parts := strings.SplitN(rel, "/", 2)
	if len(parts) != 2 {
		return 0, "", false
	}
	blockNum, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		return 0, "", false
	}
	dataType := strings.TrimSuffix(parts[1], ".json")
	if dataType == parts[1] {
		// no .json suffix
		return 0, "", false
	}
	return blockNum, dataType, true
}

// streamAll reads all bytes from a StreamFile call, closing the reader when done.
func streamAll(ctx context.Context, getter s3Lister, bucket, key string) (json.RawMessage, error) {
	rc, err := getter.StreamFile(ctx, bucket, key)
	if err != nil {
		return nil, err
	}
	defer rc.Close()
	data, err := io.ReadAll(rc)
	if err != nil {
		return nil, err
	}
	return json.RawMessage(data), nil
}

// NewFixtureDataStore returns a DataStore pre-populated with 3 synthetic blocks.
// It is intended for use in tests and stress-test binaries that need a ready-made data source.
func NewFixtureDataStore() *DataStore {
	ds := NewDataStore()

	for i := range 3 {
		hash := fmt.Sprintf("0x%064x", i+1)
		parentHash := "0x" + strings.Repeat("0", 64)
		if i > 0 {
			parentHash = fmt.Sprintf("0x%064x", i)
		}
		header := outbound.BlockHeader{
			Number:     fmt.Sprintf("0x%x", i+1),
			Hash:       hash,
			ParentHash: parentHash,
			Timestamp:  "0x67c00000",
		}

		headerJSON, err := json.Marshal(header)
		if err != nil {
			// Unreachable: json.Marshal cannot fail on a struct with only string fields.
			panic(fmt.Sprintf("mockchain: marshalling test header: %v", err))
		}

		ds.AddHeader(header)
		ds.Add(i, "block", headerJSON)
		ds.Add(i, "receipts", json.RawMessage(`[]`))
		ds.Add(i, "traces", json.RawMessage(`[]`))
		ds.Add(i, "blobs", json.RawMessage(`[]`))
	}

	return ds
}
