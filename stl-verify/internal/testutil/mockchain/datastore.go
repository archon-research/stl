// Implements the thread-safe in-memory store for block headers and associated data (block, receipts, traces, blobs).
package mockchain

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// DataStore is a thread-safe in-memory store for block headers and associated
// per-block data (block body, receipts, traces, blobs), keyed by block index.
type DataStore struct {
	mu      sync.RWMutex
	data    map[int]map[string]json.RawMessage
	headers []outbound.BlockHeader
}

// NewDataStore returns an empty DataStore.
func NewDataStore() *DataStore {
	return &DataStore{
		data: make(map[int]map[string]json.RawMessage),
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
