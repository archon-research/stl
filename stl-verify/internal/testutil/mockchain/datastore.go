// Implements the thread-safe in-memory store for block headers and associated data (block, receipts, traces, blobs).
package mockchain

import (
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

type DataStore struct {
	mu      sync.RWMutex
	data    map[int]map[string]json.RawMessage
	headers []outbound.BlockHeader
}

func NewDataStore() *DataStore {
	return &DataStore{
		data: make(map[int]map[string]json.RawMessage),
	}
}

func (ds *DataStore) Add(index int, dataType string, raw json.RawMessage) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	if ds.data[index] == nil {
		ds.data[index] = make(map[string]json.RawMessage)
	}
	ds.data[index][dataType] = raw
}

func (ds *DataStore) AddHeader(header outbound.BlockHeader) {
	ds.mu.Lock()
	defer ds.mu.Unlock()

	ds.headers = append(ds.headers, header)
}

func (ds *DataStore) Get(index int, dataType string) (json.RawMessage, bool) {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	inner, ok := ds.data[index]
	if !ok {
		return nil, false
	}
	raw, ok := inner[dataType]
	return raw, ok
}

func (ds *DataStore) Headers() []outbound.BlockHeader {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	result := make([]outbound.BlockHeader, len(ds.headers))
	copy(result, ds.headers)
	return result
}

func (ds *DataStore) Len() int {
	ds.mu.RLock()
	defer ds.mu.RUnlock()

	return len(ds.headers)
}

// NewTestDataStore returns a DataStore pre-populated with 3 synthetic blocks for use in unit tests.
func NewTestDataStore() *DataStore {
	ds := NewDataStore()

	for i := 0; i < 3; i++ {
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
