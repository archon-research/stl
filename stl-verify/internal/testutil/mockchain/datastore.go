// Package mockchain provides an in-memory mock blockchain server for stress testing.
package mockchain

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

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

	return ds.headers
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
		hash := fmt.Sprintf("0x%064x", i)
		parentHash := fmt.Sprintf("0x%064x", i-1)
		header := outbound.BlockHeader{
			Number:     fmt.Sprintf("0x%x", i),
			Hash:       hash,
			ParentHash: parentHash,
			Timestamp:  fmt.Sprintf("0x%x", time.Now().Unix()),
		}

		headerJSON, _ := json.Marshal(header)

		ds.AddHeader(header)
		ds.Add(i, "block", headerJSON)
		ds.Add(i, "receipts", json.RawMessage(`[]`))
		ds.Add(i, "traces", json.RawMessage(`[]`))
		ds.Add(i, "blobs", json.RawMessage(`[]`))
	}

	return ds
}
