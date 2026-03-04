package mockchain

import (
	"encoding/json"
	"testing"
)

// TestDataStore_Get verifies Get across valid indexes, missing indexes, and all data types.
func TestDataStore_Get(t *testing.T) {
	ds := NewFixtureDataStore()

	tests := []struct {
		name     string
		index    int
		dataType string
		wantOK   bool
	}{
		{"block at index 0", 0, "block", true},
		{"receipts at index 0", 0, "receipts", true},
		{"traces at index 0", 0, "traces", true},
		{"blobs at index 0", 0, "blobs", true},
		{"missing index", 99, "block", false},
		{"missing data type", 0, "unknown", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, ok := ds.Get(tt.index, tt.dataType)
			if ok != tt.wantOK {
				t.Errorf("Get(%d, %q) ok = %v, want %v", tt.index, tt.dataType, ok, tt.wantOK)
			}
		})
	}
}

// TestDataStore_Add verifies that Add overwrites an existing entry.
func TestDataStore_Add(t *testing.T) {
	ds := NewFixtureDataStore()
	ds.Add(0, "block", json.RawMessage(`"overwritten"`))
	raw, ok := ds.Get(0, "block")
	if !ok {
		t.Fatal("expected entry after overwrite")
	}
	if string(raw) != `"overwritten"` {
		t.Errorf("expected overwritten value, got %s", raw)
	}
}

// TestDataStore_Len verifies the header count after population.
func TestDataStore_Len(t *testing.T) {
	tests := []struct {
		name    string
		store   *DataStore
		wantLen int
	}{
		{"empty store", NewDataStore(), 0},
		{"test store", NewFixtureDataStore(), 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := tt.store.Len(); got != tt.wantLen {
				t.Errorf("Len() = %d, want %d", got, tt.wantLen)
			}
		})
	}
}

// TestHeaders_ReturnsCopy verifies that mutating the slice returned by Headers does not affect the store.
func TestHeaders_ReturnsCopy(t *testing.T) {
	ds := NewFixtureDataStore()
	original := ds.Headers()[0].Hash

	h := ds.Headers()
	h[0].Hash = "0xmutated"

	if ds.Headers()[0].Hash != original {
		t.Fatal("mutating returned slice must not affect the DataStore")
	}
}

// TestNewDataStore verifies that a freshly created store is empty.
func TestNewDataStore(t *testing.T) {
	ds := NewDataStore()
	if ds.Len() != 0 {
		t.Fatalf("expected empty store, got %d", ds.Len())
	}
	_, ok := ds.Get(0, "block")
	if ok {
		t.Fatal("expected empty store to return not found")
	}
}
