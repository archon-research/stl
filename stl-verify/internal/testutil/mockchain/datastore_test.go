package mockchain

import (
	"encoding/json"
	"testing"
)

// TestDataStore verifies Get, Headers, and Len on a pre-populated store.
func TestDataStore(t *testing.T) {
	ds := NewTestDataStore()

	if ds.Len() != 3 {
		t.Fatalf("expected 3 headers, got %d", ds.Len())
	}

	_, ok := ds.Get(0, "block")
	if !ok {
		t.Fatal("expected block data for index 0")
	}

	_, ok = ds.Get(99, "block")
	if ok {
		t.Fatal("expected no block data for index 99")
	}

	headers := ds.Headers()
	if len(headers) != 3 {
		t.Fatalf("expected 3 headers, got %d", len(headers))
	}

	_, ok = ds.Get(0, "receipts")
	if !ok {
		t.Fatal("expected receipts for index 0")
	}

	ds.Add(0, "block", json.RawMessage(`"overwritten"`))
	raw, _ := ds.Get(0, "block")
	if string(raw) != `"overwritten"` {
		t.Fatal("expected overwrite")
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
