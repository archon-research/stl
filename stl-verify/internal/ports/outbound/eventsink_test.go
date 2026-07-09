package outbound

import (
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

func TestBlockEvent_ParsedBlockHash(t *testing.T) {
	tests := []struct {
		name     string
		event    BlockEvent
		wantErr  bool
		wantHash common.Hash
	}{
		{
			name:     "valid hash is parsed",
			event:    BlockEvent{BlockNumber: 100, Version: 0, BlockHash: "0xabc123"},
			wantHash: common.HexToHash("0xabc123"),
		},
		{
			// An empty hash must never resolve to the zero hash: state-read callers
			// treat the zero hash as the number-pinned backfill fallback, so a live
			// event with no hash would silently downgrade off hash-pinning (VEC-471).
			name:    "empty hash fails hard rather than yielding the zero hash",
			event:   BlockEvent{BlockNumber: 100, Version: 2, BlockHash: ""},
			wantErr: true,
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.event.ParsedBlockHash()
			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error for empty block hash, got nil")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if got != tc.wantHash {
				t.Fatalf("hash = %s, want %s", got, tc.wantHash)
			}
		})
	}
}
