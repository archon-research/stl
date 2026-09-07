package outbound

import (
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

func TestBlockEvent_ParsedBlockHash(t *testing.T) {
	tests := []struct {
		name            string
		event           BlockEvent
		wantErr         bool
		wantErrContains string
		wantHash        common.Hash
	}{
		{
			name:     "valid hash is parsed",
			event:    BlockEvent{BlockNumber: 100, Version: 0, BlockHash: "0x0000000000000000000000000000000000000000000000000000000000abc123"},
			wantHash: common.HexToHash("0x0000000000000000000000000000000000000000000000000000000000abc123"),
		},
		{
			// An empty hash must never resolve to the zero hash: state-read callers
			// treat the zero hash as the number-pinned backfill fallback, so a live
			// event with no hash would silently downgrade off hash-pinning (VEC-471).
			name:            "empty hash fails hard rather than yielding the zero hash",
			event:           BlockEvent{BlockNumber: 100, Version: 2, BlockHash: ""},
			wantErr:         true,
			wantErrContains: "missing block hash",
		},
		{
			// A malformed hash must never coerce: common.HexToHash would zero-pad
			// "0xabc" into a plausible-looking hash that pins the read to a block
			// that does not exist.
			name:            "short hash fails hard rather than being zero-padded",
			event:           BlockEvent{BlockNumber: 100, Version: 0, BlockHash: "0xabc"},
			wantErr:         true,
			wantErrContains: "malformed block hash",
		},
		{
			name:            "missing 0x prefix fails hard",
			event:           BlockEvent{BlockNumber: 100, Version: 0, BlockHash: "0000000000000000000000000000000000000000000000000000000000abc123"},
			wantErr:         true,
			wantErrContains: "malformed block hash",
		},
		{
			name:            "non-hex characters fail hard",
			event:           BlockEvent{BlockNumber: 100, Version: 0, BlockHash: "0x00000000000000000000000000000000000000000000000000000000zzabc123"},
			wantErr:         true,
			wantErrContains: "malformed block hash",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.event.ParsedBlockHash()
			if tc.wantErr {
				if err == nil {
					t.Fatalf("expected error for block hash %q, got nil", tc.event.BlockHash)
				}
				if !strings.Contains(err.Error(), tc.wantErrContains) {
					t.Fatalf("error = %q, want it to contain %q", err, tc.wantErrContains)
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
