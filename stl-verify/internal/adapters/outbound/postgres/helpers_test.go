package postgres

import (
	"strings"
	"testing"
)

func TestRequireSingleChain(t *testing.T) {
	type row struct{ chainID int64 }
	chainID := func(r row) int64 { return r.chainID }

	tests := []struct {
		name    string
		items   []row
		wantErr bool
	}{
		{name: "empty", items: nil},
		{name: "single item", items: []row{{1}}},
		{name: "all same chain", items: []row{{1}, {1}, {1}}},
		{name: "mixed chains", items: []row{{1}, {137}}, wantErr: true},
		{name: "mixed chains after a run of same", items: []row{{1}, {1}, {137}}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := requireSingleChain(tt.items, chainID, "rows")
			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if !strings.Contains(err.Error(), "mixed chain IDs") {
					t.Errorf("error = %q, want it to mention mixed chain IDs", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}
