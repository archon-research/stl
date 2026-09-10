package main

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/services/uniswapv4bootstrap"
)

func TestResumableError_NamesThePinOnlyWhenTheSnapshotCanBeResumed(t *testing.T) {
	boom := errors.New("boom")
	tests := []struct {
		name     string
		summary  uniswapv4bootstrap.Summary
		err      error
		wantHint bool
	}{
		{"failed before pinning", uniswapv4bootstrap.Summary{}, boom, false},
		{"failed after pinning", uniswapv4bootstrap.Summary{PinnedBlock: 42}, boom, true},
		{"pin moved", uniswapv4bootstrap.Summary{PinnedBlock: 42}, fmt.Errorf("reorged: %w", uniswapv4bootstrap.ErrPinMoved), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := resumableError(tt.summary, tt.err)
			if !errors.Is(err, tt.err) {
				t.Fatalf("error = %v, want it to wrap %v", err, tt.err)
			}
			if got := strings.Contains(err.Error(), "-pin 42"); got != tt.wantHint {
				t.Errorf("error = %v, resume hint present = %v, want %v", err, got, tt.wantHint)
			}
		})
	}
}
