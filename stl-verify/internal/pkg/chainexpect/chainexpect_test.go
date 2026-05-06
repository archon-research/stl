package chainexpect

import "testing"

func TestForChain_Known(t *testing.T) {
	t.Parallel()

	tests := []struct {
		chainID int64
		want    Expectation
	}{
		{1, Expectation{ExpectReceipts: true, ExpectTraces: true, ExpectBlobs: false}},
		{43114, Expectation{ExpectReceipts: true, ExpectTraces: false, ExpectBlobs: false}},
	}
	for _, tt := range tests {
		got, ok := ForChain(tt.chainID)
		if !ok {
			t.Errorf("ForChain(%d) returned ok=false, want true", tt.chainID)
			continue
		}
		if got != tt.want {
			t.Errorf("ForChain(%d) = %#v, want %#v", tt.chainID, got, tt.want)
		}
	}
}

func TestForChain_Unknown(t *testing.T) {
	t.Parallel()

	for _, chainID := range []int64{0, 10, 8453, 42161, 130, 9999999} {
		if _, ok := ForChain(chainID); ok {
			t.Errorf("ForChain(%d) returned ok=true, want false (chain not yet registered)", chainID)
		}
	}
}

func TestAll_ReturnsCopy(t *testing.T) {
	t.Parallel()

	first := All()
	first[999] = Expectation{ExpectReceipts: true}

	second := All()
	if _, ok := second[999]; ok {
		t.Errorf("All() returned a map sharing state with the package internals")
	}
}
