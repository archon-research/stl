package testutil

import "testing"

// Two packages on one block is the failure this registry exists to stop, and it is
// invisible at the call site: the second package reads the first one's payload.
func TestReservedBlocks_AreDistinct(t *testing.T) {
	owner := map[int64]string{}
	for pkg, block := range reservedBlocks {
		if other, taken := owner[block]; taken {
			t.Errorf("%s and %s both reserve block %d", pkg, other, block)
		}
		owner[block] = pkg
	}
}
