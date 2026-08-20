package testutil

import "testing"

// Block numbers reserved by the integration packages that drive a worker binary.
//
// Such a package cannot namespace what the binary addresses: the binary builds its
// Redis cache key from chain, block and version. With one Redis per CI shard the
// block number is then all that keeps two packages apart, and reusing one hands a
// worker the other package's payload. A test that uses an adapter directly does not
// belong here — it passes its own KeyPrefix or bucket.
//
// Reusing a key is a compile error; reusing a block fails
// TestReservedBlocks_AreDistinct.
//
// Interim: VEC-569 gives the binaries a configurable key prefix, which removes the
// need to reserve anything.
var reservedBlocks = map[string]int64{
	"sparklend-indexer":        16_800_000,
	"oracle-price-indexer":     18_000_000,
	"morpho-indexer":           19_000_000,
	"prime-allocation-indexer": 19_100_000,
}

// ReservedBlock returns the block number reserved for pkg.
func ReservedBlock(t *testing.T, pkg string) int64 {
	t.Helper()

	block, ok := reservedBlocks[pkg]
	if !ok {
		t.Fatalf("no block reserved for %q: add one to reservedBlocks in internal/testutil", pkg)
	}
	return block
}
