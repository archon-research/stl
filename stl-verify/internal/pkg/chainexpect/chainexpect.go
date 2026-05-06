// Package chainexpect describes which raw block data types are expected to be
// produced and backed up to S3 for a given chain.
//
// It is the single source of truth used by:
//   - the watcher (live + backfill) to know which artifacts to write to S3,
//   - the cache reader fallback to refuse construction for unsupported chains,
//   - the deprecated raw_data_backup worker (until it is decommissioned).
//
// To onboard a new chain, add an entry to defaultExpectations and provision the
// matching S3 bucket; see VEC-217 for the full per-chain checklist.
package chainexpect

// Expectation describes the data types that must be backed up for a chain.
// Block data is always required; the booleans below indicate optional artifacts.
type Expectation struct {
	ExpectReceipts bool
	ExpectTraces   bool
	ExpectBlobs    bool
}

// defaultExpectations is the canonical map of chain ID to expected artifacts.
// Keep entries aligned with entity.ChainIDToS3Bucket.
var defaultExpectations = map[int64]Expectation{
	1: { // Ethereum Mainnet
		ExpectReceipts: true,
		ExpectTraces:   true,
		ExpectBlobs:    false, // Blobs optional post-Dencun
	},
	43114: { // Avalanche C-Chain
		ExpectReceipts: true,
		ExpectTraces:   false,
		ExpectBlobs:    false,
	},
}

// ForChain returns the Expectation registered for chainID. The second return
// value is false when the chain is not registered, indicating the chain has no
// S3 backup coverage and any component that depends on it should fail loudly.
func ForChain(chainID int64) (Expectation, bool) {
	exp, ok := defaultExpectations[chainID]
	return exp, ok
}

// All returns a copy of the full chain-to-expectation map. Callers must not
// mutate the returned map; it is used by tests and by code that needs to
// iterate over every supported chain.
func All() map[int64]Expectation {
	out := make(map[int64]Expectation, len(defaultExpectations))
	for k, v := range defaultExpectations {
		out[k] = v
	}
	return out
}
