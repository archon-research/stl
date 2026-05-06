package s3backup

import (
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/memory"
)

// TestBucket is the canonical bucket name used by NewForTesting.
const TestBucket = "test-bucket"

// NewForTesting constructs a Backup wired to an in-memory S3 writer. It uses
// chain ID 1 (Ethereum mainnet) so the chainexpect lookup succeeds. The
// returned writer is the same instance the Backup writes to, so tests can
// inspect it via writer.Get / writer.Keys / writer.Count or inject failures
// with writer.FailNext.
//
// Tests that need a different chain should use NewForTestingChain.
func NewForTesting(tb testing.TB) (*Backup, *memory.S3Writer) {
	tb.Helper()
	return NewForTestingChain(tb, 1)
}

// NewForTestingBackup is the no-introspection variant: returns just the Backup
// for tests that exercise upstream services and have no need to inspect S3
// contents. Equivalent to discarding the writer from NewForTesting.
func NewForTestingBackup(tb testing.TB) *Backup {
	tb.Helper()
	b, _ := NewForTesting(tb)
	return b
}

// NewForTestingChain is like NewForTesting but lets the caller pick the chain.
// Fails the test if chainID is not registered in chainexpect.
func NewForTestingChain(tb testing.TB, chainID int64) (*Backup, *memory.S3Writer) {
	tb.Helper()
	writer := memory.NewS3Writer()
	backup, err := NewBackup(Config{
		Writer:  writer,
		Bucket:  TestBucket,
		ChainID: chainID,
	})
	if err != nil {
		tb.Fatalf("s3backup.NewForTestingChain(%d): %v", chainID, err)
	}
	return backup, writer
}
