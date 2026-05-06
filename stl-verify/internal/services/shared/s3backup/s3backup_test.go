package s3backup_test

import (
	"context"
	"encoding/json"
	"errors"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/memory"
	"github.com/archon-research/stl/stl-verify/internal/services/shared/s3backup"
)

// fixedArtifacts returns block/receipts/traces payloads consistent with the
// chain-1 (Ethereum mainnet) expectation registered in chainexpect.
func fixedArtifacts() s3backup.Artifacts {
	return s3backup.Artifacts{
		Block:    json.RawMessage(`{"hash":"0xabc","number":"0x1"}`),
		Receipts: json.RawMessage(`[{"status":"0x1"}]`),
		Traces:   json.RawMessage(`[{"type":"call"}]`),
	}
}

// The S3 write semantics (key format, gzip, idempotency) are exercised by the
// raw_data_backup service tests via the shared s3backup.WriteArtifactByKey
// helper. The tests in this file cover only the behaviour that is unique to
// the inline-watcher orchestration: the construction-time chain guard, the
// constructor's required-field checks, and the errgroup parallelism contract.

func TestNewBackup_RejectsUnknownChain(t *testing.T) {
	t.Parallel()

	_, err := s3backup.NewBackup(s3backup.Config{
		Writer:  memory.NewS3Writer(),
		Bucket:  "b",
		ChainID: 9999, // not registered in chainexpect
	})
	if err == nil {
		t.Fatal("expected error for unknown chain ID")
	}
}

func TestNewBackup_RequiresWriterAndBucket(t *testing.T) {
	t.Parallel()

	if _, err := s3backup.NewBackup(s3backup.Config{Bucket: "b", ChainID: 1}); err == nil {
		t.Error("expected error when writer is nil")
	}
	if _, err := s3backup.NewBackup(s3backup.Config{Writer: memory.NewS3Writer(), ChainID: 1}); err == nil {
		t.Error("expected error when bucket is empty")
	}
}

// TestStart_PropagatesPutErrors covers the errgroup contract: a single
// goroutine failure becomes the group's error and Wait surfaces it. This is
// new behaviour relative to the sequential write loop in raw_data_backup.
func TestStart_PropagatesPutErrors(t *testing.T) {
	t.Parallel()

	backup, writer := s3backup.NewForTesting(t)
	wantErr := errors.New("synthetic s3 outage")
	writer.FailNext("*", wantErr)

	g := backup.Start(context.Background(), 100, 1, fixedArtifacts())
	err := g.Wait(context.Background())
	if err == nil {
		t.Fatal("expected error from forced put failure")
	}
	if !errors.Is(err, wantErr) {
		t.Errorf("error should wrap put failure, got %v", err)
	}
}

// TestStart_ParallelismOverlapsWithCallerWork is the central guarantee of
// this package: Start returns immediately so the caller can do other work
// (cache writes, etc.) while PUTs run in goroutines. Regressing this would
// silently double the watcher's per-block latency.
func TestStart_ParallelismOverlapsWithCallerWork(t *testing.T) {
	t.Parallel()

	backup, writer := s3backup.NewForTesting(t)
	const callerWork = 10 * time.Millisecond
	startedAt := time.Now()

	g := backup.Start(context.Background(), 100, 1, fixedArtifacts())
	time.Sleep(callerWork) // simulate cache-write or other in-process work
	if err := g.Wait(context.Background()); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	elapsed := time.Since(startedAt)

	// Memory writer is effectively instant, so total elapsed should be roughly
	// callerWork. Generous slack for slow CI; the regression we care about
	// would multiply elapsed by N (one PUT per data type).
	if elapsed > callerWork*5 {
		t.Errorf("expected ~%s elapsed, got %s — Start may not be running PUTs concurrently", callerWork, elapsed)
	}
	if writer.Count(s3backup.TestBucket) != 3 {
		t.Errorf("expected 3 objects written, got %d", writer.Count(s3backup.TestBucket))
	}
}
