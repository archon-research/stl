package blockversion

import (
	"context"
	"errors"
	"testing"

	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// The counter is how a red run's cause is read without pulling its logs, so every answer
// lands under the outcome that decides the response.
func TestResolver_CountsEveryAnswerUnderItsOutcome(t *testing.T) {
	const forked, unarchived, unreadable = archiveHeight + 1, archiveHeight + 2, archiveHeight + 3
	reader := testutil.InstallMeterProvider(t)
	archive := archiveHolding(map[int64]archivedHeight{
		archiveHeight: {version: 1, hash: canonicalHash},
		forked:        {version: 0, hash: orphanedHash},
	})
	resolver := resolverOver(archive)

	if _, err := resolver.ResolveBlockVersion(context.Background(), archiveHeight, canonicalHash); err != nil {
		t.Fatalf("reading a height the archive holds: %v", err)
	}
	if _, err := resolver.ResolveBlockVersion(context.Background(), archiveHeight, canonicalHash); err != nil {
		t.Fatalf("answering the same height from the memo: %v", err)
	}
	if _, err := resolver.ResolveBlockVersion(context.Background(), forked, canonicalHash); !errors.Is(err, ErrArchivedBlockMismatch) {
		t.Fatalf("error = %v, want ErrArchivedBlockMismatch", err)
	}
	if _, err := resolver.ResolveBlockVersion(context.Background(), unarchived, canonicalHash); !errors.Is(err, ErrHeightNotArchived) {
		t.Fatalf("error = %v, want ErrHeightNotArchived", err)
	}
	archive.err = errors.New("503 SlowDown")
	if _, err := resolver.ResolveBlockVersion(context.Background(), unreadable, canonicalHash); err == nil {
		t.Fatal("a failing archive read must surface")
	}

	want := map[string]int64{"archive": 1, "memo": 1, "mismatch": 1, "not_archived": 1, "read_failed": 1}
	for outcome, count := range want {
		got := testutil.CounterValue(t, reader, "block_version.resolved.total", map[string]string{"outcome": outcome})
		if got != count {
			t.Errorf("outcome=%s counted %d, want %d", outcome, got, count)
		}
	}
}

// A counter that failed to register leaves the metric disabled, and a replay must run
// anyway rather than panic on it.
func TestTelemetry_DisabledRecordsNothing(t *testing.T) {
	telemetry{}.record(context.Background(), outcomeArchive)
}
