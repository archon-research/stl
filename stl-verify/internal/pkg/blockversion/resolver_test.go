package blockversion

import (
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

const (
	archiveName   = "s3://stl-sentinelstaging-ethereum-raw-89d540d0"
	archiveHeight = int64(25395651)
)

var (
	canonicalHash = common.HexToHash("0x4d1c1a52b1f5e5a0c6f0b0a0d9e8c7b6a594837261504f3e2d1c0b9a8f7e6d5c")
	orphanedHash  = common.HexToHash("0x9a8f7e6d5c4b3a2910ff0e1d2c3b4a5968778695a4b3c2d1e0f9a8b7c6d5e4f3")
)

// fakeArchive is the raw archive as the port describes it: a top version per height, the
// block that version identifies (empty when nothing there names one), and the heights it
// was asked about.
type fakeArchive struct {
	heights map[int64]archivedHeight
	// corrupt overrides what a height's stored object names, for the strings a real
	// object can carry that are not a block hash.
	corrupt map[int64]string
	asked   []int64
	err     error
}

type archivedHeight struct {
	version int
	hash    common.Hash
}

func archiveHolding(heights map[int64]archivedHeight) *fakeArchive {
	return &fakeArchive{heights: heights}
}

func (a *fakeArchive) HighestVersion(_ context.Context, blockNumber int64) (int, bool, error) {
	a.asked = append(a.asked, blockNumber)
	if a.err != nil {
		return 0, false, a.err
	}
	held, archived := a.heights[blockNumber]
	return held.version, archived, nil
}

func (a *fakeArchive) BlockHashAt(_ context.Context, blockNumber int64, version int) (string, bool, error) {
	if a.err != nil {
		return "", false, a.err
	}
	if raw, corrupt := a.corrupt[blockNumber]; corrupt {
		return raw, true, nil
	}
	held, archived := a.heights[blockNumber]
	if !archived || held.version != version || held.hash == (common.Hash{}) {
		return "", false, nil
	}
	return held.hash.Hex(), true, nil
}

// The version a correction wrote is the one live indexing stamped its rows with, so the
// version the archive holds is the one a replay of that block must carry — the same rule
// the morpho-vault-backfill reads off the key it replays.
func TestResolver_AnswersTheVersionTheArchiveHolds(t *testing.T) {
	archive := archiveHolding(map[int64]archivedHeight{
		archiveHeight: {version: 2, hash: canonicalHash},
	})

	version, err := NewResolver(archive, archiveName).ResolveBlockVersion(context.Background(), archiveHeight, canonicalHash)

	if err != nil {
		t.Fatalf("ResolveBlockVersion: %v", err)
	}
	if version != 2 {
		t.Errorf("version = %d, want 2: the correction is what the live row carries", version)
	}
}

// The ARCT-379 hole shape: the archive kept an orphaned fork and never received the
// canonical block. Its version speaks for the fork, so stamping rows with it would file
// the replayed history under a block that never happened.
func TestResolver_StopsWhenTheArchiveHoldsAnotherBlock(t *testing.T) {
	archive := archiveHolding(map[int64]archivedHeight{
		archiveHeight: {version: 0, hash: orphanedHash},
	})

	_, err := NewResolver(archive, archiveName).ResolveBlockVersion(context.Background(), archiveHeight, canonicalHash)

	if !errors.Is(err, ErrArchivedBlockMismatch) {
		t.Fatalf("error = %v, want ErrArchivedBlockMismatch", err)
	}
	for _, want := range []string{"25395651", archiveName, orphanedHash.Hex(), canonicalHash.Hex()} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error = %v, want it to name %q", err, want)
		}
	}
}

func TestResolver_StopsAtAHeightTheArchiveCannotAnswerFor(t *testing.T) {
	tests := []struct {
		name    string
		archive *fakeArchive
	}{
		{
			name:    "a height the archive never received",
			archive: archiveHolding(map[int64]archivedHeight{}),
		},
		{
			name: "a version occupied by objects that name no block",
			archive: archiveHolding(map[int64]archivedHeight{
				archiveHeight: {version: 0},
			}),
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := NewResolver(tc.archive, archiveName).ResolveBlockVersion(context.Background(), archiveHeight, canonicalHash)

			if !errors.Is(err, ErrHeightNotArchived) {
				t.Fatalf("error = %v, want ErrHeightNotArchived", err)
			}
			for _, want := range []string{"25395651", archiveName} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error = %v, want it to name %q", err, want)
				}
			}
		})
	}
}

// common.HexToHash crops, pads and swallows decode errors, so a corrupt object naming
// "0x1234" would become the zero hash and the height would fail as a MISMATCH — the one
// verdict the runbook answers by republishing over a slot that is already occupied.
func TestResolver_StopsAtAnArchivedObjectThatNamesNoBlockHash(t *testing.T) {
	archive := archiveHolding(map[int64]archivedHeight{archiveHeight: {version: 1, hash: canonicalHash}})
	archive.corrupt = map[int64]string{archiveHeight: "0x1234"}

	_, err := NewResolver(archive, archiveName).ResolveBlockVersion(context.Background(), archiveHeight, canonicalHash)

	if !errors.Is(err, ErrHeightNotArchived) {
		t.Fatalf("error = %v, want ErrHeightNotArchived", err)
	}
	if errors.Is(err, ErrArchivedBlockMismatch) {
		t.Errorf("error = %v, want it not to send the operator to the republisher", err)
	}
	for _, want := range []string{"25395651", "0x1234", archiveName} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error = %v, want it to name %q", err, want)
		}
	}
}

// A read that fails is not a height to repair: the archive said nothing either way, so
// the run stops on the failure rather than treating it as an unarchived height.
func TestResolver_BubblesAFailedArchiveRead(t *testing.T) {
	archive := archiveHolding(map[int64]archivedHeight{archiveHeight: {version: 0, hash: canonicalHash}})
	archive.err = errors.New("access denied")

	_, err := NewResolver(archive, archiveName).ResolveBlockVersion(context.Background(), archiveHeight, canonicalHash)

	if err == nil || !strings.Contains(err.Error(), "access denied") {
		t.Fatalf("error = %v, want the archive's own failure", err)
	}
	if errors.Is(err, ErrHeightNotArchived) {
		t.Errorf("error = %v, want it not to pass as an unarchived height", err)
	}
}

// One block carries many logs and the seed asks again for the head it pins, so a height
// read per call would be one archive round trip per log.
func TestResolver_ReadsEachHeightOnce(t *testing.T) {
	const neighbour = archiveHeight + 1
	archive := archiveHolding(map[int64]archivedHeight{
		archiveHeight: {version: 0, hash: canonicalHash},
		neighbour:     {version: 0, hash: canonicalHash},
	})
	resolver := NewResolver(archive, archiveName)

	for _, height := range []int64{archiveHeight, archiveHeight, neighbour, archiveHeight} {
		if _, err := resolver.ResolveBlockVersion(context.Background(), height, canonicalHash); err != nil {
			t.Fatalf("ResolveBlockVersion(%d): %v", height, err)
		}
	}

	want := []int64{archiveHeight, neighbour}
	if !slices.Equal(archive.asked, want) {
		t.Errorf("archive asked about %v, want %v", archive.asked, want)
	}
}

// A run is asked afterwards which versions its rows landed at and over what range, so the
// summary reports one extent per version rather than a list of heights.
func TestResolver_SummaryReportsWhatEachVersionCovers(t *testing.T) {
	const middle, top = archiveHeight + 1, archiveHeight + 2
	archive := archiveHolding(map[int64]archivedHeight{
		archiveHeight: {version: 1, hash: canonicalHash},
		middle:        {version: 0, hash: canonicalHash},
		top:           {version: 1, hash: canonicalHash},
	})
	resolver := NewResolver(archive, archiveName)

	for _, height := range []int64{archiveHeight, middle, top, top} {
		if _, err := resolver.ResolveBlockVersion(context.Background(), height, canonicalHash); err != nil {
			t.Fatalf("ResolveBlockVersion(%d): %v", height, err)
		}
	}

	summary := resolver.Summary()
	if summary.HeightsResolved != 3 {
		t.Errorf("heights resolved = %d, want 3", summary.HeightsResolved)
	}
	want := []VersionExtent{
		{Version: 0, Heights: 1, From: middle, To: middle},
		{Version: 1, Heights: 2, From: archiveHeight, To: top},
	}
	if !slices.Equal(summary.Versions, want) {
		t.Errorf("versions = %+v, want them ascending with each one's extent %+v", summary.Versions, want)
	}
}

// A memoised height is proved only for the block it was proved against, so the hash is
// re-checked on a memo hit too: answering a second, different block at that height with
// the first one's version would file its rows under a block that never happened. The
// failure is not the archive holding another block — it is the node handing this run two.
func TestResolver_ReProvesAMemoizedHeightAgainstTheBlockBeingReplayed(t *testing.T) {
	archive := archiveHolding(map[int64]archivedHeight{archiveHeight: {version: 1, hash: canonicalHash}})
	resolver := NewResolver(archive, archiveName)
	if _, err := resolver.ResolveBlockVersion(context.Background(), archiveHeight, canonicalHash); err != nil {
		t.Fatalf("ResolveBlockVersion: %v", err)
	}

	_, err := resolver.ResolveBlockVersion(context.Background(), archiveHeight, orphanedHash)

	if !errors.Is(err, ErrArchivedBlockMismatch) {
		t.Fatalf("error = %v, want ErrArchivedBlockMismatch", err)
	}
	if want := []int64{archiveHeight}; !slices.Equal(archive.asked, want) {
		t.Errorf("archive asked about %v, want the memoised height re-proved without another read %v", archive.asked, want)
	}
	for _, want := range []string{"already proved", canonicalHash.Hex(), orphanedHash.Hex()} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error = %v, want it to name %q", err, want)
		}
	}
}

// A mismatch is the one failure an operator clears by repairing the archive and starting
// a new run, so the height it failed at must not stay answered from the memo: the
// repaired archive is what the next call has to read.
func TestResolver_ReadsARepairedHeightAgainAfterAMismatch(t *testing.T) {
	archive := archiveHolding(map[int64]archivedHeight{
		archiveHeight: {version: 0, hash: orphanedHash},
	})
	resolver := NewResolver(archive, archiveName)
	if _, err := resolver.ResolveBlockVersion(context.Background(), archiveHeight, canonicalHash); !errors.Is(err, ErrArchivedBlockMismatch) {
		t.Fatalf("error = %v, want ErrArchivedBlockMismatch", err)
	}
	archive.heights[archiveHeight] = archivedHeight{version: 1, hash: canonicalHash}

	version, err := resolver.ResolveBlockVersion(context.Background(), archiveHeight, canonicalHash)

	if err != nil {
		t.Fatalf("ResolveBlockVersion once the archive holds the canonical block: %v", err)
	}
	if version != 1 {
		t.Errorf("version = %d, want the republished 1", version)
	}
	if want := []int64{archiveHeight, archiveHeight}; !slices.Equal(archive.asked, want) {
		t.Errorf("archive asked about %v, want the repaired height read again %v", archive.asked, want)
	}
}
