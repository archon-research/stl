package memory

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// saveCanonicalBlock saves one canonical block with an explicit parent hash.
func saveCanonicalBlock(t *testing.T, ctx context.Context, repo *BlockStateRepository, number int64, parentHash string) {
	t.Helper()
	if _, err := repo.SaveBlock(ctx, outbound.BlockState{
		Number:         number,
		Hash:           fmt.Sprintf("0xhash%d", number),
		ParentHash:     parentHash,
		ReceivedAt:     time.Now().Unix(),
		BlockTimestamp: time.Now().Unix(),
	}); err != nil {
		t.Fatalf("save block %d: %v", number, err)
	}
}

// saveCanonicalBlockAs saves one canonical block with an explicit hash, for the
// cases that need two rows at one height.
func saveCanonicalBlockAs(t *testing.T, ctx context.Context, repo *BlockStateRepository, number int64, hash, parentHash string) {
	t.Helper()
	if _, err := repo.SaveBlock(ctx, outbound.BlockState{
		Number:         number,
		Hash:           hash,
		ParentHash:     parentHash,
		ReceivedAt:     time.Now().Unix(),
		BlockTimestamp: time.Now().Unix(),
	}); err != nil {
		t.Fatalf("save block %d: %v", number, err)
	}
}

// seedCanonicalChain saves a linked canonical chain over [from, to].
func seedCanonicalChain(t *testing.T, ctx context.Context, repo *BlockStateRepository, from, to int64) {
	t.Helper()
	for i := from; i <= to; i++ {
		saveCanonicalBlock(t, ctx, repo, i, fmt.Sprintf("0xhash%d", i-1))
	}
}

func reorgAt(commonAncestor int64) (outbound.ReorgEvent, outbound.BlockState) {
	newBlock := outbound.BlockState{
		Number:         106,
		Hash:           "0xhash106_prime",
		ParentHash:     "0xhash105_prime",
		ReceivedAt:     time.Now().Unix(),
		BlockTimestamp: time.Now().Unix(),
	}
	return outbound.ReorgEvent{
		DetectedAt:  time.Now(),
		BlockNumber: newBlock.Number,
		OldHash:     "0xhash105",
		NewHash:     newBlock.Hash,
		Depth:       int(newBlock.Number - commonAncestor),
	}, newBlock
}

func TestHandleReorgAtomic_RewindsWatermark(t *testing.T) {
	tests := []struct {
		name       string
		watermark  int64
		wantCursor outbound.BackfillCursor
	}{
		{name: "above the common ancestor rewinds", watermark: 105, wantCursor: outbound.BackfillCursor{Watermark: 103, Generation: 1}},
		{name: "below the common ancestor keeps its value and still counts the reorg", watermark: 50, wantCursor: outbound.BackfillCursor{Watermark: 50, Generation: 1}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			repo := NewBlockStateRepository()
			seedCanonicalChain(t, ctx, repo, 100, 105)
			repo.SeedBackfillCursor(tt.watermark, 0)

			event, newBlock := reorgAt(103)
			if _, err := repo.HandleReorgAtomic(ctx, 103, event, newBlock); err != nil {
				t.Fatalf("HandleReorgAtomic: %v", err)
			}

			got, err := repo.GetBackfillCursor(ctx)
			if err != nil {
				t.Fatalf("GetBackfillCursor: %v", err)
			}
			if got != tt.wantCursor {
				t.Errorf("cursor = %+v, want %+v", got, tt.wantCursor)
			}
		})
	}
}

func TestFindOrphanOnlyHeights(t *testing.T) {
	ctx := context.Background()
	repo := NewBlockStateRepository()
	seedCanonicalChain(t, ctx, repo, 100, 105)

	// 102 keeps only an orphaned row; 104 also gets a canonical replacement.
	if err := repo.MarkBlockOrphaned(ctx, "0xhash102"); err != nil {
		t.Fatalf("orphan 102: %v", err)
	}
	if err := repo.MarkBlockOrphaned(ctx, "0xhash104"); err != nil {
		t.Fatalf("orphan 104: %v", err)
	}
	if _, err := repo.SaveBlock(ctx, outbound.BlockState{
		Number:         104,
		Hash:           "0xhash104_prime",
		ParentHash:     "0xhash103",
		ReceivedAt:     time.Now().Unix(),
		BlockTimestamp: time.Now().Unix(),
	}); err != nil {
		t.Fatalf("save replacement 104: %v", err)
	}

	tests := []struct {
		name     string
		from, to int64
		want     []int64
	}{
		{name: "orphan-only height reported, replaced height is not", from: 100, to: 105, want: []int64{102}},
		{name: "range bounds exclude the height", from: 103, to: 105, want: nil},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := repo.FindOrphanOnlyHeights(ctx, tt.from, tt.to)
			if err != nil {
				t.Fatalf("FindOrphanOnlyHeights: %v", err)
			}
			if len(got) != len(tt.want) {
				t.Fatalf("heights = %v, want %v", got, tt.want)
			}
			for i := range got {
				if got[i] != tt.want[i] {
					t.Fatalf("heights = %v, want %v", got, tt.want)
				}
			}
		})
	}
}

func TestAdvanceBackfillWatermark(t *testing.T) {
	tests := []struct {
		name          string
		unset         bool
		expected      outbound.BackfillCursor
		wantAdvanced  bool
		wantWatermark int64
	}{
		{name: "matching expected advances", expected: outbound.BackfillCursor{Watermark: 100, Generation: 2}, wantAdvanced: true, wantWatermark: 150},
		{name: "stale watermark leaves the stored value", expected: outbound.BackfillCursor{Watermark: 90, Generation: 2}, wantAdvanced: false, wantWatermark: 100},
		{name: "a reorg since the scan leaves the stored value", expected: outbound.BackfillCursor{Watermark: 100, Generation: 1}, wantAdvanced: false, wantWatermark: 100},
		{name: "unset expected seeds a cursor that was never written", unset: true, wantAdvanced: true, wantWatermark: 150},
		{name: "stale expected against an unwritten cursor refuses", unset: true, expected: outbound.BackfillCursor{Watermark: 90}, wantAdvanced: false, wantWatermark: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			repo := NewBlockStateRepository()
			if !tt.unset {
				repo.SeedBackfillCursor(100, 2)
			}

			advanced, err := repo.AdvanceBackfillWatermark(ctx, tt.expected, 150)
			if err != nil {
				t.Fatalf("AdvanceBackfillWatermark: %v", err)
			}
			if advanced != tt.wantAdvanced {
				t.Errorf("advanced = %v, want %v", advanced, tt.wantAdvanced)
			}

			got, err := repo.GetBackfillWatermark(ctx)
			if err != nil {
				t.Fatalf("GetBackfillWatermark: %v", err)
			}
			if got != tt.wantWatermark {
				t.Errorf("watermark = %d, want %d", got, tt.wantWatermark)
			}
		})
	}
}

// TestClearBlocksOrphaned_ClearsTheSegmentAsOneUnit mirrors the postgres
// adapter: the whole set is healed, or nothing is.
func TestClearBlocksOrphaned_ClearsTheSegmentAsOneUnit(t *testing.T) {
	tests := []struct {
		name          string
		hashes        []string
		wantErr       bool
		wantCanonical int
	}{
		{name: "the whole segment is cleared", hashes: []string{"0xhash101", "0xhash102", "0xhash103"}, wantCanonical: 3},
		{name: "an unknown hash clears none", hashes: []string{"0xhash101", "0xhash102", "0xmissing"}, wantErr: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			repo := NewBlockStateRepository()
			seedCanonicalChain(t, ctx, repo, 100, 105)
			for _, hash := range []string{"0xhash101", "0xhash102", "0xhash103"} {
				if err := repo.MarkBlockOrphaned(ctx, hash); err != nil {
					t.Fatalf("orphan %s: %v", hash, err)
				}
			}

			err := repo.ClearBlocksOrphaned(ctx, tt.hashes)
			if tt.wantErr && err == nil {
				t.Fatal("ClearBlocksOrphaned = nil, want an error")
			}
			if !tt.wantErr && err != nil {
				t.Fatalf("ClearBlocksOrphaned: %v", err)
			}

			canonical := 0
			for _, number := range []int64{101, 102, 103} {
				block, err := repo.GetBlockByNumber(ctx, number)
				if err != nil {
					t.Fatalf("GetBlockByNumber(%d): %v", number, err)
				}
				if block != nil {
					canonical++
				}
			}
			if canonical != tt.wantCanonical {
				t.Errorf("canonical rows = %d, want %d", canonical, tt.wantCanonical)
			}
		})
	}
}

func TestGetLowestCanonicalAbove(t *testing.T) {
	tests := []struct {
		name       string
		number     int64
		maxNumber  int64
		wantNumber int64
	}{
		{name: "returns the lowest canonical block above", number: 100, maxNumber: 105, wantNumber: 104},
		{name: "bound excludes the canonical block", number: 100, maxNumber: 103},
		{name: "no canonical block above", number: 105, maxNumber: 110},
	}

	ctx := context.Background()
	repo := NewBlockStateRepository()
	seedCanonicalChain(t, ctx, repo, 100, 105)
	for _, hash := range []string{"0xhash101", "0xhash102", "0xhash103", "0xhash105"} {
		if err := repo.MarkBlockOrphaned(ctx, hash); err != nil {
			t.Fatalf("orphan %s: %v", hash, err)
		}
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := repo.GetLowestCanonicalAbove(ctx, tt.number, tt.maxNumber)
			if err != nil {
				t.Fatalf("GetLowestCanonicalAbove: %v", err)
			}
			if tt.wantNumber == 0 {
				if got != nil {
					t.Fatalf("block = %+v, want none", got)
				}
				return
			}
			if got == nil {
				t.Fatalf("block = nil, want the canonical block at %d", tt.wantNumber)
			}
			if got.Number != tt.wantNumber {
				t.Errorf("block number = %d, want %d", got.Number, tt.wantNumber)
			}
		})
	}
}

// TestVerifyChainIntegrity_ReportsFirstViolation covers the ARCT-379 hole: two
// canonical rows with a missing height between them used to read as a valid
// chain, because only pairs where prev = number - 1 were compared.
func TestVerifyChainIntegrity_ReportsFirstViolation(t *testing.T) {
	tests := []struct {
		name            string
		seed            func(t *testing.T, ctx context.Context, repo *BlockStateRepository)
		from, to        int64
		wantErrContains string
	}{
		{
			name: "missing height between two canonical blocks",
			seed: func(t *testing.T, ctx context.Context, repo *BlockStateRepository) {
				seedCanonicalChain(t, ctx, repo, 100, 102)
				seedCanonicalChain(t, ctx, repo, 104, 105)
			},
			from:            100,
			to:              105,
			wantErrContains: "canonical block(s) 103 to 103 missing between blocks 102 and 104",
		},
		{
			name: "parent hash break",
			seed: func(t *testing.T, ctx context.Context, repo *BlockStateRepository) {
				seedCanonicalChain(t, ctx, repo, 100, 103)
				saveCanonicalBlock(t, ctx, repo, 104, "0xhash101")
				saveCanonicalBlock(t, ctx, repo, 105, "0xhash104")
			},
			from:            100,
			to:              105,
			wantErrContains: "chain integrity violation at block 104",
		},
		{
			name: "heights below the first canonical block",
			seed: func(t *testing.T, ctx context.Context, repo *BlockStateRepository) {
				seedCanonicalChain(t, ctx, repo, 100, 105)
			},
			from: 0,
			to:   105,
		},
		{
			name: "missing heights after the last canonical block",
			seed: func(t *testing.T, ctx context.Context, repo *BlockStateRepository) {
				seedCanonicalChain(t, ctx, repo, 100, 103)
			},
			from:            100,
			to:              105,
			wantErrContains: "canonical block(s) 104 to 105 missing after block 103",
		},
		{
			name: "two canonical rows at one height",
			seed: func(t *testing.T, ctx context.Context, repo *BlockStateRepository) {
				seedCanonicalChain(t, ctx, repo, 100, 105)
				saveCanonicalBlockAs(t, ctx, repo, 103, "0xhash103_twin", "0xhash102")
			},
			from:            100,
			to:              105,
			wantErrContains: "duplicate canonical rows at height 103",
		},
		{
			name: "parent break below a missing height is reported first",
			seed: func(t *testing.T, ctx context.Context, repo *BlockStateRepository) {
				seedCanonicalChain(t, ctx, repo, 100, 101)
				saveCanonicalBlock(t, ctx, repo, 102, "0xhash100")
				saveCanonicalBlock(t, ctx, repo, 103, "0xhash102")
				saveCanonicalBlock(t, ctx, repo, 105, "0xhash104")
			},
			from:            100,
			to:              105,
			wantErrContains: "chain integrity violation at block 102",
		},
		{
			name: "missing height below a parent break is reported first",
			seed: func(t *testing.T, ctx context.Context, repo *BlockStateRepository) {
				seedCanonicalChain(t, ctx, repo, 100, 101)
				saveCanonicalBlock(t, ctx, repo, 103, "0xhash102")
				saveCanonicalBlock(t, ctx, repo, 104, "0xhash101")
				saveCanonicalBlock(t, ctx, repo, 105, "0xhash104")
			},
			from:            100,
			to:              105,
			wantErrContains: "canonical block(s) 102 to 102 missing between blocks 101 and 103",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			repo := NewBlockStateRepository()
			tt.seed(t, ctx, repo)

			err := repo.VerifyChainIntegrity(ctx, tt.from, tt.to)
			if tt.wantErrContains == "" {
				if err != nil {
					t.Fatalf("VerifyChainIntegrity = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("VerifyChainIntegrity = nil, want error containing %q", tt.wantErrContains)
			}
			if !strings.Contains(err.Error(), tt.wantErrContains) {
				t.Errorf("VerifyChainIntegrity = %v, want error containing %q", err, tt.wantErrContains)
			}
		})
	}
}

// TestVerifyParentLinks covers the check the validator runs above the backfill
// watermark: a missing height up there is the gap filler's live work, but a
// broken link or a duplicated height never repairs itself (ARCT-379).
func TestVerifyParentLinks(t *testing.T) {
	tests := []struct {
		name            string
		seed            func(t *testing.T, ctx context.Context, repo *BlockStateRepository)
		from, to        int64
		wantErrContains string
	}{
		{
			name: "parent hash break is reported",
			seed: func(t *testing.T, ctx context.Context, repo *BlockStateRepository) {
				seedCanonicalChain(t, ctx, repo, 100, 103)
				saveCanonicalBlock(t, ctx, repo, 104, "0xhash101")
			},
			from:            100,
			to:              105,
			wantErrContains: "chain integrity violation at block 104",
		},
		{
			name: "missing height is not reported",
			seed: func(t *testing.T, ctx context.Context, repo *BlockStateRepository) {
				seedCanonicalChain(t, ctx, repo, 100, 102)
				seedCanonicalChain(t, ctx, repo, 104, 105)
			},
			from: 100,
			to:   105,
		},
		{
			name: "missing heights after the last canonical block are not reported",
			seed: func(t *testing.T, ctx context.Context, repo *BlockStateRepository) {
				seedCanonicalChain(t, ctx, repo, 100, 103)
			},
			from: 100,
			to:   105,
		},
		{
			name: "two canonical rows at one height are reported",
			seed: func(t *testing.T, ctx context.Context, repo *BlockStateRepository) {
				seedCanonicalChain(t, ctx, repo, 100, 105)
				saveCanonicalBlockAs(t, ctx, repo, 103, "0xhash103_twin", "0xhash102")
			},
			from:            100,
			to:              105,
			wantErrContains: "duplicate canonical rows at height 103",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			repo := NewBlockStateRepository()
			tt.seed(t, ctx, repo)

			err := repo.VerifyParentLinks(ctx, tt.from, tt.to)
			if tt.wantErrContains == "" {
				if err != nil {
					t.Fatalf("VerifyParentLinks = %v, want nil", err)
				}
				return
			}
			if err == nil {
				t.Fatalf("VerifyParentLinks = nil, want error containing %q", tt.wantErrContains)
			}
			if !strings.Contains(err.Error(), tt.wantErrContains) {
				t.Errorf("VerifyParentLinks = %v, want error containing %q", err, tt.wantErrContains)
			}
		})
	}
}
