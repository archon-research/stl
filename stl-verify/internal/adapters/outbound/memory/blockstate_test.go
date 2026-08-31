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
		name          string
		watermark     int64
		wantWatermark int64
	}{
		{name: "above the common ancestor rewinds", watermark: 105, wantWatermark: 103},
		{name: "below the common ancestor is left alone", watermark: 50, wantWatermark: 50},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			repo := NewBlockStateRepository()
			seedCanonicalChain(t, ctx, repo, 100, 105)
			if err := repo.SetBackfillWatermark(ctx, tt.watermark); err != nil {
				t.Fatalf("SetBackfillWatermark: %v", err)
			}

			event, newBlock := reorgAt(103)
			if _, err := repo.HandleReorgAtomic(ctx, 103, event, newBlock); err != nil {
				t.Fatalf("HandleReorgAtomic: %v", err)
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
		expected      int64
		wantAdvanced  bool
		wantWatermark int64
	}{
		{name: "matching expected advances", expected: 100, wantAdvanced: true, wantWatermark: 150},
		{name: "stale expected leaves the stored value", expected: 90, wantAdvanced: false, wantWatermark: 100},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx := context.Background()
			repo := NewBlockStateRepository()
			if err := repo.SetBackfillWatermark(ctx, 100); err != nil {
				t.Fatalf("SetBackfillWatermark: %v", err)
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
