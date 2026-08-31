package memory

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// seedCanonicalChain saves a linked canonical chain over [from, to].
func seedCanonicalChain(t *testing.T, ctx context.Context, repo *BlockStateRepository, from, to int64) {
	t.Helper()
	for i := from; i <= to; i++ {
		if _, err := repo.SaveBlock(ctx, outbound.BlockState{
			Number:         i,
			Hash:           fmt.Sprintf("0xhash%d", i),
			ParentHash:     fmt.Sprintf("0xhash%d", i-1),
			ReceivedAt:     time.Now().Unix(),
			BlockTimestamp: time.Now().Unix(),
		}); err != nil {
			t.Fatalf("seed block %d: %v", i, err)
		}
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
