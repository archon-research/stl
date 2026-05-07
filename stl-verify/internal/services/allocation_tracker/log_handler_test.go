package allocation_tracker

import (
	"context"
	"fmt"
	"log/slog"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
)

// mockHandler is a test double for AllocationHandler.
type mockHandler struct {
	batches []*SnapshotBatch
	err     error
}

func (m *mockHandler) HandleBatch(ctx context.Context, batch *SnapshotBatch) error {
	m.batches = append(m.batches, batch)
	return m.err
}

func TestLogHandler_HandleBatch_Empty(t *testing.T) {
	h := NewLogHandler(slog.Default())
	err := h.HandleBatch(context.Background(), &SnapshotBatch{})
	if err != nil {
		t.Errorf("expected nil error for empty batch, got %v", err)
	}
}

func TestLogHandler_HandleBatch_NoError(t *testing.T) {
	h := NewLogHandler(slog.Default())

	batch := &SnapshotBatch{
		Snapshots: []*PositionSnapshot{
			{
				Entry: &TokenEntry{
					ContractAddress: common.HexToAddress("0xaaaa"),
					Star:            "spark",
					Chain:           "ethereum",
					Protocol:        "morpho",
					TokenType:       "erc4626",
				},
				Balance:     big.NewInt(1000000),
				BlockNumber: 100,
			},
		},
		Supplies: []*TokenTotalSupplySnapshot{
			{
				ChainID:      1,
				TokenAddress: common.HexToAddress("0xaaaa"),
				TotalSupply:  big.NewInt(2000000),
				BlockNumber:  100,
				Source:       "sweep",
			},
		},
	}

	err := h.HandleBatch(context.Background(), batch)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestMultiHandler_FansOut(t *testing.T) {
	h1 := &mockHandler{}
	h2 := &mockHandler{}

	multi := NewMultiHandler(h1, h2)

	batch := &SnapshotBatch{
		Snapshots: []*PositionSnapshot{
			{
				Entry:       &TokenEntry{ContractAddress: common.HexToAddress("0xaaaa")},
				Balance:     big.NewInt(100),
				BlockNumber: 50,
			},
		},
	}

	err := multi.HandleBatch(context.Background(), batch)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(h1.batches) != 1 {
		t.Errorf("handler1 should receive 1 batch, got %d", len(h1.batches))
	}
	if len(h2.batches) != 1 {
		t.Errorf("handler2 should receive 1 batch, got %d", len(h2.batches))
	}
}

func TestMultiHandler_CollectsErrors(t *testing.T) {
	h1 := &mockHandler{err: fmt.Errorf("handler1 failed")}
	h2 := &mockHandler{}
	h3 := &mockHandler{err: fmt.Errorf("handler3 failed")}

	multi := NewMultiHandler(h1, h2, h3)

	batch := &SnapshotBatch{
		Snapshots: []*PositionSnapshot{
			{
				Entry:       &TokenEntry{ContractAddress: common.HexToAddress("0xaaaa")},
				Balance:     big.NewInt(100),
				BlockNumber: 50,
			},
		},
	}

	err := multi.HandleBatch(context.Background(), batch)
	if err == nil {
		t.Fatal("expected error when handlers fail")
	}

	// h2 should still receive the batch even though h1 failed
	if len(h2.batches) != 1 {
		t.Errorf("handler2 should still receive batch despite h1 failure, got %d", len(h2.batches))
	}
}

func TestMultiHandler_Empty(t *testing.T) {
	multi := NewMultiHandler()
	err := multi.HandleBatch(context.Background(), &SnapshotBatch{})
	if err != nil {
		t.Errorf("empty MultiHandler should not error: %v", err)
	}
}
