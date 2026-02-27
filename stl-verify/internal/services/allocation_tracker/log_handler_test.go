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
	snapshots []*PositionSnapshot
	err       error
}

func (m *mockHandler) HandleSnapshots(ctx context.Context, snapshots []*PositionSnapshot) error {
	m.snapshots = append(m.snapshots, snapshots...)
	return m.err
}

func TestLogHandler_HandleSnapshots_Empty(t *testing.T) {
	h := NewLogHandler(slog.Default())
	err := h.HandleSnapshots(context.Background(), nil)
	if err != nil {
		t.Errorf("expected nil error for empty snapshots, got %v", err)
	}
}

func TestLogHandler_HandleSnapshots_NoError(t *testing.T) {
	h := NewLogHandler(slog.Default())

	snapshots := []*PositionSnapshot{
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
	}

	err := h.HandleSnapshots(context.Background(), snapshots)
	if err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestMultiHandler_FansOut(t *testing.T) {
	h1 := &mockHandler{}
	h2 := &mockHandler{}

	multi := NewMultiHandler(h1, h2)

	snapshots := []*PositionSnapshot{
		{
			Entry:       &TokenEntry{ContractAddress: common.HexToAddress("0xaaaa")},
			Balance:     big.NewInt(100),
			BlockNumber: 50,
		},
	}

	err := multi.HandleSnapshots(context.Background(), snapshots)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(h1.snapshots) != 1 {
		t.Errorf("handler1 should receive 1 snapshot, got %d", len(h1.snapshots))
	}
	if len(h2.snapshots) != 1 {
		t.Errorf("handler2 should receive 1 snapshot, got %d", len(h2.snapshots))
	}
}

func TestMultiHandler_CollectsErrors(t *testing.T) {
	h1 := &mockHandler{err: fmt.Errorf("handler1 failed")}
	h2 := &mockHandler{}
	h3 := &mockHandler{err: fmt.Errorf("handler3 failed")}

	multi := NewMultiHandler(h1, h2, h3)

	snapshots := []*PositionSnapshot{
		{
			Entry:       &TokenEntry{ContractAddress: common.HexToAddress("0xaaaa")},
			Balance:     big.NewInt(100),
			BlockNumber: 50,
		},
	}

	err := multi.HandleSnapshots(context.Background(), snapshots)
	if err == nil {
		t.Fatal("expected error when handlers fail")
	}

	// h2 should still receive snapshots even though h1 failed
	if len(h2.snapshots) != 1 {
		t.Errorf("handler2 should still receive snapshots despite h1 failure, got %d", len(h2.snapshots))
	}
}

func TestMultiHandler_Empty(t *testing.T) {
	multi := NewMultiHandler()
	err := multi.HandleSnapshots(context.Background(), []*PositionSnapshot{})
	if err != nil {
		t.Errorf("empty MultiHandler should not error: %v", err)
	}
}
