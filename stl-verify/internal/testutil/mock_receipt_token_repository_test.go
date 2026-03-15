package testutil

import (
	"context"
	"errors"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Spec coverage: plan-transfer-user-discovery.md step 3 keeps this slice scoped
// to the repository/port/mock layer and calls for a dedicated receipt-token mock.

var _ outbound.ReceiptTokenRepository = (*MockReceiptTokenRepository)(nil)

func TestMockReceiptTokenRepository_ListTrackedReceiptTokens_DelegatesToConfiguredFunction(t *testing.T) {
	t.Parallel()

	expectedErr := errors.New("boom")
	expectedTokens := []outbound.TrackedReceiptToken{{
		ID:                  10,
		ProtocolID:          20,
		ProtocolAddress:     common.HexToAddress("0x5555555555555555555555555555555555555555"),
		UnderlyingTokenID:   30,
		ReceiptTokenAddress: common.HexToAddress("0x6666666666666666666666666666666666666666"),
		Symbol:              "spDAI",
		ChainID:             1,
	}}

	called := false
	repo := &MockReceiptTokenRepository{
		ListTrackedReceiptTokensFn: func(ctx context.Context, chainID int64) ([]outbound.TrackedReceiptToken, error) {
			called = true
			if chainID != 1 {
				t.Fatalf("chainID = %d, want 1", chainID)
			}
			return expectedTokens, expectedErr
		},
	}

	got, err := repo.ListTrackedReceiptTokens(context.Background(), 1)
	if !called {
		t.Fatal("ListTrackedReceiptTokensFn was not called")
	}
	if !errors.Is(err, expectedErr) {
		t.Fatalf("error = %v, want %v", err, expectedErr)
	}
	if len(got) != len(expectedTokens) {
		t.Fatalf("len(got) = %d, want %d", len(got), len(expectedTokens))
	}
	if got[0] != expectedTokens[0] {
		t.Fatalf("got[0] = %#v, want %#v", got[0], expectedTokens[0])
	}
}
