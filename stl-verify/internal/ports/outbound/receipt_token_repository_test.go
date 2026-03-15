package outbound_test

import (
	"context"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// Spec coverage: plan-transfer-user-discovery.md step 3 defines the new
// TrackedReceiptToken DTO and ReceiptTokenRepository port shape.

type stubReceiptTokenRepository struct {
	tokens []outbound.TrackedReceiptToken
	err    error
}

func (s stubReceiptTokenRepository) ListTrackedReceiptTokens(ctx context.Context, chainID int64) ([]outbound.TrackedReceiptToken, error) {
	return s.tokens, s.err
}

var _ outbound.ReceiptTokenRepository = stubReceiptTokenRepository{}

func TestTrackedReceiptToken_ExposesJoinedProtocolAddressAndChainID(t *testing.T) {
	t.Parallel()

	// This test encodes the plan requirement that tracked receipt tokens include
	// the joined protocol address and chain ID alongside receipt-token fields.
	token := outbound.TrackedReceiptToken{
		ID:                  11,
		ProtocolID:          22,
		ProtocolAddress:     common.HexToAddress("0x1111111111111111111111111111111111111111"),
		UnderlyingTokenID:   33,
		ReceiptTokenAddress: common.HexToAddress("0x2222222222222222222222222222222222222222"),
		Symbol:              "spUSDC",
		ChainID:             1,
	}

	if token.ProtocolAddress != common.HexToAddress("0x1111111111111111111111111111111111111111") {
		t.Fatalf("ProtocolAddress = %s, want joined protocol address", token.ProtocolAddress.Hex())
	}

	if token.ChainID != 1 {
		t.Fatalf("ChainID = %d, want 1", token.ChainID)
	}

	if token.ReceiptTokenAddress != common.HexToAddress("0x2222222222222222222222222222222222222222") {
		t.Fatalf("ReceiptTokenAddress = %s, want receipt token address", token.ReceiptTokenAddress.Hex())
	}
}

func TestReceiptTokenRepository_ListTrackedReceiptTokens_ReturnsTrackedReceiptTokensForChain(t *testing.T) {
	t.Parallel()

	// This test encodes the port contract: the repository returns tracked receipt
	// tokens for the requested chain without requiring any worker/service code.
	expected := []outbound.TrackedReceiptToken{{
		ID:                  1,
		ProtocolID:          2,
		ProtocolAddress:     common.HexToAddress("0x3333333333333333333333333333333333333333"),
		UnderlyingTokenID:   3,
		ReceiptTokenAddress: common.HexToAddress("0x4444444444444444444444444444444444444444"),
		Symbol:              "spWETH",
		ChainID:             1,
	}}

	repo := stubReceiptTokenRepository{tokens: expected}

	got, err := repo.ListTrackedReceiptTokens(context.Background(), 1)
	if err != nil {
		t.Fatalf("ListTrackedReceiptTokens returned error: %v", err)
	}

	if len(got) != 1 {
		t.Fatalf("len(got) = %d, want 1", len(got))
	}

	if got[0] != expected[0] {
		t.Fatalf("got[0] = %#v, want %#v", got[0], expected[0])
	}
}
