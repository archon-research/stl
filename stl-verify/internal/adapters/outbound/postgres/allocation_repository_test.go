package postgres

import (
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5/pgtype"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

func assertNumeric(t *testing.T, name string, got pgtype.Numeric, wantInt *big.Int, wantExp int32) {
	t.Helper()
	if !got.Valid {
		t.Fatalf("%s: expected valid numeric, got invalid", name)
	}
	if got.Int.Cmp(wantInt) != 0 {
		t.Errorf("%s Int: got %s, want %s", name, got.Int, wantInt)
	}
	if got.Exp != wantExp {
		t.Errorf("%s Exp: got %d, want %d", name, got.Exp, wantExp)
	}
}

func TestBuildInsertArgs_TxAmountPlainERC20(t *testing.T) {
	// Plain ERC20 (no AssetDecimals) — tx_amount should use TokenDecimals.
	r := &AllocationRepository{}

	pos := &entity.AllocationPosition{
		ChainID:       1,
		TokenAddress:  common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
		TokenSymbol:   "USDC",
		TokenDecimals: 6,
		PrimeID:       1,
		ProxyAddress:  common.HexToAddress("0x1111111111111111111111111111111111111111"),
		Balance:       big.NewInt(0),
		BlockNumber:   24584100,
		TxHash:        "0xda50e73f9d4722402ae4ec6e506c3726a78fc5f6146b4957bfadc2c1fffc8f8c",
		LogIndex:      10,
		TxAmount:      big.NewInt(32361621161), // 32361.621161 USDC
		Direction:     "in",
	}

	_, args, err := r.buildInsertArgs(pos, 3)
	if err != nil {
		t.Fatalf("buildInsertArgs: %v", err)
	}

	txAmount, ok := args[10].(pgtype.Numeric)
	if !ok {
		t.Fatalf("expected args[10] to be pgtype.Numeric, got %T", args[10])
	}
	assertNumeric(t, "txAmount", txAmount, big.NewInt(32361621161), -6)
}

func TestBuildInsertArgs_VaultPositionUsesTokenDecimals(t *testing.T) {
	// Vault-like position (ERC4626 / Curve LP): Balance and ScaledBalance are
	// both denominated in the held token's units. balance, scaled_balance, and
	// tx_amount must all be encoded with TokenDecimals so the three columns
	// stay on a single decimal basis — guards against re-introducing a
	// mixed-decimals path for positions with an underlying asset.
	r := &AllocationRepository{}
	rawShares, _ := new(big.Int).SetString("1759386773255205923032", 10)

	pos := &entity.AllocationPosition{
		ChainID:       1,
		TokenAddress:  common.HexToAddress("0x00836Fe54625BE242BcFA286207795405ca4fD10"),
		TokenSymbol:   "sUSDSUSDT",
		TokenDecimals: 18,
		PrimeID:       1,
		ProxyAddress:  common.HexToAddress("0x1111111111111111111111111111111111111111"),
		Balance:       rawShares,
		ScaledBalance: rawShares,
		BlockNumber:   24584100,
		TxHash:        "0xda50e73f9d4722402ae4ec6e506c3726a78fc5f6146b4957bfadc2c1fffc8f8c",
		LogIndex:      42,
		TxAmount:      rawShares,
		Direction:     "out",
	}

	_, args, err := r.buildInsertArgs(pos, 609)
	if err != nil {
		t.Fatalf("buildInsertArgs: %v", err)
	}

	balance, ok := args[4].(pgtype.Numeric)
	if !ok {
		t.Fatalf("expected args[4] to be pgtype.Numeric, got %T", args[4])
	}
	assertNumeric(t, "balance", balance, rawShares, -18)

	scaled, ok := args[5].(pgtype.Numeric)
	if !ok {
		t.Fatalf("expected args[5] to be pgtype.Numeric, got %T", args[5])
	}
	assertNumeric(t, "scaledBalance", scaled, rawShares, -18)

	txAmount, ok := args[10].(pgtype.Numeric)
	if !ok {
		t.Fatalf("expected args[10] to be pgtype.Numeric, got %T", args[10])
	}
	assertNumeric(t, "txAmount", txAmount, rawShares, -18)
}

func TestBuildInsertArgs_NilScaledBalanceIsNull(t *testing.T) {
	// ScaledBalance is optional — nil should produce a NULL numeric.
	r := &AllocationRepository{}

	pos := &entity.AllocationPosition{
		ChainID:       1,
		TokenAddress:  common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
		TokenSymbol:   "USDC",
		TokenDecimals: 6,
		PrimeID:       1,
		ProxyAddress:  common.HexToAddress("0x1111111111111111111111111111111111111111"),
		Balance:       big.NewInt(1000000),
		BlockNumber:   24584100,
		TxHash:        "0xabc",
		LogIndex:      1,
		TxAmount:      big.NewInt(1000000),
		Direction:     "in",
	}

	_, args, err := r.buildInsertArgs(pos, 3)
	if err != nil {
		t.Fatalf("buildInsertArgs: %v", err)
	}

	// nil scaled_balance → invalid (NULL)
	scaled := args[5].(pgtype.Numeric)
	if scaled.Valid {
		t.Fatal("expected scaled_balance to be NULL (invalid)")
	}
}
