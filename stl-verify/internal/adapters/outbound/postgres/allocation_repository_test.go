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

func TestBuildInsertArgs_TxAmountUsesTokenDecimals(t *testing.T) {
	// Curve LP token: 18-decimal LP token, but underlying asset is 6-decimal USDT.
	// TxAmount is from a Transfer event and is always in the LP token's decimals.
	r := &AllocationRepository{}

	lpTokenRaw, _ := new(big.Int).SetString("1759386773255205923032", 10) // ~1759.39 LP tokens (18 dec)

	pos := &entity.AllocationPosition{
		ChainID:       1,
		TokenAddress:  common.HexToAddress("0x00836Fe54625BE242BcFA286207795405ca4fD10"),
		TokenSymbol:   "sUSDSUSDT",
		TokenDecimals: 18,
		AssetDecimals: new(6), // underlying USDT
		Star:          "spark",
		ProxyAddress:  common.HexToAddress("0x1111111111111111111111111111111111111111"),
		Balance:       big.NewInt(27758109970696), // underlying USDT amount (6 dec)
		ScaledBalance: lpTokenRaw,                 // LP shares (18 dec)
		BlockNumber:   24584100,
		TxHash:        "0xda50e73f9d4722402ae4ec6e506c3726a78fc5f6146b4957bfadc2c1fffc8f8c",
		LogIndex:      42,
		TxAmount:      lpTokenRaw, // Transfer event value — in LP token decimals (18)
		Direction:     "out",
	}

	_, args, err := r.buildInsertArgs(pos, 609)
	if err != nil {
		t.Fatalf("buildInsertArgs: %v", err)
	}

	// args[10] is txAmount — pgtype.Numeric with raw Int and Exp=-18
	txAmount, ok := args[10].(pgtype.Numeric)
	if !ok {
		t.Fatalf("expected args[10] to be pgtype.Numeric, got %T", args[10])
	}
	assertNumeric(t, "txAmount", txAmount, lpTokenRaw, -18)

	// args[4] is balance — uses AssetDecimals (6) for Curve LP
	balance, ok := args[4].(pgtype.Numeric)
	if !ok {
		t.Fatalf("expected args[4] to be pgtype.Numeric, got %T", args[4])
	}
	assertNumeric(t, "balance", balance, big.NewInt(27758109970696), -6)

	// args[5] is scaled_balance — uses TokenDecimals (18)
	scaled, ok := args[5].(pgtype.Numeric)
	if !ok {
		t.Fatalf("expected args[5] to be pgtype.Numeric, got %T", args[5])
	}
	assertNumeric(t, "scaledBalance", scaled, lpTokenRaw, -18)
}

func TestBuildInsertArgs_TxAmountPlainERC20(t *testing.T) {
	// Plain ERC20 (no AssetDecimals) — tx_amount should use TokenDecimals.
	r := &AllocationRepository{}

	pos := &entity.AllocationPosition{
		ChainID:       1,
		TokenAddress:  common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
		TokenSymbol:   "USDC",
		TokenDecimals: 6,
		Star:          "spark",
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

func TestBuildInsertArgs_NilScaledBalanceIsNull(t *testing.T) {
	// ScaledBalance is optional — nil should produce a NULL numeric.
	r := &AllocationRepository{}

	pos := &entity.AllocationPosition{
		ChainID:       1,
		TokenAddress:  common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
		TokenSymbol:   "USDC",
		TokenDecimals: 6,
		Star:          "spark",
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
