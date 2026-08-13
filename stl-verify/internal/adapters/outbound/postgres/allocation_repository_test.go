package postgres

import (
	"bytes"
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

	_, args, err := r.buildInsertArgs(pos, 3, nil)
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

	_, args, err := r.buildInsertArgs(pos, 609, nil)
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

func TestEncodeTxHash_SweepUsesZeroSentinel(t *testing.T) {
	// Sweep (reconciliation) positions have no originating transaction. Rather
	// than fabricating a realistic-looking hash that masquerades as an on-chain
	// transaction (VEC-340), encodeTxHash stores the zero hash as an explicit
	// "no transaction" sentinel — a value no real transaction hash takes in
	// practice (a collision is cryptographically negligible).
	pos := &entity.AllocationPosition{
		ChainID:      1,
		TokenAddress: common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48"),
		ProxyAddress: common.HexToAddress("0x1111111111111111111111111111111111111111"),
		BlockNumber:  24584100,
		Direction:    "sweep",
		// TxHash intentionally empty: sweeps are not transaction-driven.
	}

	got, err := encodeTxHash(pos)
	if err != nil {
		t.Fatalf("encodeTxHash: %v", err)
	}

	want := make([]byte, common.HashLength)
	if !bytes.Equal(got, want) {
		t.Errorf("sweep tx_hash = %x, want all-zero %d-byte sentinel", got, common.HashLength)
	}
}

func TestEncodeTxHash_NonSweepMissingHashErrors(t *testing.T) {
	// Only sweeps may omit a transaction hash. A transaction-driven position
	// without one signals an upstream bug and must be rejected, not silently
	// stored as the "no transaction" sentinel.
	pos := &entity.AllocationPosition{Direction: "in"}

	if _, err := encodeTxHash(pos); err == nil {
		t.Fatal("expected error for non-sweep position with empty tx_hash")
	}
}

func TestEncodeTxHash_TruncatedHashErrors(t *testing.T) {
	// common.FromHex decodes short/truncated hex without complaint; encodeTxHash
	// must reject anything that isn't a full 32-byte transaction hash.
	pos := &entity.AllocationPosition{Direction: "in", TxHash: "0xabc"}

	if _, err := encodeTxHash(pos); err == nil {
		t.Fatal("expected error for truncated tx_hash")
	}
}

func TestEncodeTxHash_RealTransactionDecoded(t *testing.T) {
	// Transaction-driven positions keep their on-chain hash verbatim.
	pos := &entity.AllocationPosition{
		TxHash: "0xda50e73f9d4722402ae4ec6e506c3726a78fc5f6146b4957bfadc2c1fffc8f8c",
	}

	got, err := encodeTxHash(pos)
	if err != nil {
		t.Fatalf("encodeTxHash: %v", err)
	}

	want := common.FromHex(pos.TxHash)
	if !bytes.Equal(got, want) {
		t.Errorf("tx_hash = %x, want %x", got, want)
	}
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
		TxHash:        "0xda50e73f9d4722402ae4ec6e506c3726a78fc5f6146b4957bfadc2c1fffc8f8c",
		LogIndex:      1,
		TxAmount:      big.NewInt(1000000),
		Direction:     "in",
	}

	_, args, err := r.buildInsertArgs(pos, 3, nil)
	if err != nil {
		t.Fatalf("buildInsertArgs: %v", err)
	}

	// nil scaled_balance → invalid (NULL)
	scaled := args[5].(pgtype.Numeric)
	if scaled.Valid {
		t.Fatal("expected scaled_balance to be NULL (invalid)")
	}
}

func TestBuildInsertArgs_UnderlyingValueUsesAssetDecimals(t *testing.T) {
	r := &AllocationRepository{}
	underlyingID := int64(77)
	pos := &entity.AllocationPosition{
		ChainID:       1,
		TokenAddress:  common.HexToAddress("0x38464507e02c983f20428a6e8566693fe9e422a9"),
		TokenDecimals: 18, // share token decimals — must NOT drive the value
		Balance:       big.NewInt(1),
		TxAmount:      big.NewInt(0),
		Direction:     "sweep", // minimal valid direction for encodeTxHash
		Underlying: &entity.UnderlyingValuation{
			Value:         big.NewInt(20_102_052_000_000), // 20,102,052 USDC raw
			AssetAddress:  common.HexToAddress("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48"),
			AssetSymbol:   "USDC",
			AssetDecimals: 6, // regression guard for the grove-bbqUSDC-V2 decimals class
		},
	}
	_, args, err := r.buildInsertArgs(pos, 1, &underlyingID)
	if err != nil {
		t.Fatalf("buildInsertArgs: %v", err)
	}
	underlying := args[14].(pgtype.Numeric) // $15
	if !underlying.Valid {
		t.Fatal("underlying_value must be non-NULL")
	}
	if underlying.Exp != -6 {
		t.Fatalf("underlying_value Exp = %d, want -6 (asset decimals, not share decimals)", underlying.Exp)
	}
	if got := args[15].(*int64); got == nil || *got != 77 { // $16
		t.Fatalf("underlying_token_id = %v, want 77", got)
	}
}

func TestBuildInsertArgs_NilUnderlyingWritesBothNull(t *testing.T) {
	r := &AllocationRepository{}
	pos := &entity.AllocationPosition{
		ChainID:      1,
		TokenAddress: common.HexToAddress("0x38464507e02c983f20428a6e8566693fe9e422a9"),
		Balance:      big.NewInt(1),
		TxAmount:     big.NewInt(0),
		Direction:    "sweep", // minimal valid direction for encodeTxHash
	}
	_, args, err := r.buildInsertArgs(pos, 1, nil)
	if err != nil {
		t.Fatalf("buildInsertArgs: %v", err)
	}
	if args[14].(pgtype.Numeric).Valid {
		t.Fatal("underlying_value must be NULL when no valuation")
	}
	if args[15].(*int64) != nil {
		t.Fatal("underlying_token_id must be NULL when no valuation")
	}
}
