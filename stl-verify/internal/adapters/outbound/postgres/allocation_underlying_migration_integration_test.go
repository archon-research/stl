//go:build integration

package postgres

import (
	"context"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

const allocUnderlyingSchemaName = "test_alloc_underlying"

var allocUnderlyingPool *pgxpool.Pool

func init() {
	registerTestFileSetup(allocUnderlyingSchemaName, func() {
		allocUnderlyingPool = testutil.SetupSchemaForMain(sharedDSN, allocUnderlyingSchemaName)
	}, func() {
		testutil.CleanupSchemaForMain(sharedDSN, allocUnderlyingPool, allocUnderlyingSchemaName)
	})
}

func TestAllocationPositionUnderlyingColumnsExist(t *testing.T) {
	ctx := context.Background()
	var count int
	err := allocUnderlyingPool.QueryRow(ctx, `
		SELECT count(*) FROM information_schema.columns
		WHERE table_name = 'allocation_position'
		  AND column_name IN ('underlying_value', 'underlying_token_id')
		  AND is_nullable = 'YES'`).Scan(&count)
	if err != nil {
		t.Fatalf("query columns: %v", err)
	}
	if count != 2 {
		t.Fatalf("expected 2 nullable underlying columns, got %d", count)
	}
}

func TestAllocationPositionUnderlyingPairCheckConstraintExists(t *testing.T) {
	ctx := context.Background()
	// Constraint metadata is enough: inserting a full row needs the whole
	// natural key; the CHECK's presence + definition is the behaviour under test.
	var def string
	err := allocUnderlyingPool.QueryRow(ctx, `
		SELECT pg_get_constraintdef(oid) FROM pg_constraint
		WHERE conname = 'allocation_position_underlying_pair_check'`).Scan(&def)
	if err != nil {
		t.Fatalf("CHECK constraint missing: %v", err)
	}
	if !strings.Contains(def, "underlying_value IS NULL") || !strings.Contains(def, "underlying_token_id IS NULL") || !strings.Contains(def, "=") {
		t.Fatalf("CHECK definition = %q, want both-NULL equality expression", def)
	}
}

func TestAllocationPositionUnderlyingTokenFKExists(t *testing.T) {
	ctx := context.Background()
	var def string
	err := allocUnderlyingPool.QueryRow(ctx, `
		SELECT pg_get_constraintdef(oid) FROM pg_constraint
		WHERE conname = 'allocation_position_underlying_token_id_fkey'`).Scan(&def)
	if err != nil {
		t.Fatalf("FK constraint missing: %v", err)
	}
	if !strings.Contains(def, "REFERENCES token(id)") {
		t.Fatalf("FK definition = %q, want REFERENCES token(id)", def)
	}
}

func TestSavePositions_PersistsUnderlyingValuation(t *testing.T) {
	ctx := context.Background()

	// Seed chain idempotently — allocation_position needs a chain row.
	if _, err := allocUnderlyingPool.Exec(ctx,
		`INSERT INTO chain (chain_id, name) VALUES (1, 'mainnet') ON CONFLICT (chain_id) DO NOTHING`,
	); err != nil {
		t.Fatalf("seed chain: %v", err)
	}

	// Seed prime idempotently — allocation_position needs a prime_id FK.
	var primeID int64
	if err := allocUnderlyingPool.QueryRow(ctx,
		`SELECT id FROM prime WHERE name = 'spark'`).Scan(&primeID); err != nil {
		t.Fatalf("look up spark prime: %v", err)
	}

	// Clean up any allocation_position rows from prior runs so each run is
	// self-contained. We delete only allocation_position, avoiding a CASCADE
	// truncate on token (which can cascade cross-schema via public.* FK chains
	// and disrupt sibling test schemas that share the same PG instance).
	if _, err := allocUnderlyingPool.Exec(ctx, `DELETE FROM allocation_position`); err != nil {
		t.Fatalf("delete allocation_position: %v", err)
	}

	tokenRepo, err := NewTokenRepository(allocUnderlyingPool, nil, 0)
	if err != nil {
		t.Fatalf("NewTokenRepository: %v", err)
	}

	txm, err := NewTxManager(allocUnderlyingPool, nil)
	if err != nil {
		t.Fatalf("NewTxManager: %v", err)
	}

	repo := NewAllocationRepository(allocUnderlyingPool, txm, tokenRepo, nil, buildregistry.BuildID(1))

	vaultAddr := common.HexToAddress("0x38464507e02c983f20428a6e8566693fe9e422a9")
	proxyAddr := common.HexToAddress("0x1111111111111111111111111111111111111111")
	usdcAddr := common.HexToAddress("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")
	blockTime := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)

	// Position A: vault share with USDC underlying valuation.
	posA := &entity.AllocationPosition{
		ChainID:        1,
		TokenAddress:   vaultAddr,
		TokenSymbol:    "bbqUSDC",
		TokenDecimals:  18,
		PrimeID:        primeID,
		ProxyAddress:   proxyAddr,
		Balance:        big.NewInt(1_000_000_000_000_000_000),
		BlockNumber:    24_584_100,
		BlockVersion:   0,
		TxHash:         "0xda50e73f9d4722402ae4ec6e506c3726a78fc5f6146b4957bfadc2c1fffc8f8c",
		LogIndex:       1,
		TxAmount:       big.NewInt(1_000_000_000_000_000_000),
		Direction:      "in",
		CreatedAtBlock: 24_584_100,
		CreatedAt:      blockTime,
		Underlying: &entity.UnderlyingValuation{
			Value:         big.NewInt(20_102_052_000_000), // 20,102,052 USDC raw
			AssetAddress:  usdcAddr,
			AssetSymbol:   "USDC",
			AssetDecimals: 6,
		},
	}

	// Position B: same vault, no Underlying.
	posB := &entity.AllocationPosition{
		ChainID:        1,
		TokenAddress:   vaultAddr,
		TokenSymbol:    "bbqUSDC",
		TokenDecimals:  18,
		PrimeID:        primeID,
		ProxyAddress:   proxyAddr,
		Balance:        big.NewInt(500_000_000_000_000_000),
		BlockNumber:    24_584_200,
		BlockVersion:   0,
		TxHash:         "0xee50e73f9d4722402ae4ec6e506c3726a78fc5f6146b4957bfadc2c1fffc8f8c",
		LogIndex:       2,
		TxAmount:       big.NewInt(500_000_000_000_000_000),
		Direction:      "out",
		CreatedAtBlock: 24_584_100,
		CreatedAt:      blockTime,
	}

	tx, err := allocUnderlyingPool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer tx.Rollback(ctx)

	if err := repo.SavePositions(ctx, tx, []*entity.AllocationPosition{posA, posB}); err != nil {
		t.Fatalf("SavePositions: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	// Verify position A: underlying_value = 20102052.000000, underlying_token_id
	// equals the token row for (chain_id=1, usdcAddr).
	var underlyingValueStr string
	var underlyingTokenID int64
	if err := allocUnderlyingPool.QueryRow(ctx, `
		SELECT underlying_value::text, underlying_token_id
		FROM allocation_position
		WHERE block_number = 24584100 AND log_index = 1`,
	).Scan(&underlyingValueStr, &underlyingTokenID); err != nil {
		t.Fatalf("query position A: %v", err)
	}
	if underlyingValueStr != "20102052.000000" {
		t.Fatalf("position A underlying_value = %q, want 20102052.000000", underlyingValueStr)
	}

	// Confirm that underlying_token_id points to the USDC token row.
	var usdcTokenID int64
	if err := allocUnderlyingPool.QueryRow(ctx,
		`SELECT id FROM token WHERE chain_id = 1 AND address = $1`,
		usdcAddr.Bytes(),
	).Scan(&usdcTokenID); err != nil {
		t.Fatalf("look up USDC token: %v", err)
	}
	if underlyingTokenID != usdcTokenID {
		t.Fatalf("position A underlying_token_id = %d, want %d (USDC token.id)", underlyingTokenID, usdcTokenID)
	}

	// Verify position B: both columns NULL.
	var nullValue *string
	var nullTokenID *int64
	if err := allocUnderlyingPool.QueryRow(ctx, `
		SELECT underlying_value::text, underlying_token_id
		FROM allocation_position
		WHERE block_number = 24584200 AND log_index = 2`,
	).Scan(&nullValue, &nullTokenID); err != nil {
		t.Fatalf("query position B: %v", err)
	}
	if nullValue != nil {
		t.Fatalf("position B underlying_value = %v, want NULL", *nullValue)
	}
	if nullTokenID != nil {
		t.Fatalf("position B underlying_token_id = %v, want NULL", *nullTokenID)
	}
}
