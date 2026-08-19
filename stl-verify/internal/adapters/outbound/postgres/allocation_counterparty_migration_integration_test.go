//go:build integration

package postgres

import (
	"bytes"
	"context"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/adapters/outbound/postgres/buildregistry"
	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

const allocCounterpartySchemaName = "test_alloc_counterparty"

var allocCounterpartyPool *pgxpool.Pool

func init() {
	registerTestFileSetup(allocCounterpartySchemaName, func() {
		allocCounterpartyPool = testutil.SetupSchemaForMain(sharedDSN, allocCounterpartySchemaName)
	}, func() {
		testutil.CleanupSchemaForMain(sharedDSN, allocCounterpartyPool, allocCounterpartySchemaName)
	})
}

func TestAllocationPositionCounterpartyColumnIsNullableBytea(t *testing.T) {
	ctx := context.Background()

	var dataType, isNullable string
	if err := allocCounterpartyPool.QueryRow(ctx, `
		SELECT data_type, is_nullable FROM information_schema.columns
		WHERE table_name = 'allocation_position'
		  AND column_name = 'counterparty_address'`,
	).Scan(&dataType, &isNullable); err != nil {
		t.Fatalf("query counterparty_address column: %v", err)
	}
	if dataType != "bytea" {
		t.Errorf("data_type = %q, want bytea", dataType)
	}
	if isNullable != "YES" {
		t.Errorf("is_nullable = %q, want YES (sweep rows and pre-existing rows carry NULL)", isNullable)
	}
}

func TestAllocationPositionCounterpartyColumnIsDocumented(t *testing.T) {
	ctx := context.Background()

	var comment *string
	if err := allocCounterpartyPool.QueryRow(ctx, `
		SELECT col_description('allocation_position'::regclass, attnum)
		FROM pg_attribute
		WHERE attrelid = 'allocation_position'::regclass AND attname = 'counterparty_address'`,
	).Scan(&comment); err != nil {
		t.Fatalf("query column comment: %v", err)
	}
	if comment == nil || *comment == "" {
		t.Fatal("counterparty_address has no COMMENT; the catalogue is the source of truth for column semantics")
	}
}

func TestSavePositions_PersistsCounterparty(t *testing.T) {
	ctx := context.Background()

	if _, err := allocCounterpartyPool.Exec(ctx,
		`INSERT INTO chain (chain_id, name) VALUES (1, 'mainnet') ON CONFLICT (chain_id) DO NOTHING`,
	); err != nil {
		t.Fatalf("seed chain: %v", err)
	}

	if _, err := allocCounterpartyPool.Exec(ctx,
		`INSERT INTO prime (name, vault_address)
		 VALUES ('spark', '\x691a6c29e9e96dd897718305427ad5d534db16ba')
		 ON CONFLICT DO NOTHING`,
	); err != nil {
		t.Fatalf("seed prime: %v", err)
	}

	var primeID int64
	if err := allocCounterpartyPool.QueryRow(ctx,
		`SELECT id FROM prime WHERE name = 'spark'`).Scan(&primeID); err != nil {
		t.Fatalf("look up spark prime: %v", err)
	}

	tokenRepo, err := NewTokenRepository(allocCounterpartyPool, nil, 0)
	if err != nil {
		t.Fatalf("NewTokenRepository: %v", err)
	}
	txm, err := NewTxManager(allocCounterpartyPool, nil)
	if err != nil {
		t.Fatalf("NewTxManager: %v", err)
	}
	repo := NewAllocationRepository(allocCounterpartyPool, txm, tokenRepo, nil, buildregistry.BuildID(1))

	usdcAddr := common.HexToAddress("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")
	proxyAddr := common.HexToAddress("0x2222222222222222222222222222222222222222")
	counterparty := common.HexToAddress("0x9999999999999999999999999999999999999999")
	blockTime := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)

	newPos := func(blockNumber int64, direction, txHash string, cp *common.Address) *entity.AllocationPosition {
		return &entity.AllocationPosition{
			ChainID:        1,
			TokenAddress:   usdcAddr,
			TokenSymbol:    "USDC",
			TokenDecimals:  6,
			PrimeID:        primeID,
			ProxyAddress:   proxyAddr,
			Balance:        big.NewInt(1_000_000),
			BlockNumber:    blockNumber,
			TxHash:         txHash,
			LogIndex:       1,
			TxAmount:       big.NewInt(1_000_000),
			Direction:      direction,
			Counterparty:   cp,
			CreatedAtBlock: blockNumber,
			CreatedAt:      blockTime,
		}
	}

	transferPos := newPos(24_600_100, "in",
		"0xda50e73f9d4722402ae4ec6e506c3726a78fc5f6146b4957bfadc2c1fffc8f8c", &counterparty)
	sweepPos := newPos(24_600_200, "sweep", "", nil)

	tx, err := allocCounterpartyPool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer tx.Rollback(ctx)

	if err := repo.SavePositions(ctx, tx, []*entity.AllocationPosition{transferPos, sweepPos}); err != nil {
		t.Fatalf("SavePositions: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	var got []byte
	if err := allocCounterpartyPool.QueryRow(ctx,
		`SELECT counterparty_address FROM allocation_position WHERE block_number = 24600100`,
	).Scan(&got); err != nil {
		t.Fatalf("query transfer row: %v", err)
	}
	if !bytes.Equal(got, counterparty.Bytes()) {
		t.Errorf("transfer row counterparty_address = %x, want %x", got, counterparty.Bytes())
	}

	var sweepCounterparty []byte
	if err := allocCounterpartyPool.QueryRow(ctx,
		`SELECT counterparty_address FROM allocation_position WHERE block_number = 24600200`,
	).Scan(&sweepCounterparty); err != nil {
		t.Fatalf("query sweep row: %v", err)
	}
	if sweepCounterparty != nil {
		t.Errorf("sweep row counterparty_address = %x, want NULL (no originating transfer)", sweepCounterparty)
	}
}
