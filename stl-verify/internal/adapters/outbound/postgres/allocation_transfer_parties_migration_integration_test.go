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

const allocTransferPartiesDBName = "test_alloc_transfer_parties"

var allocTransferPartiesPool *pgxpool.Pool

func init() {
	registerTestFileSetup(func() {
		allocTransferPartiesPool = testutil.SetupDBForMain(sharedDSN, allocTransferPartiesDBName)
	}, func() {
		testutil.CleanupDBForMain(sharedDSN, allocTransferPartiesPool, allocTransferPartiesDBName)
	})
}

func TestAllocationPositionTransferPartyColumnsAreNullableBytea(t *testing.T) {
	ctx := context.Background()

	for _, column := range []string{"from_address", "to_address"} {
		var dataType, isNullable string
		if err := allocTransferPartiesPool.QueryRow(ctx, `
			SELECT data_type, is_nullable FROM information_schema.columns
			WHERE table_name = 'allocation_position' AND column_name = $1`,
			column,
		).Scan(&dataType, &isNullable); err != nil {
			t.Fatalf("query %s column: %v", column, err)
		}
		if dataType != "bytea" {
			t.Errorf("%s data_type = %q, want bytea", column, dataType)
		}
		if isNullable != "YES" {
			t.Errorf("%s is_nullable = %q, want YES (sweep rows and pre-existing rows carry NULL)", column, isNullable)
		}
	}
}

func TestAllocationPositionTransferPartyColumnsAreDocumented(t *testing.T) {
	ctx := context.Background()

	for _, column := range []string{"from_address", "to_address"} {
		var comment *string
		if err := allocTransferPartiesPool.QueryRow(ctx, `
			SELECT col_description('allocation_position'::regclass, attnum)
			FROM pg_attribute
			WHERE attrelid = 'allocation_position'::regclass AND attname = $1`,
			column,
		).Scan(&comment); err != nil {
			t.Fatalf("query %s comment: %v", column, err)
		}
		if comment == nil || *comment == "" {
			t.Fatalf("%s has no COMMENT; the catalogue is the source of truth for column semantics", column)
		}
	}
}

func TestSavePositions_PersistsTransferParties(t *testing.T) {
	ctx := context.Background()

	if _, err := allocTransferPartiesPool.Exec(ctx,
		`INSERT INTO chain (chain_id, name) VALUES (1, 'mainnet') ON CONFLICT (chain_id) DO NOTHING`,
	); err != nil {
		t.Fatalf("seed chain: %v", err)
	}

	if _, err := allocTransferPartiesPool.Exec(ctx,
		`INSERT INTO prime (name, vault_address)
		 VALUES ('spark', '\x691a6c29e9e96dd897718305427ad5d534db16ba')
		 ON CONFLICT DO NOTHING`,
	); err != nil {
		t.Fatalf("seed prime: %v", err)
	}

	var primeID int64
	if err := allocTransferPartiesPool.QueryRow(ctx,
		`SELECT id FROM prime WHERE name = 'spark'`).Scan(&primeID); err != nil {
		t.Fatalf("look up spark prime: %v", err)
	}

	tokenRepo, err := NewTokenRepository(allocTransferPartiesPool, nil, 0, buildregistry.RunID(1))
	if err != nil {
		t.Fatalf("NewTokenRepository: %v", err)
	}
	txm, err := NewTxManager(allocTransferPartiesPool, nil)
	if err != nil {
		t.Fatalf("NewTxManager: %v", err)
	}
	repo := NewAllocationRepository(allocTransferPartiesPool, txm, tokenRepo, nil, buildregistry.BuildID(1), buildregistry.RunID(1))

	usdcAddr := common.HexToAddress("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")
	proxyAddr := common.HexToAddress("0x2222222222222222222222222222222222222222")
	counterparty := common.HexToAddress("0x9999999999999999999999999999999999999999")
	mint := common.Address{}
	blockTime := time.Date(2026, 8, 19, 12, 0, 0, 0, time.UTC)

	newPos := func(blockNumber int64, direction, txHash string, from, to *common.Address) *entity.AllocationPosition {
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
			FromAddress:    from,
			ToAddress:      to,
			CreatedAtBlock: blockNumber,
			CreatedAt:      blockTime,
		}
	}

	inboundPos := newPos(24_600_100, "in",
		"0xda50e73f9d4722402ae4ec6e506c3726a78fc5f6146b4957bfadc2c1fffc8f8c", &counterparty, &proxyAddr)
	outboundPos := newPos(24_600_200, "out",
		"0xee50e73f9d4722402ae4ec6e506c3726a78fc5f6146b4957bfadc2c1fffc8f8c", &proxyAddr, &counterparty)
	mintPos := newPos(24_600_300, "in",
		"0xaa50e73f9d4722402ae4ec6e506c3726a78fc5f6146b4957bfadc2c1fffc8f8c", &mint, &proxyAddr)
	sweepPos := newPos(24_600_400, "sweep", "", nil, nil)

	tx, err := allocTransferPartiesPool.Begin(ctx)
	if err != nil {
		t.Fatalf("Begin: %v", err)
	}
	defer tx.Rollback(ctx)

	if err := repo.SavePositions(ctx, tx, []*entity.AllocationPosition{
		inboundPos, outboundPos, mintPos, sweepPos,
	}); err != nil {
		t.Fatalf("SavePositions: %v", err)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatalf("Commit: %v", err)
	}

	cases := []struct {
		name             string
		blockNumber      int64
		wantFrom, wantTo []byte
	}{
		{
			name:        "inbound row stores both sides as decoded",
			blockNumber: 24_600_100,
			wantFrom:    counterparty.Bytes(),
			wantTo:      proxyAddr.Bytes(),
		},
		{
			name:        "outbound row stores both sides as decoded",
			blockNumber: 24_600_200,
			wantFrom:    proxyAddr.Bytes(),
			wantTo:      counterparty.Bytes(),
		},
		{
			// The zero address is a real party, so it must survive the round trip
			// as 20 zero bytes rather than collapsing to NULL, which means "no
			// transfer at all".
			name:        "mint stores the zero address, not NULL",
			blockNumber: 24_600_300,
			wantFrom:    make([]byte, common.AddressLength),
			wantTo:      proxyAddr.Bytes(),
		},
		{
			name:        "sweep row stores NULL on both sides",
			blockNumber: 24_600_400,
			wantFrom:    nil,
			wantTo:      nil,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var gotFrom, gotTo []byte
			if err := allocTransferPartiesPool.QueryRow(ctx,
				`SELECT from_address, to_address FROM allocation_position WHERE block_number = $1`,
				tc.blockNumber,
			).Scan(&gotFrom, &gotTo); err != nil {
				t.Fatalf("query row: %v", err)
			}
			if !bytes.Equal(gotFrom, tc.wantFrom) {
				t.Errorf("from_address = %x, want %x", gotFrom, tc.wantFrom)
			}
			if !bytes.Equal(gotTo, tc.wantTo) {
				t.Errorf("to_address = %x, want %x", gotTo, tc.wantTo)
			}
		})
	}
}
