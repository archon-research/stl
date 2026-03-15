//go:build integration

package postgres

import (
	"context"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

const receiptTokenSchemaName = "test_receipt_token"

var receiptTokenPool *pgxpool.Pool

func init() {
	registerTestFileSetup(receiptTokenSchemaName, func() {
		receiptTokenPool = testutil.SetupSchemaForMain(sharedDSN, receiptTokenSchemaName)
	}, func() {
		testutil.CleanupSchemaForMain(sharedDSN, receiptTokenPool, receiptTokenSchemaName)
	})
}

// truncateReceiptTokenTables clears repository tables for test isolation.
func truncateReceiptTokenTables(t *testing.T, ctx context.Context) {
	t.Helper()
	_, err := receiptTokenPool.Exec(ctx, `TRUNCATE receipt_token, protocol, token RESTART IDENTITY CASCADE`)
	if err != nil {
		t.Fatalf("failed to truncate receipt-token tables: %v", err)
	}
}

func TestListTrackedReceiptTokens_ReturnsSeededRowsForChain(t *testing.T) {
	// Spec coverage: plan-transfer-user-discovery.md acceptance criterion 1.
	truncateReceiptTokenTables(t, context.Background())
	ctx := context.Background()

	repo, err := NewReceiptTokenRepository(receiptTokenPool, nil, 0)
	if err != nil {
		t.Fatalf("NewReceiptTokenRepository: %v", err)
	}

	chain1ProtocolAddressA := common.HexToAddress("0x1000000000000000000000000000000000000001")
	chain1ProtocolAddressB := common.HexToAddress("0x1000000000000000000000000000000000000002")
	chain137ProtocolAddress := common.HexToAddress("0x2000000000000000000000000000000000000001")

	chain1ReceiptTokenA := common.HexToAddress("0x3000000000000000000000000000000000000001")
	chain1ReceiptTokenB := common.HexToAddress("0x3000000000000000000000000000000000000002")
	chain137ReceiptToken := common.HexToAddress("0x4000000000000000000000000000000000000001")

	seeded := seedTrackedReceiptTokens(t, ctx, []trackedReceiptTokenSeed{
		{
			chainID:             1,
			protocolAddress:     chain1ProtocolAddressA,
			protocolName:        "SparkLend Main Pool A",
			underlyingAddress:   common.HexToAddress("0x5000000000000000000000000000000000000001"),
			underlyingSymbol:    "USDC",
			receiptTokenAddress: chain1ReceiptTokenA,
			receiptTokenSymbol:  "spUSDC",
		},
		{
			chainID:             1,
			protocolAddress:     chain1ProtocolAddressB,
			protocolName:        "SparkLend Main Pool B",
			underlyingAddress:   common.HexToAddress("0x5000000000000000000000000000000000000002"),
			underlyingSymbol:    "WETH",
			receiptTokenAddress: chain1ReceiptTokenB,
			receiptTokenSymbol:  "spWETH",
		},
		{
			chainID:             137,
			protocolAddress:     chain137ProtocolAddress,
			protocolName:        "Other Chain Pool",
			underlyingAddress:   common.HexToAddress("0x6000000000000000000000000000000000000001"),
			underlyingSymbol:    "USDT",
			receiptTokenAddress: chain137ReceiptToken,
			receiptTokenSymbol:  "spUSDT",
		},
	})

	got, err := repo.ListTrackedReceiptTokens(ctx, 1)
	if err != nil {
		t.Fatalf("ListTrackedReceiptTokens: %v", err)
	}

	if len(got) != 2 {
		t.Fatalf("len(got) = %d, want 2", len(got))
	}

	byReceiptTokenAddress := make(map[common.Address]outbound.TrackedReceiptToken, len(got))
	for _, token := range got {
		byReceiptTokenAddress[token.ReceiptTokenAddress] = token
		if token.ChainID != 1 {
			t.Fatalf("token.ChainID = %d, want 1 for all returned rows", token.ChainID)
		}
	}

	assertTrackedReceiptToken(t, byReceiptTokenAddress[chain1ReceiptTokenA], seeded[chain1ReceiptTokenA])
	assertTrackedReceiptToken(t, byReceiptTokenAddress[chain1ReceiptTokenB], seeded[chain1ReceiptTokenB])

	if _, exists := byReceiptTokenAddress[chain137ReceiptToken]; exists {
		t.Fatal("returned receipt token from another chain")
	}
}

func TestListTrackedReceiptTokens_UnknownChainReturnsEmptyResult(t *testing.T) {
	// Spec coverage: plan-transfer-user-discovery.md acceptance criterion 2.
	truncateReceiptTokenTables(t, context.Background())
	ctx := context.Background()

	repo, err := NewReceiptTokenRepository(receiptTokenPool, nil, 0)
	if err != nil {
		t.Fatalf("NewReceiptTokenRepository: %v", err)
	}

	seedTrackedReceiptTokens(t, ctx, []trackedReceiptTokenSeed{{
		chainID:             1,
		protocolAddress:     common.HexToAddress("0x7000000000000000000000000000000000000001"),
		protocolName:        "Known Chain Pool",
		underlyingAddress:   common.HexToAddress("0x8000000000000000000000000000000000000001"),
		underlyingSymbol:    "DAI",
		receiptTokenAddress: common.HexToAddress("0x9000000000000000000000000000000000000001"),
		receiptTokenSymbol:  "spDAI",
	}})

	got, err := repo.ListTrackedReceiptTokens(ctx, 999999)
	if err != nil {
		t.Fatalf("ListTrackedReceiptTokens: %v", err)
	}

	if len(got) != 0 {
		t.Fatalf("len(got) = %d, want 0 for unknown chain", len(got))
	}
}

type trackedReceiptTokenSeed struct {
	chainID             int64
	protocolAddress     common.Address
	protocolName        string
	underlyingAddress   common.Address
	underlyingSymbol    string
	receiptTokenAddress common.Address
	receiptTokenSymbol  string
}

func seedTrackedReceiptTokens(t *testing.T, ctx context.Context, seeds []trackedReceiptTokenSeed) map[common.Address]outbound.TrackedReceiptToken {
	t.Helper()

	seeded := make(map[common.Address]outbound.TrackedReceiptToken, len(seeds))
	insertedChains := make(map[int64]bool)

	for _, seed := range seeds {
		if !insertedChains[seed.chainID] {
			_, err := receiptTokenPool.Exec(ctx,
				`INSERT INTO chain (chain_id, name) VALUES ($1, $2) ON CONFLICT (chain_id) DO NOTHING`,
				seed.chainID, chainNameForTest(seed.chainID),
			)
			if err != nil {
				t.Fatalf("insert chain: %v", err)
			}
			insertedChains[seed.chainID] = true
		}

		var protocolID int64
		err := receiptTokenPool.QueryRow(ctx,
			`INSERT INTO protocol (chain_id, address, name, protocol_type, created_at_block) VALUES ($1, $2, $3, $4, $5) RETURNING id`,
			seed.chainID, seed.protocolAddress.Bytes(), seed.protocolName, "lending", int64(100),
		).Scan(&protocolID)
		if err != nil {
			t.Fatalf("insert protocol: %v", err)
		}

		var underlyingTokenID int64
		err = receiptTokenPool.QueryRow(ctx,
			`INSERT INTO token (chain_id, address, symbol, decimals, created_at_block) VALUES ($1, $2, $3, $4, $5) RETURNING id`,
			seed.chainID, seed.underlyingAddress.Bytes(), seed.underlyingSymbol, 18, int64(100),
		).Scan(&underlyingTokenID)
		if err != nil {
			t.Fatalf("insert token: %v", err)
		}

		var receiptTokenID int64
		err = receiptTokenPool.QueryRow(ctx,
			`INSERT INTO receipt_token (protocol_id, underlying_token_id, receipt_token_address, symbol, created_at_block) VALUES ($1, $2, $3, $4, $5) RETURNING id`,
			protocolID, underlyingTokenID, seed.receiptTokenAddress.Bytes(), seed.receiptTokenSymbol, int64(100),
		).Scan(&receiptTokenID)
		if err != nil {
			t.Fatalf("insert receipt_token: %v", err)
		}

		seeded[seed.receiptTokenAddress] = outbound.TrackedReceiptToken{
			ID:                  receiptTokenID,
			ProtocolID:          protocolID,
			ProtocolAddress:     seed.protocolAddress,
			UnderlyingTokenID:   underlyingTokenID,
			ReceiptTokenAddress: seed.receiptTokenAddress,
			Symbol:              seed.receiptTokenSymbol,
			ChainID:             seed.chainID,
		}
	}

	return seeded
}

func chainNameForTest(chainID int64) string {
	if chainID == 1 {
		return "Ethereum Mainnet"
	}
	return "Chain under test"
}

func assertTrackedReceiptToken(t *testing.T, got, want outbound.TrackedReceiptToken) {
	t.Helper()

	if got.ID != want.ID {
		t.Fatalf("ID = %d, want %d", got.ID, want.ID)
	}
	if got.ProtocolID != want.ProtocolID {
		t.Fatalf("ProtocolID = %d, want %d", got.ProtocolID, want.ProtocolID)
	}
	if got.ProtocolAddress != want.ProtocolAddress {
		t.Fatalf("ProtocolAddress = %s, want %s", got.ProtocolAddress.Hex(), want.ProtocolAddress.Hex())
	}
	if got.UnderlyingTokenID != want.UnderlyingTokenID {
		t.Fatalf("UnderlyingTokenID = %d, want %d", got.UnderlyingTokenID, want.UnderlyingTokenID)
	}
	if got.ReceiptTokenAddress != want.ReceiptTokenAddress {
		t.Fatalf("ReceiptTokenAddress = %s, want %s", got.ReceiptTokenAddress.Hex(), want.ReceiptTokenAddress.Hex())
	}
	if got.Symbol != want.Symbol {
		t.Fatalf("Symbol = %q, want %q", got.Symbol, want.Symbol)
	}
	if got.ChainID != want.ChainID {
		t.Fatalf("ChainID = %d, want %d", got.ChainID, want.ChainID)
	}
}
