package allocation_tracker

import (
	"context"
	"log/slog"
	"maps"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
)

// fakeAllocRepo captures positions passed to SavePositions.
type fakeAllocRepo struct {
	saved []*entity.AllocationPosition
	err   error
}

func (r *fakeAllocRepo) SavePositions(_ context.Context, positions []*entity.AllocationPosition) error {
	if r.err != nil {
		return r.err
	}
	r.saved = append(r.saved, positions...)
	return nil
}

// newTestHandler creates a PrimePositionHandler with a pre-populated metadata
// cache, bypassing the multicaller. This isolates handler logic from RPC calls.
func newTestHandler(
	repo *fakeAllocRepo,
	primeLookup map[string]int64,
	metadata map[common.Address]tokenMeta,
) *PrimePositionHandler {
	h := &PrimePositionHandler{
		repo:        repo,
		primeLookup: primeLookup,
		metadata:    newMetadataCache(nil, nil, slog.Default()),
		logger:      slog.Default().With("component", "test-handler"),
	}
	// Pre-populate the cache so we don't need a multicaller.
	maps.Copy(h.metadata.cache, metadata)
	return h
}

func TestHandleSnapshots_ERC20(t *testing.T) {
	usdc := common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
	wallet := common.HexToAddress("0x1601843c5e9bc251a3272907010afa41fa18347e")

	repo := &fakeAllocRepo{}
	handler := newTestHandler(repo,
		map[string]int64{"spark": 1},
		map[common.Address]tokenMeta{
			usdc: {symbol: "USDC", decimals: 6},
		},
	)

	err := handler.HandleSnapshots(context.Background(), []*PositionSnapshot{
		{
			Entry: &TokenEntry{
				ContractAddress: usdc,
				WalletAddress:   wallet,
				Star:            "spark",
				Chain:           "mainnet",
				TokenType:       "erc20",
			},
			Balance:     big.NewInt(1000000),
			ChainID:     1,
			BlockNumber: 100,
			TxAmount:    big.NewInt(1000000),
			Direction:   DirectionSweep,
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(repo.saved) != 1 {
		t.Fatalf("expected 1 saved position, got %d", len(repo.saved))
	}

	pos := repo.saved[0]
	if pos.TokenSymbol != "USDC" {
		t.Errorf("symbol = %q, want USDC", pos.TokenSymbol)
	}
	if pos.TokenDecimals != 6 {
		t.Errorf("decimals = %d, want 6", pos.TokenDecimals)
	}
	if pos.PrimeID != 1 {
		t.Errorf("prime_id = %d, want 1", pos.PrimeID)
	}
}

func TestHandleSnapshots_UniV3Pool_UsesAssetMetadata(t *testing.T) {
	pool := common.HexToAddress("0xbafead7c60ea473758ed6c6021505e8bbd7e8e5d")
	usdc := common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
	wallet := common.HexToAddress("0x491edfb0b8b608044e227225c715981a30f3a44e")

	repo := &fakeAllocRepo{}
	handler := newTestHandler(repo,
		map[string]int64{"grove": 2},
		map[common.Address]tokenMeta{
			usdc: {symbol: "USDC", decimals: 6},
		},
	)

	err := handler.HandleSnapshots(context.Background(), []*PositionSnapshot{
		{
			Entry: &TokenEntry{
				ContractAddress: pool,
				WalletAddress:   wallet,
				AssetAddress:    &usdc,
				Star:            "grove",
				Chain:           "mainnet",
				TokenType:       "uni_v3_pool",
			},
			Balance:       big.NewInt(17229995299715),
			ScaledBalance: big.NewInt(24999464528264),
			ChainID:       1,
			BlockNumber:   100,
			TxAmount:      big.NewInt(0),
			Direction:     DirectionSweep,
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(repo.saved) != 1 {
		t.Fatalf("expected 1 saved position, got %d", len(repo.saved))
	}

	pos := repo.saved[0]
	// Should use USDC metadata, not the pool contract.
	if pos.TokenSymbol != "USDC" {
		t.Errorf("symbol = %q, want USDC (from asset, not pool)", pos.TokenSymbol)
	}
	if pos.TokenDecimals != 6 {
		t.Errorf("decimals = %d, want 6 (from asset)", pos.TokenDecimals)
	}
	if pos.TokenAddress != pool {
		t.Errorf("token_address should be pool contract, got %s", pos.TokenAddress.Hex())
	}
	if pos.PrimeID != 2 {
		t.Errorf("prime_id = %d, want 2", pos.PrimeID)
	}
}

func TestHandleSnapshots_UniV3LP_UsesAssetMetadata(t *testing.T) {
	lp := common.HexToAddress("0x6b405dca74897c9442d369dcf6c0ec230f7e1c7c")
	ausd := common.HexToAddress("0x00000000efe302beaa2b3e6e1b18d08d69a9012a")
	wallet := common.HexToAddress("0x94b398acb2fce988871218221ea6a4a2b26cccbc")

	repo := &fakeAllocRepo{}
	handler := newTestHandler(repo,
		map[string]int64{"grove": 2},
		map[common.Address]tokenMeta{
			ausd: {symbol: "AUSD", decimals: 18},
		},
	)

	err := handler.HandleSnapshots(context.Background(), []*PositionSnapshot{
		{
			Entry: &TokenEntry{
				ContractAddress: lp,
				WalletAddress:   wallet,
				AssetAddress:    &ausd,
				Star:            "grove",
				Chain:           "monad",
				TokenType:       "uni_v3_lp",
			},
			Balance:     big.NewInt(5000000000000000000),
			ChainID:     1,
			BlockNumber: 100,
			TxAmount:    big.NewInt(0),
			Direction:   DirectionSweep,
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(repo.saved) != 1 {
		t.Fatalf("expected 1 saved position, got %d", len(repo.saved))
	}

	pos := repo.saved[0]
	if pos.TokenSymbol != "AUSD" {
		t.Errorf("symbol = %q, want AUSD (from asset)", pos.TokenSymbol)
	}
	if pos.TokenDecimals != 18 {
		t.Errorf("decimals = %d, want 18 (from asset)", pos.TokenDecimals)
	}
}

func TestHandleSnapshots_ERC4626_PreservesTokenUnits(t *testing.T) {
	susds := common.HexToAddress("0xa3931d71877c0e7a3148cb7eb4463524fec27fbd")
	usds := common.HexToAddress("0xdc035d45d973e3ec169d2276ddab16f1e407384f")
	wallet := common.HexToAddress("0x1601843c5e9bc251a3272907010afa41fa18347e")
	rawShares, _ := new(big.Int).SetString("1201619730663240195228985093", 10)

	repo := &fakeAllocRepo{}
	handler := newTestHandler(repo,
		map[string]int64{"spark": 1},
		map[common.Address]tokenMeta{
			susds: {symbol: "sUSDS", decimals: 18},
			usds:  {symbol: "USDS", decimals: 18},
		},
	)

	err := handler.HandleSnapshots(context.Background(), []*PositionSnapshot{
		{
			Entry: &TokenEntry{
				ContractAddress: susds,
				WalletAddress:   wallet,
				AssetAddress:    &usds,
				Star:            "spark",
				Chain:           "mainnet",
				TokenType:       "erc4626",
			},
			Balance:       rawShares,
			ScaledBalance: rawShares,
			ChainID:       1,
			BlockNumber:   100,
			TxAmount:      big.NewInt(0),
			Direction:     DirectionSweep,
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(repo.saved) != 1 {
		t.Fatalf("expected 1 saved position, got %d", len(repo.saved))
	}

	pos := repo.saved[0]
	if pos.TokenSymbol != "sUSDS" {
		t.Errorf("symbol = %q, want sUSDS", pos.TokenSymbol)
	}
	if pos.TokenDecimals != 18 {
		t.Errorf("decimals = %d, want 18", pos.TokenDecimals)
	}
	if pos.Balance.Cmp(rawShares) != 0 {
		t.Errorf("balance = %s, want %s", pos.Balance, rawShares)
	}
	if pos.ScaledBalance == nil || pos.ScaledBalance.Cmp(rawShares) != 0 {
		t.Errorf("scaled balance = %v, want %s", pos.ScaledBalance, rawShares)
	}
	if pos.AssetDecimals == nil || *pos.AssetDecimals != 18 {
		t.Errorf("asset decimals = %v, want 18", pos.AssetDecimals)
	}
}

func TestHandleSnapshots_Curve_PreservesLPUnits(t *testing.T) {
	lp := common.HexToAddress("0x00836fe54625be242bcfa286207795405ca4fd10")
	usdt := common.HexToAddress("0xdac17f958d2ee523a2206206994597c13d831ec7")
	wallet := common.HexToAddress("0x1601843c5e9bc251a3272907010afa41fa18347e")
	rawLP, _ := new(big.Int).SetString("48599111101772569976924462", 10)

	repo := &fakeAllocRepo{}
	handler := newTestHandler(repo,
		map[string]int64{"spark": 1},
		map[common.Address]tokenMeta{
			lp:   {symbol: "sUSDSUSDT", decimals: 18},
			usdt: {symbol: "USDT", decimals: 6},
		},
	)

	err := handler.HandleSnapshots(context.Background(), []*PositionSnapshot{
		{
			Entry: &TokenEntry{
				ContractAddress: lp,
				WalletAddress:   wallet,
				AssetAddress:    &usdt,
				Star:            "spark",
				Chain:           "mainnet",
				TokenType:       "curve",
			},
			Balance:       rawLP,
			ScaledBalance: rawLP,
			ChainID:       1,
			BlockNumber:   100,
			TxAmount:      big.NewInt(0),
			Direction:     DirectionSweep,
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(repo.saved) != 1 {
		t.Fatalf("expected 1 saved position, got %d", len(repo.saved))
	}

	pos := repo.saved[0]
	if pos.TokenSymbol != "sUSDSUSDT" {
		t.Errorf("symbol = %q, want sUSDSUSDT", pos.TokenSymbol)
	}
	if pos.TokenDecimals != 18 {
		t.Errorf("decimals = %d, want 18", pos.TokenDecimals)
	}
	if pos.AssetDecimals == nil || *pos.AssetDecimals != 6 {
		t.Errorf("asset decimals = %v, want 6", pos.AssetDecimals)
	}
	if pos.Balance.Cmp(rawLP) != 0 {
		t.Errorf("balance = %s, want %s", pos.Balance, rawLP)
	}
	if pos.ScaledBalance == nil || pos.ScaledBalance.Cmp(rawLP) != 0 {
		t.Errorf("scaled balance = %v, want %s", pos.ScaledBalance, rawLP)
	}
}

func TestHandleSnapshots_UniV3Pool_NoAssetAddress_Error(t *testing.T) {
	pool := common.HexToAddress("0xbafead7c60ea473758ed6c6021505e8bbd7e8e5d")
	wallet := common.HexToAddress("0x491edfb0b8b608044e227225c715981a30f3a44e")

	repo := &fakeAllocRepo{}
	handler := newTestHandler(repo,
		map[string]int64{"grove": 2},
		map[common.Address]tokenMeta{},
	)

	err := handler.HandleSnapshots(context.Background(), []*PositionSnapshot{
		{
			Entry: &TokenEntry{
				ContractAddress: pool,
				WalletAddress:   wallet,
				AssetAddress:    nil, // no asset address
				Star:            "grove",
				Chain:           "mainnet",
				TokenType:       "uni_v3_pool",
			},
			Balance:     big.NewInt(100),
			ChainID:     1,
			BlockNumber: 100,
			TxAmount:    big.NewInt(0),
			Direction:   DirectionSweep,
		},
	})
	if err == nil {
		t.Fatal("expected error for uni_v3 entry without asset address")
	}
	if len(repo.saved) != 0 {
		t.Errorf("no positions should be saved on error, got %d", len(repo.saved))
	}
}

func TestHandleSnapshots_MixedERC20AndUniV3(t *testing.T) {
	usdc := common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
	pool := common.HexToAddress("0xbafead7c60ea473758ed6c6021505e8bbd7e8e5d")
	sparkWallet := common.HexToAddress("0x1601843c5e9bc251a3272907010afa41fa18347e")
	groveWallet := common.HexToAddress("0x491edfb0b8b608044e227225c715981a30f3a44e")

	repo := &fakeAllocRepo{}
	handler := newTestHandler(repo,
		map[string]int64{"spark": 1, "grove": 2},
		map[common.Address]tokenMeta{
			usdc: {symbol: "USDC", decimals: 6},
		},
	)

	err := handler.HandleSnapshots(context.Background(), []*PositionSnapshot{
		{
			Entry: &TokenEntry{
				ContractAddress: usdc,
				WalletAddress:   sparkWallet,
				Star:            "spark",
				Chain:           "mainnet",
				TokenType:       "erc20",
			},
			Balance:     big.NewInt(1000000),
			ChainID:     1,
			BlockNumber: 100,
			TxAmount:    big.NewInt(1000000),
			Direction:   DirectionSweep,
		},
		{
			Entry: &TokenEntry{
				ContractAddress: pool,
				WalletAddress:   groveWallet,
				AssetAddress:    &usdc,
				Star:            "grove",
				Chain:           "mainnet",
				TokenType:       "uni_v3_pool",
			},
			Balance:       big.NewInt(17229995299715),
			ScaledBalance: big.NewInt(24999464528264),
			ChainID:       1,
			BlockNumber:   100,
			TxAmount:      big.NewInt(0),
			Direction:     DirectionSweep,
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(repo.saved) != 2 {
		t.Fatalf("expected 2 saved positions, got %d", len(repo.saved))
	}

	// First: ERC20 position.
	if repo.saved[0].PrimeID != 1 {
		t.Errorf("first position prime_id = %d, want 1 (spark)", repo.saved[0].PrimeID)
	}
	if repo.saved[0].TokenSymbol != "USDC" {
		t.Errorf("first position symbol = %q, want USDC", repo.saved[0].TokenSymbol)
	}

	// Second: Uni V3 position — uses asset metadata.
	if repo.saved[1].PrimeID != 2 {
		t.Errorf("second position prime_id = %d, want 2 (grove)", repo.saved[1].PrimeID)
	}
	if repo.saved[1].TokenAddress != pool {
		t.Errorf("second position token_address should be pool, got %s", repo.saved[1].TokenAddress.Hex())
	}
	if repo.saved[1].TokenSymbol != "USDC" {
		t.Errorf("second position symbol = %q, want USDC (from asset)", repo.saved[1].TokenSymbol)
	}
}

func TestHandleSnapshots_EmptySlice(t *testing.T) {
	repo := &fakeAllocRepo{}
	handler := newTestHandler(repo, map[string]int64{}, map[common.Address]tokenMeta{})

	err := handler.HandleSnapshots(context.Background(), nil)
	if err != nil {
		t.Fatalf("unexpected error for empty snapshots: %v", err)
	}
	if len(repo.saved) != 0 {
		t.Errorf("expected 0 saved positions, got %d", len(repo.saved))
	}
}

func TestHandleSnapshots_UnknownStar_Error(t *testing.T) {
	usdc := common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
	wallet := common.HexToAddress("0x1111111111111111111111111111111111111111")

	repo := &fakeAllocRepo{}
	handler := newTestHandler(repo,
		map[string]int64{"spark": 1}, // no "grove"
		map[common.Address]tokenMeta{
			usdc: {symbol: "USDC", decimals: 6},
		},
	)

	err := handler.HandleSnapshots(context.Background(), []*PositionSnapshot{
		{
			Entry: &TokenEntry{
				ContractAddress: usdc,
				WalletAddress:   wallet,
				Star:            "grove", // not in primeLookup
				Chain:           "mainnet",
				TokenType:       "erc20",
			},
			Balance:     big.NewInt(100),
			ChainID:     1,
			BlockNumber: 100,
			TxAmount:    big.NewInt(100),
			Direction:   DirectionSweep,
		},
	})
	if err == nil {
		t.Fatal("expected error for unknown star")
	}
}
