package allocation_tracker

import (
	"context"
	"fmt"
	"log/slog"
	"maps"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// fakeAllocRepo captures positions passed to SavePositions.
type fakeAllocRepo struct {
	saved []*entity.AllocationPosition
	err   error
}

func (r *fakeAllocRepo) SavePositions(_ context.Context, _ pgx.Tx, positions []*entity.AllocationPosition) error {
	if r.err != nil {
		return r.err
	}
	r.saved = append(r.saved, positions...)
	return nil
}

// fakeSupplyRepo captures supplies passed to SaveSupplies.
type fakeSupplyRepo struct {
	saved []*entity.TokenTotalSupply
	err   error
}

func (r *fakeSupplyRepo) SaveSupplies(_ context.Context, _ pgx.Tx, supplies []*entity.TokenTotalSupply) error {
	if r.err != nil {
		return r.err
	}
	r.saved = append(r.saved, supplies...)
	return nil
}

// newTestHandler creates a PrimePositionHandler with a pre-populated metadata
// cache, bypassing the multicaller. This isolates handler logic from RPC calls.
func newTestHandler(
	repo *fakeAllocRepo,
	supplyRepo *fakeSupplyRepo,
	primeLookup map[string]int64,
	metadata map[common.Address]tokenMeta,
) *PrimePositionHandler {
	h := &PrimePositionHandler{
		repo:        repo,
		supplyRepo:  supplyRepo,
		txm:         &testutil.MockTxManager{},
		primeLookup: primeLookup,
		metadata:    newMetadataCache(nil, nil, slog.Default()),
		logger:      slog.Default().With("component", "test-handler"),
	}
	// Pre-populate the cache so we don't need a multicaller.
	maps.Copy(h.metadata.cache, metadata)
	return h
}

func TestHandleBatch_ERC20(t *testing.T) {
	usdc := common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
	wallet := common.HexToAddress("0x1601843c5e9bc251a3272907010afa41fa18347e")

	repo := &fakeAllocRepo{}
	supplyRepo := &fakeSupplyRepo{}
	handler := newTestHandler(repo, supplyRepo,
		map[string]int64{"spark": 1},
		map[common.Address]tokenMeta{
			usdc: {symbol: "USDC", decimals: 6},
		},
	)

	err := handler.HandleBatch(context.Background(), &SnapshotBatch{
		Snapshots: []*PositionSnapshot{
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
	if len(supplyRepo.saved) != 0 {
		t.Errorf("expected no supplies saved, got %d", len(supplyRepo.saved))
	}
}

func TestHandleBatch_AtokenWithSupply(t *testing.T) {
	atoken := common.HexToAddress("0xe7df13b8e3d6740fe17cbe928c7334243d86c92f")
	wallet := common.HexToAddress("0x1601843c5e9bc251a3272907010afa41fa18347e")
	blockTs := time.Unix(1700000000, 0).UTC()

	repo := &fakeAllocRepo{}
	supplyRepo := &fakeSupplyRepo{}
	handler := newTestHandler(repo, supplyRepo,
		map[string]int64{"spark": 1},
		map[common.Address]tokenMeta{
			atoken: {symbol: "spUSDT", decimals: 6},
		},
	)

	err := handler.HandleBatch(context.Background(), &SnapshotBatch{
		Snapshots: []*PositionSnapshot{
			{
				Entry: &TokenEntry{
					ContractAddress: atoken,
					WalletAddress:   wallet,
					Star:            "spark",
					Chain:           "mainnet",
					Protocol:        "sparklend",
					TokenType:       "atoken",
				},
				Balance:        big.NewInt(50_000_000_000),
				ScaledBalance:  big.NewInt(49_000_000_000),
				ChainID:        1,
				BlockNumber:    100,
				BlockTimestamp: blockTs,
				TxAmount:       big.NewInt(0),
				Direction:      DirectionSweep,
			},
		},
		Supplies: []*TokenTotalSupplySnapshot{
			{
				ChainID:           1,
				TokenAddress:      atoken,
				TotalSupply:       big.NewInt(1_000_000_000_000),
				ScaledTotalSupply: big.NewInt(980_000_000_000),
				BlockNumber:       100,
				BlockTimestamp:    blockTs,
				Source:            "sweep",
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(repo.saved) != 1 {
		t.Fatalf("expected 1 saved position, got %d", len(repo.saved))
	}
	if len(supplyRepo.saved) != 1 {
		t.Fatalf("expected 1 saved supply, got %d", len(supplyRepo.saved))
	}

	sup := supplyRepo.saved[0]
	if sup.TokenAddress != atoken {
		t.Errorf("supply token = %s, want %s", sup.TokenAddress.Hex(), atoken.Hex())
	}
	if sup.TokenSymbol != "spUSDT" {
		t.Errorf("supply symbol = %q, want spUSDT", sup.TokenSymbol)
	}
	if sup.TotalSupply.Cmp(big.NewInt(1_000_000_000_000)) != 0 {
		t.Errorf("supply total = %s, want 1000000000000", sup.TotalSupply)
	}
	if sup.ScaledTotalSupply == nil || sup.ScaledTotalSupply.Cmp(big.NewInt(980_000_000_000)) != 0 {
		t.Errorf("supply scaled = %v, want 980000000000", sup.ScaledTotalSupply)
	}
	if sup.Source != "sweep" {
		t.Errorf("source = %q, want sweep", sup.Source)
	}
}

// TestHandleBatch_TransactionRollback verifies that when the supply write
// fails, the position write is also rolled back by the surrounding transaction.
func TestHandleBatch_TransactionRollback(t *testing.T) {
	atoken := common.HexToAddress("0xaaaa")
	wallet := common.HexToAddress("0xbbbb")
	blockTs := time.Unix(1700000000, 0).UTC()

	repo := &fakeAllocRepo{}
	supplyRepo := &fakeSupplyRepo{err: fmt.Errorf("supply write boom")}
	handler := newTestHandler(repo, supplyRepo,
		map[string]int64{"spark": 1},
		map[common.Address]tokenMeta{
			atoken: {symbol: "aUSDC", decimals: 6},
		},
	)

	// Use a tx-manager spy that refuses to commit if the closure returns an error.
	var committed, rolledBack bool
	handler.txm = &testutil.MockTxManager{
		WithTransactionFn: func(ctx context.Context, fn func(tx pgx.Tx) error) error {
			err := fn(nil)
			if err != nil {
				rolledBack = true
				return err
			}
			committed = true
			return nil
		},
	}

	err := handler.HandleBatch(context.Background(), &SnapshotBatch{
		Snapshots: []*PositionSnapshot{
			{
				Entry: &TokenEntry{
					ContractAddress: atoken,
					WalletAddress:   wallet,
					Star:            "spark",
					Chain:           "mainnet",
					Protocol:        "sparklend",
					TokenType:       "atoken",
				},
				Balance:        big.NewInt(1),
				ChainID:        1,
				BlockNumber:    100,
				BlockTimestamp: blockTs,
				TxAmount:       big.NewInt(0),
				Direction:      DirectionSweep,
			},
		},
		Supplies: []*TokenTotalSupplySnapshot{
			{
				ChainID:        1,
				TokenAddress:   atoken,
				TotalSupply:    big.NewInt(100),
				BlockNumber:    100,
				BlockTimestamp: blockTs,
				Source:         "sweep",
			},
		},
	})

	if err == nil {
		t.Fatal("expected error when supply write fails")
	}
	if committed {
		t.Error("transaction should not have committed")
	}
	if !rolledBack {
		t.Error("transaction should have rolled back")
	}
}

func TestHandleBatch_UniV3Pool_UsesAssetMetadata(t *testing.T) {
	pool := common.HexToAddress("0xbafead7c60ea473758ed6c6021505e8bbd7e8e5d")
	usdc := common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
	wallet := common.HexToAddress("0x491edfb0b8b608044e227225c715981a30f3a44e")

	repo := &fakeAllocRepo{}
	supplyRepo := &fakeSupplyRepo{}
	handler := newTestHandler(repo, supplyRepo,
		map[string]int64{"grove": 2},
		map[common.Address]tokenMeta{
			usdc: {symbol: "USDC", decimals: 6},
		},
	)

	err := handler.HandleBatch(context.Background(), &SnapshotBatch{
		Snapshots: []*PositionSnapshot{
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

func TestHandleBatch_UniV3LP_UsesAssetMetadata(t *testing.T) {
	lp := common.HexToAddress("0x6b405dca74897c9442d369dcf6c0ec230f7e1c7c")
	ausd := common.HexToAddress("0x00000000efe302beaa2b3e6e1b18d08d69a9012a")
	wallet := common.HexToAddress("0x94b398acb2fce988871218221ea6a4a2b26cccbc")

	repo := &fakeAllocRepo{}
	supplyRepo := &fakeSupplyRepo{}
	handler := newTestHandler(repo, supplyRepo,
		map[string]int64{"grove": 2},
		map[common.Address]tokenMeta{
			ausd: {symbol: "AUSD", decimals: 18},
		},
	)

	err := handler.HandleBatch(context.Background(), &SnapshotBatch{
		Snapshots: []*PositionSnapshot{
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

func TestHandleBatch_ERC4626_PreservesTokenUnits(t *testing.T) {
	susds := common.HexToAddress("0xa3931d71877c0e7a3148cb7eb4463524fec27fbd")
	usds := common.HexToAddress("0xdc035d45d973e3ec169d2276ddab16f1e407384f")
	wallet := common.HexToAddress("0x1601843c5e9bc251a3272907010afa41fa18347e")
	rawShares, _ := new(big.Int).SetString("1201619730663240195228985093", 10)

	repo := &fakeAllocRepo{}
	supplyRepo := &fakeSupplyRepo{}
	handler := newTestHandler(repo, supplyRepo,
		map[string]int64{"spark": 1},
		map[common.Address]tokenMeta{
			susds: {symbol: "sUSDS", decimals: 18},
			usds:  {symbol: "USDS", decimals: 18},
		},
	)

	err := handler.HandleBatch(context.Background(), &SnapshotBatch{
		Snapshots: []*PositionSnapshot{
			{
				Entry: &TokenEntry{
					ContractAddress: susds,
					WalletAddress:   wallet,
					AssetAddress:    &usds,
					Star:            "spark",
					Chain:           "mainnet",
					TokenType:       "erc4626",
				},
				Balance:       new(big.Int).Set(rawShares),
				ScaledBalance: new(big.Int).Set(rawShares),
				ChainID:       1,
				BlockNumber:   100,
				TxAmount:      big.NewInt(0),
				Direction:     DirectionSweep,
			},
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
}

func TestHandleBatch_Curve_PreservesLPUnits(t *testing.T) {
	lp := common.HexToAddress("0x00836fe54625be242bcfa286207795405ca4fd10")
	usdt := common.HexToAddress("0xdac17f958d2ee523a2206206994597c13d831ec7")
	wallet := common.HexToAddress("0x1601843c5e9bc251a3272907010afa41fa18347e")
	rawLP, _ := new(big.Int).SetString("48599111101772569976924462", 10)

	repo := &fakeAllocRepo{}
	supplyRepo := &fakeSupplyRepo{}
	handler := newTestHandler(repo, supplyRepo,
		map[string]int64{"spark": 1},
		map[common.Address]tokenMeta{
			lp:   {symbol: "sUSDSUSDT", decimals: 18},
			usdt: {symbol: "USDT", decimals: 6},
		},
	)

	err := handler.HandleBatch(context.Background(), &SnapshotBatch{
		Snapshots: []*PositionSnapshot{
			{
				Entry: &TokenEntry{
					ContractAddress: lp,
					WalletAddress:   wallet,
					AssetAddress:    &usdt,
					Star:            "spark",
					Chain:           "mainnet",
					TokenType:       "curve",
				},
				Balance:       new(big.Int).Set(rawLP),
				ScaledBalance: new(big.Int).Set(rawLP),
				ChainID:       1,
				BlockNumber:   100,
				TxAmount:      big.NewInt(0),
				Direction:     DirectionSweep,
			},
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
	if pos.Balance.Cmp(rawLP) != 0 {
		t.Errorf("balance = %s, want %s", pos.Balance, rawLP)
	}
	if pos.ScaledBalance == nil || pos.ScaledBalance.Cmp(rawLP) != 0 {
		t.Errorf("scaled balance = %v, want %s", pos.ScaledBalance, rawLP)
	}
}

func TestHandleBatch_UniV3Pool_NoAssetAddress_Error(t *testing.T) {
	pool := common.HexToAddress("0xbafead7c60ea473758ed6c6021505e8bbd7e8e5d")
	wallet := common.HexToAddress("0x491edfb0b8b608044e227225c715981a30f3a44e")

	repo := &fakeAllocRepo{}
	supplyRepo := &fakeSupplyRepo{}
	handler := newTestHandler(repo, supplyRepo,
		map[string]int64{"grove": 2},
		map[common.Address]tokenMeta{},
	)

	err := handler.HandleBatch(context.Background(), &SnapshotBatch{
		Snapshots: []*PositionSnapshot{
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
		},
	})
	if err == nil {
		t.Fatal("expected error for uni_v3 entry without asset address")
	}
	if len(repo.saved) != 0 {
		t.Errorf("no positions should be saved on error, got %d", len(repo.saved))
	}
}

func TestHandleBatch_EmptyBatch(t *testing.T) {
	repo := &fakeAllocRepo{}
	supplyRepo := &fakeSupplyRepo{}
	handler := newTestHandler(repo, supplyRepo, map[string]int64{}, map[common.Address]tokenMeta{})

	err := handler.HandleBatch(context.Background(), &SnapshotBatch{})
	if err != nil {
		t.Fatalf("unexpected error for empty batch: %v", err)
	}
	if len(repo.saved) != 0 {
		t.Errorf("expected 0 saved positions, got %d", len(repo.saved))
	}
	if len(supplyRepo.saved) != 0 {
		t.Errorf("expected 0 saved supplies, got %d", len(supplyRepo.saved))
	}
}

func TestHandleBatch_UnknownStar_Error(t *testing.T) {
	usdc := common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
	wallet := common.HexToAddress("0x1111111111111111111111111111111111111111")

	repo := &fakeAllocRepo{}
	supplyRepo := &fakeSupplyRepo{}
	handler := newTestHandler(repo, supplyRepo,
		map[string]int64{"spark": 1}, // no "grove"
		map[common.Address]tokenMeta{
			usdc: {symbol: "USDC", decimals: 6},
		},
	)

	err := handler.HandleBatch(context.Background(), &SnapshotBatch{
		Snapshots: []*PositionSnapshot{
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
		},
	})
	if err == nil {
		t.Fatal("expected error for unknown star")
	}
}
