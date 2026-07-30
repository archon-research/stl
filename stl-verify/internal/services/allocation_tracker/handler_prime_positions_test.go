package allocation_tracker

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"maps"
	"math/big"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
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

// TestHandleBatch_Centrifuge_MetadataFromShareToken proves the VEC-337-part-2
// row-metadata fix: an ERC-7540 centrifuge position takes its decimals/symbol
// from the resolved share token (surfaced as ShareToken), not from its vault
// contract_address, which reverts on decimals(). The vault address is left out
// of the seeded metadata cache, so any attempt to read metadata from it would
// fail the batch (nil multicaller) — the test passes only if the share token is
// used. The persisted row still keys on the vault address.
func TestHandleBatch_Centrifuge_MetadataFromShareToken(t *testing.T) {
	vault := common.HexToAddress("0x4880799ee5200fc58da299e965df644fbf46780b")
	share := common.HexToAddress("0x1234000000000000000000000000000000005678")
	wallet := common.HexToAddress("0x1601843c5e9bc251a3272907010afa41fa18347e")

	repo := &fakeAllocRepo{}
	handler := newTestHandler(repo, &fakeSupplyRepo{},
		map[string]int64{"grove": 2},
		map[common.Address]tokenMeta{
			// Only the share token is known; the vault is intentionally absent.
			share: {symbol: "JAAA", decimals: 6},
		},
	)

	err := handler.HandleBatch(context.Background(), &SnapshotBatch{
		Snapshots: []*PositionSnapshot{
			{
				Entry: &TokenEntry{
					ContractAddress: vault,
					WalletAddress:   wallet,
					Star:            "grove",
					Chain:           "mainnet",
					Protocol:        "centrifuge",
					TokenType:       "centrifuge",
				},
				Balance:     big.NewInt(500),
				ShareToken:  &share,
				ChainID:     1,
				BlockNumber: 100,
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
	if pos.TokenAddress != vault {
		t.Errorf("token_address = %s, want the vault %s", pos.TokenAddress.Hex(), vault.Hex())
	}
	if pos.TokenSymbol != "JAAA" || pos.TokenDecimals != 6 {
		t.Errorf("metadata = (%q, %d), want (JAAA, 6) from the share token", pos.TokenSymbol, pos.TokenDecimals)
	}
}

// TestHandleBatch_CreatedAtBlockFloor verifies that a nil/zero CreatedAtBlock
// (nil when knownCreatedAtBlocks has no entry for the contract — the axis-synome
// contract omits created_at_block entirely) is floored to the observation block
// rather than persisted as 0, while an explicit value is preserved.
func TestHandleBatch_CreatedAtBlockFloor(t *testing.T) {
	tokenNil := common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
	tokenSet := common.HexToAddress("0x6B175474E89094C44Da98b954EedeAC495271d0F")
	wallet := common.HexToAddress("0x1601843c5e9bc251a3272907010afa41fa18347e")
	explicit := int64(42)

	repo := &fakeAllocRepo{}
	supplyRepo := &fakeSupplyRepo{}
	handler := newTestHandler(repo, supplyRepo,
		map[string]int64{"spark": 1},
		map[common.Address]tokenMeta{
			tokenNil: {symbol: "USDC", decimals: 6},
			tokenSet: {symbol: "DAI", decimals: 18},
		},
	)

	err := handler.HandleBatch(context.Background(), &SnapshotBatch{
		Snapshots: []*PositionSnapshot{
			{
				Entry: &TokenEntry{
					ContractAddress: tokenNil,
					WalletAddress:   wallet,
					Star:            "spark",
					Chain:           "mainnet",
					TokenType:       "erc20",
					CreatedAtBlock:  nil,
				},
				Balance:     big.NewInt(1),
				ChainID:     1,
				BlockNumber: 100,
				Direction:   DirectionSweep,
			},
			{
				Entry: &TokenEntry{
					ContractAddress: tokenSet,
					WalletAddress:   wallet,
					Star:            "spark",
					Chain:           "mainnet",
					TokenType:       "erc20",
					CreatedAtBlock:  &explicit,
				},
				Balance:     big.NewInt(1),
				ChainID:     1,
				BlockNumber: 100,
				Direction:   DirectionSweep,
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	got := make(map[common.Address]int64)
	for _, pos := range repo.saved {
		got[pos.TokenAddress] = pos.CreatedAtBlock
	}
	if got[tokenNil] != 100 {
		t.Errorf("nil CreatedAtBlock = %d, want floored to block 100", got[tokenNil])
	}
	if got[tokenSet] != explicit {
		t.Errorf("explicit CreatedAtBlock = %d, want %d", got[tokenSet], explicit)
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

// univ3PoolBatchAddrs are the real grove AUSD/USDC constellation reused by the
// uni_v3 handler tests.
var (
	univ3TestPool   = common.HexToAddress("0xbafead7c60ea473758ed6c6021505e8bbd7e8e5d")
	univ3TestAUSD   = common.HexToAddress("0x00000000eFE302BEAA2b3e6e1b18d08D69a9012a")
	univ3TestUSDC   = common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
	univ3TestWallet = common.HexToAddress("0x491edfb0b8b608044e227225c715981a30f3a44e")
	univ3TestValue  = big.NewInt(26_927_207_299_715)
)

// handleUniV3PoolBatch runs a one-snapshot uni_v3_pool batch (hint=USDC, pair
// AUSD/USDC, both metadata pre-cached) valuing the position at the given
// value, and returns the single saved position.
func handleUniV3PoolBatch(t *testing.T, value *big.Int) *entity.AllocationPosition {
	t.Helper()
	repo := &fakeAllocRepo{}
	handler := newTestHandler(repo, &fakeSupplyRepo{},
		map[string]int64{"grove": 2},
		map[common.Address]tokenMeta{
			univ3TestAUSD: {symbol: "AUSD", decimals: 6},
			univ3TestUSDC: {symbol: "USDC", decimals: 6},
		},
	)

	err := handler.HandleBatch(context.Background(), &SnapshotBatch{
		Snapshots: []*PositionSnapshot{
			{
				Entry: &TokenEntry{
					ContractAddress: univ3TestPool,
					WalletAddress:   univ3TestWallet,
					AssetAddress:    &univ3TestUSDC,
					Star:            "grove",
					Chain:           "mainnet",
					TokenType:       "uni_v3_pool",
				},
				Balance:         new(big.Int).Set(value),
				UnderlyingValue: new(big.Int).Set(value),
				PoolToken0:      &univ3TestAUSD,
				PoolToken1:      &univ3TestUSDC,
				ChainID:         1,
				BlockNumber:     100,
				TxAmount:        big.NewInt(0),
				Direction:       DirectionSweep,
			},
		},
	})
	if err != nil {
		t.Fatalf("HandleBatch: %v", err)
	}
	if len(repo.saved) != 1 {
		t.Fatalf("expected 1 saved position, got %d", len(repo.saved))
	}
	return repo.saved[0]
}

// TestHandleBatch_UniV3Pool_ComposesPoolRowSymbol: the pool's token row must
// carry a truthful composed symbol (a V3 pool is not an ERC20 and has no
// symbol; reusing the hint asset's symbol mislabeled the row as "USDC") and
// decimals from the hint asset, the balance's denomination.
func TestHandleBatch_UniV3Pool_ComposesPoolRowSymbol(t *testing.T) {
	pos := handleUniV3PoolBatch(t, univ3TestValue)
	if pos.TokenSymbol != "UNIV3-LP-AUSD-USDC" {
		t.Errorf("symbol = %q, want UNIV3-LP-AUSD-USDC (composed from the pool pair)", pos.TokenSymbol)
	}
	if pos.TokenDecimals != 6 {
		t.Errorf("decimals = %d, want 6 (from the hint asset)", pos.TokenDecimals)
	}
	if pos.TokenAddress != univ3TestPool {
		t.Errorf("token_address should be pool contract, got %s", pos.TokenAddress.Hex())
	}
	if pos.PrimeID != 2 {
		t.Errorf("prime_id = %d, want 2", pos.PrimeID)
	}
}

// TestHandleBatch_UniV3Pool_SetsUnderlyingOnPosition: the tracker-computed
// full position value becomes the underlying valuation, denominated in the
// hint asset.
func TestHandleBatch_UniV3Pool_SetsUnderlyingOnPosition(t *testing.T) {
	pos := handleUniV3PoolBatch(t, univ3TestValue)
	if pos.Underlying == nil {
		t.Fatal("Underlying = nil, want the tracker-computed full value")
	}
	if pos.Underlying.Value.Cmp(univ3TestValue) != 0 {
		t.Errorf("Underlying.Value = %s, want %s", pos.Underlying.Value, univ3TestValue)
	}
	if pos.Underlying.AssetAddress != univ3TestUSDC {
		t.Errorf("Underlying.AssetAddress = %s, want %s", pos.Underlying.AssetAddress.Hex(), univ3TestUSDC.Hex())
	}
}

// TestHandleBatch_UniV3Pool_ZeroExitRow_PersistsZeroValuation: the source
// writes an explicit zero row when the wallet's live positions no longer
// match the pool (exit/burn/transfer); the handler must persist it as balance
// 0 with a zero valuation still denominated in the hint asset, not drop it or
// degrade it to a NULL valuation, so the API's latest-row read stops
// reporting the exposure instead of freezing the last positive row.
func TestHandleBatch_UniV3Pool_ZeroExitRow_PersistsZeroValuation(t *testing.T) {
	pos := handleUniV3PoolBatch(t, big.NewInt(0))
	if pos.Balance == nil || pos.Balance.Sign() != 0 {
		t.Errorf("Balance = %v, want explicit 0", pos.Balance)
	}
	if pos.Underlying == nil {
		t.Fatal("Underlying = nil, want a zero valuation in the hint asset")
	}
	if pos.Underlying.Value.Sign() != 0 {
		t.Errorf("Underlying.Value = %s, want 0", pos.Underlying.Value)
	}
	if pos.Underlying.AssetAddress != univ3TestUSDC {
		t.Errorf("Underlying.AssetAddress = %s, want %s", pos.Underlying.AssetAddress.Hex(), univ3TestUSDC.Hex())
	}
}

func TestHandleBatch_UniV3LP_ComposesPoolRowMetadata(t *testing.T) {
	lp := common.HexToAddress("0x6b405dca74897c9442d369dcf6c0ec230f7e1c7c")
	ausd := common.HexToAddress("0x00000000efe302beaa2b3e6e1b18d08d69a9012a")
	wmon := common.HexToAddress("0x760afe86e5de5fa0ee542fc7b7b713e1c5425701")
	wallet := common.HexToAddress("0x94b398acb2fce988871218221ea6a4a2b26cccbc")

	repo := &fakeAllocRepo{}
	supplyRepo := &fakeSupplyRepo{}
	handler := newTestHandler(repo, supplyRepo,
		map[string]int64{"grove": 2},
		map[common.Address]tokenMeta{
			ausd: {symbol: "AUSD", decimals: 18},
			wmon: {symbol: "WMON", decimals: 18},
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
				Balance:         big.NewInt(5000000000000000000),
				UnderlyingValue: big.NewInt(5000000000000000000),
				PoolToken0:      &ausd,
				PoolToken1:      &wmon,
				ChainID:         1,
				BlockNumber:     100,
				TxAmount:        big.NewInt(0),
				Direction:       DirectionSweep,
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
	if pos.TokenSymbol != "UNIV3-LP-AUSD-WMON" {
		t.Errorf("symbol = %q, want UNIV3-LP-AUSD-WMON (composed from the pool pair)", pos.TokenSymbol)
	}
	if pos.TokenDecimals != 18 {
		t.Errorf("decimals = %d, want 18 (from the hint asset)", pos.TokenDecimals)
	}
}

// TestHandleBatch_UniV3Pool_MissingPoolTokens_Error: without the pool pair the
// handler cannot name the pool's token row truthfully; a uni_v3 snapshot
// missing it signals an ingest bug and must stop the batch.
func TestHandleBatch_UniV3Pool_MissingPoolTokens_Error(t *testing.T) {
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
				Balance:         big.NewInt(100),
				UnderlyingValue: big.NewInt(100),
				ChainID:         1,
				BlockNumber:     100,
				TxAmount:        big.NewInt(0),
				Direction:       DirectionSweep,
			},
		},
	})
	if err == nil {
		t.Fatal("expected error for uni_v3 snapshot without pool token pair")
	}
	if len(repo.saved) != 0 {
		t.Errorf("no positions should be saved on error, got %d", len(repo.saved))
	}
}

// TestHandleBatch_UniV3Pool_FetchesPoolTokenMetadata drives the real
// metadataCache with a mocked multicaller to prove HandleBatch requests
// metadata for the pool pair (not just the hint asset) so the composed symbol
// can be built on a cold cache.
func TestHandleBatch_UniV3Pool_FetchesPoolTokenMetadata(t *testing.T) {
	pool := common.HexToAddress("0xbafead7c60ea473758ed6c6021505e8bbd7e8e5d")
	ausd := common.HexToAddress("0x00000000eFE302BEAA2b3e6e1b18d08D69a9012a")
	usdc := common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
	wallet := common.HexToAddress("0x491edfb0b8b608044e227225c715981a30f3a44e")

	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		t.Fatalf("GetERC20ABI: %v", err)
	}
	mc := newTokenMetadataMockMulticaller(t, erc20ABI, map[common.Address]tokenMeta{
		ausd: {symbol: "AUSD", decimals: 6},
		usdc: {symbol: "USDC", decimals: 6},
	}, nil, nil)

	repo := &fakeAllocRepo{}
	handler := NewPrimePositionHandler(repo, &fakeSupplyRepo{}, &testutil.MockTxManager{},
		mc, erc20ABI, map[string]int64{"grove": 2}, slog.Default(), nil)

	err = handler.HandleBatch(context.Background(), &SnapshotBatch{
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
				Balance:         big.NewInt(100),
				UnderlyingValue: big.NewInt(100),
				PoolToken0:      &ausd,
				PoolToken1:      &usdc,
				ChainID:         1,
				BlockNumber:     100,
				TxAmount:        big.NewInt(0),
				Direction:       DirectionSweep,
			},
		},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(repo.saved) != 1 {
		t.Fatalf("expected 1 saved position, got %d", len(repo.saved))
	}
	if repo.saved[0].TokenSymbol != "UNIV3-LP-AUSD-USDC" {
		t.Errorf("symbol = %q, want UNIV3-LP-AUSD-USDC", repo.saved[0].TokenSymbol)
	}
}

// newTokenMetadataMockMulticaller answers decimals()/symbol() calls from the
// given per-address metadata and fails any other selector. Symbol reads for
// addresses in failSymbols (nil for none) fail while decimals still succeed;
// tests can mutate the set between calls to model a transient failure.
// rawSymbols (nil for none) overrides the symbol return with raw bytes, for
// non-ABI-string shapes (bytes32 symbols, garbage payloads).
func newTokenMetadataMockMulticaller(
	t *testing.T,
	erc20ABI *abi.ABI,
	metadata map[common.Address]tokenMeta,
	failSymbols map[common.Address]bool,
	rawSymbols map[common.Address][]byte,
) *testutil.MockMulticaller {
	t.Helper()
	decimalsMethod := erc20ABI.Methods["decimals"]
	symbolMethod := erc20ABI.Methods["symbol"]

	mc := testutil.NewMockMulticaller()
	mc.ExecuteFn = func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
		results := make([]outbound.Result, len(calls))
		for i, call := range calls {
			meta, ok := metadata[call.Target]
			if !ok {
				results[i] = outbound.Result{Success: false}
				continue
			}
			switch string(call.CallData[:4]) {
			case string(decimalsMethod.ID):
				data, err := decimalsMethod.Outputs.Pack(uint8(meta.decimals))
				if err != nil {
					t.Fatalf("pack decimals: %v", err)
				}
				results[i] = outbound.Result{Success: true, ReturnData: data}
			case string(symbolMethod.ID):
				if failSymbols[call.Target] {
					results[i] = outbound.Result{Success: false}
					continue
				}
				if raw, ok := rawSymbols[call.Target]; ok {
					results[i] = outbound.Result{Success: true, ReturnData: raw}
					continue
				}
				data, err := symbolMethod.Outputs.Pack(meta.symbol)
				if err != nil {
					t.Fatalf("pack symbol: %v", err)
				}
				results[i] = outbound.Result{Success: true, ReturnData: data}
			default:
				results[i] = outbound.Result{Success: false}
			}
		}
		return results, nil
	}
	return mc
}

// handleERC20BatchWithSymbolShape runs a one-snapshot erc20 batch against the
// real metadata cache where the token's symbol() answers with the given raw
// bytes (nil = failed sub-call), returning the repo and the HandleBatch error.
func handleERC20BatchWithSymbolShape(t *testing.T, rawSymbol []byte) (*fakeAllocRepo, error) {
	t.Helper()
	usdc := common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48")
	wallet := common.HexToAddress("0x1601843c5e9bc251a3272907010afa41fa18347e")

	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		t.Fatalf("GetERC20ABI: %v", err)
	}
	failSymbols := map[common.Address]bool{}
	rawSymbols := map[common.Address][]byte{}
	if rawSymbol == nil {
		failSymbols[usdc] = true
	} else {
		rawSymbols[usdc] = rawSymbol
	}
	mc := newTokenMetadataMockMulticaller(t, erc20ABI,
		map[common.Address]tokenMeta{usdc: {symbol: "USDC", decimals: 6}},
		failSymbols, rawSymbols,
	)

	repo := &fakeAllocRepo{}
	handler := NewPrimePositionHandler(repo, &fakeSupplyRepo{}, &testutil.MockTxManager{},
		mc, erc20ABI, map[string]int64{"spark": 1}, slog.Default(), nil)

	err = handler.HandleBatch(context.Background(), &SnapshotBatch{
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
				TxAmount:    big.NewInt(0),
				Direction:   DirectionSweep,
			},
		},
	})
	return repo, err
}

// TestHandleBatch_FailedSymbolRead_Error: the symbol() read is issued
// expecting success, so a failed or undecodable read must fail the batch (SQS
// redelivers) instead of persisting the row under a fallback symbol: the
// token upsert never refreshes symbol on conflict, so an impostor-class name
// would stick forever.
func TestHandleBatch_FailedSymbolRead_Error(t *testing.T) {
	tests := []struct {
		name      string
		rawSymbol []byte // nil = failed sub-call
	}{
		{"failed sub-call", nil},
		{"undecodable 32-byte payload", bytes.Repeat([]byte{0xff}, 32)},
		{"undecodable short payload", []byte{0xde, 0xad}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			repo, err := handleERC20BatchWithSymbolShape(t, tc.rawSymbol)
			if err == nil {
				t.Fatal("expected error when the symbol read fails")
			}
			if len(repo.saved) != 0 {
				t.Errorf("no positions should be saved on error, got %d", len(repo.saved))
			}
		})
	}
}

// TestHandleBatch_Bytes32Symbol_Resolves: the ERC20 ABI declares symbol() as
// string, but MKR-class tokens return bytes32, and uni_v3 pool pair tokens
// come from on-chain token0()/token1() rather than the curated registry, so
// hard-failing on that known variant would deterministically poison the queue
// (the read never changes on redelivery). The shape is gated structurally: an
// exactly-32-byte payload decodes as a NUL-padded bytes32 symbol.
func TestHandleBatch_Bytes32Symbol_Resolves(t *testing.T) {
	mkrBytes32 := make([]byte, 32)
	copy(mkrBytes32, "MKR")

	repo, err := handleERC20BatchWithSymbolShape(t, mkrBytes32)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(repo.saved) != 1 {
		t.Fatalf("expected 1 saved position, got %d", len(repo.saved))
	}
	if repo.saved[0].TokenSymbol != "MKR" {
		t.Errorf("symbol = %q, want MKR (decoded from bytes32)", repo.saved[0].TokenSymbol)
	}
}

// TestHandleBatch_TransientSymbolFailure_RecoversOnRetry: a failed symbol
// read must not be cached (the long-lived metadata cache is only consulted
// for missing addresses), or the SQS redelivery hits the same cached fallback
// and the block fails forever until a process restart.
func TestHandleBatch_TransientSymbolFailure_RecoversOnRetry(t *testing.T) {
	erc20ABI, err := abis.GetERC20ABI()
	if err != nil {
		t.Fatalf("GetERC20ABI: %v", err)
	}
	failSymbols := map[common.Address]bool{univ3TestAUSD: true}
	mc := newTokenMetadataMockMulticaller(t, erc20ABI, map[common.Address]tokenMeta{
		univ3TestAUSD: {symbol: "AUSD", decimals: 6},
		univ3TestUSDC: {symbol: "USDC", decimals: 6},
	}, failSymbols, nil)

	repo := &fakeAllocRepo{}
	handler := NewPrimePositionHandler(repo, &fakeSupplyRepo{}, &testutil.MockTxManager{},
		mc, erc20ABI, map[string]int64{"grove": 2}, slog.Default(), nil)

	batch := &SnapshotBatch{
		Snapshots: []*PositionSnapshot{
			{
				Entry: &TokenEntry{
					ContractAddress: univ3TestPool,
					WalletAddress:   univ3TestWallet,
					AssetAddress:    &univ3TestUSDC,
					Star:            "grove",
					Chain:           "mainnet",
					TokenType:       "uni_v3_pool",
				},
				Balance:         big.NewInt(100),
				UnderlyingValue: big.NewInt(100),
				PoolToken0:      &univ3TestAUSD,
				PoolToken1:      &univ3TestUSDC,
				ChainID:         1,
				BlockNumber:     100,
				TxAmount:        big.NewInt(0),
				Direction:       DirectionSweep,
			},
		},
	}

	if err := handler.HandleBatch(context.Background(), batch); err == nil {
		t.Fatal("expected the first delivery to fail while the symbol read fails")
	}

	delete(failSymbols, univ3TestAUSD)
	if err := handler.HandleBatch(context.Background(), batch); err != nil {
		t.Fatalf("retry after the symbol read recovered must succeed, got: %v", err)
	}
	if len(repo.saved) != 1 {
		t.Fatalf("expected 1 saved position after the retry, got %d", len(repo.saved))
	}
	if repo.saved[0].TokenSymbol != "UNIV3-LP-AUSD-USDC" {
		t.Errorf("symbol = %q, want UNIV3-LP-AUSD-USDC", repo.saved[0].TokenSymbol)
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

// policyTestAddrs are canonical addresses reused across the valuation policy tests.
var (
	policyVault  = common.HexToAddress("0x38464507e02c983f20428a6e8566693fe9e422a9")
	policyUSDC   = common.HexToAddress("0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48")
	policyWallet = common.HexToAddress("0x1601843c5e9bc251a3272907010afa41fa18347e")
)

// newPolicyTestHandler returns a handler pre-populated with metadata for
// policyVault and policyUSDC, suitable for valuation-policy tests. It
// accepts an optional telemetry instance (nil is fine for most policy tests).
func newPolicyTestHandler(t *testing.T, tel *Telemetry) *PrimePositionHandler {
	t.Helper()
	repo := &fakeAllocRepo{}
	supplyRepo := &fakeSupplyRepo{}
	h := newTestHandler(repo, supplyRepo,
		map[string]int64{"spark": 1},
		map[common.Address]tokenMeta{
			policyVault: {symbol: "VAULT", decimals: 18},
			policyUSDC:  {symbol: "USDC", decimals: 6},
		},
	)
	h.telemetry = tel
	return h
}

func TestBuildPositions_UnderlyingValuationPolicy(t *testing.T) {
	tests := []struct {
		name       string
		tokenType  string
		asset      *common.Address
		balance    *big.Int
		underlying *big.Int
		wantVal    *big.Int
		wantAsset  common.Address
	}{
		{"erc4626 uses convertToAssets result denominated in asset_address", "erc4626", &policyUSDC, big.NewInt(100), big.NewInt(123), big.NewInt(123), policyUSDC},
		{"erc4626 convert failure stays NULL", "erc4626", &policyUSDC, big.NewInt(100), nil, nil, common.Address{}},
		{"erc4626 without asset_address stays NULL", "erc4626", nil, big.NewInt(100), big.NewInt(123), nil, common.Address{}},
		{"atoken uses balanceOf denominated in asset_address", "atoken", &policyUSDC, big.NewInt(555), nil, big.NewInt(555), policyUSDC},
		{"atoken without asset_address stays NULL", "atoken", nil, big.NewInt(555), nil, nil, common.Address{}},
		{"erc20 is its own underlying and ignores asset_address", "erc20", &policyUSDC, big.NewInt(42), nil, big.NewInt(42), policyVault},
		{"superstate NAV token stays NULL", "superstate", &policyUSDC, big.NewInt(7), nil, nil, common.Address{}},
		{"centrifuge NAV token stays NULL", "centrifuge", &policyUSDC, big.NewInt(7), nil, nil, common.Address{}},
		{"curve stays NULL even at zero balance", "curve", &policyUSDC, big.NewInt(0), nil, nil, common.Address{}},
		{"erc7540 deferred stays NULL even when UnderlyingValue set", "erc7540", &policyUSDC, big.NewInt(7), big.NewInt(9), nil, common.Address{}},
		{"uni_v3_pool uses tracker-computed full value in asset_address", "uni_v3_pool", &policyUSDC, big.NewInt(100), big.NewInt(999), big.NewInt(999), policyUSDC},
		{"uni_v3_lp uses tracker-computed full value in asset_address", "uni_v3_lp", &policyUSDC, big.NewInt(100), big.NewInt(888), big.NewInt(888), policyUSDC},
		{"uni_v3_pool missing tracker value stays NULL", "uni_v3_pool", &policyUSDC, big.NewInt(100), nil, nil, common.Address{}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			h := newPolicyTestHandler(t, nil)
			snap := &PositionSnapshot{
				Entry: &TokenEntry{
					ContractAddress: policyVault,
					WalletAddress:   policyWallet,
					AssetAddress:    tc.asset,
					Star:            "spark",
					TokenType:       tc.tokenType,
				},
				Balance:         tc.balance,
				UnderlyingValue: tc.underlying,
				ChainID:         1,
				BlockNumber:     100,
				Direction:       DirectionSweep,
				BlockTimestamp:  time.Unix(1750000000, 0).UTC(),
			}
			positions, err := h.buildPositions(context.Background(), []*PositionSnapshot{snap}, map[string]bool{})
			if err != nil {
				t.Fatalf("buildPositions: %v", err)
			}
			got := positions[0].Underlying
			if tc.wantVal == nil {
				if got != nil {
					t.Fatalf("Underlying = %+v, want nil", got)
				}
				return
			}
			if got == nil {
				t.Fatal("Underlying = nil, want valuation")
			}
			if got.Value.Cmp(tc.wantVal) != 0 {
				t.Fatalf("Value = %s, want %s", got.Value, tc.wantVal)
			}
			if got.AssetAddress != tc.wantAsset {
				t.Fatalf("AssetAddress = %s, want %s", got.AssetAddress.Hex(), tc.wantAsset.Hex())
			}
		})
	}
}

func TestBuildPositions_RecordsFailureMetricWhenValuationMissing(t *testing.T) {
	tel, reader := newRecordingTelemetry(t)
	h := newPolicyTestHandler(t, tel)

	snap := &PositionSnapshot{
		Entry: &TokenEntry{
			ContractAddress: policyVault,
			WalletAddress:   policyWallet,
			AssetAddress:    &policyUSDC,
			Star:            "spark",
			TokenType:       "erc4626",
		},
		Balance:         big.NewInt(100),
		UnderlyingValue: nil, // convert failed
		ChainID:         1,
		BlockNumber:     100,
		Direction:       DirectionSweep,
		BlockTimestamp:  time.Unix(1750000000, 0).UTC(),
	}

	_, err := h.buildPositions(context.Background(), []*PositionSnapshot{snap}, map[string]bool{})
	if err != nil {
		t.Fatalf("buildPositions: %v", err)
	}

	m := collectMetric(t, reader, "allocation.underlying_value.failures.total")
	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("allocation.underlying_value.failures.total is %T, want Sum[int64]", m.Data)
	}
	if len(sum.DataPoints) != 1 {
		t.Fatalf("got %d data points, want 1", len(sum.DataPoints))
	}

	dp := sum.DataPoints[0]
	if dp.Value != 1 {
		t.Errorf("datapoint value = %d, want 1", dp.Value)
	}

	reason, ok := dp.Attributes.Value("reason")
	if !ok || reason.AsString() != string(reasonConvertFailed) {
		t.Errorf("reason attribute = %v, want %q", reason, reasonConvertFailed)
	}

	tokenType, ok := dp.Attributes.Value("token_type")
	if !ok || tokenType.AsString() != "erc4626" {
		t.Errorf("token_type attribute = %v, want %q", tokenType, "erc4626")
	}

	token, ok := dp.Attributes.Value("token")
	if !ok || token.AsString() != policyVault.Hex() {
		t.Errorf("token attribute = %v, want %s", token, policyVault.Hex())
	}
}

func TestBuildPositions_RecordsFailureMetric_MissingAssetAddress(t *testing.T) {
	tel, reader := newRecordingTelemetry(t)
	h := newPolicyTestHandler(t, tel)

	snap := &PositionSnapshot{
		Entry: &TokenEntry{
			ContractAddress: policyVault,
			WalletAddress:   policyWallet,
			AssetAddress:    nil, // no asset address: missing_asset_address reason
			Star:            "spark",
			TokenType:       "erc4626",
		},
		Balance:         big.NewInt(100),
		UnderlyingValue: big.NewInt(123),
		ChainID:         1,
		BlockNumber:     100,
		Direction:       DirectionSweep,
		BlockTimestamp:  time.Unix(1750000000, 0).UTC(),
	}

	_, err := h.buildPositions(context.Background(), []*PositionSnapshot{snap}, map[string]bool{})
	if err != nil {
		t.Fatalf("buildPositions: %v", err)
	}

	m := collectMetric(t, reader, "allocation.underlying_value.failures.total")
	sum, ok := m.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("metric data is %T, want Sum[int64]", m.Data)
	}
	if len(sum.DataPoints) != 1 {
		t.Fatalf("got %d data points, want 1", len(sum.DataPoints))
	}

	dp := sum.DataPoints[0]
	if dp.Value != 1 {
		t.Errorf("datapoint value = %d, want 1", dp.Value)
	}

	reason, ok := dp.Attributes.Value("reason")
	if !ok || reason.AsString() != string(reasonMissingAssetAddress) {
		t.Errorf("reason attribute = %v, want %q", reason, reasonMissingAssetAddress)
	}
}

func TestHandleBatch_ERC4626_SetsUnderlyingOnPosition(t *testing.T) {
	susds := common.HexToAddress("0xa3931d71877c0e7a3148cb7eb4463524fec27fbd")
	usds := common.HexToAddress("0xdc035d45d973e3ec169d2276ddab16f1e407384f")
	wallet := common.HexToAddress("0x1601843c5e9bc251a3272907010afa41fa18347e")
	underlyingRaw := big.NewInt(1_050_000_000_000_000_000) // 1.05 USDS in 18-decimal raw units
	shares := big.NewInt(1_000_000_000_000_000_000)

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
				Balance:         new(big.Int).Set(shares),
				ScaledBalance:   new(big.Int).Set(shares),
				UnderlyingValue: new(big.Int).Set(underlyingRaw),
				ChainID:         1,
				BlockNumber:     100,
				TxAmount:        big.NewInt(0),
				Direction:       DirectionSweep,
			},
		},
	})
	if err != nil {
		t.Fatalf("HandleBatch: %v", err)
	}

	if len(repo.saved) != 1 {
		t.Fatalf("expected 1 saved position, got %d", len(repo.saved))
	}

	pos := repo.saved[0]
	if pos.Underlying == nil {
		t.Fatal("Underlying = nil, want valuation")
	}
	if pos.Underlying.Value.Cmp(underlyingRaw) != 0 {
		t.Errorf("Underlying.Value = %s, want %s", pos.Underlying.Value, underlyingRaw)
	}
	if pos.Underlying.AssetAddress != usds {
		t.Errorf("Underlying.AssetAddress = %s, want %s", pos.Underlying.AssetAddress.Hex(), usds.Hex())
	}
	if pos.Underlying.AssetDecimals != 18 {
		t.Errorf("Underlying.AssetDecimals = %d, want 18", pos.Underlying.AssetDecimals)
	}
	if pos.Underlying.AssetSymbol != "USDS" {
		t.Errorf("Underlying.AssetSymbol = %q, want USDS", pos.Underlying.AssetSymbol)
	}
}
