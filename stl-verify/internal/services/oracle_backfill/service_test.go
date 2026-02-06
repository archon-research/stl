package oracle_backfill

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	ethtypes "github.com/ethereum/go-ethereum/core/types"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// ---------------------------------------------------------------------------
// Test helpers
// ---------------------------------------------------------------------------

// mockRepo implements outbound.OnchainPriceRepository for testing.
type mockRepo struct {
	getOracleSourceFn   func(ctx context.Context, name string) (*entity.OracleSource, error)
	getEnabledAssetsFn  func(ctx context.Context, oracleSourceID int64) ([]*entity.OracleAsset, error)
	getLatestPricesFn   func(ctx context.Context, oracleSourceID int64) (map[int64]float64, error)
	getLatestBlockFn    func(ctx context.Context, oracleSourceID int64) (int64, error)
	getTokenAddressesFn func(ctx context.Context, oracleSourceID int64) (map[int64][]byte, error)
	upsertPricesFn      func(ctx context.Context, prices []*entity.OnchainTokenPrice) error

	// mu guards upserted for concurrent access from the batch writer goroutine.
	mu       sync.Mutex
	upserted []*entity.OnchainTokenPrice
}

func (m *mockRepo) GetOracleSource(ctx context.Context, name string) (*entity.OracleSource, error) {
	if m.getOracleSourceFn != nil {
		return m.getOracleSourceFn(ctx, name)
	}
	return nil, errors.New("GetOracleSource not mocked")
}

func (m *mockRepo) GetEnabledAssets(ctx context.Context, oracleSourceID int64) ([]*entity.OracleAsset, error) {
	if m.getEnabledAssetsFn != nil {
		return m.getEnabledAssetsFn(ctx, oracleSourceID)
	}
	return nil, errors.New("GetEnabledAssets not mocked")
}

func (m *mockRepo) GetLatestPrices(ctx context.Context, oracleSourceID int64) (map[int64]float64, error) {
	if m.getLatestPricesFn != nil {
		return m.getLatestPricesFn(ctx, oracleSourceID)
	}
	return nil, errors.New("GetLatestPrices not mocked")
}

func (m *mockRepo) GetLatestBlock(ctx context.Context, oracleSourceID int64) (int64, error) {
	if m.getLatestBlockFn != nil {
		return m.getLatestBlockFn(ctx, oracleSourceID)
	}
	return 0, nil
}

func (m *mockRepo) GetTokenAddresses(ctx context.Context, oracleSourceID int64) (map[int64][]byte, error) {
	if m.getTokenAddressesFn != nil {
		return m.getTokenAddressesFn(ctx, oracleSourceID)
	}
	return nil, errors.New("GetTokenAddresses not mocked")
}

func (m *mockRepo) UpsertPrices(ctx context.Context, prices []*entity.OnchainTokenPrice) error {
	if m.upsertPricesFn != nil {
		return m.upsertPricesFn(ctx, prices)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.upserted = append(m.upserted, prices...)
	return nil
}

func (m *mockRepo) getUpserted() []*entity.OnchainTokenPrice {
	m.mu.Lock()
	defer m.mu.Unlock()
	cp := make([]*entity.OnchainTokenPrice, len(m.upserted))
	copy(cp, m.upserted)
	return cp
}

// mockHeaderFetcher implements BlockHeaderFetcher for testing.
type mockHeaderFetcher struct {
	headerByNumberFn func(ctx context.Context, number *big.Int) (*ethtypes.Header, error)
}

func (m *mockHeaderFetcher) HeaderByNumber(ctx context.Context, number *big.Int) (*ethtypes.Header, error) {
	if m.headerByNumberFn != nil {
		return m.headerByNumberFn(ctx, number)
	}
	return &ethtypes.Header{Time: uint64(1700000000 + number.Int64())}, nil
}

// abiPackAddress packs an address as the return data for getPriceOracle.
func abiPackAddress(t *testing.T, addr common.Address) []byte {
	return testutil.PackOracleAddress(t, addr)
}

// abiPackPrices packs a slice of *big.Int as the return data for getAssetsPrices.
func abiPackPrices(t *testing.T, prices []*big.Int) []byte {
	return testutil.PackAssetPrices(t, prices)
}

// defaultOracleSource returns a standard OracleSource for testing.
func defaultOracleSource() *entity.OracleSource {
	return &entity.OracleSource{
		ID:                  1,
		Name:                "sparklend",
		DisplayName:         "SparkLend",
		ChainID:             1,
		PoolAddressProvider: common.HexToAddress("0x0000000000000000000000000000000000000AAA").Bytes(),
		DeploymentBlock:     100,
		Enabled:             true,
	}
}

// defaultAssets returns a standard set of OracleAssets for testing (two tokens).
func defaultAssets() []*entity.OracleAsset {
	return []*entity.OracleAsset{
		{ID: 1, OracleSourceID: 1, TokenID: 10, Enabled: true},
		{ID: 2, OracleSourceID: 1, TokenID: 20, Enabled: true},
	}
}

// defaultTokenAddresses returns a token ID -> address map matching defaultAssets.
func defaultTokenAddresses() map[int64]common.Address {
	return map[int64]common.Address{
		10: common.HexToAddress("0x0000000000000000000000000000000000000010"),
		20: common.HexToAddress("0x0000000000000000000000000000000000000020"),
	}
}

// oracleAddr is the oracle address returned by the mock multicall.
var oracleAddr = common.HexToAddress("0x0000000000000000000000000000000000000BBB")

// multicallResult returns the multicall results for a given set of prices.
// It handles both the initial 2-call pattern (getPriceOracle + getAssetsPrices)
// and the retry 1-call pattern (getAssetsPrices only) from FetchOraclePrices.
// On the first block, the worker's cachedOracleAddr is zero, which does not
// match oracleAddr, so FetchOraclePrices retries with a single call.
func multicallResult(t *testing.T, calls []outbound.Call, prices []*big.Int) []outbound.Result {
	t.Helper()
	if len(calls) == 2 {
		return []outbound.Result{
			{Success: true, ReturnData: abiPackAddress(t, oracleAddr)},
			{Success: true, ReturnData: abiPackPrices(t, prices)},
		}
	}
	// Retry path: single getAssetsPrices call
	return []outbound.Result{
		{Success: true, ReturnData: abiPackPrices(t, prices)},
	}
}

// defaultMulticallExecute returns a mock Execute function that returns
// valid oracle address + prices for the given raw prices per block.
// It handles both the initial 2-call pattern and the retry 1-call pattern
// from FetchOraclePrices. pricesByBlock maps blockNumber -> []*big.Int prices.
// If a block is missing, it uses defaultPrices.
func defaultMulticallExecute(t *testing.T, defaultPrices []*big.Int, pricesByBlock map[int64][]*big.Int) func(ctx context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
	t.Helper()
	return func(ctx context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		prices := defaultPrices
		if pricesByBlock != nil {
			if p, ok := pricesByBlock[blockNumber.Int64()]; ok {
				prices = p
			}
		}
		return multicallResult(t, calls, prices), nil
	}
}

// blockDependentPrices returns a mock Execute function where each block
// gets a unique price derived from the block number. This ensures all
// blocks pass change detection.
func blockDependentPrices(t *testing.T) func(ctx context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
	t.Helper()
	return func(ctx context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		bn := blockNumber.Int64()
		prices := []*big.Int{
			new(big.Int).Mul(big.NewInt(bn), big.NewInt(100_000_000)),
			new(big.Int).Mul(big.NewInt(bn), big.NewInt(200_000_000)),
		}
		return multicallResult(t, calls, prices), nil
	}
}

// ---------------------------------------------------------------------------
// TestNewService
// ---------------------------------------------------------------------------

func TestNewService(t *testing.T) {
	validFetcher := &mockHeaderFetcher{}
	validFactory := func() (outbound.Multicaller, error) {
		return &testutil.MockMulticaller{}, nil
	}
	validRepo := &mockRepo{}
	validTokenAddrs := defaultTokenAddresses()

	tests := []struct {
		name           string
		config         Config
		headerFetcher  BlockHeaderFetcher
		newMulticaller MulticallFactory
		repo           outbound.OnchainPriceRepository
		tokenAddresses map[int64]common.Address
		wantErr        bool
		errContains    string
		// checks run on the returned service when wantErr is false
		checkService func(t *testing.T, svc *Service)
	}{
		{
			name: "success with all valid params",
			config: Config{
				Concurrency:  2,
				BatchSize:    50,
				OracleSource: "test-oracle",
				Logger:       testutil.DiscardLogger(),
			},
			headerFetcher:  validFetcher,
			newMulticaller: validFactory,
			repo:           validRepo,
			tokenAddresses: validTokenAddrs,
			wantErr:        false,
			checkService: func(t *testing.T, svc *Service) {
				t.Helper()
				if svc.config.Concurrency != 2 {
					t.Errorf("Concurrency = %d, want 2", svc.config.Concurrency)
				}
				if svc.config.BatchSize != 50 {
					t.Errorf("BatchSize = %d, want 50", svc.config.BatchSize)
				}
				if svc.config.OracleSource != "test-oracle" {
					t.Errorf("OracleSource = %q, want %q", svc.config.OracleSource, "test-oracle")
				}
				if svc.providerABI == nil {
					t.Error("providerABI is nil")
				}
				if svc.oracleABI == nil {
					t.Error("oracleABI is nil")
				}
				if len(svc.tokenAddressMap) != 2 {
					t.Errorf("tokenAddressMap length = %d, want 2", len(svc.tokenAddressMap))
				}
			},
		},
		{
			name:           "success with default config values",
			config:         Config{}, // all zero values
			headerFetcher:  validFetcher,
			newMulticaller: validFactory,
			repo:           validRepo,
			tokenAddresses: validTokenAddrs,
			wantErr:        false,
			checkService: func(t *testing.T, svc *Service) {
				t.Helper()
				defaults := configDefaults()
				if svc.config.Concurrency != defaults.Concurrency {
					t.Errorf("Concurrency = %d, want default %d", svc.config.Concurrency, defaults.Concurrency)
				}
				if svc.config.BatchSize != defaults.BatchSize {
					t.Errorf("BatchSize = %d, want default %d", svc.config.BatchSize, defaults.BatchSize)
				}
				if svc.config.OracleSource != defaults.OracleSource {
					t.Errorf("OracleSource = %q, want default %q", svc.config.OracleSource, defaults.OracleSource)
				}
				if svc.config.Logger == nil {
					t.Error("Logger should not be nil after defaults applied")
				}
			},
		},
		{
			name: "success with negative concurrency uses default",
			config: Config{
				Concurrency: -5,
				Logger:      testutil.DiscardLogger(),
			},
			headerFetcher:  validFetcher,
			newMulticaller: validFactory,
			repo:           validRepo,
			tokenAddresses: validTokenAddrs,
			wantErr:        false,
			checkService: func(t *testing.T, svc *Service) {
				t.Helper()
				if svc.config.Concurrency != configDefaults().Concurrency {
					t.Errorf("Concurrency = %d, want default %d", svc.config.Concurrency, configDefaults().Concurrency)
				}
			},
		},
		{
			name: "success with negative batch size uses default",
			config: Config{
				BatchSize: -1,
				Logger:    testutil.DiscardLogger(),
			},
			headerFetcher:  validFetcher,
			newMulticaller: validFactory,
			repo:           validRepo,
			tokenAddresses: validTokenAddrs,
			wantErr:        false,
			checkService: func(t *testing.T, svc *Service) {
				t.Helper()
				if svc.config.BatchSize != configDefaults().BatchSize {
					t.Errorf("BatchSize = %d, want default %d", svc.config.BatchSize, configDefaults().BatchSize)
				}
			},
		},
		{
			name:           "error nil headerFetcher",
			config:         Config{Logger: testutil.DiscardLogger()},
			headerFetcher:  nil,
			newMulticaller: validFactory,
			repo:           validRepo,
			tokenAddresses: validTokenAddrs,
			wantErr:        true,
			errContains:    "headerFetcher cannot be nil",
		},
		{
			name:           "error nil newMulticaller",
			config:         Config{Logger: testutil.DiscardLogger()},
			headerFetcher:  validFetcher,
			newMulticaller: nil,
			repo:           validRepo,
			tokenAddresses: validTokenAddrs,
			wantErr:        true,
			errContains:    "newMulticaller cannot be nil",
		},
		{
			name:           "error nil repo",
			config:         Config{Logger: testutil.DiscardLogger()},
			headerFetcher:  validFetcher,
			newMulticaller: validFactory,
			repo:           nil,
			tokenAddresses: validTokenAddrs,
			wantErr:        true,
			errContains:    "repo cannot be nil",
		},
		{
			name:           "error empty tokenAddresses",
			config:         Config{Logger: testutil.DiscardLogger()},
			headerFetcher:  validFetcher,
			newMulticaller: validFactory,
			repo:           validRepo,
			tokenAddresses: map[int64]common.Address{},
			wantErr:        true,
			errContains:    "tokenAddresses cannot be empty",
		},
		{
			name:           "error nil tokenAddresses",
			config:         Config{Logger: testutil.DiscardLogger()},
			headerFetcher:  validFetcher,
			newMulticaller: validFactory,
			repo:           validRepo,
			tokenAddresses: nil,
			wantErr:        true,
			errContains:    "tokenAddresses cannot be empty",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, err := NewService(tt.config, tt.headerFetcher, tt.newMulticaller, tt.repo, tt.tokenAddresses)

			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.errContains)
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("error = %q, want it to contain %q", err.Error(), tt.errContains)
				}
				if svc != nil {
					t.Error("expected nil service on error")
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if svc == nil {
				t.Fatal("expected non-nil service")
			}

			if tt.checkService != nil {
				tt.checkService(t, svc)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TestRun
// ---------------------------------------------------------------------------

func TestRun(t *testing.T) {
	// Standard prices: token 10 = $1.00 (1e8), token 20 = $2500.00 (250e9)
	standardPrices := []*big.Int{big.NewInt(100_000_000), big.NewInt(250_000_000_000)}

	tests := []struct {
		name        string
		fromBlock   int64
		toBlock     int64
		config      Config
		setupRepo   func() *mockRepo
		setupHeader func() *mockHeaderFetcher
		setupMC     func(t *testing.T) MulticallFactory
		wantErr     bool
		errContains string
		// checkResult is called after Run completes successfully.
		checkResult func(t *testing.T, repo *mockRepo)
	}{
		{
			name:      "success processes 5 blocks",
			fromBlock: 100,
			toBlock:   104,
			config: Config{
				Concurrency:  1,
				BatchSize:    100,
				OracleSource: "sparklend",
				Logger:       testutil.DiscardLogger(),
			},
			setupRepo: func() *mockRepo {
				return &mockRepo{
					getOracleSourceFn:  func(_ context.Context, _ string) (*entity.OracleSource, error) { return defaultOracleSource(), nil },
					getEnabledAssetsFn: func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) { return defaultAssets(), nil },
					getLatestBlockFn:   func(_ context.Context, _ int64) (int64, error) { return 0, nil },
				}
			},
			setupHeader: func() *mockHeaderFetcher {
				return &mockHeaderFetcher{
					headerByNumberFn: func(_ context.Context, number *big.Int) (*ethtypes.Header, error) {
						return &ethtypes.Header{Time: uint64(1700000000 + number.Int64())}, nil
					},
				}
			},
			setupMC: func(t *testing.T) MulticallFactory {
				return func() (outbound.Multicaller, error) {
					return &testutil.MockMulticaller{ExecuteFn: blockDependentPrices(t)}, nil
				}
			},
			wantErr: false,
			checkResult: func(t *testing.T, repo *mockRepo) {
				t.Helper()
				upserted := repo.getUpserted()
				// 5 blocks x 2 tokens = 10 prices (all different)
				if len(upserted) != 10 {
					t.Errorf("upserted count = %d, want 10", len(upserted))
				}
			},
		},
		{
			name:      "success with resume - latestBlock >= fromBlock",
			fromBlock: 100,
			toBlock:   109,
			config: Config{
				Concurrency:  1,
				BatchSize:    100,
				OracleSource: "sparklend",
				Logger:       testutil.DiscardLogger(),
			},
			setupRepo: func() *mockRepo {
				return &mockRepo{
					getOracleSourceFn:  func(_ context.Context, _ string) (*entity.OracleSource, error) { return defaultOracleSource(), nil },
					getEnabledAssetsFn: func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) { return defaultAssets(), nil },
					getLatestBlockFn: func(_ context.Context, _ int64) (int64, error) {
						return 106, nil
					},
				}
			},
			setupHeader: func() *mockHeaderFetcher {
				return &mockHeaderFetcher{
					headerByNumberFn: func(_ context.Context, number *big.Int) (*ethtypes.Header, error) {
						return &ethtypes.Header{Time: uint64(1700000000 + number.Int64())}, nil
					},
				}
			},
			setupMC: func(t *testing.T) MulticallFactory {
				return func() (outbound.Multicaller, error) {
					return &testutil.MockMulticaller{
						ExecuteFn: func(_ context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
							bn := blockNumber.Int64()
							if bn < 107 || bn > 109 {
								t.Errorf("unexpected block number %d, expected 107-109", bn)
							}
							prices := []*big.Int{
								new(big.Int).Mul(big.NewInt(bn), big.NewInt(100_000_000)),
								new(big.Int).Mul(big.NewInt(bn), big.NewInt(200_000_000)),
							}
							return multicallResult(t, calls, prices), nil
						},
					}, nil
				}
			},
			wantErr: false,
			checkResult: func(t *testing.T, repo *mockRepo) {
				t.Helper()
				upserted := repo.getUpserted()
				// 3 blocks x 2 tokens = 6 prices
				if len(upserted) != 6 {
					t.Errorf("upserted count = %d, want 6", len(upserted))
				}
				for _, p := range upserted {
					if p.BlockNumber < 107 {
						t.Errorf("unexpected block %d in upserted prices, should be >= 107", p.BlockNumber)
					}
				}
			},
		},
		{
			name:      "success totalBlocks <= 0 after resume returns nil",
			fromBlock: 100,
			toBlock:   104,
			config: Config{
				Concurrency:  1,
				BatchSize:    100,
				OracleSource: "sparklend",
				Logger:       testutil.DiscardLogger(),
			},
			setupRepo: func() *mockRepo {
				return &mockRepo{
					getOracleSourceFn:  func(_ context.Context, _ string) (*entity.OracleSource, error) { return defaultOracleSource(), nil },
					getEnabledAssetsFn: func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) { return defaultAssets(), nil },
					getLatestBlockFn: func(_ context.Context, _ int64) (int64, error) {
						return 110, nil
					},
				}
			},
			setupHeader: func() *mockHeaderFetcher { return &mockHeaderFetcher{} },
			setupMC: func(t *testing.T) MulticallFactory {
				return func() (outbound.Multicaller, error) {
					t.Error("multicall factory should not be called when no blocks to process")
					return nil, errors.New("should not be called")
				}
			},
			wantErr: false,
			checkResult: func(t *testing.T, repo *mockRepo) {
				t.Helper()
				if len(repo.getUpserted()) != 0 {
					t.Error("expected no upserted prices when all blocks already processed")
				}
			},
		},
		{
			name:      "success more workers than blocks adjusts worker count",
			fromBlock: 100,
			toBlock:   101,
			config: Config{
				Concurrency:  10,
				BatchSize:    100,
				OracleSource: "sparklend",
				Logger:       testutil.DiscardLogger(),
			},
			setupRepo: func() *mockRepo {
				return &mockRepo{
					getOracleSourceFn:  func(_ context.Context, _ string) (*entity.OracleSource, error) { return defaultOracleSource(), nil },
					getEnabledAssetsFn: func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) { return defaultAssets(), nil },
					getLatestBlockFn:   func(_ context.Context, _ int64) (int64, error) { return 0, nil },
				}
			},
			setupHeader: func() *mockHeaderFetcher {
				return &mockHeaderFetcher{
					headerByNumberFn: func(_ context.Context, number *big.Int) (*ethtypes.Header, error) {
						return &ethtypes.Header{Time: uint64(1700000000 + number.Int64())}, nil
					},
				}
			},
			setupMC: func(t *testing.T) MulticallFactory {
				return func() (outbound.Multicaller, error) {
					return &testutil.MockMulticaller{ExecuteFn: blockDependentPrices(t)}, nil
				}
			},
			wantErr: false,
			checkResult: func(t *testing.T, repo *mockRepo) {
				t.Helper()
				upserted := repo.getUpserted()
				// 2 blocks x 2 tokens = 4 prices
				if len(upserted) != 4 {
					t.Errorf("upserted count = %d, want 4", len(upserted))
				}
			},
		},
		{
			name:      "success change detection same price across all blocks stores only first",
			fromBlock: 100,
			toBlock:   104,
			config: Config{
				Concurrency:  1,
				BatchSize:    100,
				OracleSource: "sparklend",
				Logger:       testutil.DiscardLogger(),
			},
			setupRepo: func() *mockRepo {
				return &mockRepo{
					getOracleSourceFn:  func(_ context.Context, _ string) (*entity.OracleSource, error) { return defaultOracleSource(), nil },
					getEnabledAssetsFn: func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) { return defaultAssets(), nil },
					getLatestBlockFn:   func(_ context.Context, _ int64) (int64, error) { return 0, nil },
				}
			},
			setupHeader: func() *mockHeaderFetcher {
				return &mockHeaderFetcher{
					headerByNumberFn: func(_ context.Context, number *big.Int) (*ethtypes.Header, error) {
						return &ethtypes.Header{Time: uint64(1700000000 + number.Int64())}, nil
					},
				}
			},
			setupMC: func(t *testing.T) MulticallFactory {
				return func() (outbound.Multicaller, error) {
					return &testutil.MockMulticaller{
						ExecuteFn: defaultMulticallExecute(t, standardPrices, nil),
					}, nil
				}
			},
			wantErr: false,
			checkResult: func(t *testing.T, repo *mockRepo) {
				t.Helper()
				upserted := repo.getUpserted()
				// Same price across all 5 blocks: only first block stored per token = 2 prices
				if len(upserted) != 2 {
					t.Errorf("upserted count = %d, want 2 (one per token, first block only)", len(upserted))
				}
				for _, p := range upserted {
					if p.BlockNumber != 100 {
						t.Errorf("expected price from block 100, got block %d", p.BlockNumber)
					}
				}
			},
		},
		{
			name:      "error GetOracleSource fails",
			fromBlock: 100,
			toBlock:   104,
			config: Config{
				Concurrency:  1,
				BatchSize:    100,
				OracleSource: "sparklend",
				Logger:       testutil.DiscardLogger(),
			},
			setupRepo: func() *mockRepo {
				return &mockRepo{
					getOracleSourceFn: func(_ context.Context, _ string) (*entity.OracleSource, error) {
						return nil, errors.New("database connection failed")
					},
				}
			},
			setupHeader: func() *mockHeaderFetcher { return &mockHeaderFetcher{} },
			setupMC: func(t *testing.T) MulticallFactory {
				return func() (outbound.Multicaller, error) { return &testutil.MockMulticaller{}, nil }
			},
			wantErr:     true,
			errContains: "getting oracle source",
		},
		{
			name:      "error GetEnabledAssets fails",
			fromBlock: 100,
			toBlock:   104,
			config: Config{
				Concurrency:  1,
				BatchSize:    100,
				OracleSource: "sparklend",
				Logger:       testutil.DiscardLogger(),
			},
			setupRepo: func() *mockRepo {
				return &mockRepo{
					getOracleSourceFn: func(_ context.Context, _ string) (*entity.OracleSource, error) {
						return defaultOracleSource(), nil
					},
					getEnabledAssetsFn: func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) {
						return nil, errors.New("query failed")
					},
				}
			},
			setupHeader: func() *mockHeaderFetcher { return &mockHeaderFetcher{} },
			setupMC: func(t *testing.T) MulticallFactory {
				return func() (outbound.Multicaller, error) { return &testutil.MockMulticaller{}, nil }
			},
			wantErr:     true,
			errContains: "getting enabled assets",
		},
		{
			name:      "error GetEnabledAssets returns empty",
			fromBlock: 100,
			toBlock:   104,
			config: Config{
				Concurrency:  1,
				BatchSize:    100,
				OracleSource: "sparklend",
				Logger:       testutil.DiscardLogger(),
			},
			setupRepo: func() *mockRepo {
				return &mockRepo{
					getOracleSourceFn: func(_ context.Context, _ string) (*entity.OracleSource, error) {
						return defaultOracleSource(), nil
					},
					getEnabledAssetsFn: func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) {
						return []*entity.OracleAsset{}, nil
					},
				}
			},
			setupHeader: func() *mockHeaderFetcher { return &mockHeaderFetcher{} },
			setupMC: func(t *testing.T) MulticallFactory {
				return func() (outbound.Multicaller, error) { return &testutil.MockMulticaller{}, nil }
			},
			wantErr:     true,
			errContains: "no enabled assets",
		},
		{
			name:      "error token address not found in map",
			fromBlock: 100,
			toBlock:   104,
			config: Config{
				Concurrency:  1,
				BatchSize:    100,
				OracleSource: "sparklend",
				Logger:       testutil.DiscardLogger(),
			},
			setupRepo: func() *mockRepo {
				return &mockRepo{
					getOracleSourceFn: func(_ context.Context, _ string) (*entity.OracleSource, error) {
						return defaultOracleSource(), nil
					},
					getEnabledAssetsFn: func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) {
						return []*entity.OracleAsset{
							{ID: 1, OracleSourceID: 1, TokenID: 999, Enabled: true},
						}, nil
					},
					getLatestBlockFn: func(_ context.Context, _ int64) (int64, error) { return 0, nil },
				}
			},
			setupHeader: func() *mockHeaderFetcher { return &mockHeaderFetcher{} },
			setupMC: func(t *testing.T) MulticallFactory {
				return func() (outbound.Multicaller, error) { return &testutil.MockMulticaller{}, nil }
			},
			wantErr:     true,
			errContains: "token address not found for token_id 999",
		},
		{
			name:      "error GetLatestBlock fails",
			fromBlock: 100,
			toBlock:   104,
			config: Config{
				Concurrency:  1,
				BatchSize:    100,
				OracleSource: "sparklend",
				Logger:       testutil.DiscardLogger(),
			},
			setupRepo: func() *mockRepo {
				return &mockRepo{
					getOracleSourceFn:  func(_ context.Context, _ string) (*entity.OracleSource, error) { return defaultOracleSource(), nil },
					getEnabledAssetsFn: func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) { return defaultAssets(), nil },
					getLatestBlockFn: func(_ context.Context, _ int64) (int64, error) {
						return 0, errors.New("redis unavailable")
					},
				}
			},
			setupHeader: func() *mockHeaderFetcher { return &mockHeaderFetcher{} },
			setupMC: func(t *testing.T) MulticallFactory {
				return func() (outbound.Multicaller, error) { return &testutil.MockMulticaller{}, nil }
			},
			wantErr:     true,
			errContains: "getting latest block",
		},
		{
			name:      "success with small batch size triggers mid-loop flush",
			fromBlock: 100,
			toBlock:   104,
			config: Config{
				Concurrency:  1,
				BatchSize:    3, // small batch size forces flush during for loop
				OracleSource: "sparklend",
				Logger:       testutil.DiscardLogger(),
			},
			setupRepo: func() *mockRepo {
				return &mockRepo{
					getOracleSourceFn:  func(_ context.Context, _ string) (*entity.OracleSource, error) { return defaultOracleSource(), nil },
					getEnabledAssetsFn: func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) { return defaultAssets(), nil },
					getLatestBlockFn:   func(_ context.Context, _ int64) (int64, error) { return 0, nil },
				}
			},
			setupHeader: func() *mockHeaderFetcher {
				return &mockHeaderFetcher{
					headerByNumberFn: func(_ context.Context, number *big.Int) (*ethtypes.Header, error) {
						return &ethtypes.Header{Time: uint64(1700000000 + number.Int64())}, nil
					},
				}
			},
			setupMC: func(t *testing.T) MulticallFactory {
				return func() (outbound.Multicaller, error) {
					return &testutil.MockMulticaller{ExecuteFn: blockDependentPrices(t)}, nil
				}
			},
			wantErr: false,
			checkResult: func(t *testing.T, repo *mockRepo) {
				t.Helper()
				upserted := repo.getUpserted()
				// 5 blocks x 2 tokens = 10 prices (all different)
				if len(upserted) != 10 {
					t.Errorf("upserted count = %d, want 10", len(upserted))
				}
			},
		},
		{
			name:      "error UpsertPrices fails during mid-loop flush",
			fromBlock: 100,
			toBlock:   104,
			config: Config{
				Concurrency:  1,
				BatchSize:    3, // small batch triggers mid-loop flush
				OracleSource: "sparklend",
				Logger:       testutil.DiscardLogger(),
			},
			setupRepo: func() *mockRepo {
				return &mockRepo{
					getOracleSourceFn:  func(_ context.Context, _ string) (*entity.OracleSource, error) { return defaultOracleSource(), nil },
					getEnabledAssetsFn: func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) { return defaultAssets(), nil },
					getLatestBlockFn:   func(_ context.Context, _ int64) (int64, error) { return 0, nil },
					upsertPricesFn: func(_ context.Context, _ []*entity.OnchainTokenPrice) error {
						return errors.New("disk full")
					},
				}
			},
			setupHeader: func() *mockHeaderFetcher {
				return &mockHeaderFetcher{
					headerByNumberFn: func(_ context.Context, number *big.Int) (*ethtypes.Header, error) {
						return &ethtypes.Header{Time: uint64(1700000000 + number.Int64())}, nil
					},
				}
			},
			setupMC: func(t *testing.T) MulticallFactory {
				return func() (outbound.Multicaller, error) {
					return &testutil.MockMulticaller{ExecuteFn: blockDependentPrices(t)}, nil
				}
			},
			wantErr:     true,
			errContains: "batch writer",
		},
		{
			name:      "error UpsertPrices fails from batch writer",
			fromBlock: 100,
			toBlock:   104,
			config: Config{
				Concurrency:  1,
				BatchSize:    100,
				OracleSource: "sparklend",
				Logger:       testutil.DiscardLogger(),
			},
			setupRepo: func() *mockRepo {
				return &mockRepo{
					getOracleSourceFn:  func(_ context.Context, _ string) (*entity.OracleSource, error) { return defaultOracleSource(), nil },
					getEnabledAssetsFn: func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) { return defaultAssets(), nil },
					getLatestBlockFn:   func(_ context.Context, _ int64) (int64, error) { return 0, nil },
					upsertPricesFn: func(_ context.Context, _ []*entity.OnchainTokenPrice) error {
						return errors.New("disk full")
					},
				}
			},
			setupHeader: func() *mockHeaderFetcher {
				return &mockHeaderFetcher{
					headerByNumberFn: func(_ context.Context, number *big.Int) (*ethtypes.Header, error) {
						return &ethtypes.Header{Time: uint64(1700000000 + number.Int64())}, nil
					},
				}
			},
			setupMC: func(t *testing.T) MulticallFactory {
				return func() (outbound.Multicaller, error) {
					return &testutil.MockMulticaller{ExecuteFn: blockDependentPrices(t)}, nil
				}
			},
			wantErr:     true,
			errContains: "batch writer",
		},
		{
			name:      "error multicall factory returns error worker continues",
			fromBlock: 100,
			toBlock:   104,
			config: Config{
				Concurrency:  1,
				BatchSize:    100,
				OracleSource: "sparklend",
				Logger:       testutil.DiscardLogger(),
			},
			setupRepo: func() *mockRepo {
				return &mockRepo{
					getOracleSourceFn:  func(_ context.Context, _ string) (*entity.OracleSource, error) { return defaultOracleSource(), nil },
					getEnabledAssetsFn: func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) { return defaultAssets(), nil },
					getLatestBlockFn:   func(_ context.Context, _ int64) (int64, error) { return 0, nil },
				}
			},
			setupHeader: func() *mockHeaderFetcher { return &mockHeaderFetcher{} },
			setupMC: func(t *testing.T) MulticallFactory {
				return func() (outbound.Multicaller, error) {
					return nil, errors.New("cannot connect to RPC")
				}
			},
			wantErr: false,
			checkResult: func(t *testing.T, repo *mockRepo) {
				t.Helper()
				if len(repo.getUpserted()) != 0 {
					t.Errorf("expected no upserted prices, got %d", len(repo.getUpserted()))
				}
			},
		},
		{
			name:      "error FetchOraclePrices fails for a block worker continues",
			fromBlock: 100,
			toBlock:   104,
			config: Config{
				Concurrency:  1,
				BatchSize:    100,
				OracleSource: "sparklend",
				Logger:       testutil.DiscardLogger(),
			},
			setupRepo: func() *mockRepo {
				return &mockRepo{
					getOracleSourceFn:  func(_ context.Context, _ string) (*entity.OracleSource, error) { return defaultOracleSource(), nil },
					getEnabledAssetsFn: func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) { return defaultAssets(), nil },
					getLatestBlockFn:   func(_ context.Context, _ int64) (int64, error) { return 0, nil },
				}
			},
			setupHeader: func() *mockHeaderFetcher {
				return &mockHeaderFetcher{
					headerByNumberFn: func(_ context.Context, number *big.Int) (*ethtypes.Header, error) {
						return &ethtypes.Header{Time: uint64(1700000000 + number.Int64())}, nil
					},
				}
			},
			setupMC: func(t *testing.T) MulticallFactory {
				return func() (outbound.Multicaller, error) {
					return &testutil.MockMulticaller{
						ExecuteFn: func(_ context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
							bn := blockNumber.Int64()
							// Fail on blocks 101 and 103
							if bn == 101 || bn == 103 {
								return nil, fmt.Errorf("RPC error at block %d", bn)
							}
							prices := []*big.Int{
								new(big.Int).Mul(big.NewInt(bn), big.NewInt(100_000_000)),
								new(big.Int).Mul(big.NewInt(bn), big.NewInt(200_000_000)),
							}
							return multicallResult(t, calls, prices), nil
						},
					}, nil
				}
			},
			wantErr: false,
			checkResult: func(t *testing.T, repo *mockRepo) {
				t.Helper()
				upserted := repo.getUpserted()
				// 3 successful blocks (100, 102, 104) x 2 tokens = 6 prices
				if len(upserted) != 6 {
					t.Errorf("upserted count = %d, want 6", len(upserted))
				}
				for _, p := range upserted {
					if p.BlockNumber == 101 || p.BlockNumber == 103 {
						t.Errorf("found price from failed block %d", p.BlockNumber)
					}
				}
			},
		},
		{
			name:      "error HeaderByNumber fails for a block worker continues",
			fromBlock: 100,
			toBlock:   104,
			config: Config{
				Concurrency:  1,
				BatchSize:    100,
				OracleSource: "sparklend",
				Logger:       testutil.DiscardLogger(),
			},
			setupRepo: func() *mockRepo {
				return &mockRepo{
					getOracleSourceFn:  func(_ context.Context, _ string) (*entity.OracleSource, error) { return defaultOracleSource(), nil },
					getEnabledAssetsFn: func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) { return defaultAssets(), nil },
					getLatestBlockFn:   func(_ context.Context, _ int64) (int64, error) { return 0, nil },
				}
			},
			setupHeader: func() *mockHeaderFetcher {
				return &mockHeaderFetcher{
					headerByNumberFn: func(_ context.Context, number *big.Int) (*ethtypes.Header, error) {
						bn := number.Int64()
						if bn == 102 {
							return nil, errors.New("header not found")
						}
						return &ethtypes.Header{Time: uint64(1700000000 + bn)}, nil
					},
				}
			},
			setupMC: func(t *testing.T) MulticallFactory {
				return func() (outbound.Multicaller, error) {
					return &testutil.MockMulticaller{ExecuteFn: blockDependentPrices(t)}, nil
				}
			},
			wantErr: false,
			checkResult: func(t *testing.T, repo *mockRepo) {
				t.Helper()
				upserted := repo.getUpserted()
				// 4 successful blocks (100, 101, 103, 104) x 2 tokens = 8 prices
				if len(upserted) != 8 {
					t.Errorf("upserted count = %d, want 8", len(upserted))
				}
				for _, p := range upserted {
					if p.BlockNumber == 102 {
						t.Error("found price from failed block 102")
					}
				}
			},
		},
		{
			name:      "error price count mismatch from oracle",
			fromBlock: 100,
			toBlock:   102,
			config: Config{
				Concurrency:  1,
				BatchSize:    100,
				OracleSource: "sparklend",
				Logger:       testutil.DiscardLogger(),
			},
			setupRepo: func() *mockRepo {
				return &mockRepo{
					getOracleSourceFn:  func(_ context.Context, _ string) (*entity.OracleSource, error) { return defaultOracleSource(), nil },
					getEnabledAssetsFn: func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) { return defaultAssets(), nil },
					getLatestBlockFn:   func(_ context.Context, _ int64) (int64, error) { return 0, nil },
				}
			},
			setupHeader: func() *mockHeaderFetcher {
				return &mockHeaderFetcher{
					headerByNumberFn: func(_ context.Context, number *big.Int) (*ethtypes.Header, error) {
						return &ethtypes.Header{Time: uint64(1700000000 + number.Int64())}, nil
					},
				}
			},
			setupMC: func(t *testing.T) MulticallFactory {
				return func() (outbound.Multicaller, error) {
					return &testutil.MockMulticaller{
						ExecuteFn: func(_ context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
							bn := blockNumber.Int64()
							if bn == 101 {
								// Return only 1 price for 2 tokens → mismatch
								return multicallResult(t, calls, []*big.Int{big.NewInt(100_000_000)}), nil
							}
							prices := []*big.Int{
								new(big.Int).Mul(big.NewInt(bn), big.NewInt(100_000_000)),
								new(big.Int).Mul(big.NewInt(bn), big.NewInt(200_000_000)),
							}
							return multicallResult(t, calls, prices), nil
						},
					}, nil
				}
			},
			wantErr: false,
			checkResult: func(t *testing.T, repo *mockRepo) {
				t.Helper()
				upserted := repo.getUpserted()
				// Block 101 fails with price count mismatch, blocks 100 and 102 succeed
				// 2 blocks x 2 tokens = 4 prices
				if len(upserted) != 4 {
					t.Errorf("upserted count = %d, want 4", len(upserted))
				}
				for _, p := range upserted {
					if p.BlockNumber == 101 {
						t.Error("found price from failed block 101 (price count mismatch)")
					}
				}
			},
		},
		{
			name:      "error entity validation fails with blockNumber zero",
			fromBlock: 0,
			toBlock:   0,
			config: Config{
				Concurrency:  1,
				BatchSize:    100,
				OracleSource: "sparklend",
				Logger:       testutil.DiscardLogger(),
			},
			setupRepo: func() *mockRepo {
				return &mockRepo{
					getOracleSourceFn:  func(_ context.Context, _ string) (*entity.OracleSource, error) { return defaultOracleSource(), nil },
					getEnabledAssetsFn: func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) { return defaultAssets(), nil },
					getLatestBlockFn:   func(_ context.Context, _ int64) (int64, error) { return 0, nil },
				}
			},
			setupHeader: func() *mockHeaderFetcher {
				return &mockHeaderFetcher{
					headerByNumberFn: func(_ context.Context, _ *big.Int) (*ethtypes.Header, error) {
						return &ethtypes.Header{Time: uint64(1700000000)}, nil
					},
				}
			},
			setupMC: func(t *testing.T) MulticallFactory {
				return func() (outbound.Multicaller, error) {
					return &testutil.MockMulticaller{
						ExecuteFn: defaultMulticallExecute(t, []*big.Int{big.NewInt(100_000_000), big.NewInt(250_000_000_000)}, nil),
					}, nil
				}
			},
			wantErr: false,
			checkResult: func(t *testing.T, repo *mockRepo) {
				t.Helper()
				upserted := repo.getUpserted()
				// Block 0 causes entity validation to fail (blockNumber must be positive)
				// All entities fail, so nothing gets stored
				if len(upserted) != 0 {
					t.Errorf("upserted count = %d, want 0 (all entities should fail validation)", len(upserted))
				}
			},
		},
		{
			name:      "context cancellation during Run",
			fromBlock: 100,
			toBlock:   10099,
			config: Config{
				Concurrency:  1,
				BatchSize:    100,
				OracleSource: "sparklend",
				Logger:       testutil.DiscardLogger(),
			},
			setupRepo: func() *mockRepo {
				return &mockRepo{
					getOracleSourceFn:  func(_ context.Context, _ string) (*entity.OracleSource, error) { return defaultOracleSource(), nil },
					getEnabledAssetsFn: func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) { return defaultAssets(), nil },
					getLatestBlockFn:   func(_ context.Context, _ int64) (int64, error) { return 0, nil },
				}
			},
			setupHeader: func() *mockHeaderFetcher {
				return &mockHeaderFetcher{
					headerByNumberFn: func(_ context.Context, number *big.Int) (*ethtypes.Header, error) {
						return &ethtypes.Header{Time: uint64(1700000000 + number.Int64())}, nil
					},
				}
			},
			setupMC: func(t *testing.T) MulticallFactory {
				return func() (outbound.Multicaller, error) {
					return &testutil.MockMulticaller{ExecuteFn: blockDependentPrices(t)}, nil
				}
			},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := tt.setupRepo()
			header := tt.setupHeader()
			mcFactory := tt.setupMC(t)

			svc, err := NewService(tt.config, header, mcFactory, repo, defaultTokenAddresses())
			if err != nil {
				t.Fatalf("NewService: %v", err)
			}

			ctx := context.Background()

			// For context cancellation test, create a cancellable context
			if tt.name == "context cancellation during Run" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(context.Background(), 50*time.Millisecond)
				defer cancel()
			}

			err = svc.Run(ctx, tt.fromBlock, tt.toBlock)

			if tt.wantErr {
				if err == nil {
					t.Fatalf("expected error containing %q, got nil", tt.errContains)
				}
				if tt.errContains != "" && !strings.Contains(err.Error(), tt.errContains) {
					t.Errorf("error = %q, want it to contain %q", err.Error(), tt.errContains)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			if tt.checkResult != nil {
				tt.checkResult(t, repo)
			}
		})
	}
}

// TestRun_ChangeDetection_MultiplePriceChanges verifies that when prices change
// on specific blocks, only those blocks' prices are stored.
func TestRun_ChangeDetection_MultiplePriceChanges(t *testing.T) {
	repo := &mockRepo{
		getOracleSourceFn:  func(_ context.Context, _ string) (*entity.OracleSource, error) { return defaultOracleSource(), nil },
		getEnabledAssetsFn: func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) { return defaultAssets(), nil },
		getLatestBlockFn:   func(_ context.Context, _ int64) (int64, error) { return 0, nil },
	}

	header := &mockHeaderFetcher{
		headerByNumberFn: func(_ context.Context, number *big.Int) (*ethtypes.Header, error) {
			return &ethtypes.Header{Time: uint64(1700000000 + number.Int64())}, nil
		},
	}

	// Prices by block:
	// Block 100: token10=$100, token20=$2500  (new -> stored)
	// Block 101: token10=$100, token20=$2500  (same -> NOT stored)
	// Block 102: token10=$200, token20=$2500  (token10 changed -> stored)
	// Block 103: token10=$200, token20=$2500  (same -> NOT stored)
	// Block 104: token10=$200, token20=$3000  (token20 changed -> stored)
	pricesByBlock := map[int64][]*big.Int{
		100: {big.NewInt(100_00000000), big.NewInt(2500_00000000)},
		101: {big.NewInt(100_00000000), big.NewInt(2500_00000000)},
		102: {big.NewInt(200_00000000), big.NewInt(2500_00000000)},
		103: {big.NewInt(200_00000000), big.NewInt(2500_00000000)},
		104: {big.NewInt(200_00000000), big.NewInt(3000_00000000)},
	}

	mcFactory := func() (outbound.Multicaller, error) {
		return &testutil.MockMulticaller{
			ExecuteFn: func(_ context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
				bn := blockNumber.Int64()
				prices := pricesByBlock[bn]
				return multicallResult(t, calls, prices), nil
			},
		}, nil
	}

	svc, err := NewService(Config{
		Concurrency:  1,
		BatchSize:    100,
		OracleSource: "sparklend",
		Logger:       testutil.DiscardLogger(),
	}, header, mcFactory, repo, defaultTokenAddresses())
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	err = svc.Run(context.Background(), 100, 104)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	upserted := repo.getUpserted()

	// Expected stored prices:
	// Block 100: token10 + token20 = 2
	// Block 102: token10 = 1 (only token10 changed)
	// Block 104: token20 = 1 (only token20 changed)
	// Total: 4
	if len(upserted) != 4 {
		t.Fatalf("upserted count = %d, want 4", len(upserted))
	}

	type blockToken struct {
		block   int64
		tokenID int64
	}
	stored := make(map[blockToken]bool)
	for _, p := range upserted {
		stored[blockToken{p.BlockNumber, p.TokenID}] = true
	}

	expectedStored := []blockToken{
		{100, 10}, {100, 20},
		{102, 10},
		{104, 20},
	}
	for _, exp := range expectedStored {
		if !stored[exp] {
			t.Errorf("expected price stored for block=%d token=%d, but not found", exp.block, exp.tokenID)
		}
	}

	unexpectedBlocks := []blockToken{
		{101, 10}, {101, 20},
		{103, 10}, {103, 20},
		{102, 20},
		{104, 10},
	}
	for _, unexp := range unexpectedBlocks {
		if stored[unexp] {
			t.Errorf("unexpected price stored for block=%d token=%d", unexp.block, unexp.tokenID)
		}
	}
}

// TestRun_VerifiesUpsertedPriceFields verifies that the OnchainTokenPrice entities
// stored by Run have the correct field values.
func TestRun_VerifiesUpsertedPriceFields(t *testing.T) {
	repo := &mockRepo{
		getOracleSourceFn:  func(_ context.Context, _ string) (*entity.OracleSource, error) { return defaultOracleSource(), nil },
		getEnabledAssetsFn: func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) { return defaultAssets(), nil },
		getLatestBlockFn:   func(_ context.Context, _ int64) (int64, error) { return 0, nil },
	}

	blockTimestamp := uint64(1700000100)
	header := &mockHeaderFetcher{
		headerByNumberFn: func(_ context.Context, _ *big.Int) (*ethtypes.Header, error) {
			return &ethtypes.Header{Time: blockTimestamp}, nil
		},
	}

	rawPrices := []*big.Int{big.NewInt(100_000_000), big.NewInt(250_000_000_000)}

	mcFactory := func() (outbound.Multicaller, error) {
		return &testutil.MockMulticaller{
			ExecuteFn: defaultMulticallExecute(t, rawPrices, nil),
		}, nil
	}

	svc, err := NewService(Config{
		Concurrency:  1,
		BatchSize:    100,
		OracleSource: "sparklend",
		Logger:       testutil.DiscardLogger(),
	}, header, mcFactory, repo, defaultTokenAddresses())
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	err = svc.Run(context.Background(), 100, 100)
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	upserted := repo.getUpserted()
	if len(upserted) != 2 {
		t.Fatalf("upserted count = %d, want 2", len(upserted))
	}

	expectedTimestamp := time.Unix(int64(blockTimestamp), 0).UTC()

	for _, p := range upserted {
		if p.OracleSourceID != 1 {
			t.Errorf("OracleSourceID = %d, want 1", p.OracleSourceID)
		}
		if p.BlockNumber != 100 {
			t.Errorf("BlockNumber = %d, want 100", p.BlockNumber)
		}
		if p.BlockVersion != 0 {
			t.Errorf("BlockVersion = %d, want 0", p.BlockVersion)
		}
		if !p.Timestamp.Equal(expectedTimestamp) {
			t.Errorf("Timestamp = %v, want %v", p.Timestamp, expectedTimestamp)
		}
		if len(p.OracleAddress) != 20 {
			t.Errorf("OracleAddress length = %d, want 20", len(p.OracleAddress))
		}
		expectedOracleBytes := oracleAddr.Bytes()
		for i := range p.OracleAddress {
			if p.OracleAddress[i] != expectedOracleBytes[i] {
				t.Errorf("OracleAddress[%d] = %x, want %x", i, p.OracleAddress[i], expectedOracleBytes[i])
				break
			}
		}

		switch p.TokenID {
		case 10:
			if p.PriceUSD != 1.0 {
				t.Errorf("token 10 PriceUSD = %f, want 1.0", p.PriceUSD)
			}
		case 20:
			if p.PriceUSD != 2500.0 {
				t.Errorf("token 20 PriceUSD = %f, want 2500.0", p.PriceUSD)
			}
		default:
			t.Errorf("unexpected token ID: %d", p.TokenID)
		}
	}
}
