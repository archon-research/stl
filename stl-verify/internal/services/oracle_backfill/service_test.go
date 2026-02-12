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
	getOracleFn                    func(ctx context.Context, name string) (*entity.Oracle, error)
	getEnabledAssetsFn             func(ctx context.Context, oracleID int64) ([]*entity.OracleAsset, error)
	getLatestPricesFn              func(ctx context.Context, oracleID int64) (map[int64]float64, error)
	getLatestBlockFn               func(ctx context.Context, oracleID int64) (int64, error)
	getTokenAddressesFn            func(ctx context.Context, oracleID int64) (map[int64][]byte, error)
	upsertPricesFn                 func(ctx context.Context, prices []*entity.OnchainTokenPrice) error
	getAllEnabledOraclesFn         func(ctx context.Context) ([]*entity.Oracle, error)
	getOracleByAddressFn           func(ctx context.Context, chainID int, address []byte) (*entity.Oracle, error)
	insertOracleFn                 func(ctx context.Context, oracle *entity.Oracle) (*entity.Oracle, error)
	getAllActiveProtocolOraclesFn  func(ctx context.Context) ([]*entity.ProtocolOracle, error)
	insertProtocolOracleBindingFn  func(ctx context.Context, binding *entity.ProtocolOracle) (*entity.ProtocolOracle, error)
	copyOracleAssetsFn             func(ctx context.Context, fromOracleID, toOracleID int64) error
	getAllProtocolOracleBindingsFn func(ctx context.Context) ([]*entity.ProtocolOracle, error)

	// mu guards upserted for concurrent access from the batch writer goroutine.
	mu       sync.Mutex
	upserted []*entity.OnchainTokenPrice
}

func (m *mockRepo) GetOracle(ctx context.Context, name string) (*entity.Oracle, error) {
	if m.getOracleFn != nil {
		return m.getOracleFn(ctx, name)
	}
	return nil, errors.New("GetOracle not mocked")
}

func (m *mockRepo) GetEnabledAssets(ctx context.Context, oracleID int64) ([]*entity.OracleAsset, error) {
	if m.getEnabledAssetsFn != nil {
		return m.getEnabledAssetsFn(ctx, oracleID)
	}
	return nil, errors.New("GetEnabledAssets not mocked")
}

func (m *mockRepo) GetLatestPrices(ctx context.Context, oracleID int64) (map[int64]float64, error) {
	if m.getLatestPricesFn != nil {
		return m.getLatestPricesFn(ctx, oracleID)
	}
	return nil, errors.New("GetLatestPrices not mocked")
}

func (m *mockRepo) GetLatestBlock(ctx context.Context, oracleID int64) (int64, error) {
	if m.getLatestBlockFn != nil {
		return m.getLatestBlockFn(ctx, oracleID)
	}
	return 0, nil
}

func (m *mockRepo) GetTokenAddresses(ctx context.Context, oracleID int64) (map[int64][]byte, error) {
	if m.getTokenAddressesFn != nil {
		return m.getTokenAddressesFn(ctx, oracleID)
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

func (m *mockRepo) GetAllEnabledOracles(ctx context.Context) ([]*entity.Oracle, error) {
	if m.getAllEnabledOraclesFn != nil {
		return m.getAllEnabledOraclesFn(ctx)
	}
	return nil, errors.New("GetAllEnabledOracles not mocked")
}

func (m *mockRepo) GetOracleByAddress(ctx context.Context, chainID int, address []byte) (*entity.Oracle, error) {
	if m.getOracleByAddressFn != nil {
		return m.getOracleByAddressFn(ctx, chainID, address)
	}
	return nil, errors.New("GetOracleByAddress not mocked")
}

func (m *mockRepo) InsertOracle(ctx context.Context, oracle *entity.Oracle) (*entity.Oracle, error) {
	if m.insertOracleFn != nil {
		return m.insertOracleFn(ctx, oracle)
	}
	return nil, errors.New("InsertOracle not mocked")
}

func (m *mockRepo) GetAllActiveProtocolOracles(ctx context.Context) ([]*entity.ProtocolOracle, error) {
	if m.getAllActiveProtocolOraclesFn != nil {
		return m.getAllActiveProtocolOraclesFn(ctx)
	}
	return nil, errors.New("GetAllActiveProtocolOracles not mocked")
}

func (m *mockRepo) InsertProtocolOracleBinding(ctx context.Context, binding *entity.ProtocolOracle) (*entity.ProtocolOracle, error) {
	if m.insertProtocolOracleBindingFn != nil {
		return m.insertProtocolOracleBindingFn(ctx, binding)
	}
	return nil, errors.New("InsertProtocolOracleBinding not mocked")
}

func (m *mockRepo) CopyOracleAssets(ctx context.Context, fromOracleID, toOracleID int64) error {
	if m.copyOracleAssetsFn != nil {
		return m.copyOracleAssetsFn(ctx, fromOracleID, toOracleID)
	}
	return errors.New("CopyOracleAssets not mocked")
}

func (m *mockRepo) GetAllProtocolOracleBindings(ctx context.Context) ([]*entity.ProtocolOracle, error) {
	if m.getAllProtocolOracleBindingsFn != nil {
		return m.getAllProtocolOracleBindingsFn(ctx)
	}
	return nil, nil // default: no bindings
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

// abiPackPrice packs a single *big.Int as the return data for getAssetPrice.
func abiPackPrice(t *testing.T, price *big.Int) []byte {
	return testutil.PackAssetPrice(t, price)
}

// defaultOracle returns a standard Oracle for testing.
func defaultOracle() *entity.Oracle {
	return &entity.Oracle{
		ID:              1,
		Name:            "sparklend",
		DisplayName:     "Spark: aave Oracle",
		ChainID:         1,
		Address:         common.HexToAddress("0x0000000000000000000000000000000000000BBB"),
		DeploymentBlock: 100,
		Enabled:         true,
	}
}

// defaultAssets returns a standard set of OracleAssets for testing (two tokens).
func defaultAssets() []*entity.OracleAsset {
	return []*entity.OracleAsset{
		{ID: 1, OracleID: 1, TokenID: 10, Enabled: true},
		{ID: 2, OracleID: 1, TokenID: 20, Enabled: true},
	}
}

// defaultTokenAddressBytes returns a token ID -> address bytes map matching defaultAssets.
func defaultTokenAddressBytes() map[int64][]byte {
	return map[int64][]byte{
		10: common.HexToAddress("0x0000000000000000000000000000000000000010").Bytes(),
		20: common.HexToAddress("0x0000000000000000000000000000000000000020").Bytes(),
	}
}

// multicallResult returns the multicall results for a given set of prices.
// FetchOraclePricesIndividual uses N individual getAssetPrice calls.
func multicallResult(t *testing.T, prices []*big.Int) []outbound.Result {
	t.Helper()
	results := make([]outbound.Result, len(prices))
	for i, price := range prices {
		results[i] = outbound.Result{Success: true, ReturnData: abiPackPrice(t, price)}
	}
	return results
}

// defaultMulticallExecute returns a mock Execute function that returns
// valid prices for the given raw prices per block.
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
		return multicallResult(t, prices), nil
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
		return multicallResult(t, prices), nil
	}
}

// defaultRepoSetup returns a mockRepo preconfigured for common test scenarios.
// It mocks GetAllEnabledOracles, GetEnabledAssets, GetTokenAddresses, and GetLatestBlock.
func defaultRepoSetup() *mockRepo {
	return &mockRepo{
		getAllEnabledOraclesFn: func(_ context.Context) ([]*entity.Oracle, error) {
			return []*entity.Oracle{defaultOracle()}, nil
		},
		getEnabledAssetsFn: func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) {
			return defaultAssets(), nil
		},
		getTokenAddressesFn: func(_ context.Context, _ int64) (map[int64][]byte, error) {
			return defaultTokenAddressBytes(), nil
		},
		getLatestBlockFn: func(_ context.Context, _ int64) (int64, error) { return 0, nil },
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

	tests := []struct {
		name           string
		config         Config
		headerFetcher  BlockHeaderFetcher
		newMulticaller MulticallFactory
		repo           outbound.OnchainPriceRepository
		wantErr        bool
		errContains    string
		// checks run on the returned service when wantErr is false
		checkService func(t *testing.T, svc *Service)
	}{
		{
			name: "success with all valid params",
			config: Config{
				Concurrency: 2,
				BatchSize:   50,
				Logger:      testutil.DiscardLogger(),
			},
			headerFetcher:  validFetcher,
			newMulticaller: validFactory,
			repo:           validRepo,
			wantErr:        false,
			checkService: func(t *testing.T, svc *Service) {
				t.Helper()
				if svc.config.Concurrency != 2 {
					t.Errorf("Concurrency = %d, want 2", svc.config.Concurrency)
				}
				if svc.config.BatchSize != 50 {
					t.Errorf("BatchSize = %d, want 50", svc.config.BatchSize)
				}
				if svc.oracleABI == nil {
					t.Error("oracleABI is nil")
				}
			},
		},
		{
			name:           "success with default config values",
			config:         Config{}, // all zero values
			headerFetcher:  validFetcher,
			newMulticaller: validFactory,
			repo:           validRepo,
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
			wantErr:        true,
			errContains:    "headerFetcher cannot be nil",
		},
		{
			name:           "error nil newMulticaller",
			config:         Config{Logger: testutil.DiscardLogger()},
			headerFetcher:  validFetcher,
			newMulticaller: nil,
			repo:           validRepo,
			wantErr:        true,
			errContains:    "newMulticaller cannot be nil",
		},
		{
			name:           "error nil repo",
			config:         Config{Logger: testutil.DiscardLogger()},
			headerFetcher:  validFetcher,
			newMulticaller: validFactory,
			repo:           nil,
			wantErr:        true,
			errContains:    "repo cannot be nil",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			svc, err := NewService(tt.config, tt.headerFetcher, tt.newMulticaller, tt.repo)

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
				Concurrency: 1,
				BatchSize:   100,
				Logger:      testutil.DiscardLogger(),
			},
			setupRepo: func() *mockRepo {
				return defaultRepoSetup()
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
				Concurrency: 1,
				BatchSize:   100,
				Logger:      testutil.DiscardLogger(),
			},
			setupRepo: func() *mockRepo {
				repo := defaultRepoSetup()
				repo.getLatestBlockFn = func(_ context.Context, _ int64) (int64, error) {
					return 106, nil
				}
				return repo
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
							return multicallResult(t, prices), nil
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
			name:      "success latestBlock beyond requested range still processes all blocks",
			fromBlock: 100,
			toBlock:   104,
			config: Config{
				Concurrency: 1,
				BatchSize:   100,
				Logger:      testutil.DiscardLogger(),
			},
			setupRepo: func() *mockRepo {
				repo := defaultRepoSetup()
				repo.getLatestBlockFn = func(_ context.Context, _ int64) (int64, error) {
					return 110, nil
				}
				return repo
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
			name:      "success more workers than blocks adjusts worker count",
			fromBlock: 100,
			toBlock:   101,
			config: Config{
				Concurrency: 10,
				BatchSize:   100,
				Logger:      testutil.DiscardLogger(),
			},
			setupRepo: func() *mockRepo {
				return defaultRepoSetup()
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
				Concurrency: 1,
				BatchSize:   100,
				Logger:      testutil.DiscardLogger(),
			},
			setupRepo: func() *mockRepo {
				return defaultRepoSetup()
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
			name:      "success no oracles with enabled assets returns nil",
			fromBlock: 100,
			toBlock:   104,
			config: Config{
				Concurrency: 1,
				BatchSize:   100,
				Logger:      testutil.DiscardLogger(),
			},
			setupRepo: func() *mockRepo {
				return &mockRepo{
					getAllEnabledOraclesFn: func(_ context.Context) ([]*entity.Oracle, error) {
						return []*entity.Oracle{defaultOracle()}, nil
					},
					getEnabledAssetsFn: func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) {
						return []*entity.OracleAsset{}, nil
					},
					getTokenAddressesFn: func(_ context.Context, _ int64) (map[int64][]byte, error) {
						return defaultTokenAddressBytes(), nil
					},
				}
			},
			setupHeader: func() *mockHeaderFetcher { return &mockHeaderFetcher{} },
			setupMC: func(t *testing.T) MulticallFactory {
				return func() (outbound.Multicaller, error) { return &testutil.MockMulticaller{}, nil }
			},
			wantErr: false,
			checkResult: func(t *testing.T, repo *mockRepo) {
				t.Helper()
				if len(repo.getUpserted()) != 0 {
					t.Error("expected no upserted prices when no enabled assets")
				}
			},
		},
		{
			name:      "error GetAllEnabledOracles fails",
			fromBlock: 100,
			toBlock:   104,
			config: Config{
				Concurrency: 1,
				BatchSize:   100,
				Logger:      testutil.DiscardLogger(),
			},
			setupRepo: func() *mockRepo {
				return &mockRepo{
					getAllEnabledOraclesFn: func(_ context.Context) ([]*entity.Oracle, error) {
						return nil, errors.New("database connection failed")
					},
				}
			},
			setupHeader: func() *mockHeaderFetcher { return &mockHeaderFetcher{} },
			setupMC: func(t *testing.T) MulticallFactory {
				return func() (outbound.Multicaller, error) { return &testutil.MockMulticaller{}, nil }
			},
			wantErr:     true,
			errContains: "getting enabled oracles",
		},
		{
			name:      "error GetEnabledAssets fails is warned and skipped",
			fromBlock: 100,
			toBlock:   104,
			config: Config{
				Concurrency: 1,
				BatchSize:   100,
				Logger:      testutil.DiscardLogger(),
			},
			setupRepo: func() *mockRepo {
				return &mockRepo{
					getAllEnabledOraclesFn: func(_ context.Context) ([]*entity.Oracle, error) {
						return []*entity.Oracle{defaultOracle()}, nil
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
			// With the new multi-oracle flow, buildWorkUnit errors are warned + skipped
			wantErr: false,
			checkResult: func(t *testing.T, repo *mockRepo) {
				t.Helper()
				if len(repo.getUpserted()) != 0 {
					t.Error("expected no upserted prices when GetEnabledAssets fails")
				}
			},
		},
		{
			name:      "error token address not found for token_id is warned and skipped",
			fromBlock: 100,
			toBlock:   104,
			config: Config{
				Concurrency: 1,
				BatchSize:   100,
				Logger:      testutil.DiscardLogger(),
			},
			setupRepo: func() *mockRepo {
				return &mockRepo{
					getAllEnabledOraclesFn: func(_ context.Context) ([]*entity.Oracle, error) {
						return []*entity.Oracle{defaultOracle()}, nil
					},
					getEnabledAssetsFn: func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) {
						return []*entity.OracleAsset{
							{ID: 1, OracleID: 1, TokenID: 999, Enabled: true},
						}, nil
					},
					getTokenAddressesFn: func(_ context.Context, _ int64) (map[int64][]byte, error) {
						return map[int64][]byte{}, nil // no addresses
					},
				}
			},
			setupHeader: func() *mockHeaderFetcher { return &mockHeaderFetcher{} },
			setupMC: func(t *testing.T) MulticallFactory {
				return func() (outbound.Multicaller, error) { return &testutil.MockMulticaller{}, nil }
			},
			// buildWorkUnit errors are warned + skipped
			wantErr: false,
			checkResult: func(t *testing.T, repo *mockRepo) {
				t.Helper()
				if len(repo.getUpserted()) != 0 {
					t.Error("expected no upserted prices when token address not found")
				}
			},
		},
		{
			name:      "error GetLatestBlock fails",
			fromBlock: 100,
			toBlock:   104,
			config: Config{
				Concurrency: 1,
				BatchSize:   100,
				Logger:      testutil.DiscardLogger(),
			},
			setupRepo: func() *mockRepo {
				repo := defaultRepoSetup()
				repo.getLatestBlockFn = func(_ context.Context, _ int64) (int64, error) {
					return 0, errors.New("redis unavailable")
				}
				return repo
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
				Concurrency: 1,
				BatchSize:   3, // small batch size forces flush during for loop
				Logger:      testutil.DiscardLogger(),
			},
			setupRepo: func() *mockRepo {
				return defaultRepoSetup()
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
				Concurrency: 1,
				BatchSize:   3, // small batch triggers mid-loop flush
				Logger:      testutil.DiscardLogger(),
			},
			setupRepo: func() *mockRepo {
				repo := defaultRepoSetup()
				repo.upsertPricesFn = func(_ context.Context, _ []*entity.OnchainTokenPrice) error {
					return errors.New("disk full")
				}
				return repo
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
				Concurrency: 1,
				BatchSize:   100,
				Logger:      testutil.DiscardLogger(),
			},
			setupRepo: func() *mockRepo {
				repo := defaultRepoSetup()
				repo.upsertPricesFn = func(_ context.Context, _ []*entity.OnchainTokenPrice) error {
					return errors.New("disk full")
				}
				return repo
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
				Concurrency: 1,
				BatchSize:   100,
				Logger:      testutil.DiscardLogger(),
			},
			setupRepo: func() *mockRepo {
				return defaultRepoSetup()
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
			name:      "error multicall Execute fails for a block worker continues",
			fromBlock: 100,
			toBlock:   104,
			config: Config{
				Concurrency: 1,
				BatchSize:   100,
				Logger:      testutil.DiscardLogger(),
			},
			setupRepo: func() *mockRepo {
				return defaultRepoSetup()
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
							return multicallResult(t, prices), nil
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
				Concurrency: 1,
				BatchSize:   100,
				Logger:      testutil.DiscardLogger(),
			},
			setupRepo: func() *mockRepo {
				return defaultRepoSetup()
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
			name:      "success partial token failure skips failed tokens",
			fromBlock: 100,
			toBlock:   102,
			config: Config{
				Concurrency: 1,
				BatchSize:   100,
				Logger:      testutil.DiscardLogger(),
			},
			setupRepo: func() *mockRepo {
				return defaultRepoSetup()
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
							// Token 20 (index 1) fails at block 101
							if bn == 101 {
								return []outbound.Result{
									{Success: true, ReturnData: abiPackPrice(t, big.NewInt(bn*100_000_000))},
									{Success: false, ReturnData: nil}, // token 20 has no price source
								}, nil
							}
							prices := []*big.Int{
								new(big.Int).Mul(big.NewInt(bn), big.NewInt(100_000_000)),
								new(big.Int).Mul(big.NewInt(bn), big.NewInt(200_000_000)),
							}
							return multicallResult(t, prices), nil
						},
					}, nil
				}
			},
			wantErr: false,
			checkResult: func(t *testing.T, repo *mockRepo) {
				t.Helper()
				upserted := repo.getUpserted()
				// Block 100: 2 tokens, Block 101: 1 token (token 20 failed), Block 102: 2 tokens
				// = 5 prices total (all different due to block-dependent prices)
				if len(upserted) != 5 {
					t.Errorf("upserted count = %d, want 5", len(upserted))
				}
				// Verify no token 20 price at block 101
				for _, p := range upserted {
					if p.BlockNumber == 101 && p.TokenID == 20 {
						t.Error("found price for token 20 at block 101 (should have failed)")
					}
				}
			},
		},
		{
			name:      "success all tokens fail at a block counts as processed",
			fromBlock: 100,
			toBlock:   102,
			config: Config{
				Concurrency: 1,
				BatchSize:   100,
				Logger:      testutil.DiscardLogger(),
			},
			setupRepo: func() *mockRepo {
				return defaultRepoSetup()
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
							// All tokens fail at block 101
							if bn == 101 {
								return []outbound.Result{
									{Success: false, ReturnData: nil},
									{Success: false, ReturnData: nil},
								}, nil
							}
							prices := []*big.Int{
								new(big.Int).Mul(big.NewInt(bn), big.NewInt(100_000_000)),
								new(big.Int).Mul(big.NewInt(bn), big.NewInt(200_000_000)),
							}
							return multicallResult(t, prices), nil
						},
					}, nil
				}
			},
			wantErr: false,
			checkResult: func(t *testing.T, repo *mockRepo) {
				t.Helper()
				upserted := repo.getUpserted()
				// Block 100: 2 tokens, Block 101: 0 tokens (all failed), Block 102: 2 tokens
				// = 4 prices total
				if len(upserted) != 4 {
					t.Errorf("upserted count = %d, want 4", len(upserted))
				}
				for _, p := range upserted {
					if p.BlockNumber == 101 {
						t.Errorf("found price at block 101 (all tokens should have failed)")
					}
				}
			},
		},
		{
			name:      "error entity validation fails with blockNumber zero",
			fromBlock: 0,
			toBlock:   0,
			config: Config{
				Concurrency: 1,
				BatchSize:   100,
				Logger:      testutil.DiscardLogger(),
			},
			setupRepo: func() *mockRepo {
				return defaultRepoSetup()
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
				Concurrency: 1,
				BatchSize:   100,
				Logger:      testutil.DiscardLogger(),
			},
			setupRepo: func() *mockRepo {
				return defaultRepoSetup()
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
		{
			name:      "success multiple oracles deduplicates by oracle_id",
			fromBlock: 100,
			toBlock:   101,
			config: Config{
				Concurrency: 1,
				BatchSize:   100,
				Logger:      testutil.DiscardLogger(),
			},
			setupRepo: func() *mockRepo {
				oracle := defaultOracle()
				return &mockRepo{
					getAllEnabledOraclesFn: func(_ context.Context) ([]*entity.Oracle, error) {
						// Return the same oracle twice (simulating generic + protocol-bound)
						return []*entity.Oracle{oracle, oracle}, nil
					},
					getEnabledAssetsFn: func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) {
						return defaultAssets(), nil
					},
					getTokenAddressesFn: func(_ context.Context, _ int64) (map[int64][]byte, error) {
						return defaultTokenAddressBytes(), nil
					},
					getLatestBlockFn: func(_ context.Context, _ int64) (int64, error) { return 0, nil },
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
				// Only 1 oracle (deduplicated), 2 blocks x 2 tokens = 4 prices
				if len(upserted) != 4 {
					t.Errorf("upserted count = %d, want 4 (deduplicated oracle)", len(upserted))
				}
			},
		},
		{
			name:      "success with two distinct oracles",
			fromBlock: 100,
			toBlock:   100,
			config: Config{
				Concurrency: 1,
				BatchSize:   100,
				Logger:      testutil.DiscardLogger(),
			},
			setupRepo: func() *mockRepo {
				oracle1 := defaultOracle()
				oracle2 := &entity.Oracle{
					ID:              2,
					Name:            "other-oracle",
					DisplayName:     "Other Oracle",
					ChainID:         1,
					Address:         common.HexToAddress("0x0000000000000000000000000000000000000CCC"),
					DeploymentBlock: 50,
					Enabled:         true,
				}
				return &mockRepo{
					getAllEnabledOraclesFn: func(_ context.Context) ([]*entity.Oracle, error) {
						return []*entity.Oracle{oracle1, oracle2}, nil
					},
					getEnabledAssetsFn: func(_ context.Context, oracleID int64) ([]*entity.OracleAsset, error) {
						if oracleID == 1 {
							return defaultAssets(), nil
						}
						return []*entity.OracleAsset{
							{ID: 3, OracleID: 2, TokenID: 30, Enabled: true},
						}, nil
					},
					getTokenAddressesFn: func(_ context.Context, oracleID int64) (map[int64][]byte, error) {
						if oracleID == 1 {
							return defaultTokenAddressBytes(), nil
						}
						return map[int64][]byte{
							30: common.HexToAddress("0x0000000000000000000000000000000000000030").Bytes(),
						}, nil
					},
					getLatestBlockFn: func(_ context.Context, _ int64) (int64, error) { return 0, nil },
				}
			},
			setupHeader: func() *mockHeaderFetcher {
				return &mockHeaderFetcher{
					headerByNumberFn: func(_ context.Context, _ *big.Int) (*ethtypes.Header, error) {
						return &ethtypes.Header{Time: uint64(1700000100)}, nil
					},
				}
			},
			setupMC: func(t *testing.T) MulticallFactory {
				return func() (outbound.Multicaller, error) {
					return &testutil.MockMulticaller{
						ExecuteFn: func(_ context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
							// Individual calls: oracle1 has 2 tokens (2 calls), oracle2 has 1 token (1 call)
							target := calls[0].Target
							if target == common.HexToAddress("0x0000000000000000000000000000000000000BBB") {
								return multicallResult(t, []*big.Int{big.NewInt(100_000_000), big.NewInt(200_000_000)}), nil
							}
							return multicallResult(t, []*big.Int{big.NewInt(300_000_000)}), nil
						},
					}, nil
				}
			},
			wantErr: false,
			checkResult: func(t *testing.T, repo *mockRepo) {
				t.Helper()
				upserted := repo.getUpserted()
				// oracle1: 2 tokens, oracle2: 1 token, 1 block = 3 prices total
				if len(upserted) != 3 {
					t.Errorf("upserted count = %d, want 3 (2 oracles)", len(upserted))
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := tt.setupRepo()
			header := tt.setupHeader()
			mcFactory := tt.setupMC(t)

			svc, err := NewService(tt.config, header, mcFactory, repo)
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

	repo := defaultRepoSetup()

	header := &mockHeaderFetcher{
		headerByNumberFn: func(_ context.Context, number *big.Int) (*ethtypes.Header, error) {
			return &ethtypes.Header{Time: uint64(1700000000 + number.Int64())}, nil
		},
	}

	mcFactory := func() (outbound.Multicaller, error) {
		return &testutil.MockMulticaller{
			ExecuteFn: func(_ context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
				bn := blockNumber.Int64()
				prices := pricesByBlock[bn]
				return multicallResult(t, prices), nil
			},
		}, nil
	}

	svc, err := NewService(Config{
		Concurrency: 1,
		BatchSize:   100,
		Logger:      testutil.DiscardLogger(),
	}, header, mcFactory, repo)
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
	repo := defaultRepoSetup()

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
		Concurrency: 1,
		BatchSize:   100,
		Logger:      testutil.DiscardLogger(),
	}, header, mcFactory, repo)
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
		if p.OracleID != 1 {
			t.Errorf("OracleID = %d, want 1", p.OracleID)
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

// ---------------------------------------------------------------------------
// TestRun_DuplicateBlocksSafeWithIdempotentUpsert
// ---------------------------------------------------------------------------

// TestRun_DuplicateBlocksSafeWithIdempotentUpsert verifies that re-processing
// the same block range succeeds without error. When GetLatestBlock returns a value
// equal to toBlock (not strictly less), the resume logic does not skip ahead, so all
// blocks are re-sent to UpsertPrices. The service relies on the repository's
// ON CONFLICT DO NOTHING to deduplicate — this test proves that the service
// correctly sends duplicate data without erroring.
func TestRun_DuplicateBlocksSafeWithIdempotentUpsert(t *testing.T) {
	repo := defaultRepoSetup()

	header := &mockHeaderFetcher{
		headerByNumberFn: func(_ context.Context, number *big.Int) (*ethtypes.Header, error) {
			return &ethtypes.Header{Time: uint64(1700000000 + number.Int64())}, nil
		},
	}

	mcFactory := func() (outbound.Multicaller, error) {
		return &testutil.MockMulticaller{ExecuteFn: blockDependentPrices(t)}, nil
	}

	svc, err := NewService(Config{
		Concurrency: 1,
		BatchSize:   100,
		Logger:      testutil.DiscardLogger(),
	}, header, mcFactory, repo)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	// First run: blocks 100-104
	err = svc.Run(context.Background(), 100, 104)
	if err != nil {
		t.Fatalf("Run (first): %v", err)
	}

	countAfterFirst := len(repo.getUpserted())
	if countAfterFirst != 10 { // 5 blocks x 2 tokens
		t.Fatalf("expected 10 prices after first run, got %d", countAfterFirst)
	}

	// Simulate GetLatestBlock returning toBlock (104). The resume condition
	// requires latestBlock < toBlock (104 < 104 = false), so no blocks are
	// skipped — all 5 blocks are re-processed and sent to UpsertPrices again.
	repo.getLatestBlockFn = func(_ context.Context, _ int64) (int64, error) {
		return 104, nil
	}

	// Second run: same block range
	err = svc.Run(context.Background(), 100, 104)
	if err != nil {
		t.Fatalf("Run (second): %v", err)
	}

	// The mock repo accumulated 20 total entries (10 from each run).
	// In production, ON CONFLICT DO NOTHING deduplicates these.
	countAfterSecond := len(repo.getUpserted())
	if countAfterSecond != 20 {
		t.Errorf("expected 20 total upserted prices (10 per run), got %d", countAfterSecond)
	}
}

// ---------------------------------------------------------------------------
// TestComputeOracleBlockRanges
// ---------------------------------------------------------------------------

func TestComputeOracleBlockRanges(t *testing.T) {
	tests := []struct {
		name     string
		bindings []*entity.ProtocolOracle
		want     map[int64]*oracleBlockRange
	}{
		{
			name:     "empty bindings",
			bindings: nil,
			want:     map[int64]*oracleBlockRange{},
		},
		{
			name: "single oracle active in one protocol",
			bindings: []*entity.ProtocolOracle{
				{ProtocolID: 1, OracleID: 10, FromBlock: 1000},
			},
			want: map[int64]*oracleBlockRange{
				10: {validFrom: 1000, validTo: 0},
			},
		},
		{
			name: "oracle superseded by another in same protocol",
			bindings: []*entity.ProtocolOracle{
				{ProtocolID: 1, OracleID: 10, FromBlock: 1000},
				{ProtocolID: 1, OracleID: 20, FromBlock: 2000},
			},
			want: map[int64]*oracleBlockRange{
				10: {validFrom: 1000, validTo: 1999},
				20: {validFrom: 2000, validTo: 0},
			},
		},
		{
			name: "oracle active in multiple protocols",
			bindings: []*entity.ProtocolOracle{
				{ProtocolID: 1, OracleID: 10, FromBlock: 1000},
				{ProtocolID: 2, OracleID: 10, FromBlock: 500},
			},
			want: map[int64]*oracleBlockRange{
				10: {validFrom: 500, validTo: 0},
			},
		},
		{
			name: "oracle superseded in one protocol but active in another",
			bindings: []*entity.ProtocolOracle{
				// Protocol 1: oracle 10 superseded by oracle 20 at block 2000
				{ProtocolID: 1, OracleID: 10, FromBlock: 1000},
				{ProtocolID: 1, OracleID: 20, FromBlock: 2000},
				// Protocol 2: oracle 10 still active
				{ProtocolID: 2, OracleID: 10, FromBlock: 1500},
			},
			want: map[int64]*oracleBlockRange{
				10: {validFrom: 1000, validTo: 0}, // still active in protocol 2
				20: {validFrom: 2000, validTo: 0},
			},
		},
		{
			name: "oracle superseded in all protocols",
			bindings: []*entity.ProtocolOracle{
				// Protocol 1: oracle 10 → oracle 20 at block 2000
				{ProtocolID: 1, OracleID: 10, FromBlock: 1000},
				{ProtocolID: 1, OracleID: 20, FromBlock: 2000},
				// Protocol 2: oracle 10 → oracle 30 at block 3000
				{ProtocolID: 2, OracleID: 10, FromBlock: 500},
				{ProtocolID: 2, OracleID: 30, FromBlock: 3000},
			},
			want: map[int64]*oracleBlockRange{
				10: {validFrom: 500, validTo: 2999}, // max superseded block
				20: {validFrom: 2000, validTo: 0},
				30: {validFrom: 3000, validTo: 0},
			},
		},
		{
			name: "oracle re-used after supersession",
			bindings: []*entity.ProtocolOracle{
				{ProtocolID: 1, OracleID: 10, FromBlock: 1000},
				{ProtocolID: 1, OracleID: 20, FromBlock: 2000},
				{ProtocolID: 1, OracleID: 10, FromBlock: 3000}, // oracle 10 re-used
			},
			want: map[int64]*oracleBlockRange{
				10: {validFrom: 1000, validTo: 0}, // active again
				20: {validFrom: 2000, validTo: 2999},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := computeOracleBlockRanges(tt.bindings)
			if len(got) != len(tt.want) {
				t.Fatalf("got %d ranges, want %d", len(got), len(tt.want))
			}
			for oracleID, wantRange := range tt.want {
				gotRange, ok := got[oracleID]
				if !ok {
					t.Errorf("missing range for oracle %d", oracleID)
					continue
				}
				if gotRange.validFrom != wantRange.validFrom {
					t.Errorf("oracle %d: validFrom = %d, want %d", oracleID, gotRange.validFrom, wantRange.validFrom)
				}
				if gotRange.validTo != wantRange.validTo {
					t.Errorf("oracle %d: validTo = %d, want %d", oracleID, gotRange.validTo, wantRange.validTo)
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TestClampBlockRange
// ---------------------------------------------------------------------------

func TestClampBlockRange(t *testing.T) {
	tests := []struct {
		name      string
		from, to  int64
		validFrom int64
		validTo   int64
		wantFrom  int64
		wantTo    int64
		wantOK    bool
	}{
		{
			name: "no clamping needed",
			from: 100, to: 200,
			validFrom: 50, validTo: 300,
			wantFrom: 100, wantTo: 200, wantOK: true,
		},
		{
			name: "clamp from",
			from: 100, to: 200,
			validFrom: 150, validTo: 0,
			wantFrom: 150, wantTo: 200, wantOK: true,
		},
		{
			name: "clamp to",
			from: 100, to: 200,
			validFrom: 0, validTo: 150,
			wantFrom: 100, wantTo: 150, wantOK: true,
		},
		{
			name: "clamp both",
			from: 100, to: 200,
			validFrom: 120, validTo: 180,
			wantFrom: 120, wantTo: 180, wantOK: true,
		},
		{
			name: "from > to after clamping",
			from: 100, to: 200,
			validFrom: 300, validTo: 0,
			wantFrom: 300, wantTo: 200, wantOK: false,
		},
		{
			name: "no lower bound (validFrom=0)",
			from: 100, to: 200,
			validFrom: 0, validTo: 0,
			wantFrom: 100, wantTo: 200, wantOK: true,
		},
		{
			name: "no upper bound (validTo=0)",
			from: 100, to: 200,
			validFrom: 50, validTo: 0,
			wantFrom: 100, wantTo: 200, wantOK: true,
		},
		{
			name: "exact boundaries",
			from: 100, to: 200,
			validFrom: 100, validTo: 200,
			wantFrom: 100, wantTo: 200, wantOK: true,
		},
		{
			name: "entire range before deployment",
			from: 10, to: 50,
			validFrom: 100, validTo: 0,
			wantFrom: 100, wantTo: 50, wantOK: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotFrom, gotTo, gotOK := clampBlockRange(tt.from, tt.to, tt.validFrom, tt.validTo)
			if gotFrom != tt.wantFrom {
				t.Errorf("from = %d, want %d", gotFrom, tt.wantFrom)
			}
			if gotTo != tt.wantTo {
				t.Errorf("to = %d, want %d", gotTo, tt.wantTo)
			}
			if gotOK != tt.wantOK {
				t.Errorf("ok = %v, want %v", gotOK, tt.wantOK)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TestRun_BlockRangeClamping
// ---------------------------------------------------------------------------

func TestRun_BlockRangeClamping(t *testing.T) {
	tests := []struct {
		name      string
		fromBlock int64
		toBlock   int64
		oracle    *entity.Oracle
		bindings  []*entity.ProtocolOracle
		wantErr   bool
		// checkResult is called after Run completes successfully.
		checkResult func(t *testing.T, repo *mockRepo)
	}{
		{
			name:      "clamps to deployment block",
			fromBlock: 50,
			toBlock:   104,
			oracle: &entity.Oracle{
				ID:              1,
				Name:            "sparklend",
				DisplayName:     "Spark: aave Oracle",
				ChainID:         1,
				Address:         common.HexToAddress("0x0000000000000000000000000000000000000BBB"),
				DeploymentBlock: 100,
				Enabled:         true,
			},
			bindings: nil,
			checkResult: func(t *testing.T, repo *mockRepo) {
				t.Helper()
				upserted := repo.getUpserted()
				// Blocks 100-104 = 5 blocks x 2 tokens = 10 prices
				if len(upserted) != 10 {
					t.Errorf("upserted count = %d, want 10", len(upserted))
				}
				for _, p := range upserted {
					if p.BlockNumber < 100 {
						t.Errorf("found price for block %d which is before deployment block 100", p.BlockNumber)
					}
				}
			},
		},
		{
			name:      "entire range before deployment skips oracle",
			fromBlock: 10,
			toBlock:   50,
			oracle: &entity.Oracle{
				ID:              1,
				Name:            "sparklend",
				DisplayName:     "Spark: aave Oracle",
				ChainID:         1,
				Address:         common.HexToAddress("0x0000000000000000000000000000000000000BBB"),
				DeploymentBlock: 100,
				Enabled:         true,
			},
			bindings: nil,
			checkResult: func(t *testing.T, repo *mockRepo) {
				t.Helper()
				if len(repo.getUpserted()) != 0 {
					t.Errorf("expected no upserted prices, got %d", len(repo.getUpserted()))
				}
			},
		},
		{
			name:      "clamps to supersession block",
			fromBlock: 100,
			toBlock:   300,
			oracle: &entity.Oracle{
				ID:              1,
				Name:            "old-oracle",
				DisplayName:     "Old Oracle",
				ChainID:         1,
				Address:         common.HexToAddress("0x0000000000000000000000000000000000000BBB"),
				DeploymentBlock: 50,
				Enabled:         true,
			},
			bindings: []*entity.ProtocolOracle{
				{ProtocolID: 1, OracleID: 1, FromBlock: 80},
				{ProtocolID: 1, OracleID: 2, FromBlock: 201}, // oracle 1 superseded at 200
			},
			checkResult: func(t *testing.T, repo *mockRepo) {
				t.Helper()
				upserted := repo.getUpserted()
				// Blocks 100-200 = 101 blocks x 2 tokens = 202 prices
				if len(upserted) != 202 {
					t.Errorf("upserted count = %d, want 202", len(upserted))
				}
				for _, p := range upserted {
					if p.BlockNumber > 200 {
						t.Errorf("found price for block %d which is after supersession block 200", p.BlockNumber)
					}
				}
			},
		},
		{
			name:      "deployment block 0 means no lower clamping",
			fromBlock: 1,
			toBlock:   5,
			oracle: &entity.Oracle{
				ID:              1,
				Name:            "sparklend",
				DisplayName:     "Spark: aave Oracle",
				ChainID:         1,
				Address:         common.HexToAddress("0x0000000000000000000000000000000000000BBB"),
				DeploymentBlock: 0,
				Enabled:         true,
			},
			bindings: nil,
			checkResult: func(t *testing.T, repo *mockRepo) {
				t.Helper()
				upserted := repo.getUpserted()
				// 5 blocks x 2 tokens = 10 prices
				if len(upserted) != 10 {
					t.Errorf("upserted count = %d, want 10", len(upserted))
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			repo := &mockRepo{
				getAllEnabledOraclesFn: func(_ context.Context) ([]*entity.Oracle, error) {
					return []*entity.Oracle{tt.oracle}, nil
				},
				getEnabledAssetsFn: func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) {
					return defaultAssets(), nil
				},
				getTokenAddressesFn: func(_ context.Context, _ int64) (map[int64][]byte, error) {
					return defaultTokenAddressBytes(), nil
				},
				getLatestBlockFn: func(_ context.Context, _ int64) (int64, error) { return 0, nil },
				getAllProtocolOracleBindingsFn: func(_ context.Context) ([]*entity.ProtocolOracle, error) {
					return tt.bindings, nil
				},
			}

			header := &mockHeaderFetcher{
				headerByNumberFn: func(_ context.Context, number *big.Int) (*ethtypes.Header, error) {
					return &ethtypes.Header{Time: uint64(1700000000 + number.Int64())}, nil
				},
			}

			svc, err := NewService(Config{
				Concurrency: 1,
				BatchSize:   1000,
				Logger:      testutil.DiscardLogger(),
			}, header, func() (outbound.Multicaller, error) {
				return &testutil.MockMulticaller{ExecuteFn: blockDependentPrices(t)}, nil
			}, repo)
			if err != nil {
				t.Fatalf("NewService: %v", err)
			}

			err = svc.Run(context.Background(), tt.fromBlock, tt.toBlock)

			if tt.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
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
