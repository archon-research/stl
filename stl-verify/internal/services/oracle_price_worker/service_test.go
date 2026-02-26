package oracle_price_worker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/services/shared"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// dummyMulticallerFactory returns a MulticallerFactory that creates a mock multicaller
// for any oracle type. It satisfies the non-nil factory requirement for NewService.
func dummyMulticallerFactory() MulticallerFactory {
	return func(_ entity.OracleType) (outbound.Multicaller, error) {
		return &testutil.MockMulticaller{}, nil
	}
}

// multicallFactoryFor returns a MulticallerFactory that always returns the given multicaller.
func multicallFactoryFor(mc outbound.Multicaller) MulticallerFactory {
	return func(_ entity.OracleType) (outbound.Multicaller, error) {
		return mc, nil
	}
}

// ---------------------------------------------------------------------------
// Mocks
// ---------------------------------------------------------------------------

// mockConsumer implements outbound.SQSConsumer.
type mockConsumer struct {
	mu                  sync.Mutex
	receiveMessagesFn   func(ctx context.Context, maxMessages int) ([]outbound.SQSMessage, error)
	deleteMessageFn     func(ctx context.Context, receiptHandle string) error
	deleteMessageCalls  int
	receiveMessageCalls int
}

func (m *mockConsumer) ReceiveMessages(ctx context.Context, maxMessages int) ([]outbound.SQSMessage, error) {
	m.mu.Lock()
	m.receiveMessageCalls++
	m.mu.Unlock()
	if m.receiveMessagesFn != nil {
		return m.receiveMessagesFn(ctx, maxMessages)
	}
	return nil, nil
}

func (m *mockConsumer) DeleteMessage(ctx context.Context, receiptHandle string) error {
	m.mu.Lock()
	m.deleteMessageCalls++
	m.mu.Unlock()
	if m.deleteMessageFn != nil {
		return m.deleteMessageFn(ctx, receiptHandle)
	}
	return nil
}

func (m *mockConsumer) Close() error {
	return nil
}

// mockRepo implements outbound.OnchainPriceRepository.
type mockRepo struct {
	mu                             sync.Mutex
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

	upsertPricesCalls int
	lastUpserted      []*entity.OnchainTokenPrice
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
	m.mu.Lock()
	m.upsertPricesCalls++
	m.lastUpserted = prices
	m.mu.Unlock()
	if m.upsertPricesFn != nil {
		return m.upsertPricesFn(ctx, prices)
	}
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
	return nil, nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func validConfig() shared.SQSConsumerConfig {
	return shared.SQSConsumerConfig{
		Logger:  testutil.DiscardLogger(),
		ChainID: 1,
	}
}

func defaultOracle() *entity.Oracle {
	return &entity.Oracle{
		ID:              1,
		Name:            "sparklend",
		DisplayName:     "Spark: aave Oracle",
		ChainID:         1,
		Address:         common.HexToAddress("0x8105f69D9C41644c6A0803fDA7D03Aa70996cFD9"),
		OracleType:      entity.OracleTypeAave,
		DeploymentBlock: 17000000,
		Enabled:         true,
	}
}

func defaultAssets() []*entity.OracleAsset {
	return []*entity.OracleAsset{
		{ID: 1, OracleID: 1, TokenID: 1, Enabled: true},
		{ID: 2, OracleID: 1, TokenID: 2, Enabled: true},
	}
}

func defaultTokenAddressBytes() map[int64][]byte {
	return map[int64][]byte{
		1: common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2").Bytes(), // WETH
		2: common.HexToAddress("0x6B175474E89094C44Da98b954EedeAC495271d0F").Bytes(), // DAI
	}
}

// defaultRepoSetup configures the mock repo with defaults for a successful initialization.
func defaultRepoSetup(r *mockRepo) {
	r.getAllEnabledOraclesFn = func(_ context.Context) ([]*entity.Oracle, error) {
		return []*entity.Oracle{defaultOracle()}, nil
	}
	r.getEnabledAssetsFn = func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) {
		return defaultAssets(), nil
	}
	r.getTokenAddressesFn = func(_ context.Context, _ int64) (map[int64][]byte, error) {
		return defaultTokenAddressBytes(), nil
	}
	r.getLatestPricesFn = func(_ context.Context, _ int64) (map[int64]float64, error) {
		return map[int64]float64{1: 2000.0, 2: 1.0}, nil
	}
}

// newOracleMulticallerWithT creates a mock multicaller using the provided testing.T.
func newOracleMulticallerWithT(t *testing.T, prices []*big.Int) *testutil.MockMulticaller {
	t.Helper()
	pricesData := testutil.PackAssetPrices(t, prices)
	return &testutil.MockMulticaller{
		ExecuteFn: func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
			if len(calls) != 1 {
				return nil, fmt.Errorf("expected 1 call, got %d", len(calls))
			}
			return []outbound.Result{
				{Success: true, ReturnData: pricesData},
			}, nil
		},
	}
}

func makeBlockEventJSON(blockNumber int64, version int, blockTimestamp int64) string {
	event := outbound.BlockEvent{
		ChainID:        1,
		BlockNumber:    blockNumber,
		Version:        version,
		BlockHash:      "0xabc123",
		BlockTimestamp: blockTimestamp,
	}
	data, _ := json.Marshal(event)
	return string(data)
}

// ---------------------------------------------------------------------------
// TestNewService
// ---------------------------------------------------------------------------

func TestNewService(t *testing.T) {
	consumer := &mockConsumer{}
	repo := &mockRepo{}

	tests := []struct {
		name        string
		config      shared.SQSConsumerConfig
		consumer    outbound.SQSConsumer
		repo        outbound.OnchainPriceRepository
		wantErr     bool
		errContains string
		// For checking defaults are applied:
		checkDefaults bool
	}{
		{
			name:     "success with all valid params",
			config:   validConfig(),
			consumer: consumer,
			repo:     repo,
			wantErr:  false,
		},
		{
			name:          "success with default config values",
			config:        shared.SQSConsumerConfig{ChainID: 1},
			consumer:      consumer,
			repo:          repo,
			wantErr:       false,
			checkDefaults: true,
		},
		{
			name:        "error nil consumer",
			config:      validConfig(),
			consumer:    nil,
			repo:        repo,
			wantErr:     true,
			errContains: "consumer cannot be nil",
		},
		{
			name:        "error nil repo",
			config:      validConfig(),
			consumer:    consumer,
			repo:        nil,
			wantErr:     true,
			errContains: "repo cannot be nil",
		},
	}

	// Separate test for nil newMulticaller since the table always passes dummyMulticallerFactory().
	t.Run("error nil newMulticaller", func(t *testing.T) {
		_, err := NewService(validConfig(), consumer, repo, nil)
		if err == nil {
			t.Fatal("expected error, got nil")
		}
		if !strings.Contains(err.Error(), "newMulticaller cannot be nil") {
			t.Errorf("error %q does not contain %q", err.Error(), "newMulticaller cannot be nil")
		}
	})

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			svc, err := NewService(tc.config, tc.consumer, tc.repo, dummyMulticallerFactory())

			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if tc.errContains != "" && !strings.Contains(err.Error(), tc.errContains) {
					t.Errorf("error %q does not contain %q", err.Error(), tc.errContains)
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

			// Verify service fields
			if svc.consumer == nil {
				t.Error("consumer should not be nil")
			}
			if svc.newMulticaller == nil {
				t.Error("newMulticaller should not be nil")
			}
			if svc.repo == nil {
				t.Error("repo should not be nil")
			}
			if svc.oracleABI == nil {
				t.Error("oracleABI should not be nil")
			}
			if svc.logger == nil {
				t.Error("logger should not be nil")
			}

			if tc.checkDefaults {
				defaults := shared.SQSConsumerConfigDefaults()
				if svc.config.MaxMessages != defaults.MaxMessages {
					t.Errorf("MaxMessages = %d, want %d", svc.config.MaxMessages, defaults.MaxMessages)
				}
				if svc.config.PollInterval != defaults.PollInterval {
					t.Errorf("PollInterval = %v, want %v", svc.config.PollInterval, defaults.PollInterval)
				}
				if svc.config.Logger == nil {
					t.Error("Logger should have been set to default")
				}
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TestStart
// ---------------------------------------------------------------------------

func TestStart(t *testing.T) {
	tests := []struct {
		name        string
		setupRepo   func(r *mockRepo)
		wantErr     bool
		errContains string
	}{
		{
			name:      "success",
			setupRepo: defaultRepoSetup,
			wantErr:   false,
		},
		{
			name: "error GetAllEnabledOracles fails",
			setupRepo: func(r *mockRepo) {
				r.getAllEnabledOraclesFn = func(_ context.Context) ([]*entity.Oracle, error) {
					return nil, fmt.Errorf("db connection refused")
				}
			},
			wantErr:     true,
			errContains: "getting enabled oracles",
		},
		{
			name: "error GetEnabledAssets fails returns error",
			setupRepo: func(r *mockRepo) {
				r.getAllEnabledOraclesFn = func(_ context.Context) ([]*entity.Oracle, error) {
					return []*entity.Oracle{defaultOracle()}, nil
				}
				r.getEnabledAssetsFn = func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) {
					return nil, fmt.Errorf("query timeout")
				}
			},
			wantErr:     true,
			errContains: "building oracle unit",
		},
		{
			name: "error no enabled assets returns error",
			setupRepo: func(r *mockRepo) {
				r.getAllEnabledOraclesFn = func(_ context.Context) ([]*entity.Oracle, error) {
					return []*entity.Oracle{defaultOracle()}, nil
				}
				r.getEnabledAssetsFn = func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) {
					return []*entity.OracleAsset{}, nil
				}
			},
			wantErr:     true,
			errContains: "no enabled assets",
		},
		{
			name: "error token address not found returns error",
			setupRepo: func(r *mockRepo) {
				r.getAllEnabledOraclesFn = func(_ context.Context) ([]*entity.Oracle, error) {
					return []*entity.Oracle{defaultOracle()}, nil
				}
				r.getEnabledAssetsFn = func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) {
					return []*entity.OracleAsset{
						{ID: 1, OracleID: 1, TokenID: 999, Enabled: true}, // 999 not in tokenAddresses
					}, nil
				}
				r.getTokenAddressesFn = func(_ context.Context, _ int64) (map[int64][]byte, error) {
					return defaultTokenAddressBytes(), nil // has 1, 2 but not 999
				}
			},
			wantErr:     true,
			errContains: "token address not found",
		},
		{
			name: "error GetLatestPrices fails is warned and skipped",
			setupRepo: func(r *mockRepo) {
				r.getAllEnabledOraclesFn = func(_ context.Context) ([]*entity.Oracle, error) {
					return []*entity.Oracle{defaultOracle()}, nil
				}
				r.getEnabledAssetsFn = func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) {
					return defaultAssets(), nil
				}
				r.getTokenAddressesFn = func(_ context.Context, _ int64) (map[int64][]byte, error) {
					return defaultTokenAddressBytes(), nil
				}
				r.getLatestPricesFn = func(_ context.Context, _ int64) (map[int64]float64, error) {
					return nil, fmt.Errorf("redis unavailable")
				}
			},
			wantErr:     true,
			errContains: "no oracles with enabled assets found",
		},
		{
			name: "success multiple oracles deduplicates by oracle_id",
			setupRepo: func(r *mockRepo) {
				oracle := defaultOracle()
				r.getAllEnabledOraclesFn = func(_ context.Context) ([]*entity.Oracle, error) {
					return []*entity.Oracle{oracle, oracle}, nil // same oracle twice
				}
				r.getEnabledAssetsFn = func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) {
					return defaultAssets(), nil
				}
				r.getTokenAddressesFn = func(_ context.Context, _ int64) (map[int64][]byte, error) {
					return defaultTokenAddressBytes(), nil
				}
				r.getLatestPricesFn = func(_ context.Context, _ int64) (map[int64]float64, error) {
					return map[int64]float64{}, nil
				}
			},
			wantErr: false,
		},
		{
			name: "success with two distinct oracles",
			setupRepo: func(r *mockRepo) {
				oracle1 := defaultOracle()
				oracle2 := &entity.Oracle{
					ID:              2,
					Name:            "aave-v3",
					DisplayName:     "Aave V3 Oracle",
					ChainID:         1,
					Address:         common.HexToAddress("0x54586bE62E3c3580375aE3723C145253060Ca0C2"),
					OracleType:      entity.OracleTypeAave,
					DeploymentBlock: 18000000,
					Enabled:         true,
				}
				r.getAllEnabledOraclesFn = func(_ context.Context) ([]*entity.Oracle, error) {
					return []*entity.Oracle{oracle1, oracle2}, nil
				}
				r.getEnabledAssetsFn = func(_ context.Context, oracleID int64) ([]*entity.OracleAsset, error) {
					if oracleID == 1 {
						return defaultAssets(), nil
					}
					return []*entity.OracleAsset{
						{ID: 3, OracleID: 2, TokenID: 3, Enabled: true},
					}, nil
				}
				r.getTokenAddressesFn = func(_ context.Context, oracleID int64) (map[int64][]byte, error) {
					if oracleID == 1 {
						return defaultTokenAddressBytes(), nil
					}
					return map[int64][]byte{
						3: common.HexToAddress("0xA0b86991c6218b36c1d19D4a2e9Eb0cE3606eB48").Bytes(),
					}, nil
				}
				r.getLatestPricesFn = func(_ context.Context, _ int64) (map[int64]float64, error) {
					return map[int64]float64{}, nil
				}
			},
			wantErr: false,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			repo := &mockRepo{}
			tc.setupRepo(repo)

			// For success case, provide a mock consumer that blocks on receive so the goroutine
			// doesn't spin. For error cases, it doesn't matter since Start returns early.
			consumer := &mockConsumer{
				receiveMessagesFn: func(ctx context.Context, _ int) ([]outbound.SQSMessage, error) {
					// Block until context is cancelled to avoid spinning
					<-ctx.Done()
					return nil, ctx.Err()
				},
			}

			mc := newOracleMulticallerWithT(t, []*big.Int{
				new(big.Int).Mul(big.NewInt(2000), big.NewInt(1e8)),
				new(big.Int).Mul(big.NewInt(1), big.NewInt(1e8)),
			})

			svc, err := NewService(validConfig(), consumer, repo, multicallFactoryFor(mc))
			if err != nil {
				t.Fatalf("NewService failed: %v", err)
			}

			ctx := context.Background()
			err = svc.Start(ctx)

			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if tc.errContains != "" && !strings.Contains(err.Error(), tc.errContains) {
					t.Errorf("error %q does not contain %q", err.Error(), tc.errContains)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// Verify initialization state
			if len(svc.units) == 0 {
				t.Error("units not set after Start")
			}

			// Clean up
			if stopErr := svc.Stop(); stopErr != nil {
				t.Errorf("Stop failed: %v", stopErr)
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TestStartAndProcessMessages - end-to-end message processing
// ---------------------------------------------------------------------------

func TestStartAndProcessMessages(t *testing.T) {
	blockTimestamp := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Unix()

	t.Run("end to end: new prices are upserted, same prices are skipped", func(t *testing.T) {
		repo := &mockRepo{}
		defaultRepoSetup(repo)
		repo.getLatestPricesFn = func(_ context.Context, _ int64) (map[int64]float64, error) {
			return map[int64]float64{}, nil // empty cache means all prices are new
		}
		repo.upsertPricesFn = func(_ context.Context, prices []*entity.OnchainTokenPrice) error {
			return nil
		}

		price1 := new(big.Int).Mul(big.NewInt(200000), big.NewInt(1e4)) // 2000.00 * 1e8
		price2 := new(big.Int).Mul(big.NewInt(100), big.NewInt(1e6))    // 1.00 * 1e8

		mc := newOracleMulticallerWithT(t, []*big.Int{price1, price2})

		// Deliver one message on first call, then empty on subsequent calls.
		messageDelivered := false
		body1 := makeBlockEventJSON(18000000, 1, blockTimestamp)
		receipt1 := "receipt-1"

		consumer := &mockConsumer{
			receiveMessagesFn: func(ctx context.Context, _ int) ([]outbound.SQSMessage, error) {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				if !messageDelivered {
					messageDelivered = true
					return []outbound.SQSMessage{
						{MessageID: "msg-1", Body: body1, ReceiptHandle: receipt1},
					}, nil
				}
				// After the first message, block until context is done
				<-ctx.Done()
				return nil, ctx.Err()
			},
		}

		cfg := validConfig()
		cfg.PollInterval = 1 * time.Millisecond

		svc, err := NewService(cfg, consumer, repo, multicallFactoryFor(mc))
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}

		ctx := context.Background()
		if err := svc.Start(ctx); err != nil {
			t.Fatalf("Start: %v", err)
		}

		// Wait for processing
		testutil.WaitForCondition(t, 2*time.Second, func() bool {
			repo.mu.Lock()
			defer repo.mu.Unlock()
			return repo.upsertPricesCalls >= 1
		}, "UpsertPrices to be called")

		// Verify UpsertPrices was called with 2 prices (both new)
		repo.mu.Lock()
		if len(repo.lastUpserted) != 2 {
			t.Errorf("lastUpserted length = %d, want 2", len(repo.lastUpserted))
		}
		repo.mu.Unlock()

		// Verify delete was called
		consumer.mu.Lock()
		if consumer.deleteMessageCalls < 1 {
			t.Errorf("DeleteMessage call count = %d, want >= 1", consumer.deleteMessageCalls)
		}
		consumer.mu.Unlock()

		// Now verify change detection: second block with same prices should not upsert.
		// Reset repo call count and deliver a second message.
		repo.mu.Lock()
		prevUpserts := repo.upsertPricesCalls
		repo.mu.Unlock()

		// Directly call processBlock to test change detection without SQS timing complexity
		event2 := outbound.BlockEvent{
			ChainID:        1,
			BlockNumber:    18000001,
			Version:        1,
			BlockHash:      "0xdef456",
			BlockTimestamp: blockTimestamp + 12,
		}
		err = svc.processBlock(ctx, event2)
		if err != nil {
			t.Fatalf("processBlock for second block: %v", err)
		}

		// UpsertPrices should NOT have been called again (same prices)
		repo.mu.Lock()
		if repo.upsertPricesCalls != prevUpserts {
			t.Errorf("UpsertPrices called %d times after same-price block, want %d",
				repo.upsertPricesCalls, prevUpserts)
		}
		repo.mu.Unlock()

		if stopErr := svc.Stop(); stopErr != nil {
			t.Errorf("Stop: %v", stopErr)
		}
	})

	t.Run("SQS receive error", func(t *testing.T) {
		repo := &mockRepo{}
		defaultRepoSetup(repo)
		repo.getLatestPricesFn = func(_ context.Context, _ int64) (map[int64]float64, error) {
			return map[int64]float64{}, nil
		}

		consumer := &mockConsumer{
			receiveMessagesFn: func(_ context.Context, _ int) ([]outbound.SQSMessage, error) {
				return nil, fmt.Errorf("SQS service unavailable")
			},
		}

		mc := &testutil.MockMulticaller{}
		cfg := validConfig()
		cfg.PollInterval = 1 * time.Millisecond

		svc, err := NewService(cfg, consumer, repo, multicallFactoryFor(mc))
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}

		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}

		// Let the processLoop run and encounter the error
		time.Sleep(50 * time.Millisecond)

		// Verify processMessages was attempted (no crash)
		consumer.mu.Lock()
		if consumer.receiveMessageCalls == 0 {
			t.Error("expected at least one ReceiveMessage call")
		}
		consumer.mu.Unlock()

		if stopErr := svc.Stop(); stopErr != nil {
			t.Errorf("Stop: %v", stopErr)
		}
	})

	t.Run("SQS returns empty messages", func(t *testing.T) {
		repo := &mockRepo{}
		defaultRepoSetup(repo)
		repo.getLatestPricesFn = func(_ context.Context, _ int64) (map[int64]float64, error) {
			return map[int64]float64{}, nil
		}

		consumer := &mockConsumer{
			receiveMessagesFn: func(ctx context.Context, _ int) ([]outbound.SQSMessage, error) {
				return nil, nil
			},
		}

		mc := &testutil.MockMulticaller{}
		cfg := validConfig()
		cfg.PollInterval = 1 * time.Millisecond

		svc, err := NewService(cfg, consumer, repo, multicallFactoryFor(mc))
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}

		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}

		time.Sleep(50 * time.Millisecond)

		// UpsertPrices should NOT have been called
		repo.mu.Lock()
		if repo.upsertPricesCalls != 0 {
			t.Errorf("UpsertPrices call count = %d, want 0", repo.upsertPricesCalls)
		}
		repo.mu.Unlock()

		if stopErr := svc.Stop(); stopErr != nil {
			t.Errorf("Stop: %v", stopErr)
		}
	})

	t.Run("message with nil body", func(t *testing.T) {
		repo := &mockRepo{}
		defaultRepoSetup(repo)
		repo.getLatestPricesFn = func(_ context.Context, _ int64) (map[int64]float64, error) {
			return map[int64]float64{}, nil
		}

		delivered := false
		consumer := &mockConsumer{
			receiveMessagesFn: func(ctx context.Context, _ int) ([]outbound.SQSMessage, error) {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				if !delivered {
					delivered = true
					return []outbound.SQSMessage{
						{MessageID: "msg-nil", Body: "", ReceiptHandle: "receipt-nil"},
					}, nil
				}
				<-ctx.Done()
				return nil, ctx.Err()
			},
		}

		mc := &testutil.MockMulticaller{}
		cfg := validConfig()
		cfg.PollInterval = 1 * time.Millisecond

		svc, err := NewService(cfg, consumer, repo, multicallFactoryFor(mc))
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}

		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}

		time.Sleep(50 * time.Millisecond)

		// Message should not have been deleted (process failed)
		consumer.mu.Lock()
		if consumer.deleteMessageCalls != 0 {
			t.Errorf("DeleteMessage call count = %d, want 0 (nil body should fail)", consumer.deleteMessageCalls)
		}
		consumer.mu.Unlock()

		// UpsertPrices should NOT have been called
		repo.mu.Lock()
		if repo.upsertPricesCalls != 0 {
			t.Errorf("UpsertPrices call count = %d, want 0", repo.upsertPricesCalls)
		}
		repo.mu.Unlock()

		if stopErr := svc.Stop(); stopErr != nil {
			t.Errorf("Stop: %v", stopErr)
		}
	})

	t.Run("message with invalid JSON", func(t *testing.T) {
		repo := &mockRepo{}
		defaultRepoSetup(repo)
		repo.getLatestPricesFn = func(_ context.Context, _ int64) (map[int64]float64, error) {
			return map[int64]float64{}, nil
		}

		delivered := false
		consumer := &mockConsumer{
			receiveMessagesFn: func(ctx context.Context, _ int) ([]outbound.SQSMessage, error) {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				if !delivered {
					delivered = true
					return []outbound.SQSMessage{
						{MessageID: "msg-bad-json", Body: "not valid json{{{", ReceiptHandle: "receipt-bad-json"},
					}, nil
				}
				<-ctx.Done()
				return nil, ctx.Err()
			},
		}

		mc := &testutil.MockMulticaller{}
		cfg := validConfig()
		cfg.PollInterval = 1 * time.Millisecond

		svc, err := NewService(cfg, consumer, repo, multicallFactoryFor(mc))
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}

		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}

		time.Sleep(50 * time.Millisecond)

		// Message should not have been deleted
		consumer.mu.Lock()
		if consumer.deleteMessageCalls != 0 {
			t.Errorf("DeleteMessage call count = %d, want 0 (invalid JSON)", consumer.deleteMessageCalls)
		}
		consumer.mu.Unlock()

		if stopErr := svc.Stop(); stopErr != nil {
			t.Errorf("Stop: %v", stopErr)
		}
	})

	t.Run("multicall returns error during processBlock", func(t *testing.T) {
		repo := &mockRepo{}
		defaultRepoSetup(repo)
		repo.getLatestPricesFn = func(_ context.Context, _ int64) (map[int64]float64, error) {
			return map[int64]float64{}, nil
		}

		delivered := false
		body := makeBlockEventJSON(18000000, 1, blockTimestamp)
		consumer := &mockConsumer{
			receiveMessagesFn: func(ctx context.Context, _ int) ([]outbound.SQSMessage, error) {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				if !delivered {
					delivered = true
					return []outbound.SQSMessage{
						{MessageID: "msg-mc-err", Body: body, ReceiptHandle: "receipt-mc-err"},
					}, nil
				}
				<-ctx.Done()
				return nil, ctx.Err()
			},
		}

		mc := &testutil.MockMulticaller{
			ExecuteFn: func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
				return nil, fmt.Errorf("RPC node timeout")
			},
		}

		cfg := validConfig()
		cfg.PollInterval = 1 * time.Millisecond

		svc, err := NewService(cfg, consumer, repo, multicallFactoryFor(mc))
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}

		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}

		time.Sleep(50 * time.Millisecond)

		// Message should not have been deleted (processBlock failed)
		consumer.mu.Lock()
		if consumer.deleteMessageCalls != 0 {
			t.Errorf("DeleteMessage call count = %d, want 0 (multicall error)", consumer.deleteMessageCalls)
		}
		consumer.mu.Unlock()

		// UpsertPrices should NOT have been called
		repo.mu.Lock()
		if repo.upsertPricesCalls != 0 {
			t.Errorf("UpsertPrices call count = %d, want 0", repo.upsertPricesCalls)
		}
		repo.mu.Unlock()

		if stopErr := svc.Stop(); stopErr != nil {
			t.Errorf("Stop: %v", stopErr)
		}
	})

	t.Run("DeleteMessage returns error after successful processing", func(t *testing.T) {
		repo := &mockRepo{}
		defaultRepoSetup(repo)
		repo.getLatestPricesFn = func(_ context.Context, _ int64) (map[int64]float64, error) {
			return map[int64]float64{}, nil
		}

		price1 := new(big.Int).Mul(big.NewInt(2000), big.NewInt(1e8))
		price2 := new(big.Int).Mul(big.NewInt(1), big.NewInt(1e8))
		mc := newOracleMulticallerWithT(t, []*big.Int{price1, price2})

		delivered := false
		body := makeBlockEventJSON(18000000, 1, blockTimestamp)
		consumer := &mockConsumer{
			receiveMessagesFn: func(ctx context.Context, _ int) ([]outbound.SQSMessage, error) {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				if !delivered {
					delivered = true
					return []outbound.SQSMessage{
						{MessageID: "msg-del-err", Body: body, ReceiptHandle: "receipt-del-err"},
					}, nil
				}
				<-ctx.Done()
				return nil, ctx.Err()
			},
			deleteMessageFn: func(_ context.Context, _ string) error {
				return fmt.Errorf("SQS delete failed")
			},
		}

		cfg := validConfig()
		cfg.PollInterval = 1 * time.Millisecond

		svc, err := NewService(cfg, consumer, repo, multicallFactoryFor(mc))
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}

		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}

		// Wait for processing
		testutil.WaitForCondition(t, 2*time.Second, func() bool {
			repo.mu.Lock()
			defer repo.mu.Unlock()
			return repo.upsertPricesCalls >= 1
		}, "UpsertPrices to be called")

		// DeleteMessage was attempted (even though it failed)
		consumer.mu.Lock()
		if consumer.deleteMessageCalls < 1 {
			t.Errorf("DeleteMessage call count = %d, want >= 1", consumer.deleteMessageCalls)
		}
		consumer.mu.Unlock()

		if stopErr := svc.Stop(); stopErr != nil {
			t.Errorf("Stop: %v", stopErr)
		}
	})

	t.Run("price count mismatch from oracle", func(t *testing.T) {
		repo := &mockRepo{}
		defaultRepoSetup(repo)
		repo.getLatestPricesFn = func(_ context.Context, _ int64) (map[int64]float64, error) {
			return map[int64]float64{}, nil
		}

		// Return only 1 price for 2 tokens
		onePrice := []*big.Int{new(big.Int).Mul(big.NewInt(2000), big.NewInt(1e8))}
		mc := newOracleMulticallerWithT(t, onePrice)

		delivered := false
		body := makeBlockEventJSON(18000000, 1, blockTimestamp)
		consumer := &mockConsumer{
			receiveMessagesFn: func(ctx context.Context, _ int) ([]outbound.SQSMessage, error) {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				if !delivered {
					delivered = true
					return []outbound.SQSMessage{
						{MessageID: "msg-mismatch", Body: body, ReceiptHandle: "receipt-mismatch"},
					}, nil
				}
				<-ctx.Done()
				return nil, ctx.Err()
			},
		}

		cfg := validConfig()
		cfg.PollInterval = 1 * time.Millisecond

		svc, err := NewService(cfg, consumer, repo, multicallFactoryFor(mc))
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}

		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}

		time.Sleep(50 * time.Millisecond)

		// Message should NOT have been deleted (processBlock returned error)
		consumer.mu.Lock()
		if consumer.deleteMessageCalls != 0 {
			t.Errorf("DeleteMessage call count = %d, want 0 (price mismatch)", consumer.deleteMessageCalls)
		}
		consumer.mu.Unlock()

		// UpsertPrices should NOT have been called
		repo.mu.Lock()
		if repo.upsertPricesCalls != 0 {
			t.Errorf("UpsertPrices call count = %d, want 0", repo.upsertPricesCalls)
		}
		repo.mu.Unlock()

		if stopErr := svc.Stop(); stopErr != nil {
			t.Errorf("Stop: %v", stopErr)
		}
	})

	t.Run("UpsertPrices returns error", func(t *testing.T) {
		repo := &mockRepo{}
		defaultRepoSetup(repo)
		repo.getLatestPricesFn = func(_ context.Context, _ int64) (map[int64]float64, error) {
			return map[int64]float64{}, nil
		}
		repo.upsertPricesFn = func(_ context.Context, _ []*entity.OnchainTokenPrice) error {
			return fmt.Errorf("database write failure")
		}

		price1 := new(big.Int).Mul(big.NewInt(2000), big.NewInt(1e8))
		price2 := new(big.Int).Mul(big.NewInt(1), big.NewInt(1e8))

		mc := newOracleMulticallerWithT(t, []*big.Int{price1, price2})

		delivered := false
		body := makeBlockEventJSON(18000000, 1, blockTimestamp)
		consumer := &mockConsumer{
			receiveMessagesFn: func(ctx context.Context, _ int) ([]outbound.SQSMessage, error) {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				if !delivered {
					delivered = true
					return []outbound.SQSMessage{
						{MessageID: "msg-upsert-err", Body: body, ReceiptHandle: "receipt-upsert-err"},
					}, nil
				}
				<-ctx.Done()
				return nil, ctx.Err()
			},
		}

		cfg := validConfig()
		cfg.PollInterval = 1 * time.Millisecond

		svc, err := NewService(cfg, consumer, repo, multicallFactoryFor(mc))
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}

		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}

		// Wait for the processing attempt
		testutil.WaitForCondition(t, 2*time.Second, func() bool {
			repo.mu.Lock()
			defer repo.mu.Unlock()
			return repo.upsertPricesCalls >= 1
		}, "UpsertPrices to be called")

		// Message should NOT have been deleted because processMessage returned error
		consumer.mu.Lock()
		if consumer.deleteMessageCalls != 0 {
			t.Errorf("DeleteMessage call count = %d, want 0 (UpsertPrices failed)", consumer.deleteMessageCalls)
		}
		consumer.mu.Unlock()

		if stopErr := svc.Stop(); stopErr != nil {
			t.Errorf("Stop: %v", stopErr)
		}
	})

	t.Run("entity validation error with blockNumber zero", func(t *testing.T) {
		repo := &mockRepo{}
		defaultRepoSetup(repo)
		repo.getLatestPricesFn = func(_ context.Context, _ int64) (map[int64]float64, error) {
			return map[int64]float64{}, nil
		}

		price1 := new(big.Int).Mul(big.NewInt(2000), big.NewInt(1e8))
		price2 := new(big.Int).Mul(big.NewInt(1), big.NewInt(1e8))
		mc := newOracleMulticallerWithT(t, []*big.Int{price1, price2})

		consumer := &mockConsumer{
			receiveMessagesFn: func(ctx context.Context, _ int) ([]outbound.SQSMessage, error) {
				<-ctx.Done()
				return nil, ctx.Err()
			},
		}

		cfg := validConfig()
		cfg.PollInterval = 1 * time.Millisecond

		svc, err := NewService(cfg, consumer, repo, multicallFactoryFor(mc))
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}

		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}

		// Directly call processBlock with blockNumber=0 to trigger entity validation error
		event := outbound.BlockEvent{
			ChainID:        1,
			BlockNumber:    0, // will fail entity validation: "blockNumber must be positive"
			Version:        1,
			BlockHash:      "0xabc",
			BlockTimestamp: blockTimestamp,
		}
		err = svc.processBlock(context.Background(), event)
		// processBlock should return an error because entity validation fails hard
		if err == nil {
			t.Error("processBlock with blockNumber=0 should have returned an error")
		}

		// No prices should have been upserted (validation failed before upsert)
		repo.mu.Lock()
		if repo.upsertPricesCalls != 0 {
			t.Errorf("UpsertPrices call count = %d, want 0", repo.upsertPricesCalls)
		}
		repo.mu.Unlock()

		if stopErr := svc.Stop(); stopErr != nil {
			t.Errorf("Stop: %v", stopErr)
		}
	})
}

// ---------------------------------------------------------------------------
// Feed oracle helpers
// ---------------------------------------------------------------------------

// feedOracleSetup configures the mock repo for a chainlink_feed oracle with one USD feed.
func feedOracleSetup(r *mockRepo) {
	feedAddr := common.HexToAddress("0x0000000000000000000000000000000000000F01")
	wethAddr := common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
	r.getAllEnabledOraclesFn = func(_ context.Context) ([]*entity.Oracle, error) {
		return []*entity.Oracle{{
			ID: 1, Name: "chainlink", Enabled: true,
			OracleType: entity.OracleTypeChainlinkFeed, PriceDecimals: 8,
		}}, nil
	}
	r.getEnabledAssetsFn = func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) {
		return []*entity.OracleAsset{{
			ID: 1, OracleID: 1, TokenID: 1, Enabled: true,
			FeedAddress: feedAddr, FeedDecimals: 8, QuoteCurrency: "USD",
		}}, nil
	}
	r.getTokenAddressesFn = func(_ context.Context, _ int64) (map[int64][]byte, error) {
		return map[int64][]byte{1: wethAddr.Bytes()}, nil
	}
	r.getLatestPricesFn = func(_ context.Context, _ int64) (map[int64]float64, error) {
		return map[int64]float64{}, nil
	}
}

// newFeedMulticaller creates a mock multicaller that returns latestRoundData for each feed.
func newFeedMulticaller(t *testing.T, answers []*big.Int) *testutil.MockMulticaller {
	t.Helper()
	return &testutil.MockMulticaller{
		ExecuteFn: func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
			results := make([]outbound.Result, len(calls))
			for i := range calls {
				if i < len(answers) {
					results[i] = outbound.Result{
						Success: true,
						ReturnData: testutil.PackLatestRoundData(t,
							big.NewInt(1), answers[i], big.NewInt(1000), big.NewInt(1000), big.NewInt(1)),
					}
				} else {
					results[i] = outbound.Result{Success: false}
				}
			}
			return results, nil
		},
	}
}

// ---------------------------------------------------------------------------
// TestStart_FeedOracle — chainlink_feed oracle initialization
// ---------------------------------------------------------------------------

func TestStart_FeedOracle(t *testing.T) {
	repo := &mockRepo{}
	feedOracleSetup(repo)

	consumer := &mockConsumer{
		receiveMessagesFn: func(ctx context.Context, _ int) ([]outbound.SQSMessage, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	mc := newFeedMulticaller(t, []*big.Int{big.NewInt(200_000_000_000)})

	svc, err := NewService(validConfig(), consumer, repo, multicallFactoryFor(mc))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	if err := svc.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if len(svc.units) != 1 {
		t.Fatalf("units count = %d, want 1", len(svc.units))
	}

	unit := svc.units[0]
	if unit.Oracle.OracleType != entity.OracleTypeChainlinkFeed {
		t.Errorf("OracleType = %q, want %q", unit.Oracle.OracleType, entity.OracleTypeChainlinkFeed)
	}
	if len(unit.Feeds) != 1 {
		t.Errorf("Feeds count = %d, want 1", len(unit.Feeds))
	}
	if unit.Feeds[0].QuoteCurrency != "USD" {
		t.Errorf("Feeds[0].QuoteCurrency = %q, want %q", unit.Feeds[0].QuoteCurrency, "USD")
	}
	if unit.Feeds[0].FeedDecimals != 8 {
		t.Errorf("Feeds[0].FeedDecimals = %d, want 8", unit.Feeds[0].FeedDecimals)
	}

	if stopErr := svc.Stop(); stopErr != nil {
		t.Errorf("Stop: %v", stopErr)
	}
}

// ---------------------------------------------------------------------------
// TestStart_ChronicleOracle — chronicle oracle gets its own eth_call multicaller
// ---------------------------------------------------------------------------

func TestStart_ChronicleOracle(t *testing.T) {
	feedAddr := common.HexToAddress("0x0000000000000000000000000000000000000C01")
	wethAddr := common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")

	repo := &mockRepo{}
	repo.getAllEnabledOraclesFn = func(_ context.Context) ([]*entity.Oracle, error) {
		return []*entity.Oracle{{
			ID: 2, Name: "chronicle", Enabled: true,
			OracleType: entity.OracleTypeChronicle, PriceDecimals: 18,
		}}, nil
	}
	repo.getEnabledAssetsFn = func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) {
		return []*entity.OracleAsset{{
			ID: 1, OracleID: 2, TokenID: 1, Enabled: true,
			FeedAddress: feedAddr, FeedDecimals: 18, QuoteCurrency: "USD",
		}}, nil
	}
	repo.getTokenAddressesFn = func(_ context.Context, _ int64) (map[int64][]byte, error) {
		return map[int64][]byte{1: wethAddr.Bytes()}, nil
	}
	repo.getLatestPricesFn = func(_ context.Context, _ int64) (map[int64]float64, error) {
		return map[int64]float64{}, nil
	}

	consumer := &mockConsumer{
		receiveMessagesFn: func(ctx context.Context, _ int) ([]outbound.SQSMessage, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	// Track which oracle type the factory was called with.
	var factoryCalledWith entity.OracleType
	chronicleMC := &testutil.MockMulticaller{}
	factory := func(ot entity.OracleType) (outbound.Multicaller, error) {
		factoryCalledWith = ot
		return chronicleMC, nil
	}

	svc, err := NewService(validConfig(), consumer, repo, factory)
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	if err := svc.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}

	if len(svc.units) != 1 {
		t.Fatalf("units count = %d, want 1", len(svc.units))
	}

	unit := svc.units[0]
	if unit.Oracle.OracleType != entity.OracleTypeChronicle {
		t.Errorf("OracleType = %q, want %q", unit.Oracle.OracleType, entity.OracleTypeChronicle)
	}

	// Verify the factory was called with the chronicle oracle type.
	if factoryCalledWith != entity.OracleTypeChronicle {
		t.Errorf("factory called with %q, want %q", factoryCalledWith, entity.OracleTypeChronicle)
	}

	// The unit's multicaller should be the one returned by the factory.
	if unit.multicaller != chronicleMC {
		t.Error("chronicle unit should use the multicaller returned by the factory")
	}

	if stopErr := svc.Stop(); stopErr != nil {
		t.Errorf("Stop: %v", stopErr)
	}
}

// ---------------------------------------------------------------------------
// TestProcessBlock_FeedOracle — end-to-end feed price processing
// ---------------------------------------------------------------------------

func TestProcessBlock_FeedOracle(t *testing.T) {
	blockTimestamp := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Unix()

	repo := &mockRepo{}
	feedOracleSetup(repo)

	consumer := &mockConsumer{
		receiveMessagesFn: func(ctx context.Context, _ int) ([]outbound.SQSMessage, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	answer := big.NewInt(200_000_000_000) // $2000 with 8 decimals
	mc := newFeedMulticaller(t, []*big.Int{answer})

	cfg := validConfig()
	cfg.PollInterval = 1 * time.Millisecond

	svc, err := NewService(cfg, consumer, repo, multicallFactoryFor(mc))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	if err := svc.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	svc.decimalsValidated = true // skip decimals validation for this test

	// Directly call processBlock
	event := outbound.BlockEvent{
		ChainID:        1,
		BlockNumber:    18000000,
		Version:        1,
		BlockHash:      "0xfeedblock1",
		BlockTimestamp: blockTimestamp,
	}
	if err := svc.processBlock(context.Background(), event); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	// Verify UpsertPrices was called with the correct price
	repo.mu.Lock()
	if repo.upsertPricesCalls != 1 {
		t.Errorf("UpsertPrices call count = %d, want 1", repo.upsertPricesCalls)
	}
	if len(repo.lastUpserted) != 1 {
		t.Errorf("lastUpserted length = %d, want 1", len(repo.lastUpserted))
	} else {
		p := repo.lastUpserted[0]
		if p.TokenID != 1 {
			t.Errorf("TokenID = %d, want 1", p.TokenID)
		}
		if p.PriceUSD != 2000.0 {
			t.Errorf("PriceUSD = %f, want 2000.0", p.PriceUSD)
		}
		if p.BlockNumber != 18000000 {
			t.Errorf("BlockNumber = %d, want 18000000", p.BlockNumber)
		}
	}
	repo.mu.Unlock()

	if stopErr := svc.Stop(); stopErr != nil {
		t.Errorf("Stop: %v", stopErr)
	}
}

// ---------------------------------------------------------------------------
// TestProcessBlock_FeedOracle_ChangeDetection — same prices are skipped
// ---------------------------------------------------------------------------

func TestProcessBlock_FeedOracle_ChangeDetection(t *testing.T) {
	blockTimestamp := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Unix()

	repo := &mockRepo{}
	feedOracleSetup(repo)

	consumer := &mockConsumer{
		receiveMessagesFn: func(ctx context.Context, _ int) ([]outbound.SQSMessage, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	answer := big.NewInt(200_000_000_000) // $2000
	mc := newFeedMulticaller(t, []*big.Int{answer})

	cfg := validConfig()
	cfg.PollInterval = 1 * time.Millisecond

	svc, err := NewService(cfg, consumer, repo, multicallFactoryFor(mc))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	if err := svc.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	svc.decimalsValidated = true // skip decimals validation for this test

	// First block: prices are new, should be upserted
	event1 := outbound.BlockEvent{
		ChainID:        1,
		BlockNumber:    18000000,
		Version:        1,
		BlockHash:      "0xfeed1",
		BlockTimestamp: blockTimestamp,
	}
	if err := svc.processBlock(context.Background(), event1); err != nil {
		t.Fatalf("processBlock (block 1): %v", err)
	}

	repo.mu.Lock()
	if repo.upsertPricesCalls != 1 {
		t.Errorf("after block 1: UpsertPrices call count = %d, want 1", repo.upsertPricesCalls)
	}
	repo.mu.Unlock()

	// Second block: same price, should NOT be upserted
	event2 := outbound.BlockEvent{
		ChainID:        1,
		BlockNumber:    18000001,
		Version:        1,
		BlockHash:      "0xfeed2",
		BlockTimestamp: blockTimestamp + 12,
	}
	if err := svc.processBlock(context.Background(), event2); err != nil {
		t.Fatalf("processBlock (block 2): %v", err)
	}

	repo.mu.Lock()
	if repo.upsertPricesCalls != 1 {
		t.Errorf("after block 2 (same price): UpsertPrices call count = %d, want 1 (unchanged)", repo.upsertPricesCalls)
	}
	repo.mu.Unlock()

	if stopErr := svc.Stop(); stopErr != nil {
		t.Errorf("Stop: %v", stopErr)
	}
}

// ---------------------------------------------------------------------------
// TestProcessBlock_FeedOracle_NonUSDConversion — ETH-denominated feed conversion
// ---------------------------------------------------------------------------

func TestProcessBlock_FeedOracle_NonUSDConversion(t *testing.T) {
	blockTimestamp := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Unix()

	wethAddr := common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2")
	weethAddr := common.HexToAddress("0xCd5fE23C85820F7B72D0926FC9b05b43E359b7ee")

	feedAddr1 := common.HexToAddress("0x0000000000000000000000000000000000000F01") // WETH/USD
	feedAddr2 := common.HexToAddress("0x0000000000000000000000000000000000000F02") // weETH/ETH

	repo := &mockRepo{}
	repo.getAllEnabledOraclesFn = func(_ context.Context) ([]*entity.Oracle, error) {
		return []*entity.Oracle{{
			ID: 1, Name: "chainlink-feeds", Enabled: true,
			OracleType: entity.OracleTypeChainlinkFeed, PriceDecimals: 8,
		}}, nil
	}
	repo.getEnabledAssetsFn = func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) {
		return []*entity.OracleAsset{
			{
				ID: 1, OracleID: 1, TokenID: 1, Enabled: true,
				FeedAddress: feedAddr1, FeedDecimals: 8, QuoteCurrency: "USD",
			},
			{
				ID: 2, OracleID: 1, TokenID: 2, Enabled: true,
				FeedAddress: feedAddr2, FeedDecimals: 8, QuoteCurrency: "ETH",
			},
		}, nil
	}
	repo.getTokenAddressesFn = func(_ context.Context, _ int64) (map[int64][]byte, error) {
		return map[int64][]byte{
			1: wethAddr.Bytes(),  // Token 1 = WETH → matches quoteCurrencyTokenAddr["ETH"]
			2: weethAddr.Bytes(), // Token 2 = weETH
		}, nil
	}
	repo.getLatestPricesFn = func(_ context.Context, _ int64) (map[int64]float64, error) {
		return map[int64]float64{}, nil
	}

	consumer := &mockConsumer{
		receiveMessagesFn: func(ctx context.Context, _ int) ([]outbound.SQSMessage, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	// WETH/USD = $2000, weETH/ETH = 1.05 ETH
	wethAnswer := big.NewInt(200_000_000_000) // 2000 * 1e8
	weethAnswer := big.NewInt(105_000_000)    // 1.05 * 1e8

	mc := newFeedMulticaller(t, []*big.Int{wethAnswer, weethAnswer})

	cfg := validConfig()
	cfg.PollInterval = 1 * time.Millisecond

	svc, err := NewService(cfg, consumer, repo, multicallFactoryFor(mc))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	if err := svc.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	svc.decimalsValidated = true // skip decimals validation for this test

	event := outbound.BlockEvent{
		ChainID:        1,
		BlockNumber:    18000000,
		Version:        1,
		BlockHash:      "0xnonusd",
		BlockTimestamp: blockTimestamp,
	}
	if err := svc.processBlock(context.Background(), event); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	repo.mu.Lock()
	defer repo.mu.Unlock()

	if repo.upsertPricesCalls != 1 {
		t.Fatalf("UpsertPrices call count = %d, want 1", repo.upsertPricesCalls)
	}
	if len(repo.lastUpserted) != 2 {
		t.Fatalf("lastUpserted length = %d, want 2", len(repo.lastUpserted))
	}

	// Find the weETH price (token 2) — should be 1.05 * 2000 = 2100.0
	var weethPrice *entity.OnchainTokenPrice
	for _, p := range repo.lastUpserted {
		if p.TokenID == 2 {
			weethPrice = p
		}
	}
	if weethPrice == nil {
		t.Fatal("weETH price (token 2) not found in upserted prices")
	}
	// ConvertNonUSDPrices multiplies: 1.05 * 2000.0 = 2100.0
	if weethPrice.PriceUSD != 2100.0 {
		t.Errorf("weETH PriceUSD = %f, want 2100.0", weethPrice.PriceUSD)
	}

	// Verify WETH/USD price is correct
	var wethPrice *entity.OnchainTokenPrice
	for _, p := range repo.lastUpserted {
		if p.TokenID == 1 {
			wethPrice = p
		}
	}
	if wethPrice == nil {
		t.Fatal("WETH price (token 1) not found in upserted prices")
	}
	if wethPrice.PriceUSD != 2000.0 {
		t.Errorf("WETH PriceUSD = %f, want 2000.0", wethPrice.PriceUSD)
	}

	if stopErr := svc.Stop(); stopErr != nil {
		t.Errorf("Stop: %v", stopErr)
	}
}

// ---------------------------------------------------------------------------
// TestProcessBlock_FeedOracle_AllFeedsFail — all feeds fail, no upserts
// ---------------------------------------------------------------------------

func TestProcessBlock_FeedOracle_AllFeedsFail(t *testing.T) {
	blockTimestamp := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Unix()

	repo := &mockRepo{}
	feedOracleSetup(repo)

	consumer := &mockConsumer{
		receiveMessagesFn: func(ctx context.Context, _ int) ([]outbound.SQSMessage, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}

	// All feeds return failure
	mc := &testutil.MockMulticaller{
		ExecuteFn: func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
			results := make([]outbound.Result, len(calls))
			for i := range calls {
				results[i] = outbound.Result{Success: false}
			}
			return results, nil
		},
	}

	cfg := validConfig()
	cfg.PollInterval = 1 * time.Millisecond

	svc, err := NewService(cfg, consumer, repo, multicallFactoryFor(mc))
	if err != nil {
		t.Fatalf("NewService: %v", err)
	}

	if err := svc.Start(context.Background()); err != nil {
		t.Fatalf("Start: %v", err)
	}
	svc.decimalsValidated = true // skip decimals validation for this test

	event := outbound.BlockEvent{
		ChainID:        1,
		BlockNumber:    18000000,
		Version:        1,
		BlockHash:      "0xallfail",
		BlockTimestamp: blockTimestamp,
	}
	if err := svc.processBlock(context.Background(), event); err != nil {
		t.Fatalf("processBlock: %v", err)
	}

	// No prices should have been upserted since all feeds failed
	repo.mu.Lock()
	if repo.upsertPricesCalls != 0 {
		t.Errorf("UpsertPrices call count = %d, want 0 (all feeds failed)", repo.upsertPricesCalls)
	}
	repo.mu.Unlock()

	if stopErr := svc.Stop(); stopErr != nil {
		t.Errorf("Stop: %v", stopErr)
	}
}

// ---------------------------------------------------------------------------
// TestProcessBlock_FeedDecimalsValidation — lazy validation tests
// ---------------------------------------------------------------------------

func TestProcessBlock_FeedDecimalsValidation(t *testing.T) {
	blockTimestamp := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Unix()

	t.Run("decimals mismatch blocks processing", func(t *testing.T) {
		repo := &mockRepo{}
		feedOracleSetup(repo)

		consumer := &mockConsumer{
			receiveMessagesFn: func(ctx context.Context, _ int) ([]outbound.SQSMessage, error) {
				<-ctx.Done()
				return nil, ctx.Err()
			},
		}

		// Return decimals=18 but config says 8 → mismatch
		callCount := 0
		mc := &testutil.MockMulticaller{
			ExecuteFn: func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
				callCount++
				if callCount == 1 {
					// decimals() call
					return []outbound.Result{
						{Success: true, ReturnData: testutil.PackDecimals(t, 18)},
					}, nil
				}
				// Should not be called because decimals validation fails
				t.Fatal("unexpected Execute call after decimals validation failure")
				return nil, nil
			},
		}

		svc, err := NewService(validConfig(), consumer, repo, multicallFactoryFor(mc))
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}
		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}

		event := outbound.BlockEvent{
			ChainID: 1, BlockNumber: 18000000, Version: 1,
			BlockHash: "0xdecfail", BlockTimestamp: blockTimestamp,
		}
		err = svc.processBlock(context.Background(), event)
		if err == nil {
			t.Fatal("expected decimals mismatch error, got nil")
		}
		if !strings.Contains(err.Error(), "feed decimals") {
			t.Errorf("error = %q, expected it to contain 'feed decimals'", err)
		}

		if stopErr := svc.Stop(); stopErr != nil {
			t.Errorf("Stop: %v", stopErr)
		}
	})

	t.Run("matching decimals allows processing", func(t *testing.T) {
		repo := &mockRepo{}
		feedOracleSetup(repo)

		consumer := &mockConsumer{
			receiveMessagesFn: func(ctx context.Context, _ int) ([]outbound.SQSMessage, error) {
				<-ctx.Done()
				return nil, ctx.Err()
			},
		}

		callCount := 0
		mc := &testutil.MockMulticaller{
			ExecuteFn: func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
				callCount++
				if callCount == 1 {
					// decimals() returns 8, matching config
					return []outbound.Result{
						{Success: true, ReturnData: testutil.PackDecimals(t, 8)},
					}, nil
				}
				// latestRoundData call
				results := make([]outbound.Result, len(calls))
				for i := range calls {
					results[i] = outbound.Result{
						Success: true,
						ReturnData: testutil.PackLatestRoundData(t,
							big.NewInt(1), big.NewInt(200_000_000_000),
							big.NewInt(1000), big.NewInt(1000), big.NewInt(1)),
					}
				}
				return results, nil
			},
		}

		svc, err := NewService(validConfig(), consumer, repo, multicallFactoryFor(mc))
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}
		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}

		event := outbound.BlockEvent{
			ChainID: 1, BlockNumber: 18000000, Version: 1,
			BlockHash: "0xdecok", BlockTimestamp: blockTimestamp,
		}
		if err := svc.processBlock(context.Background(), event); err != nil {
			t.Fatalf("processBlock: %v", err)
		}

		// Verify prices were stored
		repo.mu.Lock()
		if repo.upsertPricesCalls < 1 {
			t.Error("expected UpsertPrices to be called after successful decimals validation")
		}
		repo.mu.Unlock()

		if stopErr := svc.Stop(); stopErr != nil {
			t.Errorf("Stop: %v", stopErr)
		}
	})

	t.Run("validation runs only once", func(t *testing.T) {
		repo := &mockRepo{}
		feedOracleSetup(repo)

		consumer := &mockConsumer{
			receiveMessagesFn: func(ctx context.Context, _ int) ([]outbound.SQSMessage, error) {
				<-ctx.Done()
				return nil, ctx.Err()
			},
		}

		decimalsCallCount := 0
		mc := &testutil.MockMulticaller{
			ExecuteFn: func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
				// Peek at call data to detect decimals() vs latestRoundData()
				// decimals() calldata is 4 bytes, latestRoundData() is 4 bytes too, but
				// we can use callCount to distinguish: first call is decimals(), rest are latestRoundData().
				if decimalsCallCount == 0 {
					decimalsCallCount++
					return []outbound.Result{
						{Success: true, ReturnData: testutil.PackDecimals(t, 8)},
					}, nil
				}
				// latestRoundData
				results := make([]outbound.Result, len(calls))
				for i := range calls {
					results[i] = outbound.Result{
						Success: true,
						ReturnData: testutil.PackLatestRoundData(t,
							big.NewInt(1), big.NewInt(200_000_000_000),
							big.NewInt(1000), big.NewInt(1000), big.NewInt(1)),
					}
				}
				return results, nil
			},
		}

		svc, err := NewService(validConfig(), consumer, repo, multicallFactoryFor(mc))
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}
		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}

		event1 := outbound.BlockEvent{
			ChainID: 1, BlockNumber: 18000000, Version: 1,
			BlockHash: "0xonce1", BlockTimestamp: blockTimestamp,
		}
		if err := svc.processBlock(context.Background(), event1); err != nil {
			t.Fatalf("processBlock 1: %v", err)
		}

		prevDecimalsCount := decimalsCallCount

		event2 := outbound.BlockEvent{
			ChainID: 1, BlockNumber: 18000001, Version: 1,
			BlockHash: "0xonce2", BlockTimestamp: blockTimestamp + 12,
		}
		if err := svc.processBlock(context.Background(), event2); err != nil {
			t.Fatalf("processBlock 2: %v", err)
		}

		if decimalsCallCount != prevDecimalsCount {
			t.Errorf("decimals validated again on second block: count went from %d to %d",
				prevDecimalsCount, decimalsCallCount)
		}

		if stopErr := svc.Stop(); stopErr != nil {
			t.Errorf("Stop: %v", stopErr)
		}
	})

	t.Run("aave oracle skips decimals validation", func(t *testing.T) {
		repo := &mockRepo{}
		defaultRepoSetup(repo) // aave oracle

		consumer := &mockConsumer{
			receiveMessagesFn: func(ctx context.Context, _ int) ([]outbound.SQSMessage, error) {
				<-ctx.Done()
				return nil, ctx.Err()
			},
		}

		mc := newOracleMulticallerWithT(t, []*big.Int{
			new(big.Int).Mul(big.NewInt(2000), big.NewInt(1e8)),
			new(big.Int).Mul(big.NewInt(1), big.NewInt(1e8)),
		})

		svc, err := NewService(validConfig(), consumer, repo, multicallFactoryFor(mc))
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}
		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}

		event := outbound.BlockEvent{
			ChainID: 1, BlockNumber: 18000000, Version: 1,
			BlockHash: "0xaave", BlockTimestamp: blockTimestamp,
		}
		// Should succeed because aave oracles skip decimals validation
		if err := svc.processBlock(context.Background(), event); err != nil {
			t.Fatalf("processBlock: %v", err)
		}

		if !svc.decimalsValidated {
			t.Error("decimalsValidated should be true after first processBlock")
		}

		if stopErr := svc.Stop(); stopErr != nil {
			t.Errorf("Stop: %v", stopErr)
		}
	})
}

// ---------------------------------------------------------------------------
// TestStop
// ---------------------------------------------------------------------------

func TestStop(t *testing.T) {
	t.Run("stop without start (cancel is nil)", func(t *testing.T) {
		repo := &mockRepo{}
		consumer := &mockConsumer{}
		mc := &testutil.MockMulticaller{}

		svc, err := NewService(validConfig(), consumer, repo, multicallFactoryFor(mc))
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}

		// cancel is nil at this point (Start was never called)
		err = svc.Stop()
		if err != nil {
			t.Errorf("Stop without Start should not error, got: %v", err)
		}
	})

	t.Run("stop after start", func(t *testing.T) {
		repo := &mockRepo{}
		defaultRepoSetup(repo)
		repo.getLatestPricesFn = func(_ context.Context, _ int64) (map[int64]float64, error) {
			return map[int64]float64{}, nil
		}

		consumer := &mockConsumer{
			receiveMessagesFn: func(ctx context.Context, _ int) ([]outbound.SQSMessage, error) {
				<-ctx.Done()
				return nil, ctx.Err()
			},
		}
		mc := &testutil.MockMulticaller{}

		cfg := validConfig()
		cfg.PollInterval = 1 * time.Millisecond

		svc, err := NewService(cfg, consumer, repo, multicallFactoryFor(mc))
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}

		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}

		// Verify context is active
		if svc.ctx.Err() != nil {
			t.Error("context should not be cancelled before Stop")
		}

		err = svc.Stop()
		if err != nil {
			t.Errorf("Stop returned error: %v", err)
		}

		// Verify context was cancelled
		if svc.ctx.Err() == nil {
			t.Error("context should be cancelled after Stop")
		}
	})
}
