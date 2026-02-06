package oracle_price_worker

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"math/big"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/service/sqs"
	sqstypes "github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/ethereum/go-ethereum/common"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/pkg/blockchain/abis"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
)

// ---------------------------------------------------------------------------
// Mocks
// ---------------------------------------------------------------------------

// mockSQSClient implements SQSClient.
type mockSQSClient struct {
	mu                  sync.Mutex
	receiveMessageFunc  func(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	deleteMessageFunc   func(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error)
	deleteMessageCalls  int
	receiveMessageCalls int
}

func (m *mockSQSClient) ReceiveMessage(ctx context.Context, params *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	m.mu.Lock()
	m.receiveMessageCalls++
	m.mu.Unlock()
	if m.receiveMessageFunc != nil {
		return m.receiveMessageFunc(ctx, params, optFns...)
	}
	return &sqs.ReceiveMessageOutput{}, nil
}

func (m *mockSQSClient) DeleteMessage(ctx context.Context, params *sqs.DeleteMessageInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
	m.mu.Lock()
	m.deleteMessageCalls++
	m.mu.Unlock()
	if m.deleteMessageFunc != nil {
		return m.deleteMessageFunc(ctx, params, optFns...)
	}
	return &sqs.DeleteMessageOutput{}, nil
}

// mockRepo implements outbound.OnchainPriceRepository.
type mockRepo struct {
	mu                sync.Mutex
	getOracleSource   func(ctx context.Context, name string) (*entity.OracleSource, error)
	getEnabledAssets  func(ctx context.Context, oracleSourceID int64) ([]*entity.OracleAsset, error)
	getLatestPrices   func(ctx context.Context, oracleSourceID int64) (map[int64]float64, error)
	getLatestBlock    func(ctx context.Context, oracleSourceID int64) (int64, error)
	getTokenAddresses func(ctx context.Context, oracleSourceID int64) (map[int64][]byte, error)
	upsertPrices      func(ctx context.Context, prices []*entity.OnchainTokenPrice) error
	upsertPricesCalls int
	lastUpserted      []*entity.OnchainTokenPrice
}

func (m *mockRepo) GetOracleSource(ctx context.Context, name string) (*entity.OracleSource, error) {
	if m.getOracleSource != nil {
		return m.getOracleSource(ctx, name)
	}
	return nil, fmt.Errorf("not implemented")
}

func (m *mockRepo) GetEnabledAssets(ctx context.Context, oracleSourceID int64) ([]*entity.OracleAsset, error) {
	if m.getEnabledAssets != nil {
		return m.getEnabledAssets(ctx, oracleSourceID)
	}
	return nil, fmt.Errorf("not implemented")
}

func (m *mockRepo) GetLatestPrices(ctx context.Context, oracleSourceID int64) (map[int64]float64, error) {
	if m.getLatestPrices != nil {
		return m.getLatestPrices(ctx, oracleSourceID)
	}
	return nil, fmt.Errorf("not implemented")
}

func (m *mockRepo) GetLatestBlock(ctx context.Context, oracleSourceID int64) (int64, error) {
	if m.getLatestBlock != nil {
		return m.getLatestBlock(ctx, oracleSourceID)
	}
	return 0, fmt.Errorf("not implemented")
}

func (m *mockRepo) GetTokenAddresses(ctx context.Context, oracleSourceID int64) (map[int64][]byte, error) {
	if m.getTokenAddresses != nil {
		return m.getTokenAddresses(ctx, oracleSourceID)
	}
	return nil, fmt.Errorf("not implemented")
}

func (m *mockRepo) UpsertPrices(ctx context.Context, prices []*entity.OnchainTokenPrice) error {
	m.mu.Lock()
	m.upsertPricesCalls++
	m.lastUpserted = prices
	m.mu.Unlock()
	if m.upsertPrices != nil {
		return m.upsertPrices(ctx, prices)
	}
	return nil
}

// mockMulticaller implements outbound.Multicaller.
type mockMulticaller struct {
	mu           sync.Mutex
	executeFunc  func(ctx context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error)
	executeCalls int
}

func (m *mockMulticaller) Execute(ctx context.Context, calls []outbound.Call, blockNumber *big.Int) ([]outbound.Result, error) {
	m.mu.Lock()
	m.executeCalls++
	m.mu.Unlock()
	if m.executeFunc != nil {
		return m.executeFunc(ctx, calls, blockNumber)
	}
	return nil, fmt.Errorf("not implemented")
}

func (m *mockMulticaller) Address() common.Address {
	return common.HexToAddress("0x1234567890abcdef1234567890abcdef12345678")
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func validConfig() Config {
	return Config{
		QueueURL:     "https://sqs.us-east-1.amazonaws.com/123456789/test-queue",
		OracleSource: "sparklend",
		Logger:       discardLogger(),
	}
}

func validTokenAddresses() map[int64]common.Address {
	return map[int64]common.Address{
		1: common.HexToAddress("0xC02aaA39b223FE8D0A0e5C4F27eAD9083C756Cc2"), // WETH
		2: common.HexToAddress("0x6B175474E89094C44Da98b954EedeAC495271d0F"), // DAI
	}
}

func defaultOracleSource() *entity.OracleSource {
	return &entity.OracleSource{
		ID:                  1,
		Name:                "sparklend",
		DisplayName:         "SparkLend",
		ChainID:             1,
		PoolAddressProvider: common.HexToAddress("0xaaBe6FE75ADA775eEb5C9E25e9AA2E648616ad84").Bytes(),
		DeploymentBlock:     17000000,
		Enabled:             true,
	}
}

func defaultAssets() []*entity.OracleAsset {
	return []*entity.OracleAsset{
		{ID: 1, OracleSourceID: 1, TokenID: 1, Enabled: true},
		{ID: 2, OracleSourceID: 1, TokenID: 2, Enabled: true},
	}
}

// packOracleAddrResult builds ABI-encoded return data for getPriceOracle.
// Uses a testing.T-less wrapper for compatibility with newOracleMulticaller.
func packOracleAddrResult(oracleAddr common.Address) ([]byte, error) {
	providerABI, err := abis.GetPoolAddressProviderABI()
	if err != nil {
		return nil, fmt.Errorf("getting provider ABI: %w", err)
	}
	return providerABI.Methods["getPriceOracle"].Outputs.Pack(oracleAddr)
}

// packPricesResult builds ABI-encoded return data for getAssetsPrices.
// Uses a testing.T-less wrapper for compatibility with newOracleMulticaller.
func packPricesResult(prices []*big.Int) ([]byte, error) {
	oracleABI, err := abis.GetSparkLendOracleABI()
	if err != nil {
		return nil, fmt.Errorf("getting oracle ABI: %w", err)
	}
	return oracleABI.Methods["getAssetsPrices"].Outputs.Pack(prices)
}

// newOracleMulticaller creates a mock multicaller that handles both call patterns
// used by FetchOraclePrices:
//  1. Initial 2-call batch: getPriceOracle + getAssetsPrices (when cached addr matches, prices come from call 2)
//  2. Retry 1-call batch: getAssetsPrices only (when cached addr differs from actual oracle addr)
func newOracleMulticaller(oracleAddr common.Address, prices []*big.Int) *mockMulticaller {
	addrData, _ := packOracleAddrResult(oracleAddr)
	pricesData, _ := packPricesResult(prices)

	return &mockMulticaller{
		executeFunc: func(_ context.Context, calls []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
			if len(calls) == 2 {
				// Initial batch: getPriceOracle + getAssetsPrices
				return []outbound.Result{
					{Success: true, ReturnData: addrData},
					{Success: true, ReturnData: pricesData},
				}, nil
			}
			if len(calls) == 1 {
				// Retry: getAssetsPrices only (oracle address changed)
				return []outbound.Result{
					{Success: true, ReturnData: pricesData},
				}, nil
			}
			return nil, fmt.Errorf("unexpected call count: %d", len(calls))
		},
	}
}

func makeBlockEventJSON(blockNumber int64, version int, blockTimestamp int64) string {
	event := blockEvent{
		ChainID:        1,
		BlockNumber:    blockNumber,
		Version:        version,
		BlockHash:      "0xabc123",
		BlockTimestamp: blockTimestamp,
	}
	data, _ := json.Marshal(event)
	return string(data)
}

func strPtr(s string) *string {
	return &s
}

// ---------------------------------------------------------------------------
// TestNewService
// ---------------------------------------------------------------------------

func TestNewService(t *testing.T) {
	sqsClient := &mockSQSClient{}
	multicaller := &mockMulticaller{}
	repo := &mockRepo{}
	tokenAddrs := validTokenAddresses()

	tests := []struct {
		name           string
		config         Config
		sqsClient      SQSClient
		multicaller    outbound.Multicaller
		repo           outbound.OnchainPriceRepository
		tokenAddresses map[int64]common.Address
		wantErr        bool
		errContains    string
		// For checking defaults are applied:
		checkDefaults bool
	}{
		{
			name:           "success with all valid params",
			config:         validConfig(),
			sqsClient:      sqsClient,
			multicaller:    multicaller,
			repo:           repo,
			tokenAddresses: tokenAddrs,
			wantErr:        false,
		},
		{
			name: "success with default config values",
			config: Config{
				QueueURL: "https://sqs.example.com/queue",
				// All other fields left zero/empty to test defaults
			},
			sqsClient:      sqsClient,
			multicaller:    multicaller,
			repo:           repo,
			tokenAddresses: tokenAddrs,
			wantErr:        false,
			checkDefaults:  true,
		},
		{
			name:           "error nil sqsClient",
			config:         validConfig(),
			sqsClient:      nil,
			multicaller:    multicaller,
			repo:           repo,
			tokenAddresses: tokenAddrs,
			wantErr:        true,
			errContains:    "sqsClient cannot be nil",
		},
		{
			name:           "error nil multicaller",
			config:         validConfig(),
			sqsClient:      sqsClient,
			multicaller:    nil,
			repo:           repo,
			tokenAddresses: tokenAddrs,
			wantErr:        true,
			errContains:    "multicaller cannot be nil",
		},
		{
			name:           "error nil repo",
			config:         validConfig(),
			sqsClient:      sqsClient,
			multicaller:    multicaller,
			repo:           nil,
			tokenAddresses: tokenAddrs,
			wantErr:        true,
			errContains:    "repo cannot be nil",
		},
		{
			name:           "error empty tokenAddresses",
			config:         validConfig(),
			sqsClient:      sqsClient,
			multicaller:    multicaller,
			repo:           repo,
			tokenAddresses: map[int64]common.Address{},
			wantErr:        true,
			errContains:    "tokenAddresses cannot be empty",
		},
		{
			name:           "error nil tokenAddresses",
			config:         validConfig(),
			sqsClient:      sqsClient,
			multicaller:    multicaller,
			repo:           repo,
			tokenAddresses: nil,
			wantErr:        true,
			errContains:    "tokenAddresses cannot be empty",
		},
		{
			name: "error empty queueURL",
			config: Config{
				QueueURL: "",
				Logger:   discardLogger(),
			},
			sqsClient:      sqsClient,
			multicaller:    multicaller,
			repo:           repo,
			tokenAddresses: tokenAddrs,
			wantErr:        true,
			errContains:    "queueURL is required",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			svc, err := NewService(tc.config, tc.sqsClient, tc.multicaller, tc.repo, tc.tokenAddresses)

			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if tc.errContains != "" && !containsStr(err.Error(), tc.errContains) {
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
			if svc.sqsClient == nil {
				t.Error("sqsClient should not be nil")
			}
			if svc.multicaller == nil {
				t.Error("multicaller should not be nil")
			}
			if svc.repo == nil {
				t.Error("repo should not be nil")
			}
			if svc.providerABI == nil {
				t.Error("providerABI should not be nil")
			}
			if svc.oracleABI == nil {
				t.Error("oracleABI should not be nil")
			}
			if svc.priceCache == nil {
				t.Error("priceCache should not be nil")
			}
			if svc.logger == nil {
				t.Error("logger should not be nil")
			}

			if tc.checkDefaults {
				defaults := configDefaults()
				if svc.config.MaxMessages != defaults.MaxMessages {
					t.Errorf("MaxMessages = %d, want %d", svc.config.MaxMessages, defaults.MaxMessages)
				}
				if svc.config.WaitTimeSeconds != defaults.WaitTimeSeconds {
					t.Errorf("WaitTimeSeconds = %d, want %d", svc.config.WaitTimeSeconds, defaults.WaitTimeSeconds)
				}
				if svc.config.PollInterval != defaults.PollInterval {
					t.Errorf("PollInterval = %v, want %v", svc.config.PollInterval, defaults.PollInterval)
				}
				if svc.config.OracleSource != defaults.OracleSource {
					t.Errorf("OracleSource = %q, want %q", svc.config.OracleSource, defaults.OracleSource)
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
	oracleAddr := common.HexToAddress("0x8105f69D9C41644c6A0803fDA7D03Aa70996cFD2")

	tests := []struct {
		name        string
		setupRepo   func(r *mockRepo)
		tokenAddrs  map[int64]common.Address
		wantErr     bool
		errContains string
	}{
		{
			name: "success",
			setupRepo: func(r *mockRepo) {
				r.getOracleSource = func(_ context.Context, _ string) (*entity.OracleSource, error) {
					return defaultOracleSource(), nil
				}
				r.getEnabledAssets = func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) {
					return defaultAssets(), nil
				}
				r.getLatestPrices = func(_ context.Context, _ int64) (map[int64]float64, error) {
					return map[int64]float64{1: 2000.0, 2: 1.0}, nil
				}
			},
			tokenAddrs: validTokenAddresses(),
			wantErr:    false,
		},
		{
			name: "error GetOracleSource fails",
			setupRepo: func(r *mockRepo) {
				r.getOracleSource = func(_ context.Context, _ string) (*entity.OracleSource, error) {
					return nil, fmt.Errorf("db connection refused")
				}
			},
			tokenAddrs:  validTokenAddresses(),
			wantErr:     true,
			errContains: "getting oracle source",
		},
		{
			name: "error GetEnabledAssets fails",
			setupRepo: func(r *mockRepo) {
				r.getOracleSource = func(_ context.Context, _ string) (*entity.OracleSource, error) {
					return defaultOracleSource(), nil
				}
				r.getEnabledAssets = func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) {
					return nil, fmt.Errorf("query timeout")
				}
			},
			tokenAddrs:  validTokenAddresses(),
			wantErr:     true,
			errContains: "getting enabled assets",
		},
		{
			name: "error GetEnabledAssets returns empty",
			setupRepo: func(r *mockRepo) {
				r.getOracleSource = func(_ context.Context, _ string) (*entity.OracleSource, error) {
					return defaultOracleSource(), nil
				}
				r.getEnabledAssets = func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) {
					return []*entity.OracleAsset{}, nil
				}
			},
			tokenAddrs:  validTokenAddresses(),
			wantErr:     true,
			errContains: "no enabled assets",
		},
		{
			name: "error token address not found in tokenAddressMap",
			setupRepo: func(r *mockRepo) {
				r.getOracleSource = func(_ context.Context, _ string) (*entity.OracleSource, error) {
					return defaultOracleSource(), nil
				}
				r.getEnabledAssets = func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) {
					return []*entity.OracleAsset{
						{ID: 1, OracleSourceID: 1, TokenID: 999, Enabled: true}, // 999 not in tokenAddressMap
					}, nil
				}
			},
			tokenAddrs:  validTokenAddresses(), // has 1, 2 but not 999
			wantErr:     true,
			errContains: "token address not found for token_id 999",
		},
		{
			name: "error GetLatestPrices fails",
			setupRepo: func(r *mockRepo) {
				r.getOracleSource = func(_ context.Context, _ string) (*entity.OracleSource, error) {
					return defaultOracleSource(), nil
				}
				r.getEnabledAssets = func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) {
					return defaultAssets(), nil
				}
				r.getLatestPrices = func(_ context.Context, _ int64) (map[int64]float64, error) {
					return nil, fmt.Errorf("redis unavailable")
				}
			},
			tokenAddrs:  validTokenAddresses(),
			wantErr:     true,
			errContains: "loading latest prices",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			repo := &mockRepo{}
			tc.setupRepo(repo)

			// For success case, provide a mock SQS that blocks on receive so the goroutine
			// doesn't spin. For error cases, it doesn't matter since Start returns early.
			sqsClient := &mockSQSClient{
				receiveMessageFunc: func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
					// Block until context is cancelled to avoid spinning
					<-ctx.Done()
					return nil, ctx.Err()
				},
			}

			mc := newOracleMulticaller(oracleAddr, []*big.Int{
				new(big.Int).Mul(big.NewInt(2000), big.NewInt(1e8)),
				new(big.Int).Mul(big.NewInt(1), big.NewInt(1e8)),
			})

			svc, err := NewService(validConfig(), sqsClient, mc, repo, tc.tokenAddrs)
			if err != nil {
				t.Fatalf("NewService failed: %v", err)
			}

			ctx := context.Background()
			err = svc.Start(ctx)

			if tc.wantErr {
				if err == nil {
					t.Fatal("expected error, got nil")
				}
				if tc.errContains != "" && !containsStr(err.Error(), tc.errContains) {
					t.Errorf("error %q does not contain %q", err.Error(), tc.errContains)
				}
				return
			}

			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}

			// Verify initialization state
			if svc.oracleSource == nil {
				t.Error("oracleSource not set after Start")
			}
			if len(svc.assets) == 0 {
				t.Error("assets not set after Start")
			}
			if len(svc.tokenAddrs) != len(svc.assets) {
				t.Errorf("tokenAddrs length = %d, want %d", len(svc.tokenAddrs), len(svc.assets))
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
	oracleAddr := common.HexToAddress("0x8105f69D9C41644c6A0803fDA7D03Aa70996cFD2")
	blockTimestamp := time.Date(2024, 1, 1, 0, 0, 0, 0, time.UTC).Unix()

	t.Run("end to end: new prices are upserted, same prices are skipped", func(t *testing.T) {
		repo := &mockRepo{}
		repo.getOracleSource = func(_ context.Context, _ string) (*entity.OracleSource, error) {
			return defaultOracleSource(), nil
		}
		repo.getEnabledAssets = func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) {
			return defaultAssets(), nil
		}
		repo.getLatestPrices = func(_ context.Context, _ int64) (map[int64]float64, error) {
			return map[int64]float64{}, nil // empty cache means all prices are new
		}
		repo.upsertPrices = func(_ context.Context, prices []*entity.OnchainTokenPrice) error {
			return nil
		}

		price1 := new(big.Int).Mul(big.NewInt(200000), big.NewInt(1e4)) // 2000.00 * 1e8
		price2 := new(big.Int).Mul(big.NewInt(100), big.NewInt(1e6))    // 1.00 * 1e8

		mc := newOracleMulticaller(oracleAddr, []*big.Int{price1, price2})

		// Deliver one message on first call, then empty on subsequent calls.
		messageDelivered := false
		body1 := makeBlockEventJSON(18000000, 1, blockTimestamp)
		receipt1 := "receipt-1"

		sqsClient := &mockSQSClient{
			receiveMessageFunc: func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				if !messageDelivered {
					messageDelivered = true
					return &sqs.ReceiveMessageOutput{
						Messages: []sqstypes.Message{
							{Body: strPtr(body1), ReceiptHandle: strPtr(receipt1)},
						},
					}, nil
				}
				// After the first message, block until context is done
				<-ctx.Done()
				return nil, ctx.Err()
			},
		}

		cfg := validConfig()
		cfg.PollInterval = 1 * time.Millisecond

		svc, err := NewService(cfg, sqsClient, mc, repo, validTokenAddresses())
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}

		ctx := context.Background()
		if err := svc.Start(ctx); err != nil {
			t.Fatalf("Start: %v", err)
		}

		// Wait for processing
		deadline := time.After(2 * time.Second)
		for {
			repo.mu.Lock()
			upserts := repo.upsertPricesCalls
			repo.mu.Unlock()
			if upserts >= 1 {
				break
			}
			select {
			case <-deadline:
				t.Fatal("timed out waiting for UpsertPrices to be called")
			default:
				time.Sleep(5 * time.Millisecond)
			}
		}

		// Verify UpsertPrices was called with 2 prices (both new)
		repo.mu.Lock()
		if repo.upsertPricesCalls < 1 {
			t.Errorf("UpsertPrices call count = %d, want >= 1", repo.upsertPricesCalls)
		}
		if len(repo.lastUpserted) != 2 {
			t.Errorf("lastUpserted length = %d, want 2", len(repo.lastUpserted))
		}
		repo.mu.Unlock()

		// Verify delete was called
		sqsClient.mu.Lock()
		if sqsClient.deleteMessageCalls < 1 {
			t.Errorf("DeleteMessage call count = %d, want >= 1", sqsClient.deleteMessageCalls)
		}
		sqsClient.mu.Unlock()

		// Now verify change detection: second block with same prices should not upsert.
		// Reset repo call count and deliver a second message.
		repo.mu.Lock()
		prevUpserts := repo.upsertPricesCalls
		repo.mu.Unlock()

		// Directly call processBlock to test change detection without SQS timing complexity
		event2 := blockEvent{
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
		repo.getOracleSource = func(_ context.Context, _ string) (*entity.OracleSource, error) {
			return defaultOracleSource(), nil
		}
		repo.getEnabledAssets = func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) {
			return defaultAssets(), nil
		}
		repo.getLatestPrices = func(_ context.Context, _ int64) (map[int64]float64, error) {
			return map[int64]float64{}, nil
		}

		sqsClient := &mockSQSClient{
			receiveMessageFunc: func(_ context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
				return nil, fmt.Errorf("SQS service unavailable")
			},
		}

		mc := &mockMulticaller{}
		cfg := validConfig()
		cfg.PollInterval = 1 * time.Millisecond

		svc, err := NewService(cfg, sqsClient, mc, repo, validTokenAddresses())
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}

		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}

		// Let the processLoop run and encounter the error
		time.Sleep(50 * time.Millisecond)

		// Verify processMessages was attempted (no crash)
		sqsClient.mu.Lock()
		if sqsClient.receiveMessageCalls == 0 {
			t.Error("expected at least one ReceiveMessage call")
		}
		sqsClient.mu.Unlock()

		if stopErr := svc.Stop(); stopErr != nil {
			t.Errorf("Stop: %v", stopErr)
		}
	})

	t.Run("SQS returns empty messages", func(t *testing.T) {
		repo := &mockRepo{}
		repo.getOracleSource = func(_ context.Context, _ string) (*entity.OracleSource, error) {
			return defaultOracleSource(), nil
		}
		repo.getEnabledAssets = func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) {
			return defaultAssets(), nil
		}
		repo.getLatestPrices = func(_ context.Context, _ int64) (map[int64]float64, error) {
			return map[int64]float64{}, nil
		}

		sqsClient := &mockSQSClient{
			receiveMessageFunc: func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
				return &sqs.ReceiveMessageOutput{Messages: []sqstypes.Message{}}, nil
			},
		}

		mc := &mockMulticaller{}
		cfg := validConfig()
		cfg.PollInterval = 1 * time.Millisecond

		svc, err := NewService(cfg, sqsClient, mc, repo, validTokenAddresses())
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
		repo.getOracleSource = func(_ context.Context, _ string) (*entity.OracleSource, error) {
			return defaultOracleSource(), nil
		}
		repo.getEnabledAssets = func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) {
			return defaultAssets(), nil
		}
		repo.getLatestPrices = func(_ context.Context, _ int64) (map[int64]float64, error) {
			return map[int64]float64{}, nil
		}

		delivered := false
		sqsClient := &mockSQSClient{
			receiveMessageFunc: func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				if !delivered {
					delivered = true
					return &sqs.ReceiveMessageOutput{
						Messages: []sqstypes.Message{
							{Body: nil, ReceiptHandle: strPtr("receipt-nil")},
						},
					}, nil
				}
				<-ctx.Done()
				return nil, ctx.Err()
			},
		}

		mc := &mockMulticaller{}
		cfg := validConfig()
		cfg.PollInterval = 1 * time.Millisecond

		svc, err := NewService(cfg, sqsClient, mc, repo, validTokenAddresses())
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}

		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}

		time.Sleep(50 * time.Millisecond)

		// Message should not have been deleted (process failed)
		sqsClient.mu.Lock()
		if sqsClient.deleteMessageCalls != 0 {
			t.Errorf("DeleteMessage call count = %d, want 0 (nil body should fail)", sqsClient.deleteMessageCalls)
		}
		sqsClient.mu.Unlock()

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
		repo.getOracleSource = func(_ context.Context, _ string) (*entity.OracleSource, error) {
			return defaultOracleSource(), nil
		}
		repo.getEnabledAssets = func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) {
			return defaultAssets(), nil
		}
		repo.getLatestPrices = func(_ context.Context, _ int64) (map[int64]float64, error) {
			return map[int64]float64{}, nil
		}

		delivered := false
		sqsClient := &mockSQSClient{
			receiveMessageFunc: func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				if !delivered {
					delivered = true
					return &sqs.ReceiveMessageOutput{
						Messages: []sqstypes.Message{
							{Body: strPtr("not valid json{{{"), ReceiptHandle: strPtr("receipt-bad-json")},
						},
					}, nil
				}
				<-ctx.Done()
				return nil, ctx.Err()
			},
		}

		mc := &mockMulticaller{}
		cfg := validConfig()
		cfg.PollInterval = 1 * time.Millisecond

		svc, err := NewService(cfg, sqsClient, mc, repo, validTokenAddresses())
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}

		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}

		time.Sleep(50 * time.Millisecond)

		// Message should not have been deleted
		sqsClient.mu.Lock()
		if sqsClient.deleteMessageCalls != 0 {
			t.Errorf("DeleteMessage call count = %d, want 0 (invalid JSON)", sqsClient.deleteMessageCalls)
		}
		sqsClient.mu.Unlock()

		if stopErr := svc.Stop(); stopErr != nil {
			t.Errorf("Stop: %v", stopErr)
		}
	})

	t.Run("multicall returns error during processBlock", func(t *testing.T) {
		repo := &mockRepo{}
		repo.getOracleSource = func(_ context.Context, _ string) (*entity.OracleSource, error) {
			return defaultOracleSource(), nil
		}
		repo.getEnabledAssets = func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) {
			return defaultAssets(), nil
		}
		repo.getLatestPrices = func(_ context.Context, _ int64) (map[int64]float64, error) {
			return map[int64]float64{}, nil
		}

		delivered := false
		body := makeBlockEventJSON(18000000, 1, blockTimestamp)
		sqsClient := &mockSQSClient{
			receiveMessageFunc: func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				if !delivered {
					delivered = true
					return &sqs.ReceiveMessageOutput{
						Messages: []sqstypes.Message{
							{Body: strPtr(body), ReceiptHandle: strPtr("receipt-mc-err")},
						},
					}, nil
				}
				<-ctx.Done()
				return nil, ctx.Err()
			},
		}

		mc := &mockMulticaller{
			executeFunc: func(_ context.Context, _ []outbound.Call, _ *big.Int) ([]outbound.Result, error) {
				return nil, fmt.Errorf("RPC node timeout")
			},
		}

		cfg := validConfig()
		cfg.PollInterval = 1 * time.Millisecond

		svc, err := NewService(cfg, sqsClient, mc, repo, validTokenAddresses())
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}

		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}

		time.Sleep(50 * time.Millisecond)

		// Message should not have been deleted (processBlock failed)
		sqsClient.mu.Lock()
		if sqsClient.deleteMessageCalls != 0 {
			t.Errorf("DeleteMessage call count = %d, want 0 (multicall error)", sqsClient.deleteMessageCalls)
		}
		sqsClient.mu.Unlock()

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
		repo.getOracleSource = func(_ context.Context, _ string) (*entity.OracleSource, error) {
			return defaultOracleSource(), nil
		}
		repo.getEnabledAssets = func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) {
			return defaultAssets(), nil
		}
		repo.getLatestPrices = func(_ context.Context, _ int64) (map[int64]float64, error) {
			return map[int64]float64{}, nil
		}

		price1 := new(big.Int).Mul(big.NewInt(2000), big.NewInt(1e8))
		price2 := new(big.Int).Mul(big.NewInt(1), big.NewInt(1e8))
		mc := newOracleMulticaller(oracleAddr, []*big.Int{price1, price2})

		delivered := false
		body := makeBlockEventJSON(18000000, 1, blockTimestamp)
		sqsClient := &mockSQSClient{
			receiveMessageFunc: func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				if !delivered {
					delivered = true
					return &sqs.ReceiveMessageOutput{
						Messages: []sqstypes.Message{
							{Body: strPtr(body), ReceiptHandle: strPtr("receipt-del-err")},
						},
					}, nil
				}
				<-ctx.Done()
				return nil, ctx.Err()
			},
			deleteMessageFunc: func(_ context.Context, _ *sqs.DeleteMessageInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageOutput, error) {
				return nil, fmt.Errorf("SQS delete failed")
			},
		}

		cfg := validConfig()
		cfg.PollInterval = 1 * time.Millisecond

		svc, err := NewService(cfg, sqsClient, mc, repo, validTokenAddresses())
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}

		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}

		// Wait for processing
		deadline := time.After(2 * time.Second)
		for {
			repo.mu.Lock()
			upserts := repo.upsertPricesCalls
			repo.mu.Unlock()
			if upserts >= 1 {
				break
			}
			select {
			case <-deadline:
				t.Fatal("timed out waiting for UpsertPrices to be called")
			default:
				time.Sleep(5 * time.Millisecond)
			}
		}

		// UpsertPrices was called (message processed successfully)
		repo.mu.Lock()
		if repo.upsertPricesCalls < 1 {
			t.Errorf("UpsertPrices call count = %d, want >= 1", repo.upsertPricesCalls)
		}
		repo.mu.Unlock()

		// DeleteMessage was attempted (even though it failed)
		sqsClient.mu.Lock()
		if sqsClient.deleteMessageCalls < 1 {
			t.Errorf("DeleteMessage call count = %d, want >= 1", sqsClient.deleteMessageCalls)
		}
		sqsClient.mu.Unlock()

		if stopErr := svc.Stop(); stopErr != nil {
			t.Errorf("Stop: %v", stopErr)
		}
	})

	t.Run("price count mismatch from oracle", func(t *testing.T) {
		repo := &mockRepo{}
		repo.getOracleSource = func(_ context.Context, _ string) (*entity.OracleSource, error) {
			return defaultOracleSource(), nil
		}
		repo.getEnabledAssets = func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) {
			return defaultAssets(), nil
		}
		repo.getLatestPrices = func(_ context.Context, _ int64) (map[int64]float64, error) {
			return map[int64]float64{}, nil
		}

		// Return only 1 price for 2 tokens
		onePrice := []*big.Int{new(big.Int).Mul(big.NewInt(2000), big.NewInt(1e8))}
		mc := newOracleMulticaller(oracleAddr, onePrice)

		delivered := false
		body := makeBlockEventJSON(18000000, 1, blockTimestamp)
		sqsClient := &mockSQSClient{
			receiveMessageFunc: func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				if !delivered {
					delivered = true
					return &sqs.ReceiveMessageOutput{
						Messages: []sqstypes.Message{
							{Body: strPtr(body), ReceiptHandle: strPtr("receipt-mismatch")},
						},
					}, nil
				}
				<-ctx.Done()
				return nil, ctx.Err()
			},
		}

		cfg := validConfig()
		cfg.PollInterval = 1 * time.Millisecond

		svc, err := NewService(cfg, sqsClient, mc, repo, validTokenAddresses())
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}

		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}

		time.Sleep(50 * time.Millisecond)

		// Message should NOT have been deleted (processBlock returned error)
		sqsClient.mu.Lock()
		if sqsClient.deleteMessageCalls != 0 {
			t.Errorf("DeleteMessage call count = %d, want 0 (price mismatch)", sqsClient.deleteMessageCalls)
		}
		sqsClient.mu.Unlock()

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
		repo.getOracleSource = func(_ context.Context, _ string) (*entity.OracleSource, error) {
			return defaultOracleSource(), nil
		}
		repo.getEnabledAssets = func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) {
			return defaultAssets(), nil
		}
		repo.getLatestPrices = func(_ context.Context, _ int64) (map[int64]float64, error) {
			return map[int64]float64{}, nil
		}
		repo.upsertPrices = func(_ context.Context, _ []*entity.OnchainTokenPrice) error {
			return fmt.Errorf("database write failure")
		}

		price1 := new(big.Int).Mul(big.NewInt(2000), big.NewInt(1e8))
		price2 := new(big.Int).Mul(big.NewInt(1), big.NewInt(1e8))

		mc := newOracleMulticaller(oracleAddr, []*big.Int{price1, price2})

		delivered := false
		body := makeBlockEventJSON(18000000, 1, blockTimestamp)
		sqsClient := &mockSQSClient{
			receiveMessageFunc: func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				if !delivered {
					delivered = true
					return &sqs.ReceiveMessageOutput{
						Messages: []sqstypes.Message{
							{Body: strPtr(body), ReceiptHandle: strPtr("receipt-upsert-err")},
						},
					}, nil
				}
				<-ctx.Done()
				return nil, ctx.Err()
			},
		}

		cfg := validConfig()
		cfg.PollInterval = 1 * time.Millisecond

		svc, err := NewService(cfg, sqsClient, mc, repo, validTokenAddresses())
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}

		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}

		// Wait for the processing attempt
		deadline := time.After(2 * time.Second)
		for {
			repo.mu.Lock()
			calls := repo.upsertPricesCalls
			repo.mu.Unlock()
			if calls >= 1 {
				break
			}
			select {
			case <-deadline:
				t.Fatal("timed out waiting for UpsertPrices to be called")
			default:
				time.Sleep(5 * time.Millisecond)
			}
		}

		// Message should NOT have been deleted because processMessage returned error
		sqsClient.mu.Lock()
		if sqsClient.deleteMessageCalls != 0 {
			t.Errorf("DeleteMessage call count = %d, want 0 (UpsertPrices failed)", sqsClient.deleteMessageCalls)
		}
		sqsClient.mu.Unlock()

		if stopErr := svc.Stop(); stopErr != nil {
			t.Errorf("Stop: %v", stopErr)
		}
	})

	t.Run("entity validation error with blockNumber zero", func(t *testing.T) {
		repo := &mockRepo{}
		repo.getOracleSource = func(_ context.Context, _ string) (*entity.OracleSource, error) {
			return defaultOracleSource(), nil
		}
		repo.getEnabledAssets = func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) {
			return defaultAssets(), nil
		}
		repo.getLatestPrices = func(_ context.Context, _ int64) (map[int64]float64, error) {
			return map[int64]float64{}, nil
		}

		price1 := new(big.Int).Mul(big.NewInt(2000), big.NewInt(1e8))
		price2 := new(big.Int).Mul(big.NewInt(1), big.NewInt(1e8))
		mc := newOracleMulticaller(oracleAddr, []*big.Int{price1, price2})

		sqsClient := &mockSQSClient{
			receiveMessageFunc: func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
				<-ctx.Done()
				return nil, ctx.Err()
			},
		}

		cfg := validConfig()
		cfg.PollInterval = 1 * time.Millisecond

		svc, err := NewService(cfg, sqsClient, mc, repo, validTokenAddresses())
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}

		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}

		// Directly call processBlock with blockNumber=0 to trigger entity validation error
		event := blockEvent{
			ChainID:        1,
			BlockNumber:    0, // will fail entity validation: "blockNumber must be positive"
			Version:        1,
			BlockHash:      "0xabc",
			BlockTimestamp: blockTimestamp,
		}
		err = svc.processBlock(context.Background(), event)
		// processBlock should succeed (no price changes after all entities fail validation)
		if err != nil {
			t.Errorf("processBlock with blockNumber=0 returned unexpected error: %v", err)
		}

		// No prices should have been upserted (all failed validation)
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
// TestStop
// ---------------------------------------------------------------------------

func TestStop(t *testing.T) {
	t.Run("stop without start (cancel is nil)", func(t *testing.T) {
		repo := &mockRepo{}
		sqsClient := &mockSQSClient{}
		mc := &mockMulticaller{}

		svc, err := NewService(validConfig(), sqsClient, mc, repo, validTokenAddresses())
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
		repo.getOracleSource = func(_ context.Context, _ string) (*entity.OracleSource, error) {
			return defaultOracleSource(), nil
		}
		repo.getEnabledAssets = func(_ context.Context, _ int64) ([]*entity.OracleAsset, error) {
			return defaultAssets(), nil
		}
		repo.getLatestPrices = func(_ context.Context, _ int64) (map[int64]float64, error) {
			return map[int64]float64{}, nil
		}

		sqsClient := &mockSQSClient{
			receiveMessageFunc: func(ctx context.Context, _ *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
				<-ctx.Done()
				return nil, ctx.Err()
			},
		}
		mc := &mockMulticaller{}

		cfg := validConfig()
		cfg.PollInterval = 1 * time.Millisecond

		svc, err := NewService(cfg, sqsClient, mc, repo, validTokenAddresses())
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

// ---------------------------------------------------------------------------
// Utility
// ---------------------------------------------------------------------------

func containsStr(s, substr string) bool {
	return len(s) >= len(substr) && searchStr(s, substr)
}

func searchStr(s, substr string) bool {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
