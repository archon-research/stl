package maple_indexer

import (
	"context"
	"encoding/json"
	"fmt"
	"math/big"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/jackc/pgx/v5"

	"github.com/archon-research/stl/stl-verify/internal/domain/entity"
	"github.com/archon-research/stl/stl-verify/internal/ports/outbound"
	"github.com/archon-research/stl/stl-verify/internal/testutil"
)

// ---------------------------------------------------------------------------
// Mocks
// ---------------------------------------------------------------------------

type mockConsumer struct {
	mu                  sync.Mutex
	receiveMessagesFn   func(ctx context.Context, maxMessages int) ([]outbound.SQSMessage, error)
	deleteMessageFn     func(ctx context.Context, receiptHandle string) error
	closeFn             func() error
	deleteMessageCalls  int
	receiveMessageCalls int
	closeCalls          int
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
	m.mu.Lock()
	m.closeCalls++
	m.mu.Unlock()
	if m.closeFn != nil {
		return m.closeFn()
	}
	return nil
}

type mockMapleClient struct {
	mu                       sync.Mutex
	getAccountPositionsFn    func(ctx context.Context, address common.Address) ([]outbound.MaplePoolPosition, error)
	getPoolCollateralFn      func(ctx context.Context, poolAddress common.Address) (*outbound.MaplePoolData, error)
	getAccountPositionsCalls int
	getPoolCollateralCalls   int
}

func (m *mockMapleClient) GetAccountPositions(ctx context.Context, address common.Address) ([]outbound.MaplePoolPosition, error) {
	m.mu.Lock()
	m.getAccountPositionsCalls++
	m.mu.Unlock()
	if m.getAccountPositionsFn != nil {
		return m.getAccountPositionsFn(ctx, address)
	}
	return nil, nil
}

func (m *mockMapleClient) GetPoolCollateral(ctx context.Context, poolAddress common.Address) (*outbound.MaplePoolData, error) {
	m.mu.Lock()
	m.getPoolCollateralCalls++
	m.mu.Unlock()
	if m.getPoolCollateralFn != nil {
		return m.getPoolCollateralFn(ctx, poolAddress)
	}
	return nil, nil
}

type mockMapleRepo struct {
	mu                              sync.Mutex
	upsertPositionsFn               func(ctx context.Context, positions []*entity.MaplePosition) error
	upsertPoolCollateralFn          func(ctx context.Context, collaterals []*entity.MaplePoolCollateral) error
	getUsersWithMaplePositionsFn    func(ctx context.Context, protocolID int64) ([]outbound.MapleTrackedUser, error)
	upsertPositionsCalls            int
	upsertPoolCollateralCalls       int
	getUsersWithMaplePositionsCalls int
	lastUpsertedPositions           []*entity.MaplePosition
	lastUpsertedCollaterals         []*entity.MaplePoolCollateral
}

func (m *mockMapleRepo) UpsertPositions(ctx context.Context, positions []*entity.MaplePosition) error {
	m.mu.Lock()
	m.upsertPositionsCalls++
	m.lastUpsertedPositions = positions
	m.mu.Unlock()
	if m.upsertPositionsFn != nil {
		return m.upsertPositionsFn(ctx, positions)
	}
	return nil
}

func (m *mockMapleRepo) UpsertPoolCollateral(ctx context.Context, collaterals []*entity.MaplePoolCollateral) error {
	m.mu.Lock()
	m.upsertPoolCollateralCalls++
	m.lastUpsertedCollaterals = collaterals
	m.mu.Unlock()
	if m.upsertPoolCollateralFn != nil {
		return m.upsertPoolCollateralFn(ctx, collaterals)
	}
	return nil
}

func (m *mockMapleRepo) GetUsersWithMaplePositions(ctx context.Context, protocolID int64) ([]outbound.MapleTrackedUser, error) {
	m.mu.Lock()
	m.getUsersWithMaplePositionsCalls++
	m.mu.Unlock()
	if m.getUsersWithMaplePositionsFn != nil {
		return m.getUsersWithMaplePositionsFn(ctx, protocolID)
	}
	return nil, nil
}

type mockProtocolRepo struct {
	mu                     sync.Mutex
	getProtocolByAddrFn    func(ctx context.Context, chainID int64, address common.Address) (*entity.Protocol, error)
	getProtocolByAddrCalls int
}

func (m *mockProtocolRepo) GetProtocolByAddress(ctx context.Context, chainID int64, address common.Address) (*entity.Protocol, error) {
	m.mu.Lock()
	m.getProtocolByAddrCalls++
	m.mu.Unlock()
	if m.getProtocolByAddrFn != nil {
		return m.getProtocolByAddrFn(ctx, chainID, address)
	}
	return nil, nil
}

func (m *mockProtocolRepo) UpsertReserveData(ctx context.Context, tx pgx.Tx, data []*entity.SparkLendReserveData) error {
	return nil
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func validServiceConfig() Config {
	return Config{
		ChainID:         1,
		ProtocolAddress: common.HexToAddress("0x804a6F5F667170F545Bf14e5DDB48C70B788390C"),
		Logger:          testutil.DiscardLogger(),
	}
}

func defaultProtocolRepo() *mockProtocolRepo {
	return &mockProtocolRepo{
		getProtocolByAddrFn: func(_ context.Context, chainID int64, address common.Address) (*entity.Protocol, error) {
			return &entity.Protocol{
				ID:             1,
				ChainID:        int(chainID),
				Address:        address.Bytes(),
				Name:           "Maple Finance",
				ProtocolType:   "rwa",
				CreatedAtBlock: 11964925,
				Metadata:       map[string]any{},
			}, nil
		},
	}
}

// testPoolAddress returns a common.Address for "0x80ac24aa929eaf5013f6436cda2a7ba190f5cc0b".
func testPoolAddress() common.Address {
	return common.HexToAddress("0x80ac24aa929eaf5013f6436cda2a7ba190f5cc0b")
}

func testPoolAddress2() common.Address {
	return common.HexToAddress("0x356b8d89c1e1239cbbb9de4815c39a1474d5ba7d")
}

func makeMapleBlockEventJSON(blockNumber int64, blockTimestamp int64) string {
	event := blockEvent{
		ChainID:        1,
		BlockNumber:    blockNumber,
		Version:        1,
		BlockHash:      "0xabc123",
		BlockTimestamp: blockTimestamp,
	}
	data, _ := json.Marshal(event)
	return string(data)
}

func defaultTrackedUsers() []outbound.MapleTrackedUser {
	return []outbound.MapleTrackedUser{
		{UserID: 1, Address: common.HexToAddress("0x1601843c5e9bc251a3272907010afa41fa18347e")},
	}
}

func defaultAPIPositions() []outbound.MaplePoolPosition {
	return []outbound.MaplePoolPosition{
		{
			PoolAddress:    testPoolAddress(),
			PoolName:       "Syrup USDC",
			AssetSymbol:    "USDC",
			AssetDecimals:  6,
			LendingBalance: big.NewInt(200_000_000_000_000), // $200M with 6 decimals
		},
	}
}

func defaultPoolData() *outbound.MaplePoolData {
	return &outbound.MaplePoolData{
		TVL: big.NewInt(1_000_000_000_000_000), // $1B with 6 decimals
		Collaterals: []outbound.MapleCollateral{
			{Asset: "BTC", AssetValueUSD: big.NewInt(674_000_000_000_000), AssetDecimals: 8},
			{Asset: "XRP", AssetValueUSD: big.NewInt(317_000_000_000_000), AssetDecimals: 6},
		},
	}
}

func blockingConsumer() *mockConsumer {
	return &mockConsumer{
		receiveMessagesFn: func(ctx context.Context, _ int) ([]outbound.SQSMessage, error) {
			<-ctx.Done()
			return nil, ctx.Err()
		},
	}
}

// defaultRepoSetup configures the mock repo with defaults for successful operation.
func defaultRepoSetup(repo *mockMapleRepo) {
	repo.getUsersWithMaplePositionsFn = func(_ context.Context, _ int64) ([]outbound.MapleTrackedUser, error) {
		return defaultTrackedUsers(), nil
	}
}

func defaultClientSetup(client *mockMapleClient) {
	client.getAccountPositionsFn = func(_ context.Context, _ common.Address) ([]outbound.MaplePoolPosition, error) {
		return defaultAPIPositions(), nil
	}
	client.getPoolCollateralFn = func(_ context.Context, _ common.Address) (*outbound.MaplePoolData, error) {
		return defaultPoolData(), nil
	}
}

// ---------------------------------------------------------------------------
// TestNewService
// ---------------------------------------------------------------------------

func TestNewService(t *testing.T) {
	consumer := &mockConsumer{}
	mapleAPI := &mockMapleClient{}
	mapleRepo := &mockMapleRepo{}
	protocolRepo := &mockProtocolRepo{}
	protocolRepo.getProtocolByAddrFn = func(_ context.Context, _ int64, _ common.Address) (*entity.Protocol, error) {
		return &entity.Protocol{
			ID:             1,
			ChainID:        1,
			Address:        common.HexToAddress("0x804a6F5F667170F545Bf14e5DDB48C70B788390C").Bytes(),
			Name:           "Maple Finance",
			ProtocolType:   "rwa",
			CreatedAtBlock: 11964925,
			Metadata:       map[string]any{},
		}, nil
	}

	tests := []struct {
		name         string
		config       Config
		consumer     outbound.SQSConsumer
		mapleAPI     outbound.MapleClient
		mapleRepo    outbound.MapleRepository
		protocolRepo outbound.ProtocolRepository
		wantErr      bool
		errContains  string
	}{
		{
			name:         "success with all valid params",
			config:       validServiceConfig(),
			consumer:     consumer,
			mapleAPI:     mapleAPI,
			mapleRepo:    mapleRepo,
			protocolRepo: protocolRepo,
		},
		{
			name:         "error nil consumer",
			config:       validServiceConfig(),
			consumer:     nil,
			mapleAPI:     mapleAPI,
			mapleRepo:    mapleRepo,
			protocolRepo: protocolRepo,
			wantErr:      true,
			errContains:  "consumer cannot be nil",
		},
		{
			name:         "error nil mapleAPI",
			config:       validServiceConfig(),
			consumer:     consumer,
			mapleAPI:     nil,
			mapleRepo:    mapleRepo,
			protocolRepo: protocolRepo,
			wantErr:      true,
			errContains:  "mapleAPI cannot be nil",
		},
		{
			name:         "error nil mapleRepo",
			config:       validServiceConfig(),
			consumer:     consumer,
			mapleAPI:     mapleAPI,
			mapleRepo:    nil,
			protocolRepo: protocolRepo,
			wantErr:      true,
			errContains:  "mapleRepo cannot be nil",
		},
		{
			name: "error protocol address zero",
			config: Config{
				ProtocolAddress: common.Address{},
				Logger:          testutil.DiscardLogger(),
			},
			consumer:     consumer,
			mapleAPI:     mapleAPI,
			mapleRepo:    mapleRepo,
			protocolRepo: protocolRepo,
			wantErr:      true,
			errContains:  "protocolAddress must be set",
		},
		{
			name: "error nil protocol repo",
			config: Config{
				ProtocolAddress: common.HexToAddress("0x804a6F5F667170F545Bf14e5DDB48C70B788390C"),
				Logger:          testutil.DiscardLogger(),
			},
			consumer:     consumer,
			mapleAPI:     mapleAPI,
			mapleRepo:    mapleRepo,
			protocolRepo: nil,
			wantErr:      true,
			errContains:  "protocolRepo cannot be nil",
		},
		{
			name: "defaults are applied for zero config",
			config: Config{
				ProtocolAddress: common.HexToAddress("0x804a6F5F667170F545Bf14e5DDB48C70B788390C"),
			},
			consumer:     consumer,
			mapleAPI:     mapleAPI,
			mapleRepo:    mapleRepo,
			protocolRepo: protocolRepo,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			svc, err := NewService(tc.config, tc.consumer, tc.mapleAPI, tc.mapleRepo, tc.protocolRepo)

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
			if svc.consumer == nil {
				t.Error("consumer should not be nil")
			}
			if svc.mapleAPI == nil {
				t.Error("mapleAPI should not be nil")
			}
			if svc.mapleRepo == nil {
				t.Error("mapleRepo should not be nil")
			}
			if svc.logger == nil {
				t.Error("logger should not be nil")
			}
		})
	}
}

// ---------------------------------------------------------------------------
// TestStartStop
// ---------------------------------------------------------------------------

func TestStartStop(t *testing.T) {
	t.Run("start and stop", func(t *testing.T) {
		repo := &mockMapleRepo{}
		defaultRepoSetup(repo)
		client := &mockMapleClient{}
		defaultClientSetup(client)

		protocolRepo := defaultProtocolRepo()
		svc, err := NewService(validServiceConfig(), blockingConsumer(), client, repo, protocolRepo)
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}

		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}

		if svc.ctx.Err() != nil {
			t.Error("context should not be cancelled before Stop")
		}

		if err := svc.Stop(); err != nil {
			t.Errorf("Stop: %v", err)
		}

		if svc.ctx.Err() == nil {
			t.Error("context should be cancelled after Stop")
		}
	})

	t.Run("stop without start", func(t *testing.T) {
		protocolRepo := defaultProtocolRepo()
		consumer := &mockConsumer{}
		svc, err := NewService(validServiceConfig(), consumer, &mockMapleClient{}, &mockMapleRepo{}, protocolRepo)
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}

		if err := svc.Stop(); err != nil {
			t.Errorf("Stop without Start should not error, got: %v", err)
		}

		consumer.mu.Lock()
		if consumer.closeCalls != 1 {
			t.Errorf("Close calls = %d, want 1", consumer.closeCalls)
		}
		consumer.mu.Unlock()
	})
}

// ---------------------------------------------------------------------------
// TestProcessBlock (end-to-end message processing via processBlock)
// ---------------------------------------------------------------------------

func TestProcessBlock(t *testing.T) {
	blockTimestamp := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).Unix()

	t.Run("success: positions and collateral persisted", func(t *testing.T) {
		repo := &mockMapleRepo{}
		defaultRepoSetup(repo)
		client := &mockMapleClient{}
		defaultClientSetup(client)

		protocolRepo := defaultProtocolRepo()
		svc, err := NewService(validServiceConfig(), blockingConsumer(), client, repo, protocolRepo)
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}
		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer svc.Stop()

		event := blockEvent{
			ChainID:        1,
			BlockNumber:    21000000,
			Version:        1,
			BlockHash:      "0xabc",
			BlockTimestamp: blockTimestamp,
		}

		if err := svc.processBlock(context.Background(), event); err != nil {
			t.Fatalf("processBlock: %v", err)
		}

		repo.mu.Lock()
		defer repo.mu.Unlock()

		if repo.upsertPositionsCalls != 1 {
			t.Errorf("UpsertPositions calls = %d, want 1", repo.upsertPositionsCalls)
		}
		if len(repo.lastUpsertedPositions) != 1 {
			t.Errorf("positions count = %d, want 1", len(repo.lastUpsertedPositions))
		}
		if repo.upsertPoolCollateralCalls != 1 {
			t.Errorf("UpsertPoolCollateral calls = %d, want 1", repo.upsertPoolCollateralCalls)
		}
		if len(repo.lastUpsertedCollaterals) != 2 {
			t.Errorf("collaterals count = %d, want 2", len(repo.lastUpsertedCollaterals))
		}
	})

	t.Run("no users found: no API calls made", func(t *testing.T) {
		repo := &mockMapleRepo{}
		repo.getUsersWithMaplePositionsFn = func(_ context.Context, _ int64) ([]outbound.MapleTrackedUser, error) {
			return nil, nil
		}
		client := &mockMapleClient{}

		protocolRepo := defaultProtocolRepo()
		svc, err := NewService(validServiceConfig(), blockingConsumer(), client, repo, protocolRepo)
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}
		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer svc.Stop()

		event := blockEvent{ChainID: 1, BlockNumber: 21000000, Version: 1, BlockTimestamp: blockTimestamp}
		if err := svc.processBlock(context.Background(), event); err != nil {
			t.Fatalf("processBlock: %v", err)
		}

		client.mu.Lock()
		if client.getAccountPositionsCalls != 0 {
			t.Errorf("GetAccountPositions calls = %d, want 0", client.getAccountPositionsCalls)
		}
		client.mu.Unlock()
	})

	t.Run("user lookup error propagates", func(t *testing.T) {
		repo := &mockMapleRepo{}
		repo.getUsersWithMaplePositionsFn = func(_ context.Context, _ int64) ([]outbound.MapleTrackedUser, error) {
			return nil, fmt.Errorf("db connection refused")
		}

		protocolRepo := defaultProtocolRepo()
		svc, err := NewService(validServiceConfig(), blockingConsumer(), &mockMapleClient{}, repo, protocolRepo)
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}
		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer svc.Stop()

		event := blockEvent{ChainID: 1, BlockNumber: 21000000, Version: 1, BlockTimestamp: blockTimestamp}
		err = svc.processBlock(context.Background(), event)
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "getting maple users") {
			t.Errorf("error %q should contain 'getting maple users'", err.Error())
		}
	})

	t.Run("API error for user position propagates", func(t *testing.T) {
		repo := &mockMapleRepo{}
		defaultRepoSetup(repo)
		client := &mockMapleClient{}
		client.getAccountPositionsFn = func(_ context.Context, _ common.Address) ([]outbound.MaplePoolPosition, error) {
			return nil, fmt.Errorf("graphql timeout")
		}

		protocolRepo := defaultProtocolRepo()
		svc, err := NewService(validServiceConfig(), blockingConsumer(), client, repo, protocolRepo)
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}
		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer svc.Stop()

		event := blockEvent{ChainID: 1, BlockNumber: 21000000, Version: 1, BlockTimestamp: blockTimestamp}
		err = svc.processBlock(context.Background(), event)
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "graphql timeout") {
			t.Errorf("error %q should contain 'graphql timeout'", err.Error())
		}
	})

	t.Run("no positions for user: collateral not fetched", func(t *testing.T) {
		repo := &mockMapleRepo{}
		defaultRepoSetup(repo)
		client := &mockMapleClient{}
		client.getAccountPositionsFn = func(_ context.Context, _ common.Address) ([]outbound.MaplePoolPosition, error) {
			return nil, nil // no positions
		}

		protocolRepo := defaultProtocolRepo()
		svc, err := NewService(validServiceConfig(), blockingConsumer(), client, repo, protocolRepo)
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}
		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer svc.Stop()

		event := blockEvent{ChainID: 1, BlockNumber: 21000000, Version: 1, BlockTimestamp: blockTimestamp}
		if err := svc.processBlock(context.Background(), event); err != nil {
			t.Fatalf("processBlock: %v", err)
		}

		repo.mu.Lock()
		if repo.upsertPositionsCalls != 0 {
			t.Errorf("UpsertPositions calls = %d, want 0", repo.upsertPositionsCalls)
		}
		client.mu.Lock()
		if client.getPoolCollateralCalls != 0 {
			t.Errorf("GetPoolCollateral calls = %d, want 0", client.getPoolCollateralCalls)
		}
		client.mu.Unlock()
		repo.mu.Unlock()
	})

	t.Run("upsert positions error propagates", func(t *testing.T) {
		repo := &mockMapleRepo{}
		defaultRepoSetup(repo)
		repo.upsertPositionsFn = func(_ context.Context, _ []*entity.MaplePosition) error {
			return fmt.Errorf("database write failure")
		}
		client := &mockMapleClient{}
		defaultClientSetup(client)

		protocolRepo := defaultProtocolRepo()
		svc, err := NewService(validServiceConfig(), blockingConsumer(), client, repo, protocolRepo)
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}
		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer svc.Stop()

		event := blockEvent{ChainID: 1, BlockNumber: 21000000, Version: 1, BlockTimestamp: blockTimestamp}
		err = svc.processBlock(context.Background(), event)
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "database write failure") {
			t.Errorf("error %q should contain 'database write failure'", err.Error())
		}
	})

	t.Run("pool collateral fetch error is logged but not fatal", func(t *testing.T) {
		repo := &mockMapleRepo{}
		defaultRepoSetup(repo)
		client := &mockMapleClient{}
		client.getAccountPositionsFn = func(_ context.Context, _ common.Address) ([]outbound.MaplePoolPosition, error) {
			return defaultAPIPositions(), nil
		}
		client.getPoolCollateralFn = func(_ context.Context, _ common.Address) (*outbound.MaplePoolData, error) {
			return nil, fmt.Errorf("pool API error")
		}

		protocolRepo := defaultProtocolRepo()
		svc, err := NewService(validServiceConfig(), blockingConsumer(), client, repo, protocolRepo)
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}
		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer svc.Stop()

		event := blockEvent{ChainID: 1, BlockNumber: 21000000, Version: 1, BlockTimestamp: blockTimestamp}
		if err := svc.processBlock(context.Background(), event); err == nil {
			t.Fatal("expected error")
		} else if !strings.Contains(err.Error(), "pool API error") {
			t.Errorf("error %q should contain 'pool API error'", err.Error())
		}

		repo.mu.Lock()
		if repo.upsertPositionsCalls != 1 {
			t.Errorf("UpsertPositions calls = %d, want 1", repo.upsertPositionsCalls)
		}
		// Collateral was not persisted because API failed
		if repo.upsertPoolCollateralCalls != 0 {
			t.Errorf("UpsertPoolCollateral calls = %d, want 0", repo.upsertPoolCollateralCalls)
		}
		repo.mu.Unlock()
	})

	t.Run("upsert pool collateral error propagates", func(t *testing.T) {
		repo := &mockMapleRepo{}
		defaultRepoSetup(repo)
		repo.upsertPoolCollateralFn = func(_ context.Context, _ []*entity.MaplePoolCollateral) error {
			return fmt.Errorf("collateral write failure")
		}
		client := &mockMapleClient{}
		defaultClientSetup(client)

		protocolRepo := defaultProtocolRepo()
		svc, err := NewService(validServiceConfig(), blockingConsumer(), client, repo, protocolRepo)
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}
		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer svc.Stop()

		event := blockEvent{ChainID: 1, BlockNumber: 21000000, Version: 1, BlockTimestamp: blockTimestamp}
		err = svc.processBlock(context.Background(), event)
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "collateral write failure") {
			t.Errorf("error %q should contain 'collateral write failure'", err.Error())
		}
	})

	t.Run("multiple users: one fails, other succeeds, error reported", func(t *testing.T) {
		repo := &mockMapleRepo{}
		repo.getUsersWithMaplePositionsFn = func(_ context.Context, _ int64) ([]outbound.MapleTrackedUser, error) {
			return []outbound.MapleTrackedUser{
				{UserID: 1, Address: common.HexToAddress("0x1601843c5e9bc251a3272907010afa41fa18347e")},
				{UserID: 2, Address: common.HexToAddress("0x0000000000000000000000000000000000000002")},
			}, nil
		}

		userAddr1 := common.HexToAddress("0x1601843c5e9bc251a3272907010afa41fa18347e")
		client := &mockMapleClient{}
		client.getAccountPositionsFn = func(_ context.Context, address common.Address) ([]outbound.MaplePoolPosition, error) {
			if address == userAddr1 {
				return nil, fmt.Errorf("API error for user 1")
			}
			return defaultAPIPositions(), nil
		}
		client.getPoolCollateralFn = func(_ context.Context, _ common.Address) (*outbound.MaplePoolData, error) {
			return defaultPoolData(), nil
		}

		protocolRepo := defaultProtocolRepo()
		svc, err := NewService(validServiceConfig(), blockingConsumer(), client, repo, protocolRepo)
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}
		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer svc.Stop()

		event := blockEvent{ChainID: 1, BlockNumber: 21000000, Version: 1, BlockTimestamp: blockTimestamp}
		err = svc.processBlock(context.Background(), event)
		if err == nil {
			t.Fatal("expected error (one user failed)")
		}
		if !strings.Contains(err.Error(), "API error for user 1") {
			t.Errorf("error %q should contain 'API error for user 1'", err.Error())
		}

		// Second user should still have been processed
		repo.mu.Lock()
		if repo.upsertPositionsCalls != 1 {
			t.Errorf("UpsertPositions calls = %d, want 1 (second user)", repo.upsertPositionsCalls)
		}
		repo.mu.Unlock()
	})

	t.Run("deduplicates pools for collateral fetch", func(t *testing.T) {
		repo := &mockMapleRepo{}
		repo.getUsersWithMaplePositionsFn = func(_ context.Context, _ int64) ([]outbound.MapleTrackedUser, error) {
			return []outbound.MapleTrackedUser{
				{UserID: 1, Address: common.HexToAddress("0x1601843c5e9bc251a3272907010afa41fa18347e")},
			}, nil
		}

		// Return two positions in different pools
		client := &mockMapleClient{}
		client.getAccountPositionsFn = func(_ context.Context, _ common.Address) ([]outbound.MaplePoolPosition, error) {
			return []outbound.MaplePoolPosition{
				{
					PoolAddress:    testPoolAddress(),
					PoolName:       "Syrup USDC",
					AssetSymbol:    "USDC",
					AssetDecimals:  6,
					LendingBalance: big.NewInt(200_000_000_000_000),
				},
				{
					PoolAddress:    testPoolAddress2(),
					PoolName:       "Syrup USDT",
					AssetSymbol:    "USDT",
					AssetDecimals:  6,
					LendingBalance: big.NewInt(55_000_000_000_000),
				},
			}, nil
		}
		client.getPoolCollateralFn = func(_ context.Context, _ common.Address) (*outbound.MaplePoolData, error) {
			return defaultPoolData(), nil
		}

		protocolRepo := defaultProtocolRepo()
		svc, err := NewService(validServiceConfig(), blockingConsumer(), client, repo, protocolRepo)
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}
		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer svc.Stop()

		event := blockEvent{ChainID: 1, BlockNumber: 21000000, Version: 1, BlockTimestamp: blockTimestamp}
		if err := svc.processBlock(context.Background(), event); err != nil {
			t.Fatalf("processBlock: %v", err)
		}

		// Two distinct pools, so GetPoolCollateral should be called twice
		client.mu.Lock()
		if client.getPoolCollateralCalls != 2 {
			t.Errorf("GetPoolCollateral calls = %d, want 2", client.getPoolCollateralCalls)
		}
		client.mu.Unlock()
	})

	t.Run("invalid pool address from API: zero address triggers entity validation error", func(t *testing.T) {
		repo := &mockMapleRepo{}
		defaultRepoSetup(repo)
		client := &mockMapleClient{}
		// common.HexToAddress("0xZZZZinvalidhex") produces a zero address,
		// which triggers "pool address cannot be empty" in entity validation.
		client.getAccountPositionsFn = func(_ context.Context, _ common.Address) ([]outbound.MaplePoolPosition, error) {
			return []outbound.MaplePoolPosition{
				{
					PoolAddress:    common.Address{}, // zero address
					PoolName:       "Bad Pool",
					AssetSymbol:    "USDC",
					AssetDecimals:  6,
					LendingBalance: big.NewInt(100_000_000),
				},
			}, nil
		}

		protocolRepo := defaultProtocolRepo()
		svc, err := NewService(validServiceConfig(), blockingConsumer(), client, repo, protocolRepo)
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}
		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer svc.Stop()

		event := blockEvent{ChainID: 1, BlockNumber: 21000000, Version: 1, BlockTimestamp: blockTimestamp}
		err = svc.processBlock(context.Background(), event)
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "creating position entity") {
			t.Errorf("error %q should contain 'creating position entity'", err.Error())
		}
	})

	t.Run("entity validation failure: empty pool name propagates error", func(t *testing.T) {
		repo := &mockMapleRepo{}
		defaultRepoSetup(repo)
		client := &mockMapleClient{}
		client.getAccountPositionsFn = func(_ context.Context, _ common.Address) ([]outbound.MaplePoolPosition, error) {
			return []outbound.MaplePoolPosition{
				{
					PoolAddress:    testPoolAddress(),
					PoolName:       "", // empty name triggers entity validation error
					AssetSymbol:    "USDC",
					AssetDecimals:  6,
					LendingBalance: big.NewInt(100_000_000),
				},
			}, nil
		}

		protocolRepo := defaultProtocolRepo()
		svc, err := NewService(validServiceConfig(), blockingConsumer(), client, repo, protocolRepo)
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}
		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer svc.Stop()

		event := blockEvent{ChainID: 1, BlockNumber: 21000000, Version: 1, BlockTimestamp: blockTimestamp}
		err = svc.processBlock(context.Background(), event)
		if err == nil {
			t.Fatal("expected error")
		}
		if !strings.Contains(err.Error(), "creating position entity") {
			t.Errorf("error %q should contain 'creating position entity'", err.Error())
		}
	})

	t.Run("collateral entity validation error: entries are skipped", func(t *testing.T) {
		repo := &mockMapleRepo{}
		defaultRepoSetup(repo)
		client := &mockMapleClient{}
		client.getAccountPositionsFn = func(_ context.Context, _ common.Address) ([]outbound.MaplePoolPosition, error) {
			return defaultAPIPositions(), nil
		}
		// Return collateral with empty asset name (triggers entity validation error)
		client.getPoolCollateralFn = func(_ context.Context, _ common.Address) (*outbound.MaplePoolData, error) {
			return &outbound.MaplePoolData{
				TVL: big.NewInt(1_000_000_000_000_000),
				Collaterals: []outbound.MapleCollateral{
					{Asset: "", AssetValueUSD: big.NewInt(500_000_000_000_000), AssetDecimals: 8},    // invalid: empty asset
					{Asset: "BTC", AssetValueUSD: big.NewInt(500_000_000_000_000), AssetDecimals: 8}, // valid
				},
			}, nil
		}

		protocolRepo := defaultProtocolRepo()
		svc, err := NewService(validServiceConfig(), blockingConsumer(), client, repo, protocolRepo)
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}
		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}
		defer svc.Stop()

		event := blockEvent{ChainID: 1, BlockNumber: 21000000, Version: 1, BlockTimestamp: blockTimestamp}
		if err := svc.processBlock(context.Background(), event); err != nil {
			t.Fatalf("processBlock: %v", err)
		}

		repo.mu.Lock()
		defer repo.mu.Unlock()
		// Only the valid BTC entry should be persisted, empty-asset entry skipped
		if repo.upsertPoolCollateralCalls != 1 {
			t.Errorf("UpsertPoolCollateral calls = %d, want 1", repo.upsertPoolCollateralCalls)
		}
		if len(repo.lastUpsertedCollaterals) != 1 {
			t.Errorf("collaterals count = %d, want 1", len(repo.lastUpsertedCollaterals))
		}
	})
}

// ---------------------------------------------------------------------------
// TestProcessMessages (SQS message handling)
// ---------------------------------------------------------------------------

func TestProcessMessages(t *testing.T) {
	blockTimestamp := time.Date(2025, 1, 1, 0, 0, 0, 0, time.UTC).Unix()

	t.Run("end-to-end: message processed and deleted", func(t *testing.T) {
		repo := &mockMapleRepo{}
		defaultRepoSetup(repo)
		client := &mockMapleClient{}
		defaultClientSetup(client)

		messageDelivered := false
		body := makeMapleBlockEventJSON(21000000, blockTimestamp)
		consumer := &mockConsumer{
			receiveMessagesFn: func(ctx context.Context, _ int) ([]outbound.SQSMessage, error) {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				if !messageDelivered {
					messageDelivered = true
					return []outbound.SQSMessage{
						{MessageID: "msg-1", Body: body, ReceiptHandle: "receipt-1"},
					}, nil
				}
				<-ctx.Done()
				return nil, ctx.Err()
			},
		}

		cfg := validServiceConfig()
		cfg.PollInterval = 1 * time.Millisecond

		protocolRepo := defaultProtocolRepo()
		svc, err := NewService(cfg, consumer, client, repo, protocolRepo)
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}
		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}

		testutil.WaitForCondition(t, 2*time.Second, func() bool {
			repo.mu.Lock()
			defer repo.mu.Unlock()
			return repo.upsertPositionsCalls >= 1
		}, "UpsertPositions to be called")

		consumer.mu.Lock()
		if consumer.deleteMessageCalls < 1 {
			t.Errorf("DeleteMessage calls = %d, want >= 1", consumer.deleteMessageCalls)
		}
		consumer.mu.Unlock()

		svc.Stop()
	})

	t.Run("SQS receive error: logged, not fatal", func(t *testing.T) {
		consumer := &mockConsumer{
			receiveMessagesFn: func(_ context.Context, _ int) ([]outbound.SQSMessage, error) {
				return nil, fmt.Errorf("SQS unavailable")
			},
		}

		cfg := validServiceConfig()
		cfg.PollInterval = 1 * time.Millisecond

		protocolRepo := defaultProtocolRepo()
		svc, err := NewService(cfg, consumer, &mockMapleClient{}, &mockMapleRepo{}, protocolRepo)
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}
		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}

		time.Sleep(50 * time.Millisecond)

		consumer.mu.Lock()
		if consumer.receiveMessageCalls == 0 {
			t.Error("expected at least one ReceiveMessage call")
		}
		consumer.mu.Unlock()

		svc.Stop()
	})

	t.Run("empty messages: no processing", func(t *testing.T) {
		repo := &mockMapleRepo{}
		consumer := &mockConsumer{
			receiveMessagesFn: func(ctx context.Context, _ int) ([]outbound.SQSMessage, error) {
				return nil, nil
			},
		}

		cfg := validServiceConfig()
		cfg.PollInterval = 1 * time.Millisecond

		protocolRepo := defaultProtocolRepo()
		svc, err := NewService(cfg, consumer, &mockMapleClient{}, repo, protocolRepo)
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}
		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}

		time.Sleep(50 * time.Millisecond)

		repo.mu.Lock()
		if repo.upsertPositionsCalls != 0 {
			t.Errorf("UpsertPositions calls = %d, want 0", repo.upsertPositionsCalls)
		}
		repo.mu.Unlock()

		svc.Stop()
	})

	t.Run("invalid JSON: message not deleted", func(t *testing.T) {
		delivered := false
		consumer := &mockConsumer{
			receiveMessagesFn: func(ctx context.Context, _ int) ([]outbound.SQSMessage, error) {
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				if !delivered {
					delivered = true
					return []outbound.SQSMessage{
						{MessageID: "msg-bad", Body: "not json{{{", ReceiptHandle: "receipt-bad"},
					}, nil
				}
				<-ctx.Done()
				return nil, ctx.Err()
			},
		}

		cfg := validServiceConfig()
		cfg.PollInterval = 1 * time.Millisecond

		protocolRepo := defaultProtocolRepo()
		svc, err := NewService(cfg, consumer, &mockMapleClient{}, &mockMapleRepo{}, protocolRepo)
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}
		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}

		time.Sleep(50 * time.Millisecond)

		consumer.mu.Lock()
		if consumer.deleteMessageCalls != 0 {
			t.Errorf("DeleteMessage calls = %d, want 0 (invalid JSON)", consumer.deleteMessageCalls)
		}
		consumer.mu.Unlock()

		svc.Stop()
	})

	t.Run("delete message error: logged but processing continues", func(t *testing.T) {
		repo := &mockMapleRepo{}
		defaultRepoSetup(repo)
		client := &mockMapleClient{}
		defaultClientSetup(client)

		delivered := false
		body := makeMapleBlockEventJSON(21000000, blockTimestamp)
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

		cfg := validServiceConfig()
		cfg.PollInterval = 1 * time.Millisecond

		protocolRepo := defaultProtocolRepo()
		svc, err := NewService(cfg, consumer, client, repo, protocolRepo)
		if err != nil {
			t.Fatalf("NewService: %v", err)
		}
		if err := svc.Start(context.Background()); err != nil {
			t.Fatalf("Start: %v", err)
		}

		testutil.WaitForCondition(t, 2*time.Second, func() bool {
			repo.mu.Lock()
			defer repo.mu.Unlock()
			return repo.upsertPositionsCalls >= 1
		}, "UpsertPositions to be called")

		consumer.mu.Lock()
		if consumer.deleteMessageCalls < 1 {
			t.Errorf("DeleteMessage calls = %d, want >= 1", consumer.deleteMessageCalls)
		}
		consumer.mu.Unlock()

		svc.Stop()
	})
}

// ---------------------------------------------------------------------------
// TestCalculateBackedBreakdown
// ---------------------------------------------------------------------------

func TestCalculateBackedBreakdown(t *testing.T) {
	tests := []struct {
		name           string
		lendingBalance *big.Int
		poolData       *outbound.MaplePoolData
		wantCount      int
		wantSymbols    []string
		checkValues    bool
		wantPercents   []float64
	}{
		{
			name:           "nil pool data",
			lendingBalance: big.NewInt(200_000_000_000_000),
			poolData:       nil,
			wantCount:      0,
		},
		{
			name:           "zero lending balance",
			lendingBalance: big.NewInt(0),
			poolData:       defaultPoolData(),
			wantCount:      0,
		},
		{
			name:           "zero TVL",
			lendingBalance: big.NewInt(200_000_000_000_000),
			poolData: &outbound.MaplePoolData{
				TVL:         big.NewInt(0),
				Collaterals: []outbound.MapleCollateral{{Asset: "BTC", AssetValueUSD: big.NewInt(100), AssetDecimals: 8}},
			},
			wantCount: 0,
		},
		{
			name:           "standard breakdown with two assets",
			lendingBalance: big.NewInt(200_000_000_000_000), // $200M
			poolData: &outbound.MaplePoolData{
				TVL: big.NewInt(1_000_000_000_000_000), // $1B
				Collaterals: []outbound.MapleCollateral{
					{Asset: "BTC", AssetValueUSD: big.NewInt(674_000_000_000_000), AssetDecimals: 8}, // 67.4%
					{Asset: "XRP", AssetValueUSD: big.NewInt(317_000_000_000_000), AssetDecimals: 6}, // 31.7%
				},
			},
			wantCount:    2,
			wantSymbols:  []string{"BTC", "XRP"},
			checkValues:  true,
			wantPercents: []float64{0.674, 0.317},
		},
		{
			name:           "zero-value collateral entries are filtered",
			lendingBalance: big.NewInt(200_000_000_000_000),
			poolData: &outbound.MaplePoolData{
				TVL: big.NewInt(1_000_000_000_000_000),
				Collaterals: []outbound.MapleCollateral{
					{Asset: "BTC", AssetValueUSD: big.NewInt(500_000_000_000_000), AssetDecimals: 8},
					{Asset: "ZERO", AssetValueUSD: big.NewInt(0), AssetDecimals: 6},
				},
			},
			wantCount:   1,
			wantSymbols: []string{"BTC"},
		},
		{
			name:           "single asset at 100%",
			lendingBalance: big.NewInt(100_000_000_000_000), // $100M
			poolData: &outbound.MaplePoolData{
				TVL: big.NewInt(100_000_000_000_000), // $100M
				Collaterals: []outbound.MapleCollateral{
					{Asset: "ETH", AssetValueUSD: big.NewInt(100_000_000_000_000), AssetDecimals: 18},
				},
			},
			wantCount:    1,
			wantSymbols:  []string{"ETH"},
			checkValues:  true,
			wantPercents: []float64{1.0},
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			backings := CalculateBackedBreakdown(tc.lendingBalance, tc.poolData)

			if len(backings) != tc.wantCount {
				t.Fatalf("count = %d, want %d", len(backings), tc.wantCount)
			}

			for i, b := range backings {
				if i < len(tc.wantSymbols) && b.Symbol != tc.wantSymbols[i] {
					t.Errorf("backings[%d].Symbol = %q, want %q", i, b.Symbol, tc.wantSymbols[i])
				}
			}

			if tc.checkValues {
				for i, b := range backings {
					if i < len(tc.wantPercents) {
						diff := b.Percent - tc.wantPercents[i]
						if diff < -0.001 || diff > 0.001 {
							t.Errorf("backings[%d].Percent = %f, want ~%f", i, b.Percent, tc.wantPercents[i])
						}
					}

					// Verify ValueUSD is positive
					if b.ValueUSD <= 0 {
						t.Errorf("backings[%d].ValueUSD = %f, want > 0", i, b.ValueUSD)
					}

					// Verify TotalUSD is positive
					if b.TotalUSD <= 0 {
						t.Errorf("backings[%d].TotalUSD = %f, want > 0", i, b.TotalUSD)
					}
				}
			}
		})
	}

	t.Run("math precision: ValueUSD = Percent * lendingBalance / 1e6", func(t *testing.T) {
		lendingBalance := big.NewInt(200_000_000_000_000) // $200M (6 decimals)
		poolData := &outbound.MaplePoolData{
			TVL: big.NewInt(1_000_000_000_000_000), // $1B
			Collaterals: []outbound.MapleCollateral{
				{Asset: "BTC", AssetValueUSD: big.NewInt(500_000_000_000_000), AssetDecimals: 8}, // 50%
			},
		}

		backings := CalculateBackedBreakdown(lendingBalance, poolData)
		if len(backings) != 1 {
			t.Fatalf("count = %d, want 1", len(backings))
		}

		b := backings[0]
		// 50% of $200M = $100M
		expectedValueUSD := 100_000_000.0
		diff := b.ValueUSD - expectedValueUSD
		if diff < -1.0 || diff > 1.0 {
			t.Errorf("ValueUSD = %f, want ~%f", b.ValueUSD, expectedValueUSD)
		}

		// Total = $500M
		expectedTotalUSD := 500_000_000.0
		diff = b.TotalUSD - expectedTotalUSD
		if diff < -1.0 || diff > 1.0 {
			t.Errorf("TotalUSD = %f, want ~%f", b.TotalUSD, expectedTotalUSD)
		}
	})
}
